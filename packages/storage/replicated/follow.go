package replicated

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// reconnectDelay is how long the copy waits between attempts to get the stream back.
//
// It is not an interval any answer waits for, which is what R-CON-2 forbids: while the
// stream is down nothing here is answered at all, so nothing becomes visible at a boundary.
// What it bounds is how fast a server that is not there is dialled.
const reconnectDelay = 250 * time.Millisecond

// DefaultEchoGrace is how long a caller waits for the change it made to come back before it
// gives up on confirming it.
//
// What runs out here is one operation's patience, and nothing else. It does not end the
// stream: whether the stream is still being delivered is a question the transport already
// answers, by a keepalive on one side and a bound on silence on the other, and a stream that
// is merely slow is not one that is gone. Ending it from here would be worse than useless —
// a mount whose backlog takes longer than this to apply would break its own healthy stream,
// reconnect to the same backlog, and do it again.
//
// So a caller that reaches this is told that the change happened and could not be confirmed,
// and everything else carries on. The copy is behind by that one change for as long as the
// stream needs, which is the same thing that is true of every change made anywhere else.
const DefaultEchoGrace = 10 * time.Second

// build attaches to the stream and fills the copy from one picture of the tree.
//
// The subscription is opened against the lifetime rather than against the caller's context.
// It outlives the call that opened it — the mount is what it belongs to — and a caller whose
// context is cancelled a moment later would otherwise sever the stream the copy is fed by.
func (s *Storage) build(ctx context.Context) (*httprest.Subscription, error) {
	sub, err := s.remote.Subscribe(s.lifetime)
	if err != nil {
		return nil, fmt.Errorf("watching the namespace for changes: %w", err)
	}
	if err := s.fill(ctx, sub); err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
}

// fill empties the copy and puts one consistent picture of the tree into it.
//
// Nothing reads the stream while the picture is taken, and nothing needs to. The changes
// recorded meanwhile queue on the connection the subscription is already holding, in the
// order the log recorded them, and they are read once the picture is in place: the ones at or
// before its position are discarded, the ones after it are applied. Holding them in a buffer
// here instead would be a second queue in front of that one, with a size to choose and a
// failure of its own on the day it was reached.
//
// Because the picture is a cut at a single position, no bookkeeping per node is needed to
// tell which changes it already contains: at or before that position is in the picture, after
// it is not. That is also what makes a rename in the stream certain to find its source — so
// there is deliberately no rule here that treats a rename with a missing source as nothing to
// do. A rule like that would be relied upon within a week of existing.
func (s *Storage) fill(ctx context.Context, sub *httprest.Subscription) error {
	snap, err := s.remote.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("taking a picture of the namespace's tree: %w", err)
	}
	defer snap.Close()

	seeding, err := s.local.Reseed(ctx)
	if err != nil {
		return fmt.Errorf("emptying the local copy: %w", err)
	}
	defer seeding.Close()

	for {
		rows, err := snap.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading the picture of the namespace's tree: %w", err)
		}
		if err := seeding.Add(ctx, rows); err != nil {
			return fmt.Errorf("putting the picture into the local copy: %w", err)
		}
	}
	if err := seeding.Complete(ctx, snap.Position()); err != nil {
		return fmt.Errorf("putting the picture into the local copy: %w", err)
	}
	s.seeded(sub.Incarnation(), snap.Position())
	return nil
}

// follow keeps the copy current for as long as the mount lasts.
func (s *Storage) follow(sub *httprest.Subscription) {
	defer close(s.stopped)
	for {
		s.fail(s.attend(sub))
		if s.lifetime.Err() != nil {
			return
		}
		next := s.reattach()
		if next == nil {
			return
		}
		sub = next
	}
}

// attend applies what arrives until nothing does, and reports what ended it.
func (s *Storage) attend(sub *httprest.Subscription) error {
	defer sub.Close()
	for {
		change, err := sub.Next()
		if err != nil {
			return err
		}
		// A change that could not be applied ends the stream too. The copy is missing it from
		// here on and nothing later carries it again, so carrying on would be answering from a
		// copy that is known to be wrong — which is worse than the stream having failed.
		applied, err := s.local.Apply(s.lifetime, change)
		if err != nil {
			return err
		}
		// A change the copy discarded is one it already held — everything a picture covered
		// arrives again on a stream that was attached before the picture was taken — and it
		// moved nothing, so nothing here moves either.
		if applied {
			s.applied(change)
		}
	}
}

// reattach gets the stream back, and returns nil when the mount is going away.
//
// A log that cannot carry on from where the copy stands says so with a *httprest.RebuildError
// rather than by quietly starting somewhere else, and there is one thing to do about it: what
// is here is not a copy of anything any more, because the changes between what it holds and
// what the log can supply are gone. It is replaced by a fresh picture, exactly as at the
// first mount.
func (s *Storage) reattach() *httprest.Subscription {
	for {
		if !s.pause() {
			return nil
		}
		incarnation, at := s.watching()
		sub, err := s.remote.Resubscribe(s.lifetime, incarnation, at)
		if err == nil {
			s.resumed(sub.Incarnation(), sub.Tail())
			return sub
		}
		var rebuild *httprest.RebuildError
		if errors.As(err, &rebuild) {
			rebuilt, buildErr := s.build(s.lifetime)
			if buildErr == nil {
				return rebuilt
			}
			err = fmt.Errorf("%w, and building it again failed: %w", rebuild, buildErr)
		}
		s.fail(err)
	}
}

// pause waits before dialling again, and reports whether there is still any point.
//
// The first attempt waits too. A stream that has just ended is most often one whose server is
// not there, and the answer to that is not to dial as fast as the refusals come back.
func (s *Storage) pause() bool {
	timer := time.NewTimer(reconnectDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.lifetime.Done():
		return false
	}
}

// --- what the copy is worth right now ------------------------------------------------------

// seeded records a copy that has just been filled from a picture: it stands at that position,
// in that run of history, and may be answered from.
func (s *Storage) seeded(incarnation metastore.Incarnation, at metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.incarnation, s.at, s.failure, s.behind = incarnation, at, nil, 0
	s.wake()
}

// resumed records a stream that has been picked up where the copy left off.
//
// Being attached again is not the same as being current again. The log tells the stream how
// far it had reached, and everything between what the copy holds and that is on its way but
// not here yet — so until the last of it has been applied the copy is knowingly missing
// changes, and answering from it would be answering with what a name held before somebody
// else changed it. That is a stale answer given as fact, which is the one thing R-ERR-1 and
// R-ERR-2 put above every other consideration, and the difference from ordinary steady state
// is that here the gap is known rather than merely possible.
func (s *Storage) resumed(incarnation metastore.Incarnation, tail metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.incarnation = incarnation
	if s.at >= tail {
		s.behind, s.failure = 0, nil
		s.wake()
		return
	}
	s.behind = tail
	s.failure = fmt.Errorf("the stream was picked up again at position %d and the log had reached %d, so this copy is missing what happened in between",
		s.at, tail)
	s.wake()
}

// fail records that the stream is no longer being observed, which is the only thing that made
// the copy worth believing. Every operation reports it until the stream is back.
func (s *Storage) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failure, s.behind = err, 0
	s.wake()
}

// applied records a change that is now in the copy. Only a change the copy took reaches here:
// one it discarded moved nothing, and treating it as applied would take this account of where
// the copy stands backwards — to a position the copy passed when a picture carried it further
// — and would answer a caller waiting for its own change with a change that was never applied.
func (s *Storage) applied(change metastore.Change) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.at = change.Position
	// The last of what a resumed stream said it would replay. From here the copy holds
	// everything the log had when the stream began, so it may be answered from again.
	if s.behind != 0 && s.at >= s.behind {
		s.behind, s.failure = 0, nil
	}
	if len(s.waiting) > 0 {
		s.record(change)
	}
	s.wake()
}

// watching is the pair a stream is picked up again with. A position alone would let a log
// that lost its history answer "you are caught up" to a position it has never heard of.
func (s *Storage) watching() (metastore.Incarnation, metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.incarnation, s.at
}

// usable reports why an operation cannot be performed, and nil when it can.
//
// The reason travels as text rather than in the error chain, so that nothing an operation
// against the namespace is asked about can come back carrying an errno that belongs to the
// stream: EIO is what "this copy may not be believed" means to a caller, and it is the only
// errno here.
func (s *Storage) usable(op, path string) *os.PathError {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failure == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
		"the copy of this namespace is not being kept current, so nothing about it can be answered: %s: %w",
		s.failure, syscall.EIO)}
}

// wake releases everything waiting on the state that has just changed. The caller holds mu.
func (s *Storage) wake() {
	close(s.notify)
	s.notify = make(chan struct{})
}

// --- waiting for one's own change to come back ---------------------------------------------

// location is one name in one directory, named the way a change names it.
type location struct {
	parent int64
	name   string
}

// touch is the newest change at a name in each direction: where something was last put there,
// and where the name was last emptied.
//
// Both are kept because a name may change in both directions while a caller is watching it,
// and a caller waiting for one of them must be released by that one alone. A single position
// with a flag would let a removal that happened afterwards hide the creation that caller was
// waiting for, and it would wait out the grace for a change that had already arrived.
type touch struct {
	filled  metastore.Position
	emptied metastore.Position
}

// expect records that a caller is about to change the namespace and will wait for that change
// to come back. The position is read in the same breath: everything the copy already holds is
// older than the change about to be made, so anything newer than this is a candidate for it.
func (s *Storage) expect() metastore.Position {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.waiting = append(s.waiting, s.at)
	return s.at
}

// forget drops a caller's interest and everything that was only being remembered for it.
//
// What is kept is what some caller still waiting could be released by: a change at or before
// the position the earliest of them started from can release nobody, since each of them waits
// for something strictly later than where it began. Without this the map would keep an entry
// for every name touched between the first mutation and the moment the mount happened to have
// none in flight — which on a busy mount is never, and is unbounded growth (R-INT-3).
func (s *Storage) forget(after metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, at := range s.waiting {
		if at == after {
			s.waiting = append(s.waiting[:i], s.waiting[i+1:]...)
			break
		}
	}
	if len(s.waiting) == 0 {
		clear(s.touched)
		return
	}
	earliest := s.waiting[0]
	for _, at := range s.waiting[1:] {
		earliest = min(earliest, at)
	}
	for where, landed := range s.touched {
		if landed.filled <= earliest && landed.emptied <= earliest {
			delete(s.touched, where)
		}
	}
}

// record notes what a change did to the names it touched. The caller holds mu.
func (s *Storage) record(change metastore.Change) {
	at := location{parent: change.Parent, name: string(change.Name)}
	landed := s.touched[at]
	if change.Kind == metastore.Removed {
		landed.emptied = change.Position
	} else {
		landed.filled = change.Position
	}
	s.touched[at] = landed

	if change.From != nil {
		from := location{parent: change.From.Parent, name: string(change.From.Name)}
		left := s.touched[from]
		left.emptied = change.Position
		s.touched[from] = left
	}
}

// await returns once the copy holds a change made after a position that left the name the way
// the operation left it.
//
// What it waits for is a change at that name rather than a particular one, because a change
// carries no mark saying who caused it. Under a second writer changing the same name at the
// same moment, that writer's change can release this caller a moment early — and both callers
// are then told about a name two of them changed at once, which is the one case R-CC-1 has
// already declared undecided. Every other case is exact: the change this caller made is the
// only one at that name newer than the position it started from.
func (s *Storage) await(ctx context.Context, op string, after metastore.Position, want echoed) error {
	grace := time.NewTimer(s.grace)
	defer grace.Stop()

	for {
		s.mu.Lock()
		failure, notify := s.failure, s.notify
		s.mu.Unlock()
		if failure != nil {
			return &os.PathError{Op: op, Path: want.path, Err: fmt.Errorf(
				"the change was made, and this copy stopped being kept current before it came back: %s: %w",
				failure, syscall.EIO)}
		}

		// Resolved against the copy each time round, because the directory holding the name may
		// itself be arriving on this stream: a name whose parent is not here yet is one to wait
		// for rather than one to answer about.
		if at, err := s.locate(ctx, want.path); err == nil {
			s.mu.Lock()
			landed := s.touched[at]
			s.mu.Unlock()
			if want.holds && landed.filled > after {
				return nil
			}
			if !want.holds && landed.emptied > after {
				return nil
			}
		}

		select {
		case <-notify:
		case <-ctx.Done():
			return &os.PathError{Op: op, Path: want.path, Err: fmt.Errorf(
				"the change was made, and waiting for this copy to hold it was cut short: %s: %w",
				context.Cause(ctx), syscall.EIO)}
		case <-s.lifetime.Done():
			return &os.PathError{Op: op, Path: want.path, Err: fmt.Errorf(
				"the change was made, and this copy was released before it came back: %w", syscall.EIO)}
		case <-grace.C:
			// This caller gives up, and nothing else does. The stream has not said anything
			// wrong — a backlog it is working through looks exactly like this — and whether it
			// is still being delivered at all is answered by the bound the transport keeps on
			// a stream that has gone quiet, not by how long one caller has been waiting.
			return &os.PathError{Op: op, Path: want.path, Err: fmt.Errorf(
				"the change at %q was made, and this copy could not confirm it within %v: %w",
				want.path, s.grace, syscall.EIO)}
		}
	}
}

// locate reports the name a path stands for as the copy sees it: the id of the directory
// holding it, and the name it has there. That is how a change names a place, and it is why no
// translation is needed between the two — the ids in the copy are the server's own.
func (s *Storage) locate(ctx context.Context, path string) (location, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return location{}, err
	}
	// The root has no parent and no name, and that is how a change names it too: a log
	// records the node with no entry at parent zero under no name. Naming it here the way
	// every other node is named — the id of the directory holding it, which for the root is
	// itself — would produce a name no change can ever match, so a caller that changed the
	// root's attributes would wait out its whole grace for an event that had already arrived.
	if cleaned == "" {
		return location{}, nil
	}
	dir, name := "", cleaned
	if i := strings.LastIndexByte(cleaned, '/'); i >= 0 {
		dir, name = cleaned[:i], cleaned[i+1:]
	}
	node, err := s.local.Stat(ctx, dir)
	if err != nil {
		return location{}, err
	}
	return location{parent: node.ID, name: name}, nil
}
