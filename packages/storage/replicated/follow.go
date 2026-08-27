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

// echoGrace is how long a caller waits for the change it made to come back before deciding
// that the stream is no longer delivering.
//
// It bounds a failure rather than a latency. The event is written as soon as the change
// commits, over a connection that is already open, so reaching this means a change was
// committed and its event did not arrive — and a copy that is missing a change may not be
// answered from, however long ago it was told so. That is why running out here breaks the
// stream rather than failing one operation: the alternative is a mount that goes on
// answering from a copy it has just discovered is incomplete.
const echoGrace = 10 * time.Second

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
		if err := s.local.Apply(s.lifetime, change); err != nil {
			return err
		}
		s.applied(change)
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
			s.resumed(sub.Incarnation())
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

	s.incarnation, s.at, s.failure = incarnation, at, nil
	s.wake()
}

// resumed records a stream that has been picked up where the copy left off. The copy is what
// it was — nothing about it changed while the stream was down — and it may be answered from
// again.
func (s *Storage) resumed(incarnation metastore.Incarnation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.incarnation, s.failure = incarnation, nil
	s.wake()
}

// fail records that the stream is no longer being observed, which is the only thing that made
// the copy worth believing. Every operation reports it until the stream is back.
func (s *Storage) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failure = err
	s.wake()
}

// applied records a change that is now in the copy.
func (s *Storage) applied(change metastore.Change) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.at = change.Position
	if s.waiting > 0 {
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

	s.waiting++
	return s.at
}

// forget drops a caller's interest, and with it everything that was being remembered for it
// once it was the last one.
func (s *Storage) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.waiting--
	if s.waiting == 0 {
		clear(s.touched)
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
	grace := time.NewTimer(echoGrace)
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
			// A change that was committed and never delivered means this copy is missing
			// something, and there is nothing that would put it back. Saying so ends the stream:
			// an incomplete copy may not go on being answered from.
			missed := fmt.Errorf("the change at %q was made and its event did not arrive within %v", want.path, echoGrace)
			s.fail(missed)
			return &os.PathError{Op: op, Path: want.path, Err: fmt.Errorf("%s: %w", missed, syscall.EIO)}
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
	dir, name := cleaned, ""
	if i := strings.LastIndexByte(cleaned, '/'); i >= 0 {
		dir, name = cleaned[:i], cleaned[i+1:]
	} else {
		dir, name = "", cleaned
	}
	node, err := s.local.Stat(ctx, dir)
	if err != nil {
		return location{}, err
	}
	return location{parent: node.ID, name: name}, nil
}
