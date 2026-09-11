package replicated

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// reconnectDelay is how long the copy waits between attempts to get the stream back.
//
// It is not an interval any answer waits for, which is what R-CON-2 forbids: while the
// stream is down nothing here is answered at all, so nothing becomes visible at a boundary.
// What it bounds is how fast a server that is not there is dialled.
const reconnectDelay = 250 * time.Millisecond

// follow keeps the copy current for as long as the mount lasts.
func (s *Storage) follow(sub *httprest.Subscription, release context.CancelFunc) {
	defer close(s.stopped)
	for {
		s.fail(s.attend(sub, release))
		if s.lifetime.Err() != nil {
			return
		}
		next, nextRelease := s.reattach()
		if next == nil {
			return
		}
		sub = next
		release = nextRelease
	}
}

// attend applies what arrives until nothing does, and reports what ended it.
func (s *Storage) attend(sub *httprest.Subscription, release context.CancelFunc) error {
	defer func() {
		if release != nil {
			release()
		}
		sub.Close()
	}()
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
func (s *Storage) reattach() (*httprest.Subscription, context.CancelFunc) {
	for {
		if !s.pause() {
			return nil, nil
		}
		incarnation, at := s.watching()
		sub, err := s.remote.Resubscribe(s.lifetime, incarnation, at)
		if err == nil {
			s.resumed(sub.Incarnation(), sub.Tail())
			return sub, nil
		}
		var rebuild *httprest.RebuildError
		if errors.As(err, &rebuild) {
			rebuilt, release, buildErr := s.build(s.lifetime)
			if buildErr == nil {
				return rebuilt, release
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

// installed keeps the cursor aligned with a committed snapshot while its replay
// gate remains closed. The fixed target can equal at, including zero; successful
// cancellation handoff still precedes making the copy available.
func (s *Storage) installed(incarnation metastore.Incarnation, at, target metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.incarnation != incarnation {
		s.generation++
	}
	s.incarnation, s.at, s.behind = incarnation, at, target
	s.failure = fmt.Errorf("the snapshot at %d is being replayed through checkpoint %d", at, target)
	s.wake()
}

// replayed records an applied gate change without opening the gate. Ordinary
// resumed streams use applied, whose final replay change can restore availability.
func (s *Storage) replayed(change metastore.Change) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = change.Position
	s.wake()
}

// seeded makes a completely installed and replayed snapshot available after its
// temporary cancellation links have been detached and joined.
func (s *Storage) seeded(incarnation metastore.Incarnation, at metastore.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.incarnation != incarnation {
		s.generation++
	}
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

	if s.incarnation != incarnation {
		s.generation++
	}
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
	s.wakeConfirmationCapacity()
}

// applied records a change that is now in the copy. Only a change the copy took reaches here:
// one it discarded moved nothing, and treating it as applied would take this account of where
// the copy stands backwards — to a position the copy passed when a picture carried it further
// — and could satisfy a later mutation barrier with a change that was never applied.
func (s *Storage) applied(change metastore.Change) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.at = change.Position
	// The last of what a resumed stream said it would replay. From here the copy holds
	// everything the log had when the stream began, so it may be answered from again.
	if s.behind != 0 && s.at >= s.behind {
		s.behind, s.failure = 0, nil
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
// against the volume is asked about can come back carrying an errno that belongs to the
// stream: EIO is what "this copy may not be believed" means to a caller, and it is the only
// errno here.
func (s *Storage) usable(op, path string) *os.PathError {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return &os.PathError{Op: op, Path: path, Err: fmt.Errorf("the replicated storage is closing: %w", syscall.EIO)}
	}
	if s.failure == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
		"the copy of this volume is not being kept current, so nothing about it can be answered: %s: %w",
		s.failure, syscall.EIO)}
}

// wake releases everything waiting on the state that has just changed. The caller holds mu.
func (s *Storage) wake() {
	close(s.notify)
	s.notify = make(chan struct{})
}

// wakeConfirmationCapacity releases callers waiting for confirmation admission. The caller holds mu.
func (s *Storage) wakeConfirmationCapacity() {
	close(s.confirmationCapacity)
	s.confirmationCapacity = make(chan struct{})
}

// --- mutation barrier confirmation ---------------------------------------------------------

// confirmation is one fixed-size admitted mutation. The barrier is filled only after the
// server reports success; an event may already have advanced the replica by then.
type confirmation struct {
	generation uint64
	position   metastore.Position
	ready      bool
}

func (s *Storage) expect(ctx context.Context, op, path string) (*confirmation, error) {
	waiting := false
	for {
		s.mu.Lock()
		if s.closing {
			if waiting {
				s.confirmationWaiters--
				s.wakeConfirmationCapacity()
			}
			s.mu.Unlock()
			return nil, confirmationAdmissionError(op, path, errors.New("the replicated storage is closing"))
		}
		if s.failure != nil {
			if waiting {
				s.confirmationWaiters--
				s.wakeConfirmationCapacity()
			}
			failure := s.failure
			s.mu.Unlock()
			return nil, &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the copy of this volume is not being kept current, so the mutation cannot be confirmed: %s: %w",
				failure, syscall.EIO)}
		}
		if s.activeConfirmations < s.options.MaxActiveConfirmations {
			if waiting {
				s.confirmationWaiters--
			}
			s.activeConfirmations++
			confirmation := &confirmation{}
			s.mu.Unlock()
			return confirmation, nil
		}
		if !waiting {
			if s.confirmationWaiters == s.options.MaxWaitingConfirmations {
				s.mu.Unlock()
				return nil, confirmationAdmissionError(op, path, fmt.Errorf(
					"%d callers are already waiting for mutation confirmation capacity",
					s.options.MaxWaitingConfirmations))
			}
			s.confirmationWaiters++
			waiting = true
		}
		notify := s.confirmationCapacity
		s.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			s.mu.Lock()
			if waiting {
				s.confirmationWaiters--
				s.wakeConfirmationCapacity()
			}
			s.mu.Unlock()
			return nil, confirmationContextError(op, path, ctx)
		case <-s.lifetime.Done():
		}
	}
}

func (s *Storage) setBarrier(confirmation *confirmation, barrier httprest.MutationBarrier) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if barrier.Position < 0 {
		return fmt.Errorf("the mutation barrier reports negative position %d: %w", barrier.Position, syscall.EIO)
	}
	if metastore.Incarnation(barrier.Incarnation) != s.incarnation {
		return fmt.Errorf("the mutation barrier names log %q while the replica follows %q: %w",
			barrier.Incarnation, s.incarnation, syscall.EIO)
	}
	confirmation.generation = s.generation
	confirmation.position = metastore.Position(barrier.Position)
	confirmation.ready = true
	s.wake()
	return nil
}

func (s *Storage) forget(*confirmation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConfirmations == 0 {
		return
	}
	s.activeConfirmations--
	s.wakeConfirmationCapacity()
}

func (s *Storage) await(ctx context.Context, op, path string, confirmation *confirmation) error {
	grace := time.NewTimer(s.options.ConfirmationGrace)
	defer grace.Stop()

	for {
		s.mu.Lock()
		failure, generation, at, ready, notify := s.failure, s.generation, s.at, confirmation.ready, s.notify
		barrierGeneration, barrierPosition := confirmation.generation, confirmation.position
		s.mu.Unlock()
		if failure != nil {
			return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the change was made, and this copy stopped being kept current before reaching its barrier: %s: %w",
				failure, syscall.EIO)}
		}
		if ready && generation != barrierGeneration {
			return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the change was made under a different log incarnation than this copy now follows: %w", syscall.EIO)}
		}
		if ready && at >= barrierPosition {
			return nil
		}

		select {
		case <-notify:
		case <-ctx.Done():
			return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the change was made, and waiting for this copy to reach its barrier was cut short: %s: %w",
				context.Cause(ctx), syscall.EIO)}
		case <-s.lifetime.Done():
			return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the change was made, and this copy was released before reaching its barrier: %w", syscall.EIO)}
		case <-grace.C:
			return &os.PathError{Op: op, Path: path, Err: fmt.Errorf(
				"the change was made, and this copy could not confirm it by reaching position %d within %v: %w",
				barrierPosition, s.options.ConfirmationGrace, syscall.EIO)}
		}
	}
}
