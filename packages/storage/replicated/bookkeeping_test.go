package replicated

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestConfirmationAdmissionBoundsActiveRecordsAndWaiters(t *testing.T) {
	s := confirmationTestStorage(Options{
		ConfirmationGrace: time.Second, MaxActiveConfirmations: 1, MaxWaitingConfirmations: 1,
	})
	first, err := s.expect(t.Context(), "create", "active")
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		confirmation *confirmation
		err          error
	}
	waiting := make(chan result, 1)
	go func() {
		confirmation, err := s.expect(t.Context(), "create", "waiting")
		waiting <- result{confirmation, err}
	}()
	waitForConfirmationWaiters(t, s, 1)
	if _, err := s.expect(t.Context(), "create", "refused"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("caller above waiter bound returned %v", err)
	}
	s.forget(first)
	second := <-waiting
	if second.err != nil {
		t.Fatal(second.err)
	}
	s.forget(second.confirmation)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConfirmations != 0 || s.confirmationWaiters != 0 {
		t.Fatalf("released admission retains active=%d waiters=%d", s.activeConfirmations, s.confirmationWaiters)
	}
}

func TestCancelledAndClosingConfirmationWaitersLeaveNoState(t *testing.T) {
	for _, c := range []struct {
		name string
		end  func(*Storage, context.CancelFunc)
		want syscall.Errno
	}{
		{name: "cancelled", want: syscall.EINTR, end: func(_ *Storage, cancel context.CancelFunc) { cancel() }},
		{name: "closing", want: syscall.EAGAIN, end: func(s *Storage, _ context.CancelFunc) {
			s.mu.Lock()
			s.closing = true
			s.wakeConfirmationCapacity()
			s.mu.Unlock()
			s.stop()
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := confirmationTestStorage(Options{
				ConfirmationGrace: time.Second, MaxActiveConfirmations: 1, MaxWaitingConfirmations: 1,
			})
			active, err := s.expect(t.Context(), "create", "active")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.expect(ctx, "create", "waiting")
				done <- err
			}()
			waitForConfirmationWaiters(t, s, 1)
			c.end(s, cancel)
			if err := <-done; !errors.Is(err, c.want) || storage.ErrnoOf(err) != c.want {
				t.Fatalf("waiter returned %v, want %v", err, c.want)
			} else if c.want == syscall.EINTR && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled waiter lost its cause: %v", err)
			}
			s.forget(active)
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.activeConfirmations != 0 || s.confirmationWaiters != 0 {
				t.Fatalf("released waiter retains active=%d waiters=%d", s.activeConfirmations, s.confirmationWaiters)
			}
		})
	}
}

func TestBarrierConfirmationHandlesEitherNetworkOrderingAndLaterTails(t *testing.T) {
	for _, c := range []struct {
		name          string
		appliedBefore int64
		barrier       int64
	}{
		{name: "response before event", barrier: 6},
		{name: "event before response", appliedBefore: 6, barrier: 6},
		{name: "same-target writer between admission and response cannot confirm a later barrier", appliedBefore: 7, barrier: 8},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := confirmationTestStorage(DefaultOptions())
			confirmation, err := s.expect(t.Context(), "create", "target")
			if err != nil {
				t.Fatal(err)
			}
			defer s.forget(confirmation)
			if c.appliedBefore != 0 {
				s.applied(metastore.Change{Position: metastore.Position(c.appliedBefore), Name: []byte("target")})
			}
			if err := s.setBarrier(confirmation, httprest.MutationBarrier{Incarnation: "log", Position: c.barrier}); err != nil {
				t.Fatal(err)
			}
			if c.appliedBefore < c.barrier {
				cancelled, cancel := context.WithCancel(t.Context())
				cancel()
				if err := s.await(cancelled, "create", "target", confirmation); !errors.Is(err, syscall.EIO) {
					t.Fatalf("position %d below barrier %d was accepted: %v", c.appliedBefore, c.barrier, err)
				}
				s.applied(metastore.Change{Position: metastore.Position(c.barrier)})
				if err := s.await(t.Context(), "create", "target", confirmation); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := s.await(t.Context(), "create", "target", confirmation); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBarrierIncarnationMismatchAndLaterGenerationFail(t *testing.T) {
	s := confirmationTestStorage(DefaultOptions())
	confirmation, err := s.expect(t.Context(), "create", "target")
	if err != nil {
		t.Fatal(err)
	}
	defer s.forget(confirmation)
	if err := s.setBarrier(confirmation, httprest.MutationBarrier{Incarnation: "other", Position: 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("mismatched barrier returned %v", err)
	}
	if err := s.setBarrier(confirmation, httprest.MutationBarrier{Incarnation: "log", Position: 1}); err != nil {
		t.Fatal(err)
	}
	s.seeded("replacement", 1)
	if err := s.await(t.Context(), "create", "target", confirmation); !errors.Is(err, syscall.EIO) {
		t.Fatalf("confirmation survived incarnation change with %v", err)
	}
}

func TestFollowerFailureRejectsAnAcknowledgedMutation(t *testing.T) {
	for _, c := range []struct {
		name    string
		applied metastore.Position
	}{
		{name: "barrier outstanding", applied: 6},
		{name: "barrier already applied", applied: 7},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := confirmationTestStorage(DefaultOptions())
			defer s.stop()
			failure := errors.New("the change stream ended before confirmation")
			dispatched := 0
			err := s.change(t.Context(), "create", "target", func(context.Context) (httprest.MutationBarrier, error) {
				dispatched++
				s.applied(metastore.Change{Position: c.applied})
				s.fail(failure)
				return httprest.MutationBarrier{Incarnation: "log", Position: 7}, nil
			})
			if dispatched != 1 {
				t.Fatalf("mutation dispatches = %d, want one successful response", dispatched)
			}
			var pathErr *os.PathError
			if !errors.As(err, &pathErr) || pathErr.Op != "create" || pathErr.Path != "target" {
				t.Fatalf("failed confirmation lost its operation or path: %v", err)
			}
			if !errors.Is(err, syscall.EIO) || storage.ErrnoOf(err) != syscall.EIO {
				t.Fatalf("failed follower confirmed an acknowledged mutation: %v", err)
			}
			for _, detail := range []string{"the change was made", "stopped being kept current", failure.Error()} {
				if !strings.Contains(err.Error(), detail) {
					t.Errorf("confirmation failure %q lost diagnostic %q", err, detail)
				}
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.activeConfirmations != 0 || s.confirmationWaiters != 0 {
				t.Fatalf("failed confirmation retained active=%d waiters=%d", s.activeConfirmations, s.confirmationWaiters)
			}
		})
	}
}

func confirmationTestStorage(options Options) *Storage {
	lifetime, stop := context.WithCancel(context.Background())
	return &Storage{
		lifetime: lifetime, stop: stop, notify: make(chan struct{}),
		confirmationCapacity: make(chan struct{}), options: options,
		incarnation: "log", generation: 1,
	}
}

func waitForConfirmationWaiters(t *testing.T, s *Storage, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := s.confirmationWaiters
		s.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("confirmation waiter count did not reach %d", want)
}
