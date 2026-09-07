package replicated

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestConfirmationWaiterCancellationPreservesCustomCause(t *testing.T) {
	s := confirmationTestStorage(Options{
		ConfirmationGrace: time.Second, MaxActiveConfirmations: 1, MaxWaitingConfirmations: 1,
	})
	defer s.stop()
	active, err := s.expect(t.Context(), "write", "active")
	if err != nil {
		t.Fatal(err)
	}
	defer s.forget(active)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	var sent atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- s.change(ctx, "write", "waiting", func(context.Context) (httprest.MutationBarrier, error) {
			sent.Add(1)
			return httprest.MutationBarrier{}, errors.New("unexpected mutation dispatch")
		})
	}()
	waitForConfirmationWaiters(t, s, 1)
	cause := errors.New("the owner withdrew this request")
	cancel(cause)
	err = awaitConfirmationCancellation(t, done)
	if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, syscall.EINTR) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("cancelled admission lost its classification or cause: %v", err)
	}
	if sent.Load() != 0 {
		t.Fatal("cancelled confirmation waiter dispatched a mutation")
	}
	waitForConfirmationWaiters(t, s, 0)
	requireIndependentAdmissionFaultWins(t, err)
}

func TestConfirmationWaiterDeadlineDoesNotSend(t *testing.T) {
	s := confirmationTestStorage(Options{
		ConfirmationGrace: time.Second, MaxActiveConfirmations: 1, MaxWaitingConfirmations: 1,
	})
	defer s.stop()
	active, err := s.expect(t.Context(), "write", "active")
	if err != nil {
		t.Fatal(err)
	}
	defer s.forget(active)
	cause := errors.New("the request exceeded its deadline")
	ctx, cancel := context.WithTimeoutCause(t.Context(), time.Second, cause)
	defer cancel()
	var sent atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- s.change(ctx, "write", "waiting", func(context.Context) (httprest.MutationBarrier, error) {
			sent.Add(1)
			return httprest.MutationBarrier{}, errors.New("unexpected mutation dispatch")
		})
	}()
	waitForConfirmationWaiters(t, s, 1)
	err = awaitConfirmationCancellation(t, done)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, syscall.EIO) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) {
		t.Fatalf("expired admission lost its classification or cause: %v", err)
	}
	if sent.Load() != 0 {
		t.Fatal("expired confirmation waiter dispatched a mutation")
	}
	waitForConfirmationWaiters(t, s, 0)
}

func TestConfirmationCapacityRefusalRetainsItsCause(t *testing.T) {
	cause := errors.New("confirmation capacity is full")
	err := confirmationAdmissionError("write", "f", cause)
	if storage.ErrnoOf(err) != syscall.EAGAIN || !errors.Is(err, syscall.EAGAIN) || !errors.Is(err, cause) {
		t.Fatalf("capacity refusal lost its classification or cause: %v", err)
	}
	for _, joined := range []error{errors.Join(err, context.Canceled), errors.Join(context.Canceled, err)} {
		if storage.ErrnoOf(joined) != syscall.EAGAIN {
			t.Errorf("cancellation hid the independent capacity refusal: %v", joined)
		}
	}
	requireIndependentAdmissionFaultWins(t, err)
}

func TestCanceledMutationDoesNotHideReplicaFailure(t *testing.T) {
	s := confirmationTestStorage(DefaultOptions())
	defer s.stop()
	s.failure = errors.New("the change stream failed")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := s.change(ctx, "write", "f", func(context.Context) (httprest.MutationBarrier, error) {
		t.Fatal("an unusable replica dispatched a mutation")
		return httprest.MutationBarrier{}, nil
	})
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINTR) {
		t.Fatalf("cancellation hid the independent replica failure: %v", err)
	}
}

func requireIndependentAdmissionFaultWins(t *testing.T, err error) {
	t.Helper()
	fault := errors.New("independent operation failure")
	for _, joined := range []error{errors.Join(err, fault), errors.Join(fault, err)} {
		if storage.ErrnoOf(joined) != syscall.EIO {
			t.Errorf("admission classification hid an independent failure: %v", joined)
		}
	}
}

func awaitConfirmationCancellation(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("ended confirmation waiter did not return")
		return nil
	}
}
