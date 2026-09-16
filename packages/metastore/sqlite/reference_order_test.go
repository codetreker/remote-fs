package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestReferenceOrderKeepsDetachedIdentityAndRejectsRetirement(t *testing.T) {
	s, file := openPublicationFile(t)
	f := file.(*retainedFile)
	before, err := f.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	transitionErr := errors.New("transition refused")
	if err := f.Order(t.Context(), func() error {
		calls++
		if f.id != before.ID || s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 {
			t.Fatal("transition lost the retained identity")
		}
		return transitionErr
	}); !errors.Is(err, transitionErr) || calls != 1 {
		t.Fatalf("detached transition = %v; calls=%d", err, calls)
	}
	if err := f.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.Order(t.Context(), func() error { calls++; return nil }); !errors.Is(err, syscall.ESTALE) || calls != 1 {
		t.Fatalf("retired transition = %v; calls=%d", err, calls)
	}
}

func TestReferenceOrderChecksCancellationAndPublicationBeforeTransition(t *testing.T) {
	s, file := openPublicationFile(t)
	f := file.(*retainedFile)
	calls := 0
	transition := func() error { calls++; return nil }
	if err := f.Order(t.Context(), nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil transition = %v", err)
	}
	guardErr := errors.New("session lifetime ended")
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error { return guardErr })
	if err := f.Order(guarded, transition); !errors.Is(err, guardErr) || calls != 0 {
		t.Fatalf("guarded transition = %v; calls=%d", err, calls)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan error, 1)
	go func() { finished <- f.Order(ctx, transition) }()
	cancel()
	err := <-finished
	s.coordinator.commit.release()
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled queued transition = %v; calls=%d", err, calls)
	}
	if err := f.Order(t.Context(), transition); err != nil || calls != 1 {
		t.Fatalf("transition after cancellation = %v; calls=%d", err, calls)
	}
}
