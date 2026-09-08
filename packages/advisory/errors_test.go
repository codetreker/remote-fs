package advisory

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestInvalidActionIdentityCannotMutateGrant(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	lock := posix(storage.Exclusive, 0, 99)
	first := set(t, s, 1, 1, lock)
	wantState(t, first, storage.LockGranted, 0)
	for _, test := range []struct {
		node  uint64
		owner storage.LockOwner
		lock  storage.FileLock
	}{{2, 1, lock}, {1, 2, lock}, {1, 1, posix(storage.Unlock, 0, 99)}} {
		if _, err := s.Set(background, test.node, test.owner, test.lock, first.Request); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mismatched request = %v", err)
		}
	}
	for _, call := range []func() error{
		func() error { _, err := s.Query(background, 2, 1, first.Request); return err },
		func() error { _, err := s.Cancel(background, 1, 2, first.Request); return err },
		func() error { _, err := s.Query(background, 1, 1, "malformed"); return err },
		func() error { _, err := s.Set(background, 1, 1, lock, "malformed"); return err },
	} {
		if err := call(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mismatched query = %v", err)
		}
	}
	if _, err := s.Cancel(background, 1, 1, requestID(t, s)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("unknown cancellation = %v", err)
	}
	conflict, err := s.Get(background, 1, 2, lock)
	if err != nil || !conflict.Found {
		t.Fatalf("invalid action changed grant: %+v %v", conflict, err)
	}
	if c.requests != 1 {
		t.Fatal("invalid identity consumed receipt capacity")
	}
}

func TestCancelledCallsLeaveGrantedAndPendingState(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	lock := flock(storage.Exclusive)
	first := set(t, a, 1, 1, lock)
	wantState(t, first, storage.LockGranted, 0)
	lock.Wait = true
	pending := set(t, b, 1, 1, lock)
	wantState(t, pending, storage.LockPending, 0)
	ctx, cancel := context.WithCancel(background)
	cancel()
	for _, call := range []func() error{
		func() error { _, err := a.Get(ctx, 1, 1, lock); return err },
		func() error { _, err := a.Set(ctx, 1, 1, lock, requestID(t, a)); return err },
		func() error { _, err := a.Query(ctx, 1, 1, first.Request); return err },
		func() error { _, err := b.Cancel(ctx, 1, 1, pending.Request); return err },
		func() error { return a.Drop(ctx, 1, 1, storage.Flock) },
		func() error { return a.Retire(ctx) },
		func() error { return a.IOHealth(ctx, 1) },
		func() error { _, _, err := a.History(ctx); return err },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call = %v", err)
		}
	}
	got, err := b.Query(background, 1, 1, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockPending, 0)
	conflict, err := b.Get(background, 1, 1, lock)
	if err != nil || !conflict.Found {
		t.Fatalf("cancelled cleanup released grant: %+v %v", conflict, err)
	}
}

func TestRetiredSessionRejectsAllActions(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	lock := flock(storage.Exclusive)
	first := set(t, s, 1, 1, lock)
	if err := s.Retire(background); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { _, err := s.Get(background, 1, 1, lock); return err },
		func() error { _, err := s.Set(background, 1, 1, lock, first.Request); return err },
		func() error { _, err := s.Query(background, 1, 1, first.Request); return err },
		func() error { _, err := s.Cancel(background, 1, 1, first.Request); return err },
		func() error { return s.Drop(background, 1, 1, storage.Flock) },
		func() error { _, _, err := s.History(background); return err },
	} {
		if err := call(); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("retired call = %v", err)
		}
	}
}

func TestInvalidLimitsAndLockQueries(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero config = %v", err)
	}
	c := fixture(t, DefaultConfig())
	if _, err := c.NewSession(storage.FileSessionOptions{}, func() error { return nil }); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero session options = %v", err)
	}
	if _, err := c.NewSession(storage.DefaultFileSessionOptions(), nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("absent fence = %v", err)
	}
	s := session(t, c)
	for _, call := range []func() error{
		func() error { _, err := s.Get(background, 0, 1, flock(storage.Shared)); return err },
		func() error { _, err := s.Get(background, 1, 1, storage.FileLock{}); return err },
		func() error { _, err := s.Get(background, 1, 1, flock(storage.Unlock)); return err },
		func() error { _, err := s.Set(background, 1, 1, storage.FileLock{}, requestID(t, s)); return err },
		func() error { return s.Drop(background, 1, 1, 0) },
	} {
		if err := call(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid lock call = %v", err)
		}
	}
	if c.requests != 0 || c.ranges != 0 || len(c.owners) != 0 {
		t.Fatal("invalid request retained coordinator state")
	}
}
