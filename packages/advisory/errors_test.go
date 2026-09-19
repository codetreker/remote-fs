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
	o, other := owner(t, s, 1, 0), owner(t, s, 1, 0)
	lock := record(storage.RangeExclusive, 0, 100)
	first := apply(t, s, 1, o, lock)
	for _, call := range []func() error{
		func() error {
			_, err := s.Apply(background, 1, other, []storage.RangeCommand{lock}, first.Request, ordered)
			return err
		},
		func() error {
			_, err := s.Apply(background, 1, o, []storage.RangeCommand{subtract(lock)}, first.Request, ordered)
			return err
		},
		func() error { _, err := s.Query(background, 2, o, first.Request); return err },
		func() error { _, err := s.Cancel(background, 1, other, first.Request); return err },
		func() error { _, err := s.Query(background, 1, o, "malformed"); return err },
		func() error {
			_, err := s.Apply(background, 1, o, []storage.RangeCommand{lock}, "malformed", ordered)
			return err
		},
	} {
		if err := call(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mismatched request = %v", err)
		}
	}
	if _, err := s.Cancel(background, 1, o, requestID(t, s)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("unknown cancellation = %v", err)
	}
	conflict, err := s.GetConflict(background, 1, other, lock, ordered)
	if err != nil || !conflict.Found || c.requests != 1 {
		t.Fatalf("invalid action changed state: %+v %v requests=%d", conflict, err, c.requests)
	}
}

func TestCancelledCallsLeaveGrantedAndPendingState(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	lock := whole(storage.RangeExclusive)
	first := apply(t, a, 1, ao, lock)
	lock.Wait = true
	pending := apply(t, b, 1, bo, lock)
	ctx, cancel := context.WithCancel(background)
	cancel()
	for _, call := range []func() error{
		func() error { _, err := a.GetConflict(ctx, 1, ao, lock, ordered); return err },
		func() error {
			_, err := a.Apply(ctx, 1, ao, []storage.RangeCommand{lock}, requestID(t, a), ordered)
			return err
		},
		func() error { _, err := a.Query(ctx, 1, ao, first.Request); return err },
		func() error { _, err := b.Cancel(ctx, 1, bo, pending.Request); return err },
		func() error { return a.Drop(ctx, 1, ao, storage.DomainWholeFile) },
		func() error { return a.Retire(ctx) },
		func() error { return a.IOHealth(ctx, 1) },
		func() error { _, _, err := a.History(ctx); return err },
		func() error {
			_, err := a.NewOwner(ctx, 1, storage.UseScope{Token: "cancelled"}, storage.OwnerOptions{Lifetime: storage.OwnerReference})
			return err
		},
		func() error { _, err := a.OwnerNode(ctx, ao); return err },
		func() error { _, err := a.OwnerScope(ctx, ao); return err },
		func() error { return a.RetireOwner(ctx, ao) },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call = %v", err)
		}
	}
	got, err := b.Query(background, 1, bo, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Pending, "")
}

func TestRetiredSessionRejectsActions(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	lock := whole(storage.RangeExclusive)
	first := apply(t, s, 1, o, lock)
	if err := s.Retire(background); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { _, err := s.GetConflict(background, 1, o, lock, ordered); return err },
		func() error {
			_, err := s.Apply(background, 1, o, []storage.RangeCommand{lock}, first.Request, ordered)
			return err
		},
		func() error { _, err := s.Query(background, 1, o, first.Request); return err },
		func() error { _, err := s.Cancel(background, 1, o, first.Request); return err },
		func() error { return s.Drop(background, 1, o, storage.DomainWholeFile) },
		func() error { _, _, err := s.History(background); return err },
		func() error { _, err := s.OwnerNode(background, o); return err },
		func() error { _, err := s.OwnerScope(background, o); return err },
	} {
		if err := call(); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("retired call = %v", err)
		}
	}
}

func TestInvalidLimitsOwnersAndCommands(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero config = %v", err)
	}
	c := fixture(t, DefaultConfig())
	if _, err := c.NewSession(storage.FileSessionOptions{}, func() error { return nil }); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero session = %v", err)
	}
	if _, err := c.NewSession(storage.DefaultFileSessionOptions(), nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing fence = %v", err)
	}
	s := session(t, c)
	o := owner(t, s, 1, 0)
	lock := whole(storage.RangeShared)
	for _, call := range []func() error{
		func() error {
			_, err := s.NewOwner(background, 0, storage.UseScope{Token: "valid"}, storage.OwnerOptions{Lifetime: storage.OwnerReference})
			return err
		},
		func() error {
			_, err := s.NewOwner(background, 1, storage.UseScope{}, storage.OwnerOptions{Lifetime: storage.OwnerReference})
			return err
		},
		func() error {
			_, err := s.NewOwner(background, 1, storage.UseScope{Token: "valid"}, storage.OwnerOptions{})
			return err
		},
		func() error { _, err := s.GetConflict(background, 0, o, lock, ordered); return err },
		func() error { _, err := s.GetConflict(background, 1, o, storage.RangeCommand{}, ordered); return err },
		func() error { _, err := s.GetConflict(background, 1, o, subtract(lock), ordered); return err },
		func() error { _, err := s.GetConflict(background, 1, o, lock, nil); return err },
		func() error { _, err := s.Apply(background, 1, o, nil, requestID(t, s), ordered); return err },
		func() error {
			_, err := s.Apply(background, 1, o, []storage.RangeCommand{lock}, requestID(t, s), nil)
			return err
		},
		func() error {
			_, err := s.Apply(background, 1, o, []storage.RangeCommand{lock, lock}, requestID(t, s), ordered)
			return err
		},
		func() error { return s.Drop(background, 1, o, 0) },
		func() error { return s.RetireOwner(background, 0) },
	} {
		if err := call(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid command = %v", err)
		}
	}
	if c.requests != 0 || c.ranges != 0 {
		t.Fatal("invalid command consumed action or range capacity")
	}
	if node, err := s.OwnerNode(background, o); err != nil || node != 1 {
		t.Fatalf("owner node = %d %v", node, err)
	}
	if scope, err := s.OwnerScope(background, o); err != nil || scope.Token == "" {
		t.Fatalf("owner scope = %+v %v", scope, err)
	}
	if err := s.RetireOwner(background, o); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OwnerNode(background, o); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired registration node = %v", err)
	}
	if _, err := s.OwnerScope(background, o); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired registration scope = %v", err)
	}
}

func TestNativeReplyFailureCanBeReconciled(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	id := requestID(t, s)
	order := func(ctx context.Context, transition func() error) error {
		if err := transition(); err != nil {
			return err
		}
		return syscall.EIO
	}
	returned, err := s.Apply(background, 1, o, []storage.RangeCommand{whole(storage.RangeExclusive)}, id, order)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("lost native reply = %v", err)
	}
	if returned.State != storage.Granted || !returned.EverGranted || len(returned.Effects) != 1 {
		t.Fatalf("known grant was discarded with reply failure: %+v", returned)
	}
	got, err := s.Cancel(background, 1, o, id)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Granted, "")
	if !got.EverGranted || c.ranges != 1 {
		t.Fatal("cancellation discarded grant with lost reply")
	}
}

func TestInterruptedConversionPreservesReleaseReceipt(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	wantState(t, apply(t, s, 1, o, whole(storage.RangeShared)), storage.Granted, "")
	calls := 0
	order := func(ctx context.Context, transition func() error) error {
		calls++
		if calls == 2 {
			return syscall.EIO
		}
		return transition()
	}
	id := requestID(t, s)
	returned, err := s.Apply(background, 1, o, []storage.RangeCommand{whole(storage.RangeExclusive)}, id, order)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("interrupted conversion = %v", err)
	}
	if returned.State != storage.Pending || len(returned.Effects) != 1 || !returned.Effects[0].Released || returned.EverGranted {
		t.Fatalf("known conversion release was discarded with order failure: %+v", returned)
	}
	got, err := s.Cancel(background, 1, o, id)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Cancelled, "")
	if len(got.Effects) != 1 || !got.Effects[0].Released || got.EverGranted || c.ranges != 0 {
		t.Fatalf("interrupted conversion lost release: %+v", got)
	}
}
