package advisory

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSessionBoundRecoversAfterRetirement(t *testing.T) {
	config := DefaultConfig()
	config.MaxSessions = 1
	c := fixture(t, config)
	s := session(t, c)
	if _, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil }); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("session capacity = %v", err)
	}
	if err := s.Retire(background); err != nil {
		t.Fatal(err)
	}
	_ = session(t, c)
}

func TestOwnerBoundsAndRetirement(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config, options := DefaultConfig(), storage.DefaultFileSessionOptions()
		if perSession {
			options.MaxLockOwners = 1
		} else {
			config.MaxOwners = 1
		}
		c := fixture(t, config)
		s, err := c.NewSession(options, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		o := owner(t, s, 1, 0)
		wantState(t, apply(t, s, 1, o, whole(storage.RangeShared)), storage.Granted, "")
		if _, err := s.NewOwner(background, 2, storage.UseScope{Token: "next"}, storage.OwnerOptions{Lifetime: storage.OwnerReference}); !errors.Is(err, syscall.ENOLCK) {
			t.Fatalf("owner capacity = %v", err)
		}
		if c.ranges != 1 {
			t.Fatal("owner admission lost existing grant")
		}
		if err := s.RetireOwner(background, o); err != nil {
			t.Fatal(err)
		}
		if err := s.RetireOwner(background, o); err != nil {
			t.Fatal(err)
		}
		_ = owner(t, s, 2, 0)
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
		if c.registeredOwners != 0 || c.ranges != 0 {
			t.Fatal("retirement retained registration capacity")
		}
	}
}

func TestRangeBoundsKeepOriginalOnFailedSplit(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config, options := DefaultConfig(), storage.DefaultFileSessionOptions()
		if perSession {
			options.MaxLockRanges = 1
		} else {
			config.MaxRanges = 1
		}
		c := fixture(t, config)
		s, err := c.NewSession(options, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		o, observer := owner(t, s, 1, 0), owner(t, s, 1, 0)
		wantState(t, apply(t, s, 1, o, record(storage.RangeExclusive, 0, 100)), storage.Granted, "")
		wantState(t, apply(t, s, 1, o, subtract(record(storage.RangeShared, 20, 20))), storage.Rejected, storage.RangeExhausted)
		got, err := s.GetConflict(background, 1, observer, record(storage.RangeShared, 20, 20), ordered)
		if err != nil || !got.Found || got.Range.Start != 0 || got.Range.Length != 100 {
			t.Fatalf("failed split changed original: %+v %v", got, err)
		}
		wantState(t, apply(t, s, 1, o, subtract(record(storage.RangeShared, 0, 100))), storage.Released, "")
		wantState(t, apply(t, s, 1, o, record(storage.RangeShared, 0, 1)), storage.Granted, "")
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
	}
}

func TestActionBoundsPreserveReplayAndRecover(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config, options := DefaultConfig(), storage.DefaultFileSessionOptions()
		if perSession {
			options.MaxLockActions = 1
		} else {
			config.MaxRequests = 1
		}
		c := fixture(t, config)
		now := time.Unix(1000, 0)
		c.now = func() time.Time { return now }
		s, err := c.NewSession(options, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		o := owner(t, s, 1, 0)
		lock := whole(storage.RangeShared)
		first := apply(t, s, 1, o, lock)
		if _, err := s.Apply(background, 1, o, []storage.RangeCommand{lock}, requestID(t, s), ordered); !errors.Is(err, syscall.ENOLCK) {
			t.Fatalf("action capacity = %v", err)
		}
		got, err := s.Apply(background, 1, o, []storage.RangeCommand{lock}, first.Request, ordered)
		if err != nil {
			t.Fatal(err)
		}
		wantState(t, got, storage.Granted, "")
		now = now.Add(2 * options.History)
		wantState(t, apply(t, s, 1, o, whole(storage.RangeExclusive)), storage.Granted, "")
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingAdmissionBoundsAndCleanupCapacity(t *testing.T) {
	for _, limit := range []string{"volume", "session", "waiters"} {
		t.Run(limit, func(t *testing.T) {
			config, options := DefaultConfig(), storage.DefaultFileSessionOptions()
			switch limit {
			case "volume":
				config.MaxWaiters = 1
			case "session":
				options.MaxPendingLocks = 1
			case "waiters":
				options.MaxWaiters = 1
			}
			c := fixture(t, config)
			holder := session(t, c)
			waiter, err := c.NewSession(options, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			h, a, b := owner(t, holder, 1, 0), owner(t, waiter, 1, 0), owner(t, waiter, 1, 0)
			wantState(t, apply(t, holder, 1, h, whole(storage.RangeExclusive)), storage.Granted, "")
			lock := whole(storage.RangeExclusive)
			lock.Wait = true
			first := apply(t, waiter, 1, a, lock)
			wantState(t, first, storage.Pending, "")
			wantState(t, apply(t, waiter, 1, b, lock), storage.Rejected, storage.RangeExhausted)
			cancelled, err := waiter.Cancel(background, 1, a, first.Request)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, cancelled, storage.Cancelled, "")
			wantState(t, apply(t, waiter, 1, b, lock), storage.Pending, "")
			if err := waiter.Retire(background); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeadlockEdgeCapacity(t *testing.T) {
	config := DefaultConfig()
	config.MaxDeadlockEdges = 1
	c := fixture(t, config)
	a, b, waiter := session(t, c), session(t, c), session(t, c)
	ao, bo, wo := owner(t, a, 1, 0), owner(t, b, 1, 0), owner(t, waiter, 1, 0)
	shared := record(storage.RangeShared, 0, 100)
	wantState(t, apply(t, a, 1, ao, shared), storage.Granted, "")
	wantState(t, apply(t, b, 1, bo, shared), storage.Granted, "")
	lock := record(storage.RangeExclusive, 0, 100)
	lock.Wait = true
	wantState(t, apply(t, waiter, 1, wo, lock), storage.Rejected, storage.RangeExhausted)
	if err := b.Drop(background, 1, bo, storage.DomainRecord); err != nil {
		t.Fatal(err)
	}
	wantState(t, apply(t, waiter, 1, wo, lock), storage.Pending, "")
}

func TestGlobalActionAdmissionPrunesOtherSessions(t *testing.T) {
	config := DefaultConfig()
	config.MaxRequests = 1
	c := fixture(t, config)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 2, 0)
	first := apply(t, a, 1, ao, whole(storage.RangeShared))
	now = now.Add(2 * a.options.History)
	wantState(t, apply(t, b, 2, bo, whole(storage.RangeShared)), storage.Granted, "")
	if _, err := a.Apply(background, 1, ao, []storage.RangeCommand{whole(storage.RangeShared)}, first.Request, ordered); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired global receipt = %v", err)
	}
}
