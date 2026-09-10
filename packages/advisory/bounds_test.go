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

func TestOwnerBoundsPreserveExistingLocks(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config := DefaultConfig()
		options := storage.DefaultFileSessionOptions()
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
		wantState(t, set(t, s, 1, 1, flock(storage.Shared)), storage.LockGranted, 0)
		wantState(t, set(t, s, 2, 1, flock(storage.Shared)), storage.LockRejected, syscall.ENOLCK)
		got, err := s.Get(background, 1, 2, flock(storage.Exclusive))
		if err != nil || !got.Found {
			t.Fatalf("capacity rejection lost prior lock: %+v %v", got, err)
		}
		if err := s.Drop(background, 1, 1, storage.Flock); err != nil {
			t.Fatal(err)
		}
		wantState(t, set(t, s, 2, 1, flock(storage.Shared)), storage.LockGranted, 0)
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRangeBoundsKeepOriginalOnFailedSplit(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config := DefaultConfig()
		options := storage.DefaultFileSessionOptions()
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
		wantState(t, set(t, s, 1, 1, posix(storage.Exclusive, 0, 99)), storage.LockGranted, 0)
		wantState(t, set(t, s, 1, 1, posix(storage.Unlock, 20, 39)), storage.LockRejected, syscall.ENOLCK)
		got, err := s.Get(background, 1, 2, posix(storage.Shared, 20, 39))
		if err != nil || !got.Found || got.Lock.Start != 0 || got.Lock.End != 99 {
			t.Fatalf("failed split changed prior lock: %+v %v", got, err)
		}
		wantState(t, set(t, s, 1, 1, posix(storage.Unlock, 0, 99)), storage.LockReleased, 0)
		wantState(t, set(t, s, 2, 1, posix(storage.Shared, 0, 0)), storage.LockGranted, 0)
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
	}
}

func TestActionBoundsPreserveReplayAndRecover(t *testing.T) {
	for _, perSession := range []bool{false, true} {
		config := DefaultConfig()
		options := storage.DefaultFileSessionOptions()
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
		lock := flock(storage.Shared)
		first := set(t, s, 1, 1, lock)
		wantState(t, first, storage.LockGranted, 0)
		if _, err := s.Set(background, 1, 1, flock(storage.Exclusive), requestID(t, s)); !errors.Is(err, syscall.ENOLCK) {
			t.Fatalf("action capacity = %v", err)
		}
		got, err := s.Set(background, 1, 1, lock, first.Request)
		if err != nil {
			t.Fatal(err)
		}
		wantState(t, got, storage.LockGranted, 0)
		now = now.Add(2 * time.Minute)
		wantState(t, set(t, s, 1, 1, flock(storage.Exclusive)), storage.LockGranted, 0)
		if _, err := s.Query(background, 1, 1, first.Request); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("expired receipt = %v", err)
		}
		if err := s.Retire(background); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWaitingBoundsRecoverAfterCancellation(t *testing.T) {
	for _, limit := range []string{"volume", "session", "pending"} {
		t.Run(limit, func(t *testing.T) {
			config := DefaultConfig()
			options := storage.DefaultFileSessionOptions()
			switch limit {
			case "volume":
				config.MaxWaiters = 1
			case "session":
				options.MaxWaiters = 1
			case "pending":
				options.MaxPendingLocks = 1
			}
			c := fixture(t, config)
			holder := session(t, c)
			waiter, err := c.NewSession(options, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, set(t, holder, 1, 1, flock(storage.Exclusive)), storage.LockGranted, 0)
			lock := flock(storage.Exclusive)
			lock.Wait = true
			first := set(t, waiter, 1, 1, lock)
			wantState(t, first, storage.LockPending, 0)
			wantState(t, set(t, waiter, 1, 2, lock), storage.LockRejected, syscall.ENOLCK)
			got, err := waiter.Cancel(background, 1, 1, first.Request)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, got, storage.LockCancelled, 0)
			wantState(t, set(t, waiter, 1, 2, lock), storage.LockPending, 0)
			if err := waiter.Retire(background); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBoundedDeadlockGraphRefusesUnresolvedWait(t *testing.T) {
	config := DefaultConfig()
	config.MaxDeadlockEdges = 1
	c := fixture(t, config)
	a, b, waiter := session(t, c), session(t, c), session(t, c)
	wantState(t, set(t, a, 1, 1, posix(storage.Shared, 0, 99)), storage.LockGranted, 0)
	wantState(t, set(t, b, 1, 1, posix(storage.Shared, 0, 99)), storage.LockGranted, 0)
	lock := posix(storage.Exclusive, 0, 99)
	lock.Wait = true
	wantState(t, set(t, waiter, 1, 1, lock), storage.LockRejected, syscall.ENOLCK)
	if err := a.Drop(background, 1, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	wantState(t, set(t, waiter, 1, 1, lock), storage.LockPending, 0)
}

func TestExpiredReceiptReleasesAggregateCapacityWithoutPollingOwner(t *testing.T) {
	config := DefaultConfig()
	config.MaxRequests = 1
	c := fixture(t, config)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	a, b := session(t, c), session(t, c)
	first := set(t, a, 1, 1, flock(storage.Shared))
	wantState(t, first, storage.LockGranted, 0)
	now = now.Add(2 * time.Minute)
	wantState(t, set(t, b, 2, 1, flock(storage.Shared)), storage.LockGranted, 0)
	if _, err := a.Set(background, 1, 1, flock(storage.Shared), first.Request); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("collected action replay = %v", err)
	}
}
