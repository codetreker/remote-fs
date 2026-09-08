package advisory

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

var background = context.Background()

func fixture(t *testing.T, config Config) *Coordinator {
	t.Helper()
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func session(t *testing.T, c *Coordinator) *Session {
	t.Helper()
	s, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Retire(background); err != nil {
			t.Error(err)
		}
	})
	return s
}

func requestID(t *testing.T, s *Session) storage.LockRequestID {
	t.Helper()
	epoch, err := s.Epoch(background)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func set(t *testing.T, s *Session, node uint64, owner storage.LockOwner, lock storage.FileLock) storage.LockAttempt {
	t.Helper()
	result, err := s.Set(background, node, owner, lock, requestID(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func wantState(t *testing.T, result storage.LockAttempt, state storage.LockAttemptState, errno syscall.Errno) {
	t.Helper()
	if result.State != state || result.Errno != errno {
		t.Fatalf("result = %+v; want state %d errno %v", result, state, errno)
	}
}

func posix(mode storage.LockType, start, end uint64) storage.FileLock {
	return storage.FileLock{Family: storage.POSIX, Type: mode, Start: start, End: end}
}

func flock(mode storage.LockType) storage.FileLock {
	return storage.FileLock{Family: storage.Flock, Type: mode, End: math.MaxInt64}
}

func TestPOSIXRangeReplacement(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	key := s.key(1, 7, storage.POSIX)
	steps := []struct {
		lock   storage.FileLock
		ranges []storage.FileLock
	}{
		{posix(storage.Shared, 0, 99), []storage.FileLock{posix(storage.Shared, 0, 99)}},
		{posix(storage.Exclusive, 20, 39), []storage.FileLock{posix(storage.Shared, 0, 19), posix(storage.Exclusive, 20, 39), posix(storage.Shared, 40, 99)}},
		{posix(storage.Shared, 20, 39), []storage.FileLock{posix(storage.Shared, 0, 99)}},
		{posix(storage.Unlock, 20, 39), []storage.FileLock{posix(storage.Shared, 0, 19), posix(storage.Shared, 40, 99)}},
		{posix(storage.Exclusive, 50, math.MaxInt64), []storage.FileLock{posix(storage.Shared, 0, 19), posix(storage.Shared, 40, 49), posix(storage.Exclusive, 50, math.MaxInt64)}},
		{posix(storage.Unlock, math.MaxInt64, math.MaxInt64), []storage.FileLock{posix(storage.Shared, 0, 19), posix(storage.Shared, 40, 49), posix(storage.Exclusive, 50, math.MaxInt64-1)}},
	}
	for _, step := range steps {
		result := set(t, s, 1, 7, step.lock)
		if step.lock.Type == storage.Unlock {
			wantState(t, result, storage.LockReleased, 0)
		} else {
			wantState(t, result, storage.LockGranted, 0)
		}
		if got := c.owners[key].ranges; !reflect.DeepEqual(got, step.ranges) {
			t.Fatalf("ranges = %+v; want %+v", got, step.ranges)
		}
	}
}

func TestFamiliesOwnersAndDiagnosticPID(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	lock := posix(storage.Exclusive, 3, 8)
	lock.PID = 123
	wantState(t, set(t, a, 1, 9, lock), storage.LockGranted, 0)
	wantState(t, set(t, b, 1, 9, flock(storage.Exclusive)), storage.LockGranted, 0)
	wantState(t, set(t, b, 1, 9, lock), storage.LockRejected, syscall.EAGAIN)
	for _, test := range []struct {
		s     *Session
		owner storage.LockOwner
		found bool
		pid   uint32
	}{{a, 9, false, 0}, {a, 10, true, 123}, {b, 9, true, 0}} {
		got, err := test.s.Get(background, 1, test.owner, lock)
		if err != nil || got.Found != test.found || got.Lock.PID != test.pid {
			t.Fatalf("conflict = %+v, %v", got, err)
		}
	}
	if err := b.IOHealth(background, 1); err != nil {
		t.Fatalf("ordinary I/O joined advisory conflict checks: %v", err)
	}
}

func TestFlockAndPOSIXConversion(t *testing.T) {
	for _, family := range []storage.LockFamily{storage.Flock, storage.POSIX} {
		t.Run(map[storage.LockFamily]string{storage.Flock: "flock", storage.POSIX: "POSIX"}[family], func(t *testing.T) {
			c := fixture(t, DefaultConfig())
			a, b := session(t, c), session(t, c)
			shared := flock(storage.Shared)
			shared.Family = family
			wantState(t, set(t, a, 1, 1, shared), storage.LockGranted, 0)
			wantState(t, set(t, a, 1, 1, shared), storage.LockGranted, 0)
			if c.ranges != 1 {
				t.Fatal("repeated same-mode lock increased range count")
			}
			wantState(t, set(t, b, 1, 1, shared), storage.LockGranted, 0)
			exclusive := shared
			exclusive.Type = storage.Exclusive
			wantState(t, set(t, a, 1, 1, exclusive), storage.LockRejected, syscall.EAGAIN)
			result := set(t, b, 1, 1, exclusive)
			if family == storage.Flock {
				wantState(t, result, storage.LockGranted, 0)
			} else {
				wantState(t, result, storage.LockRejected, syscall.EAGAIN)
			}
		})
	}
}

func TestCrossFileDeadlock(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	lock := posix(storage.Exclusive, 0, 99)
	wantState(t, set(t, a, 1, 1, lock), storage.LockGranted, 0)
	wantState(t, set(t, b, 2, 1, lock), storage.LockGranted, 0)
	lock.Wait = true
	pending := set(t, a, 2, 1, lock)
	wantState(t, pending, storage.LockPending, 0)
	wantState(t, set(t, b, 1, 1, lock), storage.LockRejected, syscall.EDEADLK)
	if err := b.Drop(background, 2, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	got, err := a.Query(background, 2, 1, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockGranted, 0)
}

func TestCancelGrantRaceHasKnownOutcome(t *testing.T) {
	for range 32 {
		c := fixture(t, DefaultConfig())
		a, b, observer := session(t, c), session(t, c), session(t, c)
		lock := posix(storage.Exclusive, 0, 99)
		wantState(t, set(t, a, 1, 1, lock), storage.LockGranted, 0)
		lock.Wait = true
		pending := set(t, b, 1, 1, lock)
		wantState(t, pending, storage.LockPending, 0)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelled storage.LockAttempt
		var cancelErr, dropErr error
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cancelled, cancelErr = b.Cancel(background, 1, 1, pending.Request) }()
		go func() { defer wg.Done(); <-start; dropErr = a.Drop(background, 1, 1, storage.POSIX) }()
		close(start)
		wg.Wait()
		if cancelErr != nil || dropErr != nil {
			t.Fatalf("cancel %v; drop %v", cancelErr, dropErr)
		}
		conflict, err := observer.Get(background, 1, 1, lock)
		if err != nil {
			t.Fatal(err)
		}
		switch cancelled.State {
		case storage.LockCancelled:
			if cancelled.EverGranted || conflict.Found {
				t.Fatalf("cancelled acquisition survived: %+v %+v", cancelled, conflict)
			}
		case storage.LockGranted:
			if !cancelled.EverGranted || !conflict.Found {
				t.Fatalf("granted acquisition disappeared: %+v %+v", cancelled, conflict)
			}
		default:
			t.Fatalf("unsettled cancellation: %+v", cancelled)
		}
	}
}

func TestEpochRetainsPendingAndRefusesExpiredReplay(t *testing.T) {
	c := fixture(t, DefaultConfig())
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	a, b := session(t, c), session(t, c)
	lock := posix(storage.Exclusive, 0, 99)
	wantState(t, set(t, a, 1, 1, lock), storage.LockGranted, 0)
	lock.Wait = true
	pending := set(t, b, 1, 1, lock)
	wantState(t, pending, storage.LockPending, 0)
	for range 3 {
		now = now.Add(2 * time.Minute)
		if _, err := b.Epoch(background); err != nil {
			t.Fatal(err)
		}
		got, err := b.Query(background, 1, 1, pending.Request)
		if err != nil {
			t.Fatal(err)
		}
		wantState(t, got, storage.LockPending, 0)
	}
	if err := a.Drop(background, 1, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	got, err := b.Query(background, 1, 1, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockGranted, 0)
	if got.HistoryRemaining != b.options.History {
		t.Fatal("old pending epoch shortened completed history")
	}
	if err := b.Drop(background, 1, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	got, err = b.Set(background, 1, 1, lock, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockReleased, 0)
	now = now.Add(2 * time.Minute)
	if _, err := b.Set(background, 1, 1, lock, pending.Request); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired replay = %v", err)
	}
	conflict, err := a.Get(background, 1, 1, lock)
	if err != nil || conflict.Found {
		t.Fatalf("expired replay reacquired lock: %+v %v", conflict, err)
	}
}

func TestDropIsScopedToFileOwnerAndFamily(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s, observer := session(t, c), session(t, c)
	for _, test := range []struct {
		node  uint64
		owner storage.LockOwner
		lock  storage.FileLock
	}{{1, 1, posix(storage.Exclusive, 0, 10)}, {2, 1, posix(storage.Exclusive, 0, 10)}, {1, 2, posix(storage.Exclusive, 20, 30)}, {1, 1, flock(storage.Exclusive)}} {
		wantState(t, set(t, s, test.node, test.owner, test.lock), storage.LockGranted, 0)
	}
	if err := s.Drop(background, 1, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		node  uint64
		lock  storage.FileLock
		found bool
	}{{1, posix(storage.Exclusive, 0, 10), false}, {2, posix(storage.Exclusive, 0, 10), true}, {1, posix(storage.Exclusive, 20, 30), true}, {1, flock(storage.Exclusive), true}} {
		got, err := observer.Get(background, test.node, 1, test.lock)
		if err != nil || got.Found != test.found {
			t.Fatalf("conflict = %+v, %v", got, err)
		}
	}
}

func TestRetirementFencesBeforeGrantRelease(t *testing.T) {
	c := fixture(t, DefaultConfig())
	entered, release := make(chan struct{}), make(chan struct{})
	a, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	b := session(t, c)
	wantState(t, set(t, a, 1, 1, flock(storage.Exclusive)), storage.LockGranted, 0)
	lock := flock(storage.Exclusive)
	lock.Wait = true
	pending := set(t, b, 1, 1, lock)
	done := make(chan error, 1)
	go func() { done <- a.Retire(background) }()
	<-entered
	if err := a.IOHealth(background, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("retiring I/O = %v", err)
	}
	got, err := b.Query(background, 1, 1, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockPending, 0)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err = b.Query(background, 1, 1, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockGranted, 0)
	if err := a.IOHealth(background, 1); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired I/O = %v", err)
	}
	if err := a.Retire(background); err != nil {
		t.Fatal(err)
	}
}

func TestFailedRetirementPreservesGrants(t *testing.T) {
	c := fixture(t, DefaultConfig())
	failure := errors.New("publication fence failed")
	a, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error { return failure })
	if err != nil {
		t.Fatal(err)
	}
	b := session(t, c)
	wantState(t, set(t, a, 1, 1, flock(storage.Exclusive)), storage.LockGranted, 0)
	if err := a.Retire(background); !errors.Is(err, failure) {
		t.Fatalf("retire = %v", err)
	}
	wantState(t, set(t, b, 1, 1, flock(storage.Exclusive)), storage.LockRejected, syscall.EAGAIN)
	if err := a.IOHealth(background, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed-retirement I/O = %v", err)
	}
	failure = nil
	if err := a.Retire(background); err != nil {
		t.Fatal(err)
	}
	wantState(t, set(t, b, 1, 1, flock(storage.Exclusive)), storage.LockGranted, 0)
}

func TestQueuedRangeDowngradeWakesEarlierWaiter(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b, waiter := session(t, c), session(t, c), session(t, c)
	wantState(t, set(t, a, 1, 1, posix(storage.Exclusive, 10, 19)), storage.LockGranted, 0)
	wantState(t, set(t, b, 1, 1, posix(storage.Exclusive, 0, 9)), storage.LockGranted, 0)
	firstLock := posix(storage.Shared, 0, 9)
	firstLock.Wait = true
	first := set(t, waiter, 1, 1, firstLock)
	wantState(t, first, storage.LockPending, 0)
	secondLock := posix(storage.Shared, 0, 19)
	secondLock.Wait = true
	second := set(t, b, 1, 1, secondLock)
	wantState(t, second, storage.LockPending, 0)
	if err := a.Drop(background, 1, 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	got, err := waiter.Query(background, 1, 1, first.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.LockGranted, 0)
}
