package advisory

import (
	"context"
	"errors"
	"fmt"
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

func owner(t *testing.T, s *Session, node, group uint64) storage.UseOwner {
	t.Helper()
	scope := storage.UseScope{Token: fmt.Sprintf("%d/%d/%d", s.id, node, s.nextOwner+1)}
	o, err := s.NewOwner(background, node, scope, storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Group: group})
	if err != nil {
		t.Fatal(err)
	}
	return o
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

func ordered(ctx context.Context, apply func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return apply()
}

func apply(t *testing.T, s *Session, node uint64, o storage.UseOwner, commands ...storage.RangeCommand) storage.RangeAttempt {
	t.Helper()
	result, err := s.Apply(background, node, o, commands, requestID(t, s), ordered)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func wantState(t *testing.T, result storage.RangeAttempt, state storage.AttemptState, rejection storage.RejectionCode) {
	t.Helper()
	if result.State != state || result.Rejection != rejection {
		t.Fatalf("result = %+v; want state %d rejection %q", result, state, rejection)
	}
}

func record(mode storage.RangeMode, start, length uint64) storage.RangeCommand {
	return storage.RangeCommand{Domain: storage.DomainRecord, Mode: mode, Range: storage.Range{Kind: storage.Bytes, Start: start, Length: length}, Edit: storage.Replace}
}

func whole(mode storage.RangeMode) storage.RangeCommand {
	c := record(mode, 0, 1<<63)
	c.Domain, c.Conversion = storage.DomainWholeFile, storage.DropBeforeAcquire
	return c
}

func subtract(command storage.RangeCommand) storage.RangeCommand {
	command.Edit, command.Wait, command.Conversion = storage.Subtract, false, storage.PreserveBeforeAcquire
	return command
}

func TestRecordRangeReplacement(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 1)
	steps := []struct {
		command storage.RangeCommand
		want    []storage.RangeCommand
	}{
		{record(storage.RangeShared, 0, 100), []storage.RangeCommand{record(storage.RangeShared, 0, 100)}},
		{record(storage.RangeExclusive, 20, 20), []storage.RangeCommand{record(storage.RangeShared, 0, 20), record(storage.RangeExclusive, 20, 20), record(storage.RangeShared, 40, 60)}},
		{record(storage.RangeShared, 20, 20), []storage.RangeCommand{record(storage.RangeShared, 0, 100)}},
		{subtract(record(storage.RangeShared, 20, 20)), []storage.RangeCommand{record(storage.RangeShared, 0, 20), record(storage.RangeShared, 40, 60)}},
		{record(storage.RangeExclusive, 50, math.MaxInt64-49), []storage.RangeCommand{record(storage.RangeShared, 0, 20), record(storage.RangeShared, 40, 10), record(storage.RangeExclusive, 50, math.MaxInt64-49)}},
		{subtract(record(storage.RangeShared, math.MaxInt64, 1)), []storage.RangeCommand{record(storage.RangeShared, 0, 20), record(storage.RangeShared, 40, 10), record(storage.RangeExclusive, 50, math.MaxInt64-50)}},
	}
	for _, step := range steps {
		result := apply(t, s, 1, o, step.command)
		state := storage.Granted
		if step.command.Edit == storage.Subtract {
			state = storage.Released
		}
		wantState(t, result, state, "")
		var got []storage.RangeCommand
		for _, held := range c.owners[s.key(1, o, storage.DomainRecord)].ranges {
			got = append(got, held.command)
		}
		if !reflect.DeepEqual(got, step.want) {
			t.Fatalf("ranges = %+v; want %+v", got, step.want)
		}
	}
}

func TestDomainsOwnersAndDiagnostics(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	ao, another, bo := owner(t, a, 1, 0), owner(t, a, 1, 0), owner(t, b, 1, 0)
	command := record(storage.RangeExclusive, 3, 6)
	wantState(t, apply(t, a, 1, ao, command), storage.Granted, "")
	wantState(t, apply(t, b, 1, bo, whole(storage.RangeExclusive)), storage.Granted, "")
	wantState(t, apply(t, b, 1, bo, command), storage.Rejected, storage.RangeBlocked)
	for _, test := range []struct {
		s     *Session
		owner storage.UseOwner
		found bool
		diag  storage.OwnerDiagnostic
	}{{a, ao, false, 0}, {a, another, true, storage.OwnerDiagnostic(ao)}, {b, bo, true, 0}} {
		got, err := test.s.GetConflict(background, 1, test.owner, command, ordered)
		if err != nil || got.Found != test.found || got.Owner != test.diag {
			t.Fatalf("conflict = %+v, %v", got, err)
		}
	}
	if err := b.IOHealth(background, 1); err != nil {
		t.Fatalf("I/O health joined advisory conflicts: %v", err)
	}
}

func TestConversionPreservesOrDropsOriginal(t *testing.T) {
	for _, domain := range []storage.ConflictDomain{storage.DomainRecord, storage.DomainWholeFile} {
		t.Run(fmt.Sprint(domain), func(t *testing.T) {
			c := fixture(t, DefaultConfig())
			a, b := session(t, c), session(t, c)
			ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
			shared := whole(storage.RangeShared)
			shared.Domain = domain
			if domain == storage.DomainRecord {
				shared.Conversion = storage.PreserveBeforeAcquire
			}
			wantState(t, apply(t, a, 1, ao, shared), storage.Granted, "")
			wantState(t, apply(t, a, 1, ao, shared), storage.Granted, "")
			if c.ranges != 1 {
				t.Fatal("repeated acquisition increased range count")
			}
			wantState(t, apply(t, b, 1, bo, shared), storage.Granted, "")
			exclusive := shared
			exclusive.Mode = storage.RangeExclusive
			failed := apply(t, a, 1, ao, exclusive)
			wantState(t, failed, storage.Rejected, storage.RangeBlocked)
			next := apply(t, b, 1, bo, exclusive)
			if domain == storage.DomainWholeFile {
				wantState(t, next, storage.Granted, "")
				if len(failed.Effects) != 1 || !failed.Effects[0].Released {
					t.Fatal("conversion receipt lost the original release")
				}
			} else {
				wantState(t, next, storage.Rejected, storage.RangeBlocked)
				if len(failed.Effects) != 0 {
					t.Fatal("preserving conversion reported a release")
				}
			}
		})
	}
}

func TestConversionAllowsEarlierWaiterToAcquire(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	wantState(t, apply(t, a, 1, ao, whole(storage.RangeExclusive)), storage.Granted, "")
	wait := whole(storage.RangeExclusive)
	wait.Wait = true
	pending := apply(t, b, 1, bo, wait)
	wantState(t, pending, storage.Pending, "")
	wantState(t, apply(t, a, 1, ao, whole(storage.RangeShared)), storage.Rejected, storage.RangeBlocked)
	got, err := b.Query(background, 1, bo, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Granted, "")
}

func TestDeadlockGroupsAcrossFilesAndSessionIsolation(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	a1, a2 := owner(t, a, 1, 7), owner(t, a, 2, 7)
	b1, b2 := owner(t, b, 1, 7), owner(t, b, 2, 7)
	lock := record(storage.RangeExclusive, 0, 100)
	wantState(t, apply(t, a, 1, a1, lock), storage.Granted, "")
	wantState(t, apply(t, b, 2, b2, lock), storage.Granted, "")
	lock.Wait = true
	pending := apply(t, a, 2, a2, lock)
	wantState(t, pending, storage.Pending, "")
	wantState(t, apply(t, b, 1, b1, lock), storage.Rejected, storage.RangeDeadlock)
	if err := b.RetireOwner(background, b2); err != nil {
		t.Fatal(err)
	}
	got, err := a.Query(background, 2, a2, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Granted, "")
}

func TestCancellationAndGrantRace(t *testing.T) {
	for range 50 {
		c := fixture(t, DefaultConfig())
		a, b := session(t, c), session(t, c)
		ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
		lock := whole(storage.RangeExclusive)
		wantState(t, apply(t, a, 1, ao, lock), storage.Granted, "")
		lock.Wait = true
		pending := apply(t, b, 1, bo, lock)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelled storage.RangeAttempt
		var cancelErr, dropErr error
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cancelled, cancelErr = b.Cancel(background, 1, bo, pending.Request) }()
		go func() { defer wg.Done(); <-start; dropErr = a.Drop(background, 1, ao, storage.DomainWholeFile) }()
		close(start)
		wg.Wait()
		if cancelErr != nil || dropErr != nil {
			t.Fatalf("cancel=%v drop=%v", cancelErr, dropErr)
		}
		conflict, err := a.GetConflict(background, 1, ao, whole(storage.RangeExclusive), ordered)
		if err != nil {
			t.Fatal(err)
		}
		switch cancelled.State {
		case storage.Cancelled:
			if conflict.Found || cancelled.EverGranted {
				t.Fatal("confirmed cancellation left a grant")
			}
		case storage.Granted:
			if !conflict.Found || !cancelled.EverGranted {
				t.Fatal("winning grant disappeared")
			}
		default:
			t.Fatalf("unexpected cancel state %+v", cancelled)
		}
	}
}

func TestPendingGrantRechecksNativeOrder(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	wantState(t, apply(t, a, 1, ao, whole(storage.RangeExclusive)), storage.Granted, "")
	alive := true
	var gate sync.Mutex
	order := func(ctx context.Context, transition func() error) error {
		if !c.mu.TryLock() {
			t.Error("native order entered with coordinator mutex held")
			return syscall.EIO
		}
		c.mu.Unlock()
		gate.Lock()
		defer gate.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if !alive {
			return syscall.ESTALE
		}
		return transition()
	}
	lock := whole(storage.RangeExclusive)
	lock.Wait = true
	ctx, cancel := context.WithCancel(background)
	pending, err := b.Apply(ctx, 1, bo, []storage.RangeCommand{lock}, requestID(t, b), order)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, pending, storage.Pending, "")
	alive = false
	if err := a.Drop(background, 1, ao, storage.DomainWholeFile); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Query(background, 1, bo, pending.Request); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("stale native reference granted pending lock: %v", err)
	}
	alive = true
	got, err := b.Query(background, 1, bo, pending.Request)
	if err != nil {
		t.Fatalf("new query retained original cancelled context: %v", err)
	}
	wantState(t, got, storage.Granted, "")
}

func TestPendingHistorySurvivesEpochChanges(t *testing.T) {
	c := fixture(t, DefaultConfig())
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	lock := whole(storage.RangeExclusive)
	wantState(t, apply(t, a, 1, ao, lock), storage.Granted, "")
	lock.Wait = true
	pending := apply(t, b, 1, bo, lock)
	for range 3 {
		now = now.Add(2 * b.options.History)
		got, err := b.Query(background, 1, bo, pending.Request)
		if err != nil {
			t.Fatal(err)
		}
		wantState(t, got, storage.Pending, "")
	}
	if err := a.Drop(background, 1, ao, storage.DomainWholeFile); err != nil {
		t.Fatal(err)
	}
	got, err := b.Query(background, 1, bo, pending.Request)
	if err != nil || got.HistoryRemaining != b.options.History {
		t.Fatalf("completion history = %+v %v", got, err)
	}
	wantState(t, got, storage.Granted, "")
	if err := b.Drop(background, 1, bo, storage.DomainWholeFile); err != nil {
		t.Fatal(err)
	}
	replayed, err := b.Apply(background, 1, bo, []storage.RangeCommand{lock}, pending.Request, ordered)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, replayed, storage.Released, "")
	now = now.Add(2 * b.options.History)
	if _, err := b.Apply(background, 1, bo, []storage.RangeCommand{lock}, pending.Request, ordered); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired request was executed again: %v", err)
	}
}

func TestRetirementPreservesProtectionUntilFenceSucceeds(t *testing.T) {
	c := fixture(t, DefaultConfig())
	failed := true
	a, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error {
		if failed {
			return syscall.EIO
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b := session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	wantState(t, apply(t, a, 1, ao, whole(storage.RangeExclusive)), storage.Granted, "")
	if err := a.Retire(background); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed fence = %v", err)
	}
	wantState(t, apply(t, b, 1, bo, whole(storage.RangeExclusive)), storage.Rejected, storage.RangeBlocked)
	if err := a.IOHealth(background, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed retirement accepted I/O: %v", err)
	}
	failed = false
	if err := a.Retire(background); err != nil {
		t.Fatal(err)
	}
	wantState(t, apply(t, b, 1, bo, whole(storage.RangeExclusive)), storage.Granted, "")
}

func TestDropIsScopedToNodeOwnerAndDomain(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s, observer := session(t, c), session(t, c)
	a, b, other := owner(t, s, 1, 1), owner(t, s, 2, 1), owner(t, s, 1, 0)
	w1, w2 := owner(t, observer, 1, 0), owner(t, observer, 2, 0)
	for _, test := range []struct {
		node    uint64
		owner   storage.UseOwner
		command storage.RangeCommand
	}{{1, a, record(storage.RangeExclusive, 0, 10)}, {2, b, record(storage.RangeExclusive, 0, 10)},
		{1, other, record(storage.RangeExclusive, 20, 10)}, {1, a, whole(storage.RangeExclusive)}} {
		wantState(t, apply(t, s, test.node, test.owner, test.command), storage.Granted, "")
	}
	if err := s.Drop(background, 1, a, storage.DomainRecord); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		node    uint64
		owner   storage.UseOwner
		command storage.RangeCommand
		found   bool
	}{{1, w1, record(storage.RangeExclusive, 0, 10), false}, {2, w2, record(storage.RangeExclusive, 0, 10), true},
		{1, w1, record(storage.RangeExclusive, 20, 10), true}, {1, w1, whole(storage.RangeExclusive), true}} {
		got, err := observer.GetConflict(background, test.node, test.owner, test.command, ordered)
		if err != nil || got.Found != test.found {
			t.Fatalf("scoped drop conflict = %+v %v", got, err)
		}
	}
}

func TestConcurrentRetirementFencesBeforeGrantRelease(t *testing.T) {
	c := fixture(t, DefaultConfig())
	entered, release := make(chan struct{}), make(chan struct{})
	a, err := c.NewSession(storage.DefaultFileSessionOptions(), func() error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	b := session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	wantState(t, apply(t, a, 1, ao, whole(storage.RangeExclusive)), storage.Granted, "")
	lock := whole(storage.RangeExclusive)
	lock.Wait = true
	pending := apply(t, b, 1, bo, lock)
	done := make(chan error, 1)
	go func() { done <- a.Retire(background) }()
	<-entered
	ctx, cancel := context.WithCancel(background)
	cancel()
	if err := a.Retire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled concurrent retirement = %v", err)
	}
	if err := a.IOHealth(background, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("retiring I/O = %v", err)
	}
	got, err := b.Query(background, 1, bo, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Pending, "")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err = b.Query(background, 1, bo, pending.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Granted, "")
	if err := a.Retire(background); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedRangeDowngradeWakesEarlierWaiter(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b, waiter := session(t, c), session(t, c), session(t, c)
	ao, bo, wo := owner(t, a, 1, 0), owner(t, b, 1, 0), owner(t, waiter, 1, 0)
	wantState(t, apply(t, a, 1, ao, record(storage.RangeExclusive, 10, 10)), storage.Granted, "")
	wantState(t, apply(t, b, 1, bo, record(storage.RangeExclusive, 0, 10)), storage.Granted, "")
	firstLock := record(storage.RangeShared, 0, 10)
	firstLock.Wait = true
	first := apply(t, waiter, 1, wo, firstLock)
	wantState(t, first, storage.Pending, "")
	secondLock := record(storage.RangeShared, 0, 20)
	secondLock.Wait = true
	wantState(t, apply(t, b, 1, bo, secondLock), storage.Pending, "")
	if err := a.Drop(background, 1, ao, storage.DomainRecord); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	got := waiter.receiptLocked(waiter.actions[first.Request], c.now())
	c.mu.Unlock()
	wantState(t, got, storage.Granted, "")
}

func TestIndependentOwnerDoesNotAliasSameNumberedGroup(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	independent := owner(t, s, 1, 0)
	grouped := owner(t, s, 1, uint64(independent))
	wantState(t, apply(t, s, 1, independent, record(storage.RangeExclusive, 0, 10)), storage.Granted, "")
	wait := record(storage.RangeExclusive, 0, 10)
	wait.Wait = true
	wantState(t, apply(t, s, 1, grouped, wait), storage.Pending, "")
}
