package locking

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type resourceTimerCall struct {
	deadline time.Time
	release  chan struct{}
}

type resourceClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []struct {
		deadline time.Time
		ready    chan time.Time
	}
	calls chan resourceTimerCall
	stop  chan struct{}
}

func newResourceClock() *resourceClock {
	return &resourceClock{now: time.Unix(1_700_000_000, 0), calls: make(chan resourceTimerCall, 4), stop: make(chan struct{})}
}

func (c *resourceClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *resourceClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	ready := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	if d <= 0 {
		ready <- c.now
	} else {
		c.timers = append(c.timers, struct {
			deadline time.Time
			ready    chan time.Time
		}{deadline, ready})
	}
	c.mu.Unlock()
	call := resourceTimerCall{deadline: deadline, release: make(chan struct{})}
	select {
	case c.calls <- call:
	case <-c.stop:
		return ready
	}
	select {
	case <-call.release:
	case <-c.stop:
	}
	return ready
}

func (c *resourceClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, timer := range c.timers {
		if timer.deadline.After(c.now) {
			remaining = append(remaining, timer)
		} else {
			timer.ready <- c.now
		}
	}
	c.timers = remaining
}

func resourceAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("resource transition did not reach its synchronization point")
		var zero T
		return zero
	}
}

type resourceNative struct{}

func (resourceNative) Discover(context.Context, string, func(BackendKey) (bool, error)) error {
	return errors.New("unexpected native discovery")
}
func (resourceNative) Guard(context.Context, BackendKey, func() error) error {
	return errors.New("unexpected native grant admission")
}
func (resourceNative) Forget(context.Context, BackendKey) error { return nil }

func queuedResourceFixture(deadlines ...time.Duration) (*Authority, *resourceRecord, *ownerRecord, *resourceClock) {
	clock := newResourceClock()
	now := clock.Now()
	s := &sessionRecord{id: "session", expires: now.Add(time.Hour), owners: make(map[OwnerID]*ownerRecord)}
	o := &ownerRecord{ref: OwnerRef{Session: s.id, Owner: "owner"}, session: s, actions: make(map[RequestID]*actionRecord), grants: make(map[GrantID]*grantRecord)}
	s.owners[o.ref.Owner] = o
	r := &resourceRecord{id: "resource", key: "file", referenceUntil: now.Add(time.Hour), queue: make([]*actionRecord, 0, len(deadlines)+1), grants: make(map[GrantID]*grantRecord), wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	a := &Authority{options: DefaultOptions(), clock: clock, start: now, native: resourceNative{}, sessions: map[SessionID]*sessionRecord{s.id: s}, owners: map[OwnerID]*ownerRecord{o.ref.Owner: o}, resources: map[ResourceID]*resourceRecord{r.id: r}, keys: map[BackendKey]*resourceRecord{r.key: r}, stop: make(chan struct{}), wake: make(chan struct{}, 1), workerContext: ctx, cancelWorkers: cancel}
	for i, wait := range deadlines {
		id := RequestID(fmt.Sprintf("wait-%d", i))
		action := &actionRecord{owner: o, resource: r, waitUntil: now.Add(wait), receipt: ActionReceipt{Kind: AcquireAction, Request: id, Outcome: Pending, Acquire: &AcquireRequest{Owner: o.ref, Request: id, Mode: Exclusive, TTL: time.Second, Wait: wait}}}
		r.queue = append(r.queue, action)
		o.actions[id] = action
		o.queued++
		a.actions++
		a.queued++
	}
	return a, r, o, clock
}

func TestQueueRemovalClearsBackingReferencesAndPreservesOrder(t *testing.T) {
	for _, position := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(position), func(t *testing.T) {
			a, r, o, clock := queuedResourceFixture(time.Second, time.Second, time.Second)
			defer a.cancelWorkers()
			defer close(clock.stop)
			original := append([]*actionRecord(nil), r.queue...)
			backing := r.queue[:cap(r.queue)]
			a.mu.Lock()
			a.finishLocked(original[position], Cancelled, "")
			a.mu.Unlock()
			if len(r.queue) != 2 || a.queued != 2 || o.queued != 2 {
				t.Fatalf("queue removal counters: len=%d authority=%d owner=%d", len(r.queue), a.queued, o.queued)
			}
			j := 0
			for i, action := range original {
				if i != position {
					if r.queue[j] != action {
						t.Fatal("queue removal changed survivor order")
					}
					j++
				}
			}
			for _, action := range backing[len(r.queue):] {
				if action != nil {
					t.Fatal("removed queue entry still retains an action in its backing array")
				}
			}
			a.mu.Lock()
			a.retireOwnerLocked(o)
			a.mu.Unlock()
			if a.queued != 0 || o.queued != 0 || a.actions != 0 || len(a.owners) != 0 || len(o.session.owners) != 0 {
				t.Fatal("owner retirement left queued or historical admission state")
			}
			for _, action := range backing {
				if action != nil {
					t.Fatal("retired owner's action remains reachable from queue backing array")
				}
			}
		})
	}
}

func TestFencedWorkerExitsWithoutReadmissionOrInventedOutcome(t *testing.T) {
	a, r, o, clock := queuedResourceFixture(20 * time.Millisecond)
	cause := errors.New("native outcome is uncertain")
	t.Cleanup(func() {
		close(clock.stop)
		if err := a.Close(); err != nil && !errors.Is(err, cause) {
			t.Errorf("Close lost fence cause: %v", err)
		}
	})
	blocker := &grantRecord{owner: &ownerRecord{}, resource: r, ref: GrantRef{ID: "blocker"}, mode: Exclusive, state: Active, deadline: clock.Now().Add(time.Second)}
	r.grants[blocker.ref.ID] = blocker
	a.mu.Lock()
	a.startWorkerLocked(r)
	a.mu.Unlock()
	call := resourceAwait(t, clock.calls)
	if !call.deadline.Equal(o.actions["wait-0"].waitUntil) {
		t.Fatal("worker did not wait for the queued action deadline")
	}
	a.Fence(cause)
	close(call.release)
	drained := make(chan struct{})
	go func() { a.wg.Wait(); close(drained) }()
	resourceAwait(t, drained)
	a.mu.Lock()
	if r.worker || o.actions["wait-0"].receipt.Outcome != Pending || a.queued != 1 {
		a.mu.Unlock()
		t.Fatal("fenced worker did not exit with its pending receipt intact")
	}
	a.startWorkerLocked(r)
	readmitted := r.worker
	a.mu.Unlock()
	// A regressed admission starts at most one worker in this fixture; no
	// maintenance loop is running, so this check cannot amplify into a spin loop.
	drained = make(chan struct{})
	go func() { a.wg.Wait(); close(drained) }()
	resourceAwait(t, drained)
	if readmitted {
		t.Fatal("unavailable authority admitted a resource worker")
	}
	if err := a.Check(context.Background()); !errors.Is(err, cause) || CodeOf(err) != Unavailable {
		t.Fatalf("fence lost its authoritative error: %v", err)
	}
}

func TestMaintenanceExpiresEveryQueuedDeadlineWhileFenced(t *testing.T) {
	a, r, o, clock := queuedResourceFixture(20*time.Millisecond, 10*time.Millisecond)
	cause := errors.New("publication outcome is uncertain")
	a.Fence(cause)
	t.Cleanup(func() {
		close(clock.stop)
		if err := a.Close(); !errors.Is(err, cause) {
			t.Errorf("Close lost fence cause: %v", err)
		}
	})
	first, second := r.queue[0], r.queue[1]
	backing := r.queue[:cap(r.queue)]
	a.wg.Add(1)
	go a.maintain()
	call := resourceAwait(t, clock.calls)
	if !call.deadline.Equal(second.waitUntil) {
		t.Fatalf("maintenance timer = %v, want earliest queued deadline %v", call.deadline, second.waitUntil)
	}
	clock.advance(10 * time.Millisecond)
	close(call.release)
	call = resourceAwait(t, clock.calls)
	if !call.deadline.Equal(first.waitUntil) {
		t.Fatalf("maintenance did not retain the later pending deadline: %v", call.deadline)
	}
	a.mu.Lock()
	if r.worker || a.queued != 1 || o.queued != 1 || len(r.queue) != 1 || r.queue[0] != first || first.receipt.Outcome != Pending || second.receipt.Outcome != TimedOut {
		a.mu.Unlock()
		t.Fatal("fenced maintenance did not expire the earlier non-head waiter exactly once")
	}
	if backing[1] != nil {
		a.mu.Unlock()
		t.Fatal("expired non-head waiter remained in the queue backing array")
	}
	a.mu.Unlock()
	if _, err := a.QueryAction(context.Background(), o.ref, second.receipt.Request); CodeOf(err) != Unavailable || !errors.Is(err, cause) {
		t.Fatalf("fenced action query reported a usable outcome: %v", err)
	}
	clock.advance(10 * time.Millisecond)
	close(call.release)
	call = resourceAwait(t, clock.calls)
	a.mu.Lock()
	if r.worker || a.queued != 0 || o.queued != 0 || len(r.queue) != 0 || first.receipt.Outcome != TimedOut || second.receipt.Outcome != TimedOut || a.actions != 2 || len(o.actions) != 2 {
		a.mu.Unlock()
		t.Fatal("fenced expiry lost counters, retained receipts, or worker admission state")
	}
	for _, action := range backing {
		if action != nil {
			a.mu.Unlock()
			t.Fatal("expired waiter still retains owner state through queue storage")
		}
	}
	a.mu.Unlock()
	called := false
	err := a.Publish(context.Background(), Publication{Kind: WriteMutation, Targets: []BackendKey{r.key}}, func() PublicationOutcome { called = true; return PublicationOutcome{Known: true} })
	if called || CodeOf(err) != Unavailable || !errors.Is(err, cause) {
		t.Fatalf("expiry reopened a fenced authority: called=%t err=%v", called, err)
	}
	close(call.release)
}
