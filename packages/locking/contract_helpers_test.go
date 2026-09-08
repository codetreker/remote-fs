package locking_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

type contractClock struct {
	mu        sync.Mutex
	now       time.Time
	timers    []contractTimer
	heldTimer *contractHeldTimer
}

type contractHeldTimer struct {
	deadline   time.Time
	registered chan struct{}
	release    chan struct{}
}

type contractTimer struct {
	when time.Time
	ch   chan time.Time
}

func newContractClock() *contractClock {
	return &contractClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *contractClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *contractClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	if d <= 0 {
		ch <- c.now
	} else {
		c.timers = append(c.timers, contractTimer{when: deadline, ch: ch})
	}
	var held *contractHeldTimer
	if c.heldTimer != nil && c.heldTimer.deadline.Equal(deadline) {
		held = c.heldTimer
		c.heldTimer = nil
		close(held.registered)
	}
	c.mu.Unlock()
	if held != nil {
		<-held.release
	}
	return ch
}

func (c *contractClock) holdTimer(deadline time.Time) (<-chan struct{}, func()) {
	held := &contractHeldTimer{deadline: deadline, registered: make(chan struct{}), release: make(chan struct{})}
	c.mu.Lock()
	c.heldTimer = held
	c.mu.Unlock()
	var once sync.Once
	return held.registered, func() { once.Do(func() { close(held.release) }) }
}

func (c *contractClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	retained := c.timers[:0]
	for _, timer := range c.timers {
		if timer.when.After(c.now) {
			retained = append(retained, timer)
		} else {
			timer.ch <- c.now
		}
	}
	c.timers = retained
}

type contractNode struct {
	mu      sync.Mutex
	live    bool
	pins    int
	forgets int
	forget  func() error
}

type contractNative struct {
	nodes map[locking.BackendKey]*contractNode
}

func newContractNative() *contractNative {
	return &contractNative{nodes: map[locking.BackendKey]*contractNode{
		"a": {live: true}, "b": {live: true}, "c": {live: true},
	}}
}

func (n *contractNative) Discover(ctx context.Context, path string, fn func(locking.BackendKey) (bool, error)) error {
	key := locking.BackendKey(path)
	node := n.nodes[key]
	if node == nil {
		return syscall.ENOENT
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !node.live {
		return syscall.ENOENT
	}
	adopted, err := fn(key)
	if adopted {
		node.pins++
	}
	return err
}

func (n *contractNative) Guard(ctx context.Context, key locking.BackendKey, fn func() error) error {
	node := n.nodes[key]
	if node == nil {
		return syscall.ENOENT
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !node.live {
		return syscall.ENOENT
	}
	return fn()
}

func (n *contractNative) Forget(_ context.Context, key locking.BackendKey) error {
	node := n.nodes[key]
	node.mu.Lock()
	defer node.mu.Unlock()
	node.forgets++
	if node.forget != nil {
		if err := node.forget(); err != nil {
			return err
		}
	}
	node.pins--
	if node.pins < 0 {
		return fmt.Errorf("native pin released twice")
	}
	return nil
}

func (n *contractNative) setForget(key locking.BackendKey, fn func() error) {
	node := n.nodes[key]
	node.mu.Lock()
	defer node.mu.Unlock()
	node.forget = fn
}

func (n *contractNative) forgetCount(key locking.BackendKey) int {
	node := n.nodes[key]
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.forgets
}

type contractPersistence struct {
	mu       sync.Mutex
	max      time.Duration
	start    time.Time
	raise    func(context.Context, time.Duration) error
	observed []time.Duration
}

func (p *contractPersistence) MaxLease(context.Context) (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max, nil
}

func (p *contractPersistence) RaiseMaxLease(ctx context.Context, d time.Duration) error {
	p.mu.Lock()
	p.observed = append(p.observed, d)
	raise := p.raise
	p.mu.Unlock()
	if raise != nil {
		if err := raise(ctx, d); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if d < p.max {
		return fmt.Errorf("lease evidence moved backwards")
	}
	p.max = d
	return nil
}

func (p *contractPersistence) RecoveryStart() time.Time { return p.start }

func (p *contractPersistence) blockRaises() (<-chan time.Duration, func()) {
	entered := make(chan time.Duration, 1)
	resume := make(chan struct{})
	p.mu.Lock()
	p.raise = func(ctx context.Context, d time.Duration) error {
		entered <- d
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Unlock()
	var once sync.Once
	return entered, func() { once.Do(func() { close(resume) }) }
}

type contractHarness struct {
	t        *testing.T
	a        *locking.Authority
	clock    *contractClock
	native   *contractNative
	store    *contractPersistence
	closeErr error
}

func newContractHarness(t *testing.T, change func(*locking.Options, *contractPersistence)) *contractHarness {
	t.Helper()
	c := newContractClock()
	opts := locking.DefaultOptions()
	opts.Clock = c
	p := &contractPersistence{max: opts.MaxLease, start: c.Now().Add(-time.Hour)}
	if change != nil {
		change(&opts, p)
	}
	n := newContractNative()
	a, err := locking.New(context.Background(), opts, n, p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &contractHarness{t: t, a: a, clock: c, native: n, store: p}
	t.Cleanup(func() {
		if err := a.Close(); !errors.Is(err, h.closeErr) {
			t.Errorf("Close: %v", err)
		}
	})
	return h
}

func (h *contractHarness) session() (locking.EnrollmentTicket, locking.Session) {
	h.t.Helper()
	ticket, err := h.a.BeginEnrollment(context.Background())
	if err != nil {
		h.t.Fatalf("BeginEnrollment: %v", err)
	}
	session, err := h.a.OpenSession(context.Background(), ticket)
	if err != nil {
		h.t.Fatalf("OpenSession: %v", err)
	}
	return ticket, session
}

func (h *contractHarness) owner() locking.OwnerRef {
	h.t.Helper()
	_, session := h.session()
	owner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		h.t.Fatalf("CreateOwner: %v", err)
	}
	return owner.Ref
}

func (h *contractHarness) resource(owner locking.OwnerRef, path string) locking.ResourceRef {
	h.t.Helper()
	resource, err := h.a.Resolve(context.Background(), owner, path)
	if err != nil {
		h.t.Fatalf("Resolve %q: %v", path, err)
	}
	return resource
}

func (h *contractHarness) request(owner locking.OwnerRef, id locking.RequestID, path string, mode locking.Mode) locking.AcquireRequest {
	h.t.Helper()
	return locking.AcquireRequest{
		Owner: owner, Request: id, Resource: h.resource(owner, path), Mode: mode, TTL: 10 * time.Second,
	}
}

func (h *contractHarness) grant(req locking.AcquireRequest) locking.ActionResult {
	h.t.Helper()
	result, err := h.a.Acquire(context.Background(), req)
	if err != nil {
		h.t.Fatalf("Acquire: %v", err)
	}
	if !result.Recorded || result.Receipt.Outcome != locking.Granted || result.Grant == nil || result.Grant.State != locking.Active {
		h.t.Fatalf("Acquire result = %+v, want recorded active grant", result)
	}
	return result
}

func (h *contractHarness) release(owner locking.OwnerRef, grant locking.GrantRef) locking.ReleaseResult {
	h.t.Helper()
	result, err := h.a.Release(context.Background(), owner, grant)
	if err != nil {
		h.t.Fatalf("Release: %v", err)
	}
	if result.State != locking.Released {
		h.t.Fatalf("Release state = %s, want released", result.State)
	}
	return result
}

func (h *contractHarness) publish(key locking.BackendKey, scope locking.MutationScope, outcome locking.PublicationOutcome) (bool, error) {
	called := false
	err := h.native.Guard(context.Background(), key, func() error {
		return h.a.Publish(context.Background(), locking.Publication{
			Kind: locking.WriteMutation, Targets: []locking.BackendKey{key}, Scope: scope,
		}, func() locking.PublicationOutcome {
			called = true
			return outcome
		})
	})
	return called, err
}

func contractCode(t *testing.T, err error, want locking.Code) {
	t.Helper()
	if err == nil || locking.CodeOf(err) != want {
		t.Fatalf("error = %v, want code %s", err, want)
	}
}

func contractRejected(t *testing.T, result locking.ActionResult, err error, code locking.Code) {
	t.Helper()
	if !result.Recorded || result.Receipt.Outcome != locking.Rejected || result.Receipt.Code != code {
		t.Fatalf("action = %+v, want recorded rejected %s", result, code)
	}
	contractCode(t, err, code)
}

func (h *contractHarness) awaitAction(owner locking.OwnerRef, request locking.RequestID, want locking.ActionOutcome) locking.ActionResult {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		result, err := h.a.QueryAction(context.Background(), owner, request)
		if result.Receipt.Outcome == want {
			return result
		}
		if err != nil && result.Receipt.Outcome != locking.Pending {
			h.t.Fatalf("QueryAction: %v (result %+v)", err, result)
		}
		if result.Receipt.Outcome != locking.Pending || time.Now().After(deadline) {
			h.t.Fatalf("QueryAction outcome = %s, want %s", result.Receipt.Outcome, want)
		}
		runtime.Gosched()
	}
}

func contractAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not reach its synchronization point")
		var zero T
		return zero
	}
}
