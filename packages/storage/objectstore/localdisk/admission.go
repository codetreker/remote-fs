package localdisk

import (
	"context"
	"fmt"
	"syscall"
)

// admission keeps both limits under one lock so an operation never consumes one resource
// while waiting indefinitely for the other. The replaced channel is a context-aware
// condition variable: every state change closes the channel all current waiters selected.
type admission struct {
	maxOperations int
	maxBytes      int64

	mu         chan struct{}
	control    chan struct{}
	changed    chan struct{}
	operations int
	controls   int
	bytes      int64
	closing    bool
}

func newAdmission(maxOperations int, maxBytes int64) *admission {
	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	control := make(chan struct{}, 1)
	control <- struct{}{}
	return &admission{
		maxOperations: maxOperations,
		maxBytes:      maxBytes,
		mu:            mu,
		control:       control,
		changed:       make(chan struct{}),
	}
}

type controlTicket struct {
	gate *admission
	once bool
}

func (a *admission) acquireControl(ctx context.Context) (*controlTicket, error) {
	if err := take(ctx, a.control); err != nil {
		return nil, err
	}
	if err := take(ctx, a.mu); err != nil {
		a.control <- struct{}{}
		return nil, err
	}
	if a.closing {
		a.unlock()
		a.control <- struct{}{}
		return nil, fmt.Errorf("the object store is closed: %w", syscall.EIO)
	}
	a.controls++
	a.unlock()
	return &controlTicket{gate: a}, nil
}

func (t *controlTicket) release() {
	if t == nil || t.once {
		return
	}
	<-t.gate.mu
	t.once = true
	t.gate.controls--
	t.gate.notifyLocked()
	t.gate.unlock()
	t.gate.control <- struct{}{}
}

type ticket struct {
	gate  *admission
	bytes int64
	once  bool
}

func (a *admission) acquire(ctx context.Context, bytes int64) (*ticket, error) {
	for {
		if err := take(ctx, a.mu); err != nil {
			return nil, err
		}
		if a.closing {
			a.unlock()
			return nil, fmt.Errorf("the object store is closed: %w", syscall.EIO)
		}
		if bytes > a.maxBytes {
			a.unlock()
			return nil, fmt.Errorf("one operation requires %d in-flight bytes, above the %d-byte limit: %w",
				bytes, a.maxBytes, syscall.EFBIG)
		}
		if a.operations < a.maxOperations && bytes <= a.maxBytes-a.bytes {
			a.operations++
			a.bytes += bytes
			a.unlock()
			return &ticket{gate: a, bytes: bytes}, nil
		}
		changed := a.changed
		a.unlock()
		select {
		case <-ctx.Done():
			return nil, contextFailure("admit operation", "", ctx.Err())
		case <-changed:
		}
	}
}

// addBytes is used by Get after the envelope reveals the payload size. An operation that
// entered before Close must be allowed to finish acquiring its own bytes; otherwise Close
// would wait for an operation that it itself prevented from reaching its release.
func (t *ticket) addBytes(ctx context.Context, bytes int64) error {
	if bytes == 0 {
		return nil
	}
	for {
		if err := take(ctx, t.gate.mu); err != nil {
			return err
		}
		if bytes <= t.gate.maxBytes-t.gate.bytes {
			t.gate.bytes += bytes
			t.bytes += bytes
			t.gate.unlock()
			return nil
		}
		changed := t.gate.changed
		t.gate.unlock()
		select {
		case <-ctx.Done():
			return contextFailure("admit object bytes", "", ctx.Err())
		case <-changed:
		}
	}
}

func (t *ticket) release() {
	if t == nil || t.once {
		return
	}
	<-t.gate.mu
	t.once = true
	t.gate.operations--
	t.gate.bytes -= t.bytes
	t.gate.notifyLocked()
	t.gate.unlock()
}

func (a *admission) closeAndWait() {
	<-a.mu
	a.closing = true
	a.notifyLocked()
	for a.operations != 0 || a.controls != 0 {
		changed := a.changed
		a.unlock()
		<-changed
		<-a.mu
	}
	a.unlock()
}

func (a *admission) snapshot() (operations int, bytes int64, closing bool) {
	<-a.mu
	defer a.unlock()
	return a.operations, a.bytes, a.closing
}

func (a *admission) notifyLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

func (a *admission) unlock() { a.mu <- struct{}{} }

func take(ctx context.Context, lock chan struct{}) error {
	select {
	case <-ctx.Done():
		return contextFailure("wait for an object-store resource", "", ctx.Err())
	case <-lock:
		if err := ctx.Err(); err != nil {
			lock <- struct{}{}
			return contextFailure("wait for an object-store resource", "", err)
		}
		return nil
	}
}
