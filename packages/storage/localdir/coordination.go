package localdir

import (
	"context"
	"sync"

	"github.com/codetreker/remote-fs/packages/locking"
)

type pathIntent struct {
	path  string
	write bool
}

type pathGate struct {
	readers int
	writer  bool
}

type pathWait struct{ paths map[string]bool }

type nativeRuntime struct {
	limits Limits

	lifeMu      sync.Mutex
	active      int
	waiters     int
	stopped     bool
	lifeChanged chan struct{}
	context     context.Context
	cancel      context.CancelFunc
	drained     sync.WaitGroup

	gateMu      sync.Mutex
	gates       map[string]pathGate
	pending     []*pathWait
	gateChanged chan struct{}

	pinsMu     sync.Mutex
	pins       map[locking.BackendKey]*nativeTarget
	physical   map[nativeIdentity]locking.BackendKey
	sequence   uint64
	pinsClosed bool
	pinsErr    error
}

func newNativeRuntime(limits Limits) *nativeRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	return &nativeRuntime{
		limits: limits, context: ctx, cancel: cancel,
		lifeChanged: make(chan struct{}), gateChanged: make(chan struct{}),
		gates: make(map[string]pathGate), pins: make(map[locking.BackendKey]*nativeTarget),
		physical: make(map[nativeIdentity]locking.BackendKey),
	}
}

func nativeClosed() error {
	return locking.Wrap(locking.Unavailable, "local directory is closed", nil)
}

func (s *Storage) begin(ctx context.Context) (context.Context, func(), error) {
	ctx, finish, err := s.runtime.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := s.health(ctx); err != nil {
		finish()
		return nil, nil, err
	}
	return ctx, finish, nil
}

func (r *nativeRuntime) begin(parent context.Context) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	r.lifeMu.Lock()
	if r.stopped {
		r.lifeMu.Unlock()
		return nil, nil, nativeClosed()
	}
	waiting := r.active >= r.limits.MaxOperations
	if waiting && r.waiters >= r.limits.MaxWaiters {
		r.lifeMu.Unlock()
		return nil, nil, locking.Wrap(locking.Capacity, "local directory operation capacity exhausted", nil)
	}
	r.drained.Add(1)
	ctx, cancel := context.WithCancel(parent)
	stopCancel := context.AfterFunc(r.context, cancel)
	cleanup := func() { stopCancel(); cancel(); r.drained.Done() }
	if waiting {
		r.waiters++
	}
	for waiting && !r.stopped && ctx.Err() == nil && r.active >= r.limits.MaxOperations {
		changed := r.lifeChanged
		r.lifeMu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.lifeMu.Lock()
	}
	if waiting {
		r.waiters--
	}
	if r.stopped || ctx.Err() != nil {
		err := ctx.Err()
		if r.stopped {
			err = nativeClosed()
		}
		r.lifeMu.Unlock()
		cleanup()
		return nil, nil, err
	}
	r.active++
	r.lifeMu.Unlock()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			r.lifeMu.Lock()
			r.active--
			close(r.lifeChanged)
			r.lifeChanged = make(chan struct{})
			r.lifeMu.Unlock()
			cleanup()
		})
	}
	return ctx, finish, nil
}

func (r *nativeRuntime) stop() {
	r.lifeMu.Lock()
	if !r.stopped {
		r.stopped = true
		r.cancel()
		close(r.lifeChanged)
		r.lifeChanged = make(chan struct{})
	}
	r.lifeMu.Unlock()
}

func (r *nativeRuntime) wait() { r.drained.Wait() }

// Ancestors are shared claims, including the root. A directory's exclusive claim
// therefore orders all descendants without serializing siblings.
func (r *nativeRuntime) expandPaths(intents []pathIntent) (map[string]bool, error) {
	if len(intents) > 2 {
		return nil, locking.Wrap(locking.Invalid, "local directory operation has too many paths", nil)
	}
	paths := make(map[string]bool)
	for _, intent := range intents {
		if len(intent.path) > r.limits.MaxPathBytes {
			return nil, locking.Wrap(locking.Capacity, "local directory path exceeds its byte limit", nil)
		}
		paths[""] = paths[""]
		for i := range len(intent.path) {
			if intent.path[i] == '/' {
				ancestor := intent.path[:i]
				paths[ancestor] = paths[ancestor]
			}
		}
		paths[intent.path] = paths[intent.path] || intent.write
	}
	return paths, nil
}

func (s *Storage) withPaths(ctx context.Context, intents []pathIntent, fn func() error) error {
	paths, err := s.runtime.expandPaths(intents)
	if err != nil {
		return err
	}
	finish, err := s.runtime.enterPaths(ctx, paths)
	if err != nil {
		return err
	}
	defer finish()
	if err := s.health(ctx); err != nil {
		return err
	}
	return fn()
}

func conflictingPaths(a, b map[string]bool) bool {
	for path, write := range a {
		if other, exists := b[path]; exists && (write || other) {
			return true
		}
	}
	return false
}

func (r *nativeRuntime) pathsAvailable(paths map[string]bool, waiter *pathWait) bool {
	for path, write := range paths {
		gate := r.gates[path]
		if gate.writer || write && gate.readers > 0 {
			return false
		}
	}
	for _, earlier := range r.pending {
		if earlier == waiter {
			break
		}
		if conflictingPaths(paths, earlier.paths) {
			return false
		}
	}
	return true
}

func (r *nativeRuntime) removeWaiter(waiter *pathWait) {
	if waiter == nil {
		return
	}
	for i, pending := range r.pending {
		if pending == waiter {
			copy(r.pending[i:], r.pending[i+1:])
			r.pending[len(r.pending)-1] = nil
			r.pending = r.pending[:len(r.pending)-1]
			return
		}
	}
}

// Gate acquisition is atomic across every path. The mutex protects bookkeeping;
// no native operation, caller callback, or wait executes while it is held.
func (r *nativeRuntime) enterPaths(ctx context.Context, paths map[string]bool) (func(), error) {
	r.gateMu.Lock()
	var waiter *pathWait
	for {
		if err := ctx.Err(); err != nil {
			r.removeWaiter(waiter)
			close(r.gateChanged)
			r.gateChanged = make(chan struct{})
			r.gateMu.Unlock()
			return nil, err
		}
		if r.pathsAvailable(paths, waiter) {
			r.removeWaiter(waiter)
			for path, write := range paths {
				gate := r.gates[path]
				if write {
					gate.writer = true
				} else {
					gate.readers++
				}
				r.gates[path] = gate
			}
			r.gateMu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					r.gateMu.Lock()
					for path, write := range paths {
						gate := r.gates[path]
						if write {
							gate.writer = false
						} else {
							gate.readers--
						}
						if !gate.writer && gate.readers == 0 {
							delete(r.gates, path)
						} else {
							r.gates[path] = gate
						}
					}
					close(r.gateChanged)
					r.gateChanged = make(chan struct{})
					r.gateMu.Unlock()
				})
			}, nil
		}
		if waiter == nil {
			waiter = &pathWait{paths: paths}
			r.pending = append(r.pending, waiter)
		}
		changed := r.gateChanged
		r.gateMu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.gateMu.Lock()
	}
}
