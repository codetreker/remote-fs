package smb

import (
	"context"
	"sync"
)

type cleanupAttempt struct {
	done chan struct{}
	err  error
}

type cleanupGate struct {
	mu      sync.Mutex
	running *cleanupAttempt
}

type contextLock struct {
	once  sync.Once
	token chan struct{}
}

func (l *contextLock) Lock() {
	_ = l.lock(context.Background())
}

func (l *contextLock) Unlock() { l.unlock() }

func (l *contextLock) lock(ctx context.Context) error {
	l.once.Do(func() {
		l.token = make(chan struct{}, 1)
		l.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.token:
		if err := ctx.Err(); err != nil {
			l.token <- struct{}{}
			return err
		}
		return nil
	}
}

func (l *contextLock) unlock() { l.token <- struct{}{} }

// Concurrent callers share one attempt's immutable outcome. A later caller
// may retry a failed cleanup; cancellation of a waiter does not cancel it.
func (g *cleanupGate) run(ctx context.Context, work func() error) error {
	g.mu.Lock()
	if attempt := g.running; attempt != nil {
		g.mu.Unlock()
		select {
		case <-attempt.done:
			return attempt.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	attempt := &cleanupAttempt{done: make(chan struct{})}
	g.running = attempt
	g.mu.Unlock()

	err := work()
	g.mu.Lock()
	attempt.err = err
	close(attempt.done)
	g.running = nil
	g.mu.Unlock()
	return err
}
