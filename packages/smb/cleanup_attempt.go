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

// Concurrent callers share one attempt's immutable outcome. Only a later call
// can retry failure; cancellation of a waiter does not retire the attempt.
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
