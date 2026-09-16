package httprest

import (
	"context"
	"fmt"
	"sync"
	"syscall"
)

// A decoded wait must transfer its retained bytes without waiting while it owns
// a bulk slot: bulk range replacement is what allows active waits to finish.
func (a *bodyAdmission) tryAcquire(ctx context.Context, reservation int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reservation < 0 || reservation > a.maxBytes {
		return nil, fmt.Errorf("admission reservation exceeds its byte bound: %w", syscall.EFBIG)
	}
	a.mu.Lock()
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	if a.operations >= a.maxOperations || reservation > a.maxBytes-a.bytes {
		a.mu.Unlock()
		return nil, syscall.EAGAIN
	}
	a.operations++
	a.bytes += reservation
	a.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { a.mu.Lock(); a.operations--; a.bytes -= reservation; a.notifyLocked(); a.mu.Unlock() })
	}, nil
}
