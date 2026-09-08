package advisory

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"time"
)

// AcquireMaterialization reserves aggregate payload bytes and one operation
// before an object body or replacement buffer is allocated. Capacity contention
// returns EAGAIN without queuing; an intrinsically oversized request is EFBIG.
// The returned release is idempotent and owns the reservation until called.
func (c *Coordinator) AcquireMaterialization(ctx context.Context, bytes int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bytes < 0 {
		return nil, syscall.EINVAL
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if bytes > c.config.MaxMaterializedBytes {
		return nil, fmt.Errorf("file materialization exceeds namespace byte limit: %w", syscall.EFBIG)
	}
	if c.materializations >= c.config.MaxMaterializations || bytes > c.config.MaxMaterializedBytes-c.materializedBytes {
		return nil, fmt.Errorf("file materialization capacity is occupied: %w", syscall.EAGAIN)
	}
	c.materializations++
	c.materializedBytes += bytes
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.materializations--
			c.materializedBytes -= bytes
			c.mu.Unlock()
		})
	}, nil
}

// FileOperationLimits bounds one retained-file materialization and retry loop.
func (c *Coordinator) FileOperationLimits() (int64, int, time.Duration) {
	return c.config.MaxFileBytes, c.config.MaxFileAttempts, c.config.FileOperationTimeout
}
