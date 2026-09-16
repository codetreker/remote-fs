// Package filebudget bounds content buffers shared by all access paths to a volume.
package filebudget

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	MaxMaterializedBytes int64
	MaxMaterializations  int
	MaxFileBytes         int64
	MaxFileAttempts      int
	FileOperationTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{MaxMaterializedBytes: 2 << 30, MaxMaterializations: 32,
		MaxFileBytes: 1 << 30, MaxFileAttempts: 8, FileOperationTimeout: 30 * time.Second}
}

func (c Config) Check() error {
	if c.MaxMaterializedBytes <= 0 || c.MaxMaterializations <= 0 || c.MaxFileBytes <= 0 || c.MaxFileAttempts <= 0 || c.FileOperationTimeout <= 0 {
		return fmt.Errorf("file content limits must be positive: %w", syscall.EINVAL)
	}
	if c.MaxFileBytes > int64(^uint(0)>>1) {
		return fmt.Errorf("file limit exceeds the host slice size: %w", syscall.EINVAL)
	}
	if c.MaxFileBytes > c.MaxMaterializedBytes/2 {
		return fmt.Errorf("volume content budget must cover current and replacement buffers: %w", syscall.EINVAL)
	}
	return nil
}

type Budget struct {
	mu         sync.Mutex
	config     Config
	bytes      int64
	operations int
}

func New(config Config) (*Budget, error) {
	if err := config.Check(); err != nil {
		return nil, err
	}
	return &Budget{config: config}, nil
}

// AcquireMaterialization admits the aggregate body buffers before allocation.
// Its release is idempotent and remains valid after the request is cancelled.
func (b *Budget) AcquireMaterialization(ctx context.Context, bytes int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bytes < 0 {
		return nil, syscall.EINVAL
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes > b.config.MaxMaterializedBytes {
		return nil, fmt.Errorf("file materialization exceeds volume byte limit: %w", syscall.EFBIG)
	}
	if b.operations >= b.config.MaxMaterializations || bytes > b.config.MaxMaterializedBytes-b.bytes {
		return nil, fmt.Errorf("file materialization capacity is occupied: %w", syscall.EAGAIN)
	}
	b.operations++
	b.bytes += bytes
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.operations--
			b.bytes -= bytes
			b.mu.Unlock()
		})
	}, nil
}

func (b *Budget) FileOperationLimits() (int64, int, time.Duration) {
	return b.config.MaxFileBytes, b.config.MaxFileAttempts, b.config.FileOperationTimeout
}
