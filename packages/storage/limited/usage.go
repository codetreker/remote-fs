package limited

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type usageSource interface {
	Usage(context.Context) (int64, error)
}

func measureUsage(ctx context.Context, backing storage.BoundedStorage, limits MeasurementLimits) (int64, error) {
	if source, ok := backing.(usageSource); ok {
		count, err := source.Usage(ctx)
		if err == nil {
			if count < 0 {
				return 0, fmt.Errorf("authoritative usage reports %d bytes: %w", count, syscall.EIO)
			}
			return count, nil
		}
		if err != syscall.ENOSYS && err != syscall.EOPNOTSUPP {
			return 0, err
		}
	}
	if files, ok := backing.(storage.FileStorage); ok {
		err := files.CheckFileStorage()
		if err == nil {
			return 0, fmt.Errorf("retained files require authoritative usage measurement: %w", syscall.EOPNOTSUPP)
		}
		if err != syscall.EOPNOTSUPP {
			return 0, err
		}
	}
	return measure(ctx, backing, limits)
}

// Usage returns the namespace's committed byte count, including detached retained
// files. Pending object staging consumes its backend's separate bounded budget.
func (s *Storage) Usage(ctx context.Context) (int64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return 0, err
	}
	return measureUsage(ctx, s.backing, s.measurement)
}
