package sqlite

import (
	"context"
	"database/sql"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

const allocationUnit int64 = 4096

var _ storage.AllocationReporting = (*Store)(nil)

func (s *Store) CheckAllocationReporting() error { return nil }

func (s *Store) AllocationUnitBytes(ctx context.Context) (uint64, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return 0, err
	}
	defer s.coordinator.commit.release()
	if s.files == nil || s.root <= 0 {
		return 0, syscall.ESTALE
	}
	exclusive := s.leaseOwner != nil && s.leaseOwner.Exclusive()
	if exclusive {
		if err := s.checkFileOwnership(); err != nil {
			return 0, err
		}
		if s.fileDomain == nil {
			return 0, syscall.ESTALE
		}
	} else if err := s.coordinator.healthy(); err != nil {
		return 0, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return 0, err
	}
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if exclusive {
			_, err := s.captureBackendIdentity(ctx, tx)
			return err
		}
		_, err := s.captureVolumeIdentity(ctx, tx)
		return err
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return 0, sqlerr.Failure(err)
	}
	return uint64(allocationUnit), nil
}

func allocatedSize(kind storage.NodeKind, size int64) (int64, error) {
	if size < 0 {
		return 0, syscall.EINVAL
	}
	if kind != storage.NodeRegular || size == 0 {
		return 0, nil
	}
	if size > math.MaxInt64-(allocationUnit-1) {
		return 0, syscall.EOVERFLOW
	}
	return ((size + allocationUnit - 1) / allocationUnit) * allocationUnit, nil
}
