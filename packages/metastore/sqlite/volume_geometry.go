package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.VolumeGeometryAccess = (*Store)(nil)

func (s *Store) CheckVolumeGeometry() error { return s.CheckFileStore() }

func (s *Store) VolumeGeometry(ctx context.Context) (storage.VolumeGeometry, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.VolumeGeometry{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	if s.fileDomain == nil || s.root <= 0 {
		return storage.VolumeGeometry{}, syscall.ESTALE
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return storage.VolumeGeometry{}, err
	}
	var result storage.VolumeGeometry
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		identity, err := s.captureBackendIdentity(ctx, tx)
		if err != nil {
			return err
		}
		result = storage.VolumeGeometry{Volume: identity.Volume, BytesPerSector: uint32(allocationUnit), SectorsPerAllocationUnit: 1}
		return result.CheckVolume(identity.Volume)
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return storage.VolumeGeometry{}, sqlerr.Failure(err)
	}
	return result, nil
}
