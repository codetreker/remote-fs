package replicated

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (s *fileSession) CheckVolumeGeometry() error {
	if err := s.CheckFileSessionIdentity(); err != nil {
		return err
	}
	return capabilityCheck(s.remote, storage.VolumeGeometryAccess.CheckVolumeGeometry)
}

func (s *fileSession) VolumeGeometry(ctx context.Context) (storage.VolumeGeometry, error) {
	return fileCall(ctx, s, true, func(ctx context.Context) (storage.VolumeGeometry, error) {
		if err := s.CheckVolumeGeometry(); err != nil {
			return storage.VolumeGeometry{}, err
		}
		before, err := s.FileSessionIdentity(ctx)
		if err != nil {
			return storage.VolumeGeometry{}, err
		}
		backing, err := optional[storage.VolumeGeometryAccess](s.remote)
		if err != nil {
			return storage.VolumeGeometry{}, err
		}
		result, err := backing.VolumeGeometry(ctx)
		if err != nil {
			return storage.VolumeGeometry{}, err
		}
		if err := result.CheckVolume(before.Backend.Volume); err != nil {
			return storage.VolumeGeometry{}, err
		}
		after, err := s.FileSessionIdentity(ctx)
		if err != nil {
			return storage.VolumeGeometry{}, err
		}
		if before != after {
			return storage.VolumeGeometry{}, syscall.ESTALE
		}
		if err := s.base.usable("file", ""); err != nil {
			return storage.VolumeGeometry{}, err
		}
		if err := ctx.Err(); err != nil {
			return storage.VolumeGeometry{}, err
		}
		return result, nil
	})
}

var _ storage.VolumeGeometryAccess = (*fileSession)(nil)
