package limited

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (s *fileSession) CheckVolumeGeometry() error {
	if err := s.storage.healthy(); err != nil {
		return err
	}
	if err := s.CheckFileSessionIdentity(); err != nil {
		return err
	}
	_, err := capability(s.FileSession, storage.VolumeGeometryAccess.CheckVolumeGeometry)
	return err
}

func (s *fileSession) VolumeGeometry(ctx context.Context) (storage.VolumeGeometry, error) {
	s.storage.gate.RLock()
	defer s.storage.gate.RUnlock()
	if err := s.storage.healthy(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	if err := s.CheckVolumeGeometry(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	before, err := s.FileSessionIdentity(ctx)
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	inner := s.FileSession.(storage.VolumeGeometryAccess)
	result, err := inner.VolumeGeometry(ctx)
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
	if after != before {
		return storage.VolumeGeometry{}, syscall.ESTALE
	}
	if err := s.storage.healthy(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	return result, nil
}

var _ storage.VolumeGeometryAccess = (*fileSession)(nil)
