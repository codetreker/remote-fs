package service

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
)

func (s *Service) processVolume(ctx context.Context, volume management.Volume) (management.Volume, error) {
	var state management.VolumeState
	var storageErr error
	switch volume.State {
	case management.VolumeCreating:
		storageErr = s.runtimes.Initialize(ctx, volume)
		state = management.VolumeActive
	case management.VolumeDeleting:
		storageErr = s.runtimes.Delete(ctx, volume)
		state = management.VolumeDeleted
	default:
		return volume, syscall.EINVAL
	}
	errorCode := ""
	if storageErr != nil {
		state = management.VolumeFailed
		errorCode = "storage_unavailable"
	}
	changed, err := s.maintenance.store.SetVolumeState(ctx, volume.OwnerUserID, volume.ID, volume.ManagementRevision, state, errorCode)
	if err != nil {
		return volume, errors.Join(storageErr, err)
	}
	return changed, storageErr
}
