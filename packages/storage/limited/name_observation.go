package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.DirectoryMetadataObserver = (*fileSession)(nil)
	_ storage.ReferenceNameObserver     = (*file)(nil)
	_ storage.ReferenceNameObserver     = (*nodeReference)(nil)
	_ storage.ReferenceIdentity         = (*file)(nil)
	_ storage.ReferenceIdentity         = (*nodeReference)(nil)
)

func (s *fileSession) CheckDirectoryMetadataObservation() error {
	_, err := capability(s.FileSession, storage.DirectoryMetadataObserver.CheckDirectoryMetadataObservation)
	return err
}

func (s *fileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observation storage.DirectoryMetadataObservation, returned error) {
	defer func() {
		if returned != nil {
			observation = storage.DirectoryMetadataObservation{}
			if result != nil {
				result.Fail(returned)
			}
		}
	}()
	backing, err := capability(s.FileSession, storage.DirectoryMetadataObserver.CheckDirectoryMetadataObservation)
	if err != nil {
		return observation, err
	}
	observation, returned = backing.ObserveDirectoryMetadata(ctx, target, options, result)
	if returned == nil {
		returned = observation.Check(target, options)
	}
	return observation, returned
}

func (r *referenceCapabilities) ReferenceNodeID() (uint64, error) {
	return storage.ReferenceNodeID(r.backing)
}

func (r *referenceCapabilities) CheckReferenceNameObservation() error {
	if _, err := capability(r.backing, storage.ReferenceNameObserver.CheckReferenceNameObservation); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	backing, err := capability(r.backing, storage.ReferenceNameObserver.CheckReferenceNameObservation)
	if err != nil {
		return storage.NameObservation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.NameObservation{}, err
	}
	observation, err := backing.ObserveName(ctx, guards)
	if err != nil {
		return storage.NameObservation{}, err
	}
	if err := observation.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	if observation.NodeID != id {
		return storage.NameObservation{}, syscall.EIO
	}
	return observation, nil
}
