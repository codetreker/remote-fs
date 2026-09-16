package locked

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *fileSession) CheckDirectoryMetadataObservation() error {
	_, err := capability(s.FileSession, storage.DirectoryMetadataObserver.CheckDirectoryMetadataObservation)
	return err
}

func (s *fileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	backend, err := capability(s.FileSession, storage.DirectoryMetadataObserver.CheckDirectoryMetadataObservation)
	if err != nil {
		return storage.DirectoryMetadataObservation{}, result.Fail(err)
	}
	observation, err := backend.ObserveDirectoryMetadata(readContext(ctx), target, options, result)
	if err != nil {
		return storage.DirectoryMetadataObservation{}, result.Fail(err)
	}
	if err := observation.Check(target, options); err != nil {
		return storage.DirectoryMetadataObservation{}, result.Fail(err)
	}
	return observation, nil
}

func (r *referenceCapabilities) CheckReferenceNameObservation() error {
	_, err := capability(r.backend, storage.ReferenceNameObserver.CheckReferenceNameObservation)
	if err == nil {
		_, err = r.ReferenceNodeID()
	}
	return err
}

func (r *referenceCapabilities) ReferenceNodeID() (uint64, error) {
	return storage.ReferenceNodeID(r.backend)
}

func (r *referenceCapabilities) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	backend, err := capability(r.backend, storage.ReferenceNameObserver.CheckReferenceNameObservation)
	if err != nil {
		return storage.NameObservation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.NameObservation{}, err
	}
	observation, err := backend.ObserveName(readContext(ctx), guards)
	if err != nil {
		return storage.NameObservation{}, err
	}
	if err := observation.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	if observation.NodeID != id {
		return storage.NameObservation{}, fmt.Errorf("name observation substituted reference identity: %w", syscall.EIO)
	}
	return observation, nil
}

var (
	_ storage.DirectoryMetadataObserver = (*fileSession)(nil)
	_ storage.ReferenceNameObserver     = (*file)(nil)
	_ storage.ReferenceNameObserver     = (*nodeReference)(nil)
	_ storage.ReferenceIdentity         = (*file)(nil)
	_ storage.ReferenceIdentity         = (*nodeReference)(nil)
)
