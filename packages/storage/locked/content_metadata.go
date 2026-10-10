package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *fileSession) CheckOpenContentMetadata() error {
	_, err := capability(s.FileSession, storage.OpenContentMetadata.CheckOpenContentMetadata)
	return err
}

func (r *referenceCapabilities) CheckContentMetadata() error {
	if _, err := capability(r.backend, storage.ReferenceContentMetadata.CheckContentMetadata); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) ObserveContentMetadata(ctx context.Context, effect uint16) (storage.ContentMetadataObservation, error) {
	backend, err := capability(r.backend, storage.ReferenceContentMetadata.CheckContentMetadata)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	observation, err := backend.ObserveContentMetadata(readContext(ctx), effect)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := observation.Check(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if observation.NodeID != id {
		return storage.ContentMetadataObservation{}, syscall.EIO
	}
	return observation.Clone(), nil
}

var (
	_ storage.OpenContentMetadata      = (*fileSession)(nil)
	_ storage.ReferenceContentMetadata = (*file)(nil)
)
