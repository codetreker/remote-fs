package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *fileSession) CheckOpenContentMetadata() error {
	if err := s.storage.healthy(); err != nil {
		return err
	}
	_, err := capability(s.FileSession, storage.OpenContentMetadata.CheckOpenContentMetadata)
	return err
}

func (r *referenceCapabilities) CheckContentMetadata() error {
	if err := r.storage.healthy(); err != nil {
		return err
	}
	if _, err := capability(r.backing, storage.ReferenceContentMetadata.CheckContentMetadata); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) ObserveContentMetadata(ctx context.Context, effect uint16) (storage.ContentMetadataObservation, error) {
	r.storage.gate.RLock()
	defer r.storage.gate.RUnlock()
	if err := r.storage.healthy(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	backing, err := capability(r.backing, storage.ReferenceContentMetadata.CheckContentMetadata)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	observation, err := backing.ObserveContentMetadata(ctx, effect)
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
