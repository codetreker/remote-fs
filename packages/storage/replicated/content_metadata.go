package replicated

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *fileSession) CheckOpenContentMetadata() error {
	return capabilityCheck(s.remote, storage.OpenContentMetadata.CheckOpenContentMetadata)
}

func (f *retainedFile) CheckContentMetadata() error {
	return checkReferenceContentMetadata(f.remote)
}

func (f *retainedFile) ObserveContentMetadata(ctx context.Context, effect uint16) (storage.ContentMetadataObservation, error) {
	return observeReferenceContentMetadata(ctx, f.session, f.remote, effect)
}

func checkReferenceContentMetadata(remote any) error {
	if err := capabilityCheck(remote, storage.ReferenceContentMetadata.CheckContentMetadata); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(remote)
	return err
}

func observeReferenceContentMetadata(ctx context.Context, session *fileSession, remote any, effect uint16) (storage.ContentMetadataObservation, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (storage.ContentMetadataObservation, error) {
		backing, err := optional[storage.ReferenceContentMetadata](remote)
		if err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		if err := backing.CheckContentMetadata(); err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		id, err := storage.ReferenceNodeID(remote)
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
	})
}

var (
	_ storage.OpenContentMetadata      = (*fileSession)(nil)
	_ storage.ReferenceContentMetadata = (*retainedFile)(nil)
)
