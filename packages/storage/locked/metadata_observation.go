package locked

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (s *fileSession) CheckOpenMetadataObservation() error {
	_, err := capability(s.FileSession, storage.OpenMetadataObservation.CheckOpenMetadataObservation)
	return err
}

func (s *fileSession) checkMetadataObservationEnrollment(permission storage.MetadataPermissions, scopes []storage.MetadataObservationScope) error {
	if err := storage.CheckMetadataObservationScopes(scopes); err != nil {
		return err
	}
	if len(scopes) == 0 {
		return nil
	}
	if permission&storage.WriteMetadata == 0 {
		return syscall.EINVAL
	}
	return s.CheckOpenMetadataObservation()
}

func (r *referenceCapabilities) CheckMetadataObservation() error {
	if _, err := capability(r.backend, storage.ReferenceMetadataObserver.CheckMetadataObservation); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if int(index) >= len(r.metadataObservations) {
		return storage.ContentMetadataObservation{}, syscall.EINVAL
	}
	scope := r.metadataObservations[index]
	inner, err := capability(r.backend, storage.ReferenceMetadataObserver.CheckMetadataObservation)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	result, err := inner.ObserveMetadata(readContext(ctx), index)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := result.CheckScope(id, scope); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	after, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if after != id {
		return storage.ContentMetadataObservation{}, syscall.ESTALE
	}
	if err := ctx.Err(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	return result.Clone(), nil
}

var (
	_ storage.OpenMetadataObservation   = (*fileSession)(nil)
	_ storage.ReferenceMetadataObserver = (*file)(nil)
	_ storage.ReferenceMetadataObserver = (*nodeReference)(nil)
)
