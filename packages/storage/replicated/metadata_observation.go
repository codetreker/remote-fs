package replicated

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (s *fileSession) CheckOpenMetadataObservation() error {
	return capabilityCheck(s.remote, storage.OpenMetadataObservation.CheckOpenMetadataObservation)
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
func checkReferenceMetadataObservation(remote any) error {
	if err := capabilityCheck(remote, storage.ReferenceMetadataObserver.CheckMetadataObservation); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(remote)
	return err
}
func (f *retainedFile) CheckMetadataObservation() error {
	return checkReferenceMetadataObservation(f.remote)
}
func (r *nodeReference) CheckMetadataObservation() error {
	return checkReferenceMetadataObservation(r.remote)
}
func (f *retainedFile) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	return observeReferenceMetadata(ctx, f.session, f.remote, f.metadataObservations, index)
}
func (r *nodeReference) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	return observeReferenceMetadata(ctx, r.session, r.remote, r.metadataObservations, index)
}
func observeReferenceMetadata(ctx context.Context, session *fileSession, remote any, scopes []storage.MetadataObservationScope, index uint16) (storage.ContentMetadataObservation, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (storage.ContentMetadataObservation, error) {
		if int(index) >= len(scopes) {
			return storage.ContentMetadataObservation{}, syscall.EINVAL
		}
		backing, err := optional[storage.ReferenceMetadataObserver](remote)
		if err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		if err := backing.CheckMetadataObservation(); err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		id, err := storage.ReferenceNodeID(remote)
		if err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		result, err := backing.ObserveMetadata(ctx, index)
		if err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		if err := result.CheckScope(id, scopes[index]); err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		after, err := storage.ReferenceNodeID(remote)
		if err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		if after != id {
			return storage.ContentMetadataObservation{}, syscall.ESTALE
		}
		if err := session.base.usable("file", ""); err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		if err := ctx.Err(); err != nil {
			return storage.ContentMetadataObservation{}, err
		}
		return result.Clone(), nil
	})
}

var (
	_ storage.OpenMetadataObservation   = (*fileSession)(nil)
	_ storage.ReferenceMetadataObserver = (*retainedFile)(nil)
	_ storage.ReferenceMetadataObserver = (*nodeReference)(nil)
)
