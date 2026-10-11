package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.OpenMetadataObservation   = (*fileSession)(nil)
	_ storage.ReferenceMetadataObserver = (*openFile)(nil)
	_ storage.ReferenceMetadataObserver = (*nodeReference)(nil)
)

func (fs *fileSession) CheckOpenMetadataObservation() error {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return err
	}
	if err := fs.CheckNodeReferences(); err != nil {
		return err
	}
	capability, ok := fs.native.(storage.OpenMetadataObservation)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckOpenMetadataObservation()
}

func metadataObservationCapability(native any) (storage.ReferenceMetadataObserver, uint64, error) {
	capability, ok := native.(storage.ReferenceMetadataObserver)
	if !ok {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if err := capability.CheckMetadataObservation(); err != nil {
		return nil, 0, err
	}
	id, err := storage.ReferenceNodeID(native)
	return capability, id, err
}

func (f *openFile) CheckMetadataObservation() error {
	_, _, err := metadataObservationCapability(f.native)
	return err
}

func (r *nodeReference) CheckMetadataObservation() error {
	_, _, err := metadataObservationCapability(r.native)
	return err
}

func observationScope(permission storage.MetadataPermissions, scopes []storage.MetadataObservationScope, index uint16) (storage.MetadataObservationScope, error) {
	if permission&storage.WriteMetadata == 0 {
		return storage.MetadataObservationScope{}, syscall.EBADF
	}
	if int(index) >= len(scopes) {
		return storage.MetadataObservationScope{}, syscall.EINVAL
	}
	scope := scopes[index]
	return scope, scope.Check()
}

func (f *openFile) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	scope, err := observationScope(f.metadata, f.metadataObservations, index)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	capability, id, err := metadataObservationCapability(f.native)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	defer done()
	return observeMetadata(ctx, f.session, capability, id, scope, index, &f.active)
}

func (r *nodeReference) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	scope, err := observationScope(r.metadata, r.metadataObservations, index)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	capability, id, err := metadataObservationCapability(r.native)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	defer done()
	return observeMetadata(ctx, r.session, capability, id, scope, index, &r.active)
}

func observeMetadata(ctx context.Context, session *fileSession, capability storage.ReferenceMetadataObserver, id uint64, scope storage.MetadataObservationScope, index uint16, active *bool) (storage.ContentMetadataObservation, error) {
	ctx = metastore.WithReferenceSession(ctx, session.locks)
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	result, err := capability.ObserveMetadata(ctx, index)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := result.CheckScope(id, scope); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := checkObservationIdentity(capability, id); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := checkObservedReference(ctx, session, active); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	return result.Clone(), nil
}
