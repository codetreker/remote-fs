package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.ReferenceInformationAccess = (*openFile)(nil)
	_ storage.ReferenceInformationAccess = (*nodeReference)(nil)
	_ storage.VolumeGeometryAccess       = (*fileSession)(nil)
)

func informationCapability(native any) (storage.ReferenceInformationAccess, uint64, error) {
	capability, ok := native.(storage.ReferenceInformationAccess)
	if !ok {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if err := capability.CheckReferenceInformation(); err != nil {
		return nil, 0, err
	}
	id, err := storage.ReferenceNodeID(native)
	return capability, id, err
}

func (f *openFile) CheckReferenceInformation() error {
	_, _, err := informationCapability(f.native)
	return err
}

func (r *nodeReference) CheckReferenceInformation() error {
	_, _, err := informationCapability(r.native)
	return err
}

func (f *openFile) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	capability, id, err := informationCapability(f.native)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	defer done()
	return observeReferenceInformation(ctx, f.session, capability, id, &f.active)
}

func (r *nodeReference) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	capability, id, err := informationCapability(r.native)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	defer done()
	return observeReferenceInformation(ctx, r.session, capability, id, &r.active)
}

func observeReferenceInformation(ctx context.Context, session *fileSession, capability storage.ReferenceInformationAccess, id uint64, active *bool) (storage.ReferenceInformation, error) {
	ctx = metastore.WithReferenceSession(ctx, session.locks)
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.ReferenceInformation{}, err
	}
	result, err := capability.Information(ctx)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := result.Check(id); err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := checkObservationIdentity(capability, id); err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := checkObservedReference(ctx, session, active); err != nil {
		return storage.ReferenceInformation{}, err
	}
	return result, nil
}

func checkObservationIdentity(reference any, expected uint64) error {
	id, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return err
	}
	if id != expected {
		return syscall.EIO
	}
	return nil
}

func checkObservedReference(ctx context.Context, session *fileSession, active *bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.publicationAllowed(); err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if !*active {
		return syscall.EBADF
	}
	return nil
}

func (fs *fileSession) CheckVolumeGeometry() error {
	if err := fs.CheckFileSessionIdentity(); err != nil {
		return err
	}
	capability, ok := fs.native.(storage.VolumeGeometryAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckVolumeGeometry()
}

func (fs *fileSession) VolumeGeometry(ctx context.Context) (storage.VolumeGeometry, error) {
	if err := fs.CheckVolumeGeometry(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	defer done()
	before, err := fs.currentGeometryIdentity(ctx)
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	geometry, err := fs.native.(storage.VolumeGeometryAccess).VolumeGeometry(ctx)
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	if err := geometry.CheckVolume(before.Backend.Volume); err != nil {
		return storage.VolumeGeometry{}, err
	}
	after, err := fs.currentGeometryIdentity(ctx)
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	if after != before {
		return storage.VolumeGeometry{}, syscall.ESTALE
	}
	if err := ctx.Err(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	return geometry, nil
}

func (fs *fileSession) currentGeometryIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	backend, err := fs.native.(storage.BackendIdentity).BackendIdentity(ctx)
	if err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if err := backend.Check(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if backend != fs.backendIdentity {
		return storage.FileSessionIdentityResult{}, syscall.ESTALE
	}
	status, err := fs.status(ctx)
	if err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	result := storage.FileSessionIdentityResult{Backend: backend, SessionEpoch: fs.epoch}
	if err := result.Check(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if result.SessionEpoch != status.Epoch {
		return storage.FileSessionIdentityResult{}, syscall.EIO
	}
	return result, nil
}
