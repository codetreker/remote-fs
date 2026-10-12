package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.BackendIdentity         = (*Storage)(nil)
	_ storage.FileSessionIdentity     = (*fileSession)(nil)
	_ storage.StableReferenceIdentity = (*fileSession)(nil)
	_ storage.OpenMetadataAccess      = (*fileSession)(nil)
)

func (s *Storage) CheckBackendIdentity() error {
	if err := s.CheckFileStorage(); err != nil {
		return err
	}
	native, ok := s.meta.(storage.BackendIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckBackendIdentity()
}

func (s *Storage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	if err := s.CheckBackendIdentity(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	defer s.endOperation()
	result, err := s.meta.(storage.BackendIdentity).BackendIdentity(ctx)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if err := result.Check(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	return result, nil
}

func (fs *fileSession) CheckFileSessionIdentity() error {
	if err := fs.storage.CheckBackendIdentity(); err != nil {
		return err
	}
	if !fs.hasBackendIdentity {
		return syscall.EOPNOTSUPP
	}
	return fs.backendIdentity.Check()
}

func (fs *fileSession) FileSessionIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	if err := fs.CheckFileSessionIdentity(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.beginControl(ctx)
	if err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	defer done()
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
	result := storage.FileSessionIdentityResult{Backend: fs.backendIdentity, SessionEpoch: fs.epoch}
	if err := result.Check(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if result.SessionEpoch != status.Epoch {
		return storage.FileSessionIdentityResult{}, syscall.EIO
	}
	return result, nil
}

func (fs *fileSession) CheckStableReferenceIdentity() error {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return err
	}
	if err := fs.CheckNodeReferences(); err != nil {
		return err
	}
	native, ok := fs.native.(storage.StableReferenceIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckStableReferenceIdentity()
}

func (fs *fileSession) CheckOpenMetadataAccess() error {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return err
	}
	native, ok := fs.native.(storage.OpenMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckOpenMetadataAccess()
}
