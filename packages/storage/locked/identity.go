package locked

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.BackendIdentity         = (*Storage)(nil)
	_ storage.FileSessionIdentity     = (*fileSession)(nil)
	_ storage.StableReferenceIdentity = (*fileSession)(nil)
	_ storage.OpenMetadataAccess      = (*fileSession)(nil)
	_ storage.InlineCloseSettlement   = (*fileSession)(nil)
)

func (s *Storage) CheckBackendIdentity() error {
	_, err := capability(s.backend, storage.BackendIdentity.CheckBackendIdentity)
	return err
}

func (s *Storage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	inner, err := capability(s.backend, storage.BackendIdentity.CheckBackendIdentity)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	result, err := inner.BackendIdentity(readContext(ctx))
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if err := result.Check(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	return result, nil
}

func (s *fileSession) CheckFileSessionIdentity() error {
	_, err := capability(s.FileSession, storage.FileSessionIdentity.CheckFileSessionIdentity)
	return err
}

func (s *fileSession) FileSessionIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	inner, err := capability(s.FileSession, storage.FileSessionIdentity.CheckFileSessionIdentity)
	if err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	result, err := inner.FileSessionIdentity(readContext(ctx))
	if err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if err := result.Check(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	return result, nil
}

func (s *fileSession) CheckStableReferenceIdentity() error {
	_, err := capability(s.FileSession, storage.StableReferenceIdentity.CheckStableReferenceIdentity)
	return err
}

func (s *fileSession) CheckOpenMetadataAccess() error {
	_, err := capability(s.FileSession, storage.OpenMetadataAccess.CheckOpenMetadataAccess)
	return err
}

func (s *fileSession) CheckInlineCloseSettlement() error {
	_, err := capability(s.FileSession, storage.InlineCloseSettlement.CheckInlineCloseSettlement)
	return err
}
