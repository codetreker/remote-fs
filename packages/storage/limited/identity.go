package limited

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
	if err := s.healthy(); err != nil {
		return err
	}
	_, err := capability(s.backing, storage.BackendIdentity.CheckBackendIdentity)
	return err
}

func (s *Storage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	inner, err := capability(s.backing, storage.BackendIdentity.CheckBackendIdentity)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	result, err := inner.BackendIdentity(ctx)
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
	result, err := inner.FileSessionIdentity(ctx)
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
