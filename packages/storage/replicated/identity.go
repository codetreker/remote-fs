package replicated

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.BackendIdentity         = (*Storage)(nil)
	_ storage.BackendIdentity         = (*scopedStorage)(nil)
	_ storage.FileSessionIdentity     = (*fileSession)(nil)
	_ storage.StableReferenceIdentity = (*fileSession)(nil)
	_ storage.OpenMetadataAccess      = (*fileSession)(nil)
	_ storage.InlineCloseSettlement   = (*fileSession)(nil)
)

func (s *Storage) CheckBackendIdentity() error {
	return capabilityCheck(s.remote, storage.BackendIdentity.CheckBackendIdentity)
}

func (s *Storage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	inner, err := optional[storage.BackendIdentity](s.remote)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if err := inner.CheckBackendIdentity(); err != nil {
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

func (s *scopedStorage) CheckBackendIdentity() error { return s.base.CheckBackendIdentity() }

func (s *scopedStorage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	return s.base.BackendIdentity(s.mutationContext(ctx))
}

func (s *fileSession) CheckFileSessionIdentity() error {
	return capabilityCheck(s.remote, storage.FileSessionIdentity.CheckFileSessionIdentity)
}

func (s *fileSession) FileSessionIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, inner storage.FileSessionIdentity) (storage.FileSessionIdentityResult, error) {
		if err := inner.CheckFileSessionIdentity(); err != nil {
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
	})
}

func (s *fileSession) CheckStableReferenceIdentity() error {
	return capabilityCheck(s.remote, storage.StableReferenceIdentity.CheckStableReferenceIdentity)
}

func (s *fileSession) CheckOpenMetadataAccess() error {
	return capabilityCheck(s.remote, storage.OpenMetadataAccess.CheckOpenMetadataAccess)
}

func (s *fileSession) CheckInlineCloseSettlement() error { return syscall.EOPNOTSUPP }
