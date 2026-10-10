package httprest

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type backendIdentity struct {
	Volume     storage.VolumeID             `json:"volume"`
	Authority  storage.AuthorityIncarnation `json:"authority"`
	RootNodeID uint64                       `json:"rootNodeId"`
}

func backendIdentityOf(value storage.BackendIdentityResult) *backendIdentity {
	return &backendIdentity{Volume: value.Volume, Authority: value.Authority, RootNodeID: value.RootNodeID}
}

func (value backendIdentity) storage() storage.BackendIdentityResult {
	return storage.BackendIdentityResult{Volume: value.Volume, Authority: value.Authority, RootNodeID: value.RootNodeID}
}

type fileSessionIdentity struct {
	Backend      backendIdentity `json:"backend"`
	SessionEpoch string          `json:"sessionEpoch"`
}

func fileSessionIdentityOf(value storage.FileSessionIdentityResult) *fileSessionIdentity {
	return &fileSessionIdentity{Backend: *backendIdentityOf(value.Backend), SessionEpoch: value.SessionEpoch}
}

func (value fileSessionIdentity) storage() storage.FileSessionIdentityResult {
	return storage.FileSessionIdentityResult{Backend: value.Backend.storage(), SessionEpoch: value.SessionEpoch}
}

func (s *Storage) CheckBackendIdentity() error { return nil }

func (s *Storage) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	response, err := s.fileCall(ctx, fileRequest{Op: storage.OpFileBackendIdentity})
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	return response.BackendIdentity.storage(), nil
}

func (s *remoteFileSession) CheckFileSessionIdentity() error {
	return checkFileCapability(s.capabilities.SessionIdentity)
}

func (s *remoteFileSession) FileSessionIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	if err := s.CheckFileSessionIdentity(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.FileSessionIdentityResult{}, err
	}
	return s.identity, nil
}

func (s *remoteFileSession) CheckStableReferenceIdentity() error {
	return checkFileCapability(s.capabilities.StableIdentity)
}

func (s *remoteFileSession) CheckOpenMetadataAccess() error {
	return checkFileCapability(s.capabilities.OpenMetadata)
}

func (s *remoteFileSession) CheckInlineCloseSettlement() error { return syscall.EOPNOTSUPP }

func sessionIdentityOf(ctx context.Context, native storage.FileSession, caps *fileCapabilities, status storage.FileSessionStatus) (*fileSessionIdentity, error) {
	if !caps.SessionIdentity {
		return nil, nil
	}
	identity, err := native.(storage.FileSessionIdentity).FileSessionIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if err := identity.Check(); err != nil {
		return nil, err
	}
	if identity.SessionEpoch != status.Epoch {
		return nil, syscall.EIO
	}
	return fileSessionIdentityOf(identity), nil
}

func (h *Handler) backendIdentity(ctx context.Context) (fileResponse, error) {
	h.files.mu.Lock()
	closed, backend := h.files.closed, h.files.backend
	h.files.mu.Unlock()
	if closed {
		return fileResponse{}, syscall.EIO
	}
	identity, ok := backend.(storage.BackendIdentity)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := identity.CheckBackendIdentity(); err != nil {
		return fileResponse{}, err
	}
	result, err := identity.BackendIdentity(ctx)
	if err != nil {
		return fileResponse{}, err
	}
	if err := result.Check(); err != nil {
		return fileResponse{}, err
	}
	return fileResponse{BackendIdentity: backendIdentityOf(result)}, nil
}

func validateSessionIdentity(response fileResponse) error {
	if response.Capabilities.SessionIdentity != (response.SessionIdentity != nil) {
		return errors.New("file session identity capability and descriptor disagree")
	}
	if response.SessionIdentity == nil {
		return nil
	}
	identity := response.SessionIdentity.storage()
	if err := identity.Check(); err != nil {
		return err
	}
	if identity.SessionEpoch != response.Status.Epoch {
		return errors.New("file session identity does not match status epoch")
	}
	return nil
}

var (
	_ storage.BackendIdentity         = (*Storage)(nil)
	_ storage.FileSessionIdentity     = (*remoteFileSession)(nil)
	_ storage.StableReferenceIdentity = (*remoteFileSession)(nil)
	_ storage.OpenMetadataAccess      = (*remoteFileSession)(nil)
	_ storage.InlineCloseSettlement   = (*remoteFileSession)(nil)
)
