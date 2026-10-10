package localstore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.BackendIdentity = (*Store)(nil)

func (s *Store) CheckBackendIdentity() error { return s.volume.CheckBackendIdentity() }

func (s *Store) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	if err := s.CheckBackendIdentity(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	result, err := s.volume.BackendIdentity(ctx)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if err := result.Check(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	return result, nil
}
