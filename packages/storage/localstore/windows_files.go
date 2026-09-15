package localstore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.WindowsStorage = (*Store)(nil)

func (s *Store) CheckWindowsStorage() error { return s.volume.CheckWindowsStorage() }

func (s *Store) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	return s.volume.WindowsState(ctx)
}

func (s *Store) EnableWindows(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	return s.volume.EnableWindows(ctx, action)
}

func (s *Store) QueryWindowsActivation(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	return s.volume.QueryWindowsActivation(ctx, action)
}

func (s *Store) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	return s.volume.NewWindowsSession(ctx, options)
}
