package localstore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileStorage = (*Store)(nil)

func (s *Store) CheckFileStorage() error { return s.volume.CheckFileStorage() }

func (s *Store) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	return s.volume.NewFileSession(ctx, options)
}

// Usage includes retained files after their names have been removed.
func (s *Store) Usage(ctx context.Context) (int64, error) {
	return s.volume.Usage(ctx)
}
