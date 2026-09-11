package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileStorage = (*Storage)(nil)

func (s *Storage) CheckFileStorage() error {
	if err := s.healthy(); err != nil {
		return err
	}
	backend, ok := s.backing.(storage.FileStorage)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return backend.CheckFileStorage()
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.CheckFileStorage(); err != nil {
		return nil, err
	}
	// The backend retains this hook for final close and autonomous expiry. Ordinary
	// operations carry independent hooks, so cleanup never reuses a request's charge.
	inner, err := s.backing.(storage.FileStorage).NewFileSession(s.accountingContext(ctx, "retained file"), options)
	if err != nil {
		return nil, s.publicationError(err)
	}
	return &fileSession{FileSession: inner, storage: s}, nil
}

type fileSession struct {
	storage.FileSession
	storage *Storage
}

func (s *fileSession) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (storage.File, error) {
	inner, err := fileMutation(s.storage, ctx, name, func(ctx context.Context) (storage.File, error) {
		return s.FileSession.OpenFile(ctx, name, options)
	})
	if err != nil {
		return nil, err
	}
	return &file{File: inner, storage: s.storage}, nil
}

func (s *fileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	inner, err := fileMutation(s.storage, ctx, "retained file", func(ctx context.Context) (storage.File, error) {
		return s.FileSession.OpenNode(ctx, id, options)
	})
	if err != nil {
		return nil, err
	}
	return &file{File: inner, storage: s.storage}, nil
}

func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	return fileMutation(s.storage, ctx, "retained node", func(ctx context.Context) (storage.Attr, error) {
		return s.FileSession.SetNodeAttr(ctx, id, change)
	})
}

func (s *fileSession) Close(ctx context.Context) error {
	return s.storage.publicationError(s.FileSession.Close(ctx))
}

type file struct {
	storage.File
	storage *Storage
}

func (f *file) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	return fileMutation(f.storage, ctx, "retained file", func(ctx context.Context) (storage.Attr, error) {
		return f.File.WriteAt(ctx, offset, data)
	})
}

func (f *file) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	return fileMutation(f.storage, ctx, "retained file", func(ctx context.Context) (storage.Attr, error) {
		return f.File.Truncate(ctx, size)
	})
}

func (f *file) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return fileMutation(f.storage, ctx, "retained file", func(ctx context.Context) (storage.Attr, error) {
		return f.File.SetAttr(ctx, change)
	})
}

func (f *file) Close(ctx context.Context) error {
	return f.storage.publicationError(f.File.Close(ctx))
}

func fileMutation[T any](s *Storage, ctx context.Context, name string, operation func(context.Context) (T, error)) (T, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		var zero T
		return zero, err
	}
	result, err := operation(s.accountingContext(ctx, name))
	return result, s.publicationError(err)
}
