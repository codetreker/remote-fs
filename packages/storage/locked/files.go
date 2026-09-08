package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileStorage = (*Storage)(nil)

func (s *Storage) CheckFileStorage() error {
	backend, ok := s.backend.(storage.FileStorage)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return backend.CheckFileStorage()
}

func (s *Storage) CheckPublicationAccounting() error {
	backend, ok := s.backend.(interface{ CheckPublicationAccounting() error })
	if !ok {
		return syscall.ENOSYS
	}
	return backend.CheckPublicationAccounting()
}

// Usage preserves authoritative accounting for retained, unnamed objects.
func (s *Storage) Usage(ctx context.Context) (int64, error) {
	backend, ok := s.backend.(interface {
		Usage(context.Context) (int64, error)
	})
	if !ok {
		return 0, syscall.ENOSYS
	}
	return backend.Usage(readContext(ctx))
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	if err := s.CheckFileStorage(); err != nil {
		return nil, err
	}
	inner, err := s.backend.(storage.FileStorage).NewFileSession(readContext(ctx), options)
	if err != nil {
		return nil, err
	}
	return &fileSession{FileSession: inner, storage: s}, nil
}

type fileSession struct {
	storage.FileSession
	storage *Storage
}

func (s *fileSession) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (storage.File, error) {
	if options.Create || options.Truncate {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	inner, err := s.FileSession.OpenFile(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return &file{File: inner, storage: s.storage}, nil
}

func (s *fileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	if options.Truncate {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	inner, err := s.FileSession.OpenNode(ctx, id, options)
	if err != nil {
		return nil, err
	}
	return &file{File: inner, storage: s.storage}, nil
}

func (s *fileSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	return s.FileSession.StatNode(readContext(ctx), id)
}

func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	return s.FileSession.SetNodeAttr(s.storage.mutationContext(ctx), id, change)
}

func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.FileSession.Renew(readContext(ctx))
}

func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.FileSession.Status(readContext(ctx))
}

func (s *fileSession) Close(ctx context.Context) error {
	return s.FileSession.Close(readContext(ctx))
}

type file struct {
	storage.File
	storage *Storage
}

func (f *file) Stat(ctx context.Context) (storage.Attr, error) {
	return f.File.Stat(readContext(ctx))
}

func (f *file) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	return f.File.ReadAt(readContext(ctx), offset, length)
}

func (f *file) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	return f.File.WriteAt(f.storage.mutationContext(ctx), offset, data)
}

func (f *file) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	return f.File.Truncate(f.storage.mutationContext(ctx), size)
}

func (f *file) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return f.File.SetAttr(f.storage.mutationContext(ctx), change)
}

func (f *file) Sync(ctx context.Context) error {
	return f.File.Sync(readContext(ctx))
}

func (f *file) GetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock) (storage.LockConflict, error) {
	return f.File.GetLock(readContext(ctx), owner, lock)
}

func (f *file) SetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID) (storage.LockAttempt, error) {
	return f.File.SetLock(readContext(ctx), owner, lock, request)
}

func (f *file) QueryLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	return f.File.QueryLock(readContext(ctx), owner, request)
}

func (f *file) CancelLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	return f.File.CancelLock(readContext(ctx), owner, request)
}

func (f *file) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	return f.File.DropLocks(readContext(ctx), owner, family)
}

func (f *file) Close(ctx context.Context) error {
	return f.File.Close(readContext(ctx))
}
