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
	if inner == nil {
		return nil, err
	}
	return &fileSession{FileSession: inner, storage: s}, err
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
	return s.storage.wrapFile(inner), err
}

func (s *fileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	if options.Truncate {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	inner, err := s.FileSession.OpenNode(ctx, id, options)
	return s.storage.wrapFile(inner), err
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
	referenceCapabilities
}

func (s *Storage) wrapFile(inner storage.File) storage.File {
	if inner == nil {
		return nil
	}
	return &file{File: inner, referenceCapabilities: referenceCapabilities{backend: inner, storage: s}}
}

type nodeReference struct {
	storage.NodeReference
	referenceCapabilities
}

func (s *Storage) wrapNodeReference(inner storage.NodeReference) storage.NodeReference {
	if inner == nil {
		return nil
	}
	return &nodeReference{NodeReference: inner, referenceCapabilities: referenceCapabilities{backend: inner, storage: s}}
}

func (r *nodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	return r.NodeReference.Stat(readContext(ctx))
}

func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return r.NodeReference.SetAttr(r.storage.mutationContext(ctx), change)
}

func (r *nodeReference) Close(ctx context.Context) error {
	return r.NodeReference.Close(readContext(ctx))
}

func (r *nodeReference) CheckScopedReference() error {
	return r.referenceCapabilities.CheckScopedReference()
}

func (r *nodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return r.referenceCapabilities.Scope(ctx)
}

func (r *nodeReference) CheckReferenceState() error {
	return r.referenceCapabilities.CheckReferenceState()
}

func (r *nodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return r.referenceCapabilities.State(ctx)
}

var _ storage.NodeReference = (*nodeReference)(nil)

func (f *file) Stat(ctx context.Context) (storage.Attr, error) {
	return f.File.Stat(readContext(ctx))
}

func (f *file) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	return f.File.ReadAt(readContext(ctx), offset, length)
}

func (f *file) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	return f.File.WriteAt(f.referenceCapabilities.storage.mutationContext(ctx), offset, data)
}

func (f *file) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	return f.File.Truncate(f.referenceCapabilities.storage.mutationContext(ctx), size)
}

func (f *file) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return f.File.SetAttr(f.referenceCapabilities.storage.mutationContext(ctx), change)
}

func (f *file) Sync(ctx context.Context) error {
	return f.File.Sync(readContext(ctx))
}

func (f *file) Close(ctx context.Context) error {
	return f.File.Close(readContext(ctx))
}
