package replicated

import (
	"context"
	"sync"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type retainedFile struct {
	session *fileSession
	remote  httprest.FileWithBarrier
	mu      sync.Mutex
	closed  bool
}

func fileCall[T any](ctx context.Context, session *fileSession, ordinary bool, call func(context.Context) (T, error)) (T, error) {
	ctx, done, err := session.begin(ctx, ordinary)
	if err != nil {
		var zero T
		return zero, err
	}
	defer done()
	return call(ctx)
}

// Retained metadata and bytes come from one authority object, including when
// no directory entry exists in the replica for that object.
func (f *retainedFile) Stat(ctx context.Context) (storage.Attr, error) {
	return fileCall(ctx, f.session, true, f.remote.Stat)
}

func (f *retainedFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.FileRead, error) {
		return f.remote.ReadAt(ctx, offset, length)
	})
}

func (f *retainedFile) mutate(ctx context.Context, op string, send func(context.Context) (storage.Attr, *httprest.MutationBarrier, error)) (storage.Attr, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.Attr, error) {
		var attr storage.Attr
		err := f.session.confirm(ctx, op, func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			attr, barrier, err = send(ctx)
			return barrier, err
		})
		if err != nil {
			return storage.Attr{}, err
		}
		return attr, nil
	})
}

func (f *retainedFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	return f.mutate(ctx, "write-file", func(ctx context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
		return f.remote.WriteAtWithBarrier(ctx, offset, data)
	})
}

func (f *retainedFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	return f.mutate(ctx, "truncate-file", func(ctx context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
		return f.remote.TruncateWithBarrier(ctx, size)
	})
}

func (f *retainedFile) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return f.mutate(ctx, "set-file-attr", func(ctx context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
		return f.remote.SetAttrWithBarrier(ctx, change)
	})
}

func (f *retainedFile) Sync(ctx context.Context) error {
	_, err := fileCall(ctx, f.session, true, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, f.remote.Sync(ctx)
	})
	return err
}

func (f *retainedFile) GetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock) (storage.LockConflict, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.LockConflict, error) {
		return f.remote.GetLock(ctx, owner, lock)
	})
}

func (f *retainedFile) SetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID) (storage.LockAttempt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.LockAttempt, error) {
		return f.remote.SetLock(ctx, owner, lock, request)
	})
}

func (f *retainedFile) QueryLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.LockAttempt, error) {
		return f.remote.QueryLock(ctx, owner, request)
	})
}

func (f *retainedFile) CancelLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.LockAttempt, error) {
		return f.remote.CancelLock(ctx, owner, request)
	})
}

func (f *retainedFile) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	_, err := fileCall(ctx, f.session, false, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, f.remote.DropLocks(ctx, owner, family)
	})
	return err
}

func (f *retainedFile) Close(ctx context.Context) error {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	f.session.mu.Lock()
	closed = closed || f.session.closed
	f.session.mu.Unlock()
	if closed {
		return nil
	}
	_, err := fileCall(ctx, f.session, false, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, f.remote.Close(ctx)
	})
	if err == nil {
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
	}
	return err
}
