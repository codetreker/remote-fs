package replicated

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type retainedFile struct {
	session           *fileSession
	remote            httprest.FileWithBarrier
	mu                sync.Mutex
	closed            bool
	closeResult       storage.ReferenceCloseResult
	closeErr          error
	closeBarrier      *httprest.MutationBarrier
	closeAuthorityErr error
	closeRun          chan struct{}
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

func (f *retainedFile) Close(ctx context.Context) error {
	_, err := f.CloseWithResult(ctx)
	return err
}

func (f *retainedFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	for f.closeRun != nil {
		run := f.closeRun
		f.mu.Unlock()
		select {
		case <-run:
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
		f.mu.Lock()
	}
	if f.closed {
		result, err := f.closeResult, f.closeErr
		f.mu.Unlock()
		return result, err
	}
	previous := f.closeResult
	previousBarrier := f.closeBarrier
	previousAuthorityErr := f.closeAuthorityErr
	f.closeRun = make(chan struct{})
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		close(f.closeRun)
		f.closeRun = nil
		f.mu.Unlock()
	}()
	f.session.mu.Lock()
	sessionClosed := f.session.closed
	f.session.mu.Unlock()
	if sessionClosed && !previous.Released {
		result := storage.ReferenceCloseResult{Released: true}
		f.mu.Lock()
		f.closed = true
		f.closeResult = result
		f.mu.Unlock()
		return result, nil
	}
	var result storage.ReferenceCloseResult
	var barrier *httprest.MutationBarrier
	var authorityErr error
	if previous.Released && previousBarrier != nil {
		result, barrier, authorityErr = previous, previousBarrier, previousAuthorityErr
	} else if previous.Released {
		result, barrier, authorityErr = f.remote.CloseWithBarrier(ctx)
	} else {
		result, authorityErr = fileCall(ctx, f.session, false, func(ctx context.Context) (storage.ReferenceCloseResult, error) {
			var callErr error
			result, barrier, callErr = f.remote.CloseWithBarrier(ctx)
			return result, callErr
		})
	}
	if previous.Released && !result.Released {
		return previous, errors.Join(authorityErr, fmt.Errorf("released file close lost barrier replay: %w", syscall.EIO))
	}
	err := errors.Join(authorityErr, result.Check(authorityErr))
	if !result.Released {
		return result, err
	}
	settled, err := f.session.base.confirmReleasedClose(ctx, "close-file", barrier, authorityErr)
	f.mu.Lock()
	f.closeResult = result
	f.closeBarrier = barrier
	f.closeAuthorityErr = authorityErr
	if settled {
		f.closed = true
		f.closeErr = err
	}
	f.mu.Unlock()
	return result, err
}
