package replicated

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type retainedFile struct {
	session           *fileSession
	remote            httprest.FileWithBarrier
	mu                sync.Mutex
	authorityReleased bool
	closeConfirmed    bool
	closeBarrier      *httprest.MutationBarrier
	closeErr          error
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
		finished := f.closeRun
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		case <-finished:
		}
		f.mu.Lock()
	}
	released, confirmed, barrier, semanticErr := f.authorityReleased, f.closeConfirmed, f.closeBarrier, f.closeErr
	if confirmed {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{Released: true}, semanticErr
	}
	f.session.mu.Lock()
	sessionClosed := f.session.closed
	f.session.mu.Unlock()
	if sessionClosed && !released {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{Released: true}, nil
	}
	f.closeRun = make(chan struct{})
	defer func() {
		f.mu.Lock()
		close(f.closeRun)
		f.closeRun = nil
		f.mu.Unlock()
	}()
	f.mu.Unlock()
	if !released {
		var remoteResult storage.ReferenceCloseResult
		_, callErr := fileCall(ctx, f.session, false, func(ctx context.Context) (struct{}, error) {
			result, remoteBarrier, remoteErr := f.remote.CloseWithResultAndBarrier(ctx)
			remoteResult = result
			if checkErr := result.Check(remoteErr); checkErr != nil {
				return struct{}{}, checkErr
			}
			if result.Released && (remoteBarrier != nil || errors.Is(remoteErr, syscall.ENOTEMPTY)) {
				f.mu.Lock()
				f.authorityReleased = true
				f.closeBarrier = remoteBarrier
				f.closeErr = remoteErr
				f.mu.Unlock()
			}
			return struct{}{}, remoteErr
		})
		f.mu.Lock()
		released, barrier, semanticErr = f.authorityReleased, f.closeBarrier, f.closeErr
		f.mu.Unlock()
		if !released {
			if callErr == nil {
				return storage.ReferenceCloseResult{}, errors.New("released file close returned no replication outcome")
			}
			return remoteResult, callErr
		}
	}
	var confirmationErr error
	if barrier != nil || !errors.Is(semanticErr, syscall.ENOTEMPTY) {
		confirmationErr = f.session.confirmCleanup(ctx, "close-file", barrier)
	}
	if confirmationErr == nil {
		f.mu.Lock()
		f.closeConfirmed = true
		f.mu.Unlock()
	}
	return storage.ReferenceCloseResult{Released: true}, errors.Join(semanticErr, confirmationErr)
}
