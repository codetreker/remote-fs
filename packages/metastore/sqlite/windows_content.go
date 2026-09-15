package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *windowsFile) Capture(ctx context.Context, operation metastore.WindowsIO) (metastore.FileState, error) {
	if err := storage.CheckWindowsRange(operation.Offset, operation.Length); err != nil {
		return metastore.FileState{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.FileState{}, err
	}
	defer done()
	wanted := storage.WindowsReadData
	if operation.Write {
		wanted = storage.WindowsWriteData
		if !operation.Truncate && f.access&wanted == 0 {
			wanted = storage.WindowsAppendData
		}
	}
	if err := f.check(wanted); err != nil {
		return metastore.FileState{}, err
	}
	var state metastore.FileState
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = f.session.store.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if state.Mode&fs.ModeSymlink != 0 {
			return syscall.ELOOP
		}
		if !state.Mode.IsRegular() {
			return syscall.EISDIR
		}
		if operation.Write && f.access&storage.WindowsWriteData == 0 && operation.Offset != state.Size {
			return syscall.EACCES
		}
		return f.session.store.checkWindowsIOLocked(f.id, f.handle, state.Size, operation)
	})
	return state, err
}

func (f *windowsFile) Reserve(ctx context.Context, size int64) (metastore.Key, error) {
	if size < 0 {
		return "", syscall.EINVAL
	}
	if size > f.session.options.MaxFileSize || size > f.session.store.objectLimits.MaxPendingBytes {
		return "", syscall.EFBIG
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer done()
	if err := f.check(0); err != nil {
		return "", err
	}
	if f.access&(storage.WindowsWriteData|storage.WindowsAppendData) == 0 {
		return "", syscall.EACCES
	}
	key, err := sqlvalue.NewKey()
	if err != nil {
		return "", err
	}
	sec, nsec := sqlvalue.StoredTime(time.Now())
	err = f.session.store.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		node, err := f.session.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if err := f.session.store.roomFor(ctx, tx, size-node.Size); err != nil {
			return err
		}
		return f.session.store.reserveObject(ctx, tx, key, size, sec, nsec)
	})
	return key, err
}

func (f *windowsFile) BeginContent(ctx context.Context, id storage.WindowsActionID, digest [32]byte, operation metastore.WindowsIO) (metastore.WindowsResult, bool, error) {
	if !operation.Write {
		return metastore.WindowsResult{}, false, syscall.EINVAL
	}
	if err := storage.CheckWindowsRange(operation.Offset, operation.Length); err != nil {
		return metastore.WindowsResult{}, false, err
	}
	if operation.Truncate && operation.Size < 0 {
		return metastore.WindowsResult{}, false, syscall.EINVAL
	}
	fingerprint, err := windowsFingerprint(struct {
		Digest [32]byte
		IO     metastore.WindowsIO
	}{digest, operation})
	if err != nil {
		return metastore.WindowsResult{}, false, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, false, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fingerprint, f)
	if err != nil {
		return metastore.WindowsResult{}, false, err
	}
	if !fresh {
		r, e := f.session.result(a)
		return r, false, e
	}
	if err := f.check(0); err != nil {
		r, e := f.session.finish(a, err)
		return r, false, e
	}
	if operation.Truncate && f.access&storage.WindowsWriteData == 0 || !operation.Truncate && f.access&(storage.WindowsWriteData|storage.WindowsAppendData) == 0 {
		r, e := f.session.finish(a, syscall.EACCES)
		return r, false, e
	}
	a.io = operation
	if !operation.Truncate && operation.Length == 0 {
		err := f.session.store.inspect(ctx, func(tx *sql.Tx) error {
			state, err := f.session.store.fileState(ctx, tx, f.id)
			if err != nil {
				return err
			}
			if state.Mode&fs.ModeSymlink != 0 {
				return syscall.ELOOP
			}
			if !state.Mode.IsRegular() {
				return syscall.EISDIR
			}
			if err := f.session.store.checkWindowsIOLocked(f.id, f.handle, state.Size, operation); err != nil {
				return err
			}
			a.result.Attr, err = f.attr(ctx, tx)
			return err
		})
		r, e := f.session.finish(a, err)
		return r, false, e
	}
	r, e := f.session.result(a)
	return r, true, e
}

func (f *windowsFile) contentAction(id storage.WindowsActionID) (*windowsAction, error) {
	if _, err := id.Epoch(); err != nil {
		return nil, err
	}
	a := f.session.actions[id]
	if a == nil {
		return nil, syscall.ESTALE
	}
	if a.file != f || !a.io.Write {
		return nil, syscall.EINVAL
	}
	return a, nil
}

func (f *windowsFile) CommitContent(ctx context.Context, id storage.WindowsActionID, expected uint64, object metastore.Object) (metastore.WindowsResult, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	a, err := f.contentAction(id)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	if a.result.State != storage.WindowsActionPending {
		return f.session.result(a)
	}
	if err := f.check(0); err != nil {
		return f.session.finish(a, err)
	}
	if expected == 0 || object.Size < 0 {
		return f.session.finish(a, syscall.EINVAL)
	}
	if object.Size > f.session.options.MaxFileSize {
		return f.session.finish(a, syscall.EFBIG)
	}
	ctx = metastore.WithFileIO(f.session.nativeContext(ctx, f), a.io)
	err = f.session.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.WriteMutation, node: f.id}, func(tx *sql.Tx) error {
		before, err := f.session.store.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if before.Revision != expected {
			return syscall.EAGAIN
		}
		expectedSize := max(before.Size, a.io.Offset+a.io.Length)
		if a.io.Truncate {
			expectedSize = a.io.Size
		}
		if expectedSize != object.Size {
			return syscall.EINVAL
		}
		if f.access&storage.WindowsWriteData == 0 && a.io.Offset != before.Size {
			return syscall.EACCES
		}
		if err := f.session.store.replaceNodeContent(ctx, tx, before.Node, object, nil); err != nil {
			return err
		}
		a.result.Attr, err = f.attr(ctx, tx)
		return err
	})
	if errors.Is(err, syscall.EAGAIN) && f.session.store.coordinator.healthy() == nil {
		return metastore.WindowsResult{}, err
	}
	return f.session.finish(a, err)
}

func (f *windowsFile) RejectContent(ctx context.Context, id storage.WindowsActionID, cause error) (metastore.WindowsResult, error) {
	if cause == nil {
		return metastore.WindowsResult{}, syscall.EINVAL
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	a, err := f.contentAction(id)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	if a.result.State != storage.WindowsActionPending {
		return f.session.result(a)
	}
	return f.session.finish(a, cause)
}
