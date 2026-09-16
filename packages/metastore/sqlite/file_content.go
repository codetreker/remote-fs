package sqlite

import (
	"context"
	"database/sql"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *fileReference) capture(ctx context.Context, tx *sql.Tx, operation storage.FileIO) (metastore.FileState, error) {
	uses := storage.ReadContent
	if operation.Write || operation.Truncate {
		uses = storage.WriteContent
	}
	if err := f.check(uses); err != nil {
		return metastore.FileState{}, err
	}
	state, err := f.session.store.fileState(ctx, tx, f.id)
	if err != nil {
		return metastore.FileState{}, err
	}
	if operation.ExpectedSize != nil && state.Size != *operation.ExpectedSize {
		return metastore.FileState{}, &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: uint64(f.id)}}
	}
	if state.Kind == storage.NodeSymlink {
		return metastore.FileState{}, syscall.ELOOP
	}
	if state.Kind != storage.NodeRegular {
		return metastore.FileState{}, syscall.EISDIR
	}
	if err := f.session.store.checkFileIOLocked(f.nativeContext(ctx), tx, f.id, state.Size, operation); err != nil {
		return metastore.FileState{}, err
	}
	return state, nil
}

func (f *fileReference) Capture(ctx context.Context, operation storage.FileIO) (metastore.FileState, error) {
	if err := operation.Check(); err != nil {
		return metastore.FileState{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.FileState{}, err
	}
	defer done()
	var state metastore.FileState
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error { var err error; state, err = f.capture(ctx, tx, operation); return err })
	return state, err
}

func (f *fileReference) Reserve(ctx context.Context, size int64) (metastore.Key, error) {
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
	if err := f.check(storage.WriteContent); err != nil {
		return "", err
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
		if node.Kind != storage.NodeRegular {
			return syscall.EINVAL
		}
		if err := f.session.store.roomFor(ctx, tx, size-node.Size); err != nil {
			return err
		}
		return f.session.store.reserveObject(ctx, tx, key, size, sec, nsec)
	})
	return key, err
}

func (f *fileReference) BeginContent(ctx context.Context, id storage.FileActionID, digest [32]byte, operation storage.FileIO) (storage.FileActionReceipt, bool, error) {
	if err := operation.Check(); err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if !operation.Write {
		return storage.FileActionReceipt{}, false, syscall.EINVAL
	}
	fingerprint, err := fileFingerprint(struct {
		Digest [32]byte
		IO     storage.FileIO
	}{digest, operation})
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fingerprint, f)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if !fresh {
		result, err := f.session.result(a)
		return result, false, err
	}
	a.result.Operation = storage.OpFileWrite
	if operation.Truncate {
		a.result.Operation = storage.OpFileTruncate
	}
	if operation.Owner != nil {
		owner := *operation.Owner
		operation.Owner = &owner
	}
	if operation.ExpectedSize != nil {
		size := *operation.ExpectedSize
		operation.ExpectedSize = &size
	}
	a.io = operation
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error { _, err := f.capture(ctx, tx, operation); return err })
	if err != nil {
		result, err := f.session.finish(a, err)
		return result, false, err
	}
	if !operation.Truncate && operation.Length == 0 {
		err := f.session.store.inspect(ctx, func(tx *sql.Tx) error {
			var err error
			a.result.Observation, err = f.observation(ctx, tx, storage.ObservationOptions{})
			return err
		})
		result, err := f.session.finish(a, err)
		return result, false, err
	}
	result, err := f.session.result(a)
	return result, true, err
}

func (f *fileReference) CommitContent(ctx context.Context, id storage.FileActionID, expected uint64, object metastore.Object) (storage.FileActionReceipt, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, err := f.contentAction(id)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if a.result.State != storage.FileActionPending {
		return f.session.result(a)
	}
	if err := f.check(storage.WriteContent); err != nil {
		return f.session.finish(a, err)
	}
	if expected == 0 || object.Size < 0 {
		return f.session.finish(a, syscall.EINVAL)
	}
	if object.Size > f.session.options.MaxFileSize {
		return f.session.finish(a, syscall.EFBIG)
	}
	ctx = metastore.WithFileIO(f.nativeContext(ctx), a.io)
	revisionChanged := false
	var observed storage.FileObservation
	err = f.session.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.WriteMutation, node: f.id}, func(tx *sql.Tx) error {
		before, err := f.session.store.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if a.io.ExpectedSize != nil && before.Size != *a.io.ExpectedSize {
			return &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: uint64(f.id)}}
		}
		if before.Revision != expected {
			revisionChanged = true
			return syscall.EAGAIN
		}
		if before.Kind != storage.NodeRegular {
			return syscall.EINVAL
		}
		size := max(before.Size, a.io.Offset+a.io.Length)
		if a.io.Truncate {
			size = a.io.Size
		}
		if size != object.Size {
			return syscall.EINVAL
		}
		if err := f.session.store.replaceNodeContent(ctx, tx, before.Node, object, nil); err != nil {
			return err
		}
		observed, err = f.observation(ctx, tx, storage.ObservationOptions{})
		return err
	})
	if revisionChanged && f.session.store.coordinator.healthy() == nil {
		result, _ := f.session.result(a)
		return result, &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: uint64(f.id)}, Cause: err}
	}
	if err == nil {
		a.result.Effects = storage.EffectContentChanged
		a.result.Observation = observed
	}
	return f.session.finish(a, err)
}

func (f *fileReference) RejectContent(ctx context.Context, id storage.FileActionID, cause error) (storage.FileActionReceipt, error) {
	if cause == nil {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	if err := f.session.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer f.session.store.coordinator.commit.release()
	a, err := f.contentAction(id)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if a.result.State != storage.FileActionPending {
		return f.session.result(a)
	}
	return f.session.finish(a, cause)
}

func (f *fileReference) contentAction(id storage.FileActionID) (*fileAction, error) {
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
