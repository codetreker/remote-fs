package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *openFile) lockNode(ctx context.Context) (uint64, error) {
	node, err := f.state(ctx)
	return uint64(node.ID), err
}

func (f *openFile) GetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock) (storage.LockConflict, error) {
	ctx, done, err := f.admit(ctx, fileAdvisoryOperation)
	if err != nil {
		return storage.LockConflict{}, err
	}
	defer done()
	id, err := f.lockNode(ctx)
	if err != nil {
		return storage.LockConflict{}, err
	}
	return f.session.locks.Get(ctx, id, owner, lock)
}

func (f *openFile) SetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID) (storage.LockAttempt, error) {
	if err := lock.Check(); err != nil {
		return storage.LockAttempt{}, err
	}
	if lock.Family == storage.POSIX && (lock.Type == storage.Shared && !f.options.Read || lock.Type == storage.Exclusive && !f.options.Write) {
		return storage.LockAttempt{}, syscall.EBADF
	}
	class := fileAdvisoryOperation
	if lock.Type == storage.Unlock {
		class = fileCleanupOperation
	}
	ctx, done, err := f.admit(ctx, class)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	defer done()
	id, err := f.lockNode(ctx)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	if lock.Family == storage.Flock {
		f.session.mu.Lock()
		if _, exists := f.flock[owner]; !exists && len(f.flock) >= f.session.options.MaxLockOwners {
			f.session.mu.Unlock()
			return storage.LockAttempt{}, syscall.ENOLCK
		}
		f.flock[owner] = id
		f.session.mu.Unlock()
	}
	return f.session.locks.Set(ctx, id, owner, lock, request)
}

func (f *openFile) QueryLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	ctx, done, err := f.admit(ctx, fileCleanupOperation)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	defer done()
	id, err := f.lockNode(ctx)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	return f.session.locks.Query(ctx, id, owner, request)
}

func (f *openFile) CancelLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	ctx, done, err := f.admit(ctx, fileCleanupOperation)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	defer done()
	id, err := f.lockNode(ctx)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	return f.session.locks.Cancel(ctx, id, owner, request)
}

func (f *openFile) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	ctx, done, err := f.admit(ctx, fileCleanupOperation)
	if err != nil {
		return err
	}
	defer done()
	id, err := f.lockNode(ctx)
	if err != nil {
		return err
	}
	return f.session.locks.Drop(ctx, id, owner, family)
}

func (f *openFile) dropClosedFlock(node uint64, owner storage.LockOwner) error {
	f.session.mu.Lock()
	active := f.session.active
	f.session.mu.Unlock()
	if !active {
		return nil
	}
	// Native Node rejects retired references, so close uses the identity
	// captured when the owner registered its flock rather than resolving a name.
	return f.session.locks.Drop(f.session.cleanup, node, owner, storage.Flock)
}
