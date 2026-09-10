package fuse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ fs.FileGetlker  = (*handle)(nil)
	_ fs.FileSetlker  = (*handle)(nil)
	_ fs.FileSetlkwer = (*handle)(nil)
	_ fs.FileFlusher  = (*handle)(nil)
	_ fs.FileReleaser = (*handle)(nil)
)

func kernelLock(lk *gofuse.FileLock, flags uint32, wait bool) (storage.FileLock, error) {
	if lk == nil || flags&^uint32(gofuse.FUSE_LK_FLOCK) != 0 {
		return storage.FileLock{}, syscall.EINVAL
	}
	lock := storage.FileLock{Family: storage.POSIX, Start: lk.Start, End: lk.End, PID: lk.Pid, Wait: wait}
	if flags&gofuse.FUSE_LK_FLOCK != 0 {
		lock.Family = storage.Flock
		lock.Start, lock.End = 0, math.MaxInt64
	}
	switch lk.Typ {
	case syscall.F_RDLCK:
		lock.Type = storage.Shared
	case syscall.F_WRLCK:
		lock.Type = storage.Exclusive
	case syscall.F_UNLCK:
		lock.Type, lock.Wait = storage.Unlock, false
	default:
		return storage.FileLock{}, syscall.EINVAL
	}
	return lock, lock.Check()
}

func (h *handle) Getlk(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32, out *gofuse.FileLock) syscall.Errno {
	lock, err := kernelLock(lk, flags, false)
	if err != nil {
		return errnoOf(err)
	}
	if lock.Type == storage.Unlock || out == nil {
		return syscall.EINVAL
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	call, cancel := context.WithTimeout(ctx, h.node.volume.flushTimeout)
	defer cancel()
	conflict, err := h.file.GetLock(call, storage.LockOwner(owner), lock)
	if err != nil {
		return errnoOf(err)
	}
	*out = *lk
	out.Typ = syscall.F_UNLCK
	if !conflict.Found {
		return 0
	}
	other := conflict.Lock
	if err := other.Check(); err != nil || other.Type == storage.Unlock || other.Family != lock.Family ||
		other.Start > lock.End || lock.Start > other.End || other.Type == storage.Shared && lock.Type == storage.Shared {
		return syscall.EIO
	}
	out.Start, out.End, out.Pid = other.Start, other.End, other.PID
	out.Typ = syscall.F_RDLCK
	if other.Type == storage.Exclusive {
		out.Typ = syscall.F_WRLCK
	}
	return 0
}

func (h *handle) Setlk(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, storage.LockOwner(owner), lk, flags, false)
}

func (h *handle) Setlkw(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, storage.LockOwner(owner), lk, flags, true)
}

func (h *handle) setLock(ctx context.Context, owner storage.LockOwner, lk *gofuse.FileLock, flags uint32, wait bool) syscall.Errno {
	lock, err := kernelLock(lk, flags, wait)
	if err != nil {
		return errnoOf(err)
	}
	if lock.Family == storage.POSIX && (lock.Type == storage.Shared && !h.readable || lock.Type == storage.Exclusive && !h.writable) {
		return syscall.EBADF
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	call, cancel := context.WithTimeout(ctx, h.node.volume.flushTimeout)
	epoch, err := h.node.volume.actionEpoch(call)
	cancel()
	if err != nil {
		return errnoOf(err)
	}
	request, err := storage.NewLockRequestID(epoch)
	if err != nil {
		return errnoOf(err)
	}
	call, cancel = context.WithTimeout(ctx, h.node.volume.flushTimeout)
	attempt, err := h.file.SetLock(call, owner, lock, request)
	cancel()
	if err != nil {
		if errno := errnoOf(err); errno != syscall.EIO && errno != syscall.EINTR {
			return errno
		}
		return h.cancelLock(ctx, owner, lock, request, err)
	}
	interval := max(time.Nanosecond, min(100*time.Millisecond, h.node.volume.sessionOptions.Lease/4))
	for {
		if err := checkLockAttempt(attempt, request, lock); err != nil {
			return h.unknownLock(err)
		}
		switch attempt.State {
		case storage.LockGranted:
			return errnoOf(h.check())
		case storage.LockRejected:
			return attempt.Errno
		case storage.LockReleased:
			if lock.Type == storage.Unlock {
				return 0
			}
			return syscall.EINTR
		case storage.LockCancelled:
			return syscall.EINTR
		}
		if err := h.check(); err != nil {
			if errnoOf(err) == syscall.ESTALE {
				return syscall.ESTALE
			}
			return h.cancelLock(ctx, owner, lock, request, err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return h.cancelLock(ctx, owner, lock, request, ctx.Err())
		case <-timer.C:
		}
		call, cancel = context.WithTimeout(ctx, h.node.volume.flushTimeout)
		attempt, err = h.file.QueryLock(call, owner, request)
		cancel()
		if err != nil {
			return h.cancelLock(ctx, owner, lock, request, err)
		}
	}
}

func checkLockAttempt(attempt storage.LockAttempt, request storage.LockRequestID, lock storage.FileLock) error {
	if attempt.Request != request || attempt.Lock != lock {
		return fmt.Errorf("advisory receipt does not match its request: %w", syscall.EIO)
	}
	if attempt.State == storage.LockRejected {
		if attempt.Errno != 0 {
			return nil
		}
	} else if attempt.Errno == 0 {
		switch attempt.State {
		case storage.LockPending:
			if lock.Wait && lock.Type != storage.Unlock {
				return nil
			}
		case storage.LockGranted:
			if lock.Type != storage.Unlock && attempt.EverGranted {
				return nil
			}
		case storage.LockCancelled, storage.LockReleased:
			return nil
		}
	}
	return fmt.Errorf("advisory receipt has an invalid outcome: %w", syscall.EIO)
}

// Cancellation acknowledges a retained grant as success. Returning EINTR for that
// outcome would permit a retry while an acquisition the caller never observed survives.
func (h *handle) cancelLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID, cause error) syscall.Errno {
	cleanup, cancel := h.node.volume.cleanupContext(ctx)
	defer cancel()
	attempt, err := h.file.CancelLock(cleanup, owner, request)
	if err != nil {
		if h.retiredNormally(err) {
			return syscall.ESTALE
		}
		return h.unknownLock(errors.Join(cause, err))
	}
	if err := checkLockAttempt(attempt, request, lock); err != nil {
		return h.unknownLock(errors.Join(cause, err))
	}
	switch attempt.State {
	case storage.LockGranted:
		return errnoOf(h.check())
	case storage.LockCancelled, storage.LockReleased:
		if lock.Type == storage.Unlock && attempt.State == storage.LockReleased {
			return 0
		}
		return syscall.EINTR
	case storage.LockRejected:
		return attempt.Errno
	default:
		return h.unknownLock(errors.Join(cause, fmt.Errorf("advisory cancellation is unresolved: %w", syscall.EIO)))
	}
}

func (h *handle) unknownLock(cause error) syscall.Errno {
	if h.retiredNormally(cause) {
		return syscall.ESTALE
	}
	h.node.volume.fence(cause)
	return syscall.EIO
}

func (h *handle) retiredNormally(err error) bool {
	return errnoOf(err) == syscall.ESTALE && errnoOf(h.node.volume.check()) == syscall.ESTALE
}

// Every descriptor close carries its POSIX owner, including closes of a descriptor
// that never acquired a lock. The retained file supplies the object identity.
func (h *handle) Flush(ctx context.Context) syscall.Errno {
	metadata, ok := h.node.volume.raw.lookup(ctx.Done())
	if !ok || metadata.kind != rawFlush {
		return h.unknownLock(fmt.Errorf("flush lacks its kernel lock owner: %w", syscall.EIO))
	}
	cleanup, cancel := h.node.volume.cleanupContext(ctx)
	defer cancel()
	if err := h.file.DropLocks(cleanup, metadata.owner, storage.POSIX); err != nil {
		return h.unknownLock(err)
	}
	return 0
}

// The high-level go-fuse bridge discards this errno. Fence records cleanup failure
// and closes the session even when the kernel cannot receive the release outcome.
// https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L905-L918
func (h *handle) Release(ctx context.Context) syscall.Errno {
	metadata, ok := h.node.volume.raw.lookup(ctx.Done())
	cleanup, cancel := h.node.volume.cleanupContext(ctx)
	defer cancel()
	var err error
	if !ok || metadata.kind != rawRelease {
		err = fmt.Errorf("release lacks its kernel lock owner: %w", syscall.EIO)
	} else if metadata.flockUnlock {
		err = h.file.DropLocks(cleanup, metadata.owner, storage.Flock)
	}
	err = errors.Join(err, h.closeFile(cleanup))
	if err != nil {
		return h.unknownLock(err)
	}
	return 0
}
