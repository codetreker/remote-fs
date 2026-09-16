package fuse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"

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

func kernelLock(lk *gofuse.FileLock, flags uint32, wait bool) (fileLock, error) {
	if lk == nil || flags&^uint32(gofuse.FUSE_LK_FLOCK) != 0 {
		return fileLock{}, syscall.EINVAL
	}
	lock := fileLock{Family: posixFamily, Start: lk.Start, End: lk.End, PID: lk.Pid, Wait: wait}
	if flags&gofuse.FUSE_LK_FLOCK != 0 {
		lock.Family = flockFamily
		lock.Start, lock.End = 0, math.MaxInt64
	}
	switch lk.Typ {
	case syscall.F_RDLCK:
		lock.Type = sharedType
	case syscall.F_WRLCK:
		lock.Type = exclusiveType
	case syscall.F_UNLCK:
		lock.Type, lock.Wait = unlockType, false
	default:
		return fileLock{}, syscall.EINVAL
	}
	return lock, lock.check()
}

func (h *handle) Getlk(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32, out *gofuse.FileLock) syscall.Errno {
	lock, err := kernelLock(lk, flags, false)
	if err != nil {
		return errnoOf(err)
	}
	if lock.Type == unlockType || out == nil {
		return syscall.EINVAL
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	call, cancel := context.WithTimeout(ctx, h.node.volume.flushTimeout)
	defer cancel()
	snapshot, err := h.file.RangeSnapshot(call, storage.RangeOwnerID(owner), advisoryScope(lock.Family))
	if err != nil {
		return errnoOf(err)
	}
	if err := checkRangeSnapshot(snapshot, lock.Family); err != nil {
		return errnoOf(err)
	}
	*out = *lk
	out.Typ = syscall.F_UNLCK
	other, found := conflictingRange(snapshot, lock)
	if !found {
		return 0
	}
	if other.Range.Boundary {
		return syscall.EIO
	}
	out.Start, out.End, out.Pid = other.Range.Start, other.Range.End, 0
	h.node.volume.mu.Lock()
	session := h.node.volume.status.Epoch
	h.node.volume.mu.Unlock()
	if other.Owner.Session == session {
		out.Pid = h.node.volume.localOwners().process(advisoryOwnerKey{node: h.node.id.node, owner: lockOwner(other.Owner.ID), family: lock.Family})
	}
	out.Typ = syscall.F_RDLCK
	if other.Range.Exclusive {
		out.Typ = syscall.F_WRLCK
	}
	return 0
}

func (h *handle) Setlk(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, lockOwner(owner), lk, flags, false)
}

func (h *handle) Setlkw(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, lockOwner(owner), lk, flags, true)
}

func (h *handle) setLock(ctx context.Context, owner lockOwner, lk *gofuse.FileLock, flags uint32, wait bool) syscall.Errno {
	lock, err := kernelLock(lk, flags, wait)
	if err != nil {
		return errnoOf(err)
	}
	if lock.Family == posixFamily && (lock.Type == sharedType && !h.readable || lock.Type == exclusiveType && !h.writable) {
		return syscall.EBADF
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	return h.replaceAdvisory(ctx, owner, lock)
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
	if err := h.dropAdvisory(cleanup, metadata.owner, posixFamily); err != nil {
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
		err = h.dropAdvisory(cleanup, metadata.owner, flockFamily)
	}
	err = errors.Join(err, h.closeFile(cleanup))
	if err != nil {
		return h.unknownLock(err)
	}
	return 0
}
