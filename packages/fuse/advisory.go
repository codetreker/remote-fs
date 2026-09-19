package fuse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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

func kernelLock(lk *gofuse.FileLock, flags uint32, wait bool) (storage.RangeCommand, error) {
	if lk == nil || flags & ^uint32(gofuse.FUSE_LK_FLOCK) != 0 {
		return storage.RangeCommand{}, syscall.EINVAL
	}
	start, end := lk.Start, lk.End
	domain := storage.DomainRecord
	conversion := storage.PreserveBeforeAcquire
	if flags&gofuse.FUSE_LK_FLOCK != 0 {
		domain = storage.DomainWholeFile
		start, end = 0, math.MaxInt64
		conversion = storage.DropBeforeAcquire
	}
	if start > end || end > math.MaxInt64 {
		return storage.RangeCommand{}, syscall.EINVAL
	}
	command := storage.RangeCommand{Domain: domain, Range: storage.Range{Kind: storage.Bytes, Start: start, Length: end - start + 1}, Edit: storage.Replace, Wait: wait, Conversion: conversion}
	switch lk.Typ {
	case syscall.F_RDLCK:
		command.Mode = storage.RangeShared
	case syscall.F_WRLCK:
		command.Mode = storage.RangeExclusive
	case syscall.F_UNLCK:
		command.Mode = storage.RangeShared
		command.Edit = storage.Subtract
		command.Wait = false
		command.Conversion = storage.PreserveBeforeAcquire
	default:
		return storage.RangeCommand{}, syscall.EINVAL
	}
	return command, command.Check()
}

func (h *handle) Getlk(ctx context.Context, kernel uint64, lk *gofuse.FileLock, flags uint32, out *gofuse.FileLock) (result syscall.Errno) {
	command, err := kernelLock(lk, flags, false)
	if err != nil {
		return errnoOf(err)
	}
	if command.Edit == storage.Subtract || out == nil {
		return syscall.EINVAL
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	control, ok := h.node.volume.files.(storage.RangeControl)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	call, cancel := context.WithTimeout(ctx, h.node.volume.flushTimeout)
	defer cancel()
	owner, err := h.lockOwner(call, kernel, command.Domain, lk.Pid, false)
	if err != nil {
		return errnoOf(err)
	}
	defer func() {
		cleanup, finish := h.node.volume.cleanupContext(ctx)
		defer finish()
		if err := h.releaseLockOwner(cleanup, owner); err != nil {
			result = h.unknownLock(err)
		}
	}()
	conflict, err := control.GetConflict(call, owner.id, command)
	if err != nil {
		return errnoOf(err)
	}
	*out = *lk
	out.Typ = syscall.F_UNLCK
	if !conflict.Found {
		return 0
	}
	other := conflict.Range
	if err := other.Check(); err != nil || other.Kind != storage.Bytes || other.Start+other.Length-1 > math.MaxInt64 ||
		other.Start >= command.Range.Start+command.Range.Length || command.Range.Start >= other.Start+other.Length ||
		conflict.Mode != storage.RangeShared && conflict.Mode != storage.RangeExclusive || conflict.Mode == storage.RangeShared && command.Mode == storage.RangeShared {
		return syscall.EIO
	}
	out.Start, out.End, out.Pid = other.Start, other.Start+other.Length-1, h.node.volume.lockPID(conflict.Owner)
	out.Typ = syscall.F_RDLCK
	if conflict.Mode == storage.RangeExclusive {
		out.Typ = syscall.F_WRLCK
	}
	return 0
}

func (h *handle) Setlk(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, owner, lk, flags, false)
}

func (h *handle) Setlkw(ctx context.Context, owner uint64, lk *gofuse.FileLock, flags uint32) syscall.Errno {
	return h.setLock(ctx, owner, lk, flags, true)
}

func (h *handle) setLock(ctx context.Context, kernel uint64, lk *gofuse.FileLock, flags uint32, wait bool) (result syscall.Errno) {
	lock, err := kernelLock(lk, flags, wait)
	if err != nil {
		return errnoOf(err)
	}
	if lock.Domain == storage.DomainRecord && lock.Edit != storage.Subtract && (lock.Mode == storage.RangeShared && !h.readable || lock.Mode == storage.RangeExclusive && !h.writable) {
		return syscall.EBADF
	}
	if err := h.check(); err != nil {
		return errnoOf(err)
	}
	control, ok := h.node.volume.files.(storage.RangeControl)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	call, cancel := context.WithTimeout(ctx, h.node.volume.flushTimeout)
	owner, err := h.lockOwner(call, kernel, lock.Domain, lk.Pid, true)
	cancel()
	if err != nil {
		return errnoOf(err)
	}
	defer func() {
		cleanup, finish := h.node.volume.cleanupContext(ctx)
		defer finish()
		if err := h.releaseLockOwner(cleanup, owner); err != nil {
			result = h.unknownLock(err)
		}
	}()
	call, cancel = context.WithTimeout(ctx, h.node.volume.flushTimeout)
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
	attempt, err := control.Apply(call, owner.id, []storage.RangeCommand{lock}, request)
	cancel()
	if err != nil {
		if h.node.volume.lockOwnerRetired(owner) {
			return syscall.EINTR
		}
		if errno := errnoOf(err); errno != syscall.EIO && errno != syscall.EINTR {
			return errno
		}
		if h.node.volume.lockOwnerRetired(owner) {
			return syscall.EINTR
		}
		return h.cancelLock(ctx, owner, lock, request, err)
	}
	interval := max(time.Nanosecond, min(100*time.Millisecond, h.node.volume.sessionOptions.Lease/4))
	for {
		if err := checkLockAttempt(attempt, request, lock); err != nil {
			return h.unknownLock(err)
		}
		switch attempt.State {
		case storage.Granted:
			if h.node.volume.lockOwnerRetired(owner) {
				return syscall.EINTR
			}
			return errnoOf(h.check())
		case storage.Rejected:
			return rangeErrno(attempt.Rejection)
		case storage.Released:
			if lock.Edit == storage.Subtract {
				return 0
			}
			return syscall.EINTR
		case storage.Cancelled:
			return syscall.EINTR
		}
		if err := h.check(); err != nil {
			if errnoOf(err) == syscall.ESTALE {
				return syscall.ESTALE
			}
			if h.node.volume.lockOwnerRetired(owner) {
				return syscall.EINTR
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
		attempt, err = control.Query(call, owner.id, request)
		cancel()
		if err != nil {
			if h.node.volume.lockOwnerRetired(owner) {
				return syscall.EINTR
			}
			return h.cancelLock(ctx, owner, lock, request, err)
		}
	}
}

func checkLockAttempt(attempt storage.RangeAttempt, request storage.LockRequestID, lock storage.RangeCommand) error {
	if attempt.Request != request || !slices.Equal(attempt.Commands, []storage.RangeCommand{lock}) {
		return fmt.Errorf("advisory receipt does not match its request: %w", syscall.EIO)
	}
	if attempt.State == storage.Rejected {
		if errno := rangeErrno(attempt.Rejection); errno != 0 && errno != syscall.EIO {
			return nil
		}
	} else if attempt.Rejection == "" {
		switch attempt.State {
		case storage.Pending:
			if lock.Wait && lock.Edit != storage.Subtract {
				return nil
			}
		case storage.Granted:
			if lock.Edit != storage.Subtract && attempt.EverGranted {
				return nil
			}
		case storage.Cancelled, storage.Released:
			return nil
		}
	}
	return fmt.Errorf("advisory receipt has an invalid outcome: %w", syscall.EIO)
}

// Cancellation acknowledges a retained grant as success. Returning EINTR for that
// outcome would permit a retry while an acquisition the caller never observed survives.
func (h *handle) cancelLock(ctx context.Context, owner *localLockOwner, lock storage.RangeCommand, request storage.LockRequestID, cause error) syscall.Errno {
	cleanup, cancel := h.node.volume.cleanupContext(ctx)
	defer cancel()
	control, ok := h.node.volume.files.(storage.RangeControl)
	if !ok {
		return h.unknownLock(syscall.EOPNOTSUPP)
	}
	attempt, err := control.Cancel(cleanup, owner.id, request)
	if err != nil {
		if h.node.volume.lockOwnerRetired(owner) {
			return syscall.EINTR
		}
		if h.retiredNormally(err) {
			return syscall.ESTALE
		}
		return h.unknownLock(errors.Join(cause, err))
	}
	if err := checkLockAttempt(attempt, request, lock); err != nil {
		return h.unknownLock(errors.Join(cause, err))
	}
	switch attempt.State {
	case storage.Granted:
		if h.node.volume.lockOwnerRetired(owner) {
			return syscall.EINTR
		}
		return errnoOf(h.check())
	case storage.Cancelled, storage.Released:
		if lock.Edit == storage.Subtract && attempt.State == storage.Released {
			return 0
		}
		return syscall.EINTR
	case storage.Rejected:
		return rangeErrno(attempt.Rejection)
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
	if err := h.dropRecordOwner(cleanup, metadata.owner); err != nil {
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
	} else {
		err = h.retireDescriptionOwners(cleanup)
	}
	err = errors.Join(err, h.closeFile(cleanup))
	if err != nil {
		return h.unknownLock(err)
	}
	return 0
}

func rangeErrno(code storage.RejectionCode) syscall.Errno {
	switch code {
	case "":
		return 0
	case storage.RangeBlocked:
		return syscall.EAGAIN
	case storage.RangeNotHeld:
		return syscall.EINVAL
	case storage.RangeExhausted:
		return syscall.ENOLCK
	case storage.RangeTooLarge:
		return syscall.EFBIG
	case storage.RangeDeadlock:
		return syscall.EDEADLK
	case storage.RangeInvalid:
		return syscall.EINVAL
	case storage.RangeUnsupported:
		return syscall.EOPNOTSUPP
	case storage.RangeExpired:
		return syscall.ESTALE
	default:
		return syscall.EIO
	}
}
