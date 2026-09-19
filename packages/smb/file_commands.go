package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusFileClosed           uint32 = 0xc0000128
	statusInvalidDeviceRequest uint32 = 0xc0000010
	statusEndOfFile            uint32 = 0xc0000011
)

func fileCommandStatus(err error) uint32 {
	if err == nil {
		return statusOK
	}
	errno := storage.ErrnoOf(err)
	if errno == syscall.EIO {
		return statusIO
	}
	if errno == syscall.EAGAIN {
		if errors.Is(err, storage.ErrRangeConflict) {
			return 0xc0000054
		}
		if errors.Is(err, storage.ErrUseConflict) {
			return 0xc0000043
		}
	}
	if errno == syscall.EBUSY && errors.Is(err, storage.ErrPendingDelete) {
		return 0xc0000056
	}
	switch errno {
	case syscall.EBADF, syscall.ESTALE:
		return statusFileClosed
	case syscall.EDQUOT:
		return 0xc0000802
	case syscall.ENOSPC:
		return 0xc000007f
	case syscall.EAGAIN:
		return 0xc000022d
	case syscall.EISDIR:
		return statusInvalidDeviceRequest
	case syscall.EROFS:
		return 0xc00000a2
	default:
		return statusError(errno)
	}
}

type fileAuthorizationError struct {
	cause          error
	classification syscall.Errno
}

func (e *fileAuthorizationError) Error() string         { return e.cause.Error() }
func (e *fileAuthorizationError) Unwrap() error         { return e.cause }
func (e *fileAuthorizationError) Classification() error { return e.classification }

func (c *connection) authorizeFileOperation(ctx context.Context, t *tree, operation storage.Operation) error {
	err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation})
	if err != nil {
		errno := syscall.EIO
		if errors.Is(err, authz.ErrDenied) {
			errno = syscall.EACCES
		}
		return &fileAuthorizationError{cause: err, classification: errno}
	}
	return nil
}

func reserveFileCommandResult(ctx context.Context, t *tree, state bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
	if err != nil {
		return nil, err
	}
	charge += 512
	if state {
		charge += storage.MaxLinkTargetBytes + storage.MaxObservationTokenBytes
	}
	r := t.files
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return nil, syscall.EBADF
	}
	if charge > r.limits.MaxDirectoryBytes-r.resultBytes {
		return nil, syscall.ENOMEM
	}
	r.resultBytes += charge
	return func() { r.mu.Lock(); r.resultBytes -= charge; r.mu.Unlock() }, nil
}
