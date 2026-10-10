package smb

import (
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func statusError(err error) uint32 {
	if err == nil {
		return statusOK
	}
	if errors.Is(err, authz.ErrDenied) || errors.Is(err, ErrIdentityDenied) {
		return statusDenied
	}
	switch storage.ErrnoOf(err) {
	case syscall.EINTR:
		return statusCancelled
	case syscall.EINVAL:
		return statusInvalid
	case syscall.EACCES, syscall.EPERM:
		return statusDenied
	case syscall.ENOMEM, syscall.ENOSPC, syscall.EMFILE, syscall.EFBIG, syscall.EAGAIN:
		return statusResources
	case syscall.EOPNOTSUPP, syscall.ENOSYS:
		return statusUnsupported
	case syscall.ESTALE:
		return statusSessionDeleted
	default:
		return statusIO
	}
}

const (
	statusSharingViolation uint32 = 0xc0000043
	statusRetry            uint32 = 0xc000022d
	statusDeletePending    uint32 = 0xc0000056
	statusNameNotFound     uint32 = 0xc0000034
	statusNameCollision    uint32 = 0xc0000035
	statusFileIsDirectory  uint32 = 0xc00000ba
	statusNotADirectory    uint32 = 0xc0000103
	statusInvalidHandle    uint32 = 0xc0000008
)

func createStatusError(err error) uint32 {
	if err == nil {
		return statusOK
	}
	if errors.Is(err, syscall.EIO) {
		return statusIO
	}
	var unknown *unknownNamespaceError
	if errors.As(err, &unknown) {
		return statusIO
	}
	switch {
	case errors.Is(err, storage.ErrUseConflict):
		return statusSharingViolation
	case errors.Is(err, storage.ErrConditionConflict), errors.Is(err, storage.ErrInvalidScope):
		return statusRetry
	case errors.Is(err, storage.ErrPendingDelete):
		return statusDeletePending
	case errors.Is(err, errPathMissing), errors.Is(err, errNameInvalid):
		return namespaceStatus(err)
	}
	switch storage.ErrnoOf(err) {
	case syscall.ENOENT:
		return statusNameNotFound
	case syscall.EEXIST:
		return statusNameCollision
	case syscall.EISDIR:
		return statusFileIsDirectory
	case syscall.ENOTDIR:
		return statusNotADirectory
	case syscall.EBADF:
		return statusInvalidHandle
	case syscall.EBUSY:
		return statusRetry
	default:
		return statusError(err)
	}
}

func closeStatusError(err error) uint32 {
	if errors.Is(err, syscall.EIO) {
		return statusIO
	}
	if errors.Is(err, syscall.EBADF) || errors.Is(err, syscall.ENOENT) {
		return statusInvalidHandle
	}
	return statusError(err)
}
