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
	statusInvalidHandle    uint32 = 0xc0000008
	statusNameNotFound     uint32 = 0xc0000034
	statusNameCollision    uint32 = 0xc0000035
	statusSharingViolation uint32 = 0xc0000043
	statusDeletePending    uint32 = 0xc0000056
	statusFileIsDirectory  uint32 = 0xc00000ba
	statusNotADirectory    uint32 = 0xc0000103
	statusRetry            uint32 = 0xc000022d
)

func createStatusError(err error) uint32 {
	if errors.Is(err, errNameInvalid) || errors.Is(err, errPathMissing) {
		return namespaceStatus(err)
	}
	if errors.Is(err, authz.ErrDenied) || errors.Is(err, ErrIdentityDenied) {
		return statusDenied
	}
	errno := storage.ErrnoOf(err)
	switch {
	case err == nil:
		return statusOK
	case errno == syscall.EIO:
		return statusIO
	case errors.Is(err, storage.ErrInvalidScope):
		return statusRetry
	case errors.Is(err, storage.ErrUseConflict):
		return statusSharingViolation
	case errors.Is(err, storage.ErrPendingDelete):
		return statusDeletePending
	case errors.Is(err, storage.ErrConditionConflict), errno == syscall.EAGAIN, errno == syscall.EBUSY:
		return statusRetry
	case errno == syscall.ENOENT:
		return statusNameNotFound
	case errno == syscall.EEXIST:
		return statusNameCollision
	case errno == syscall.EISDIR:
		return statusFileIsDirectory
	case errno == syscall.ENOTDIR:
		return statusNotADirectory
	case errno == syscall.EBADF:
		return statusInvalidHandle
	default:
		return statusError(err)
	}
}

func closeStatusError(err error) uint32 {
	switch storage.ErrnoOf(err) {
	case syscall.EBADF, syscall.ENOENT:
		return statusInvalidHandle
	default:
		return statusError(err)
	}
}
