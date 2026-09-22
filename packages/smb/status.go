package smb

import (
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusInvalidDeviceRequest uint32 = 0xc0000010
	statusEndOfFile            uint32 = 0xc0000011
	statusFileClosed           uint32 = 0xc0000128
	statusObjectNameInvalid    uint32 = 0xc0000033
	statusObjectNameNotFound   uint32 = 0xc0000034
	statusObjectNameCollision  uint32 = 0xc0000035
	statusObjectPathNotFound   uint32 = 0xc000003a
	statusSharingViolation     uint32 = 0xc0000043
	statusFileLockConflict     uint32 = 0xc0000054
	statusDeletePending        uint32 = 0xc0000056
	statusDiskFull             uint32 = 0xc000007f
	statusMediaWriteProtected  uint32 = 0xc00000a2
	statusFileIsADirectory     uint32 = 0xc00000ba
	statusDirectoryNotEmpty    uint32 = 0xc0000101
	statusNotADirectory        uint32 = 0xc0000103
	statusQuotaExceeded        uint32 = 0xc0000802
	statusRetry                uint32 = 0xc000022d
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
	case syscall.ENOENT:
		return statusObjectNameNotFound
	case syscall.ENOTDIR:
		return statusNotADirectory
	default:
		return statusIO
	}
}

func namespaceStatus(err error) uint32 {
	switch {
	case errors.Is(err, errNameInvalid):
		return statusObjectNameInvalid
	case errors.Is(err, errPathMissing):
		return statusObjectPathNotFound
	default:
		return statusError(err)
	}
}

func fileCommandStatus(err error) uint32 {
	if err == nil {
		return statusOK
	}
	if storage.ErrnoOf(err) == syscall.EIO {
		return statusIO
	}
	if errors.Is(err, storage.ErrRangeConflict) {
		return statusFileLockConflict
	}
	if errors.Is(err, storage.ErrUseConflict) {
		return statusSharingViolation
	}
	if errors.Is(err, storage.ErrPendingDelete) {
		return statusDeletePending
	}
	switch storage.ErrnoOf(err) {
	case syscall.EBADF, syscall.ESTALE:
		return statusFileClosed
	case syscall.EDQUOT:
		return statusQuotaExceeded
	case syscall.ENOSPC:
		return statusDiskFull
	case syscall.EAGAIN:
		return statusRetry
	case syscall.EISDIR:
		return statusInvalidDeviceRequest
	case syscall.ENOTEMPTY:
		return statusDirectoryNotEmpty
	case syscall.EROFS:
		return statusMediaWriteProtected
	default:
		return statusError(err)
	}
}
