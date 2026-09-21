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
	if errors.Is(err, authz.ErrDenied) {
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
