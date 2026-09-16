package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *clientFile) ValidateNotificationLocation(ctx context.Context, location storage.EntryLocation) error {
	if err := f.require(windowsReadData); err != nil {
		return err
	}
	err := f.session.validateLocation(ctx, location)
	if err == nil {
		return nil
	}
	var cleanup *clientCleanupError
	if errors.As(err, &cleanup) || errors.Is(err, syscall.EIO) {
		return errors.Join(syscall.EIO, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, authz.ErrDenied) {
		return err
	}
	switch storage.ErrnoOf(err) {
	case syscall.EAGAIN, syscall.ESTALE, syscall.ENOENT, syscall.EEXIST, syscall.EINVAL, syscall.ENAMETOOLONG, syscall.EFBIG:
		return errors.Join(ErrNotifyRescan, err)
	default:
		return err
	}
}
