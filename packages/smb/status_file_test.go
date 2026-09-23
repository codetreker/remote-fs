package smb

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCreateStatusError(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status uint32
	}{
		{"success", nil, statusOK},
		{"sharing", storage.ErrUseConflict, statusSharingViolation},
		{"guard", storage.ErrConditionConflict, statusRetry},
		{"scope changed", storage.ErrInvalidScope, statusRetry},
		{"pending delete", storage.ErrPendingDelete, statusDeletePending},
		{"missing name", syscall.ENOENT, statusNameNotFound},
		{"missing path", errPathMissing, 0xc000003a},
		{"invalid name", errNameInvalid, 0xc0000033},
		{"name collision", syscall.EEXIST, statusNameCollision},
		{"directory target", syscall.EISDIR, statusFileIsDirectory},
		{"file component", syscall.ENOTDIR, statusNotADirectory},
		{"invalid reference", syscall.EBADF, statusInvalidHandle},
		{"busy", syscall.EBUSY, statusRetry},
		{"unknown observation", &unknownNamespaceError{cause: syscall.ENOENT}, statusIO},
		{"joined uncertainty", errors.Join(storage.ErrUseConflict, syscall.EIO), statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.err
			if err != nil {
				err = fmt.Errorf("create: %w", err)
			}
			if got := createStatusError(err); got != test.status {
				t.Fatalf("status = %#x, want %#x", got, test.status)
			}
		})
	}
}

func TestCloseStatusError(t *testing.T) {
	for _, test := range []struct {
		err    error
		status uint32
	}{
		{nil, statusOK},
		{syscall.EBADF, statusInvalidHandle},
		{syscall.ENOENT, statusInvalidHandle},
		{syscall.EIO, statusIO},
		{errors.Join(syscall.EBADF, syscall.EIO), statusIO},
	} {
		if got := closeStatusError(test.err); got != test.status {
			t.Fatalf("close status for %v = %#x, want %#x", test.err, got, test.status)
		}
	}
}
