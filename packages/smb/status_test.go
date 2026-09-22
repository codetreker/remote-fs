package smb

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileAndNamespaceStatusPreserveWindowsDistinctions(t *testing.T) {
	for _, test := range []struct {
		err  error
		want uint32
	}{
		{syscall.EBADF, statusFileClosed}, {syscall.ENOENT, statusObjectNameNotFound},
		{syscall.ENOTDIR, statusNotADirectory}, {syscall.ENOSPC, statusDiskFull},
		{syscall.EDQUOT, statusQuotaExceeded}, {syscall.EROFS, statusMediaWriteProtected},
		{syscall.ENOTEMPTY, statusDirectoryNotEmpty},
		{syscall.EAGAIN, statusRetry}, {storage.ErrUseConflict, statusSharingViolation},
		{storage.ErrRangeConflict, statusFileLockConflict}, {storage.ErrPendingDelete, statusDeletePending},
	} {
		if got := fileCommandStatus(test.err); got != test.want {
			t.Fatalf("file status for %v = %#x, want %#x", test.err, got, test.want)
		}
	}
	if got := namespaceStatus(errNameInvalid); got != statusObjectNameInvalid {
		t.Fatalf("invalid name status = %#x", got)
	}
	if got := namespaceStatus(errPathMissing); got != statusObjectPathNotFound {
		t.Fatalf("missing path status = %#x", got)
	}
	if got := fileCommandStatus(errors.Join(syscall.EIO, syscall.EINTR)); got != statusIO {
		t.Fatalf("unknown result status = %#x", got)
	}
}
