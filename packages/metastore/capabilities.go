package metastore

import (
	"context"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type FileAccess struct {
	Uses           storage.Uses
	Offset, Length int64
	Truncate       bool
	Append         bool
	Size           int64
}

func (a FileAccess) Check() error {
	if a.Uses != storage.ReadData && a.Uses != storage.WriteData {
		return syscall.EINVAL
	}
	if a.Offset < 0 || a.Length < 0 || a.Length > math.MaxInt64-a.Offset || a.Size < 0 {
		return syscall.EINVAL
	}
	if a.Append && (a.Uses != storage.WriteData || a.Truncate || a.Offset != 0 || a.Size != 0) {
		return syscall.EINVAL
	}
	if a.Truncate {
		if a.Uses != storage.WriteData || a.Offset != 0 || a.Length != 0 {
			return syscall.EINVAL
		}
	} else if a.Size != 0 {
		return syscall.EINVAL
	}
	return nil
}

type fileAccessKey struct{}

func WithFileAccess(ctx context.Context, access FileAccess) context.Context {
	return context.WithValue(ctx, fileAccessKey{}, access)
}

func FileAccessFrom(ctx context.Context) (FileAccess, bool) {
	access, ok := ctx.Value(fileAccessKey{}).(FileAccess)
	return access, ok
}
