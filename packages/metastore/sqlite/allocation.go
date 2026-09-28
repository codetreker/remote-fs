package sqlite

import (
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

const allocationUnit int64 = 4096

func allocatedSize(kind storage.NodeKind, size int64) (int64, error) {
	if size < 0 {
		return 0, syscall.EINVAL
	}
	if kind != storage.NodeRegular || size == 0 {
		return 0, nil
	}
	if size > math.MaxInt64-(allocationUnit-1) {
		return 0, syscall.EOVERFLOW
	}
	return ((size + allocationUnit - 1) / allocationUnit) * allocationUnit, nil
}
