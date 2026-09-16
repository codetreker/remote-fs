package storage

import (
	"math"
	"syscall"
	"time"
)

// FileServiceOptions bounds aggregate authority state across all file sessions
// and access paths in a volume. The zero value is invalid.
type FileServiceOptions struct {
	MaxSessions, MaxActions, MaxPrepared, MaxDrains                      int
	MaxClaims, MaxOwners, MaxRanges, MaxOwnerRanges, MaxSetRanges        int
	MaxSnapshotRanges, MaxWaits, MaxWaitRanges, MaxDependencies, MaxWork int
	MaxMaterializedBytes                                                 int64
	MaxMaterializations                                                  int
	MaxFileBytes                                                         int64
	MaxFileAttempts                                                      int
	FileOperationTimeout                                                 time.Duration
}

func DefaultFileServiceOptions() FileServiceOptions {
	return FileServiceOptions{
		MaxSessions: 1024, MaxActions: 262144, MaxPrepared: 65536, MaxDrains: 65536,
		MaxClaims: 65536, MaxOwners: 32768, MaxRanges: 262144, MaxOwnerRanges: 8192, MaxSetRanges: 8192,
		MaxSnapshotRanges: 262144, MaxWaits: 8192, MaxWaitRanges: 65536, MaxDependencies: 65536, MaxWork: 1 << 20,
		MaxMaterializedBytes: 2 << 30, MaxMaterializations: 32, MaxFileBytes: 1 << 30, MaxFileAttempts: 8, FileOperationTimeout: 30 * time.Second,
	}
}

func (o FileServiceOptions) Check() error {
	for _, n := range []int{o.MaxSessions, o.MaxActions, o.MaxPrepared, o.MaxDrains, o.MaxClaims, o.MaxOwners, o.MaxRanges, o.MaxOwnerRanges, o.MaxSetRanges, o.MaxSnapshotRanges, o.MaxWaitRanges, o.MaxWork, o.MaxMaterializations, o.MaxFileAttempts} {
		if n <= 0 || n == math.MaxInt {
			return syscall.EINVAL
		}
	}
	if o.MaxSnapshotRanges > MaxRangeSnapshotRanges {
		return syscall.EINVAL
	}
	if o.MaxWaits < 0 || o.MaxWaits == math.MaxInt || o.MaxDependencies < 0 || o.MaxDependencies == math.MaxInt || o.MaxOwnerRanges > o.MaxRanges || o.MaxSetRanges > o.MaxOwnerRanges {
		return syscall.EINVAL
	}
	if o.MaxMaterializedBytes <= 0 || o.MaxFileBytes <= 0 || o.MaxFileBytes > int64(math.MaxInt) || o.MaxFileBytes > o.MaxMaterializedBytes/2 || o.FileOperationTimeout <= 0 {
		return syscall.EINVAL
	}
	return nil
}
