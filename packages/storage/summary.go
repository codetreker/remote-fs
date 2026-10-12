package storage

import (
	"context"
	"syscall"
)

// VolumeSummary captures named-node counters and allocation policy in one
// authoritative read. Implementations must verify the complete backing chain
// before offering this capability; walking a tree is not a summary snapshot.
type VolumeSummary interface {
	CheckVolumeSummary() error
	Summary(context.Context) (VolumeSummaryResult, error)
}

// VolumeSummaryResult counts regular files and symbolic links as files, and
// excludes the root from DirectoryCount. Detached nodes are absent from both
// counters but remain in AllocationUsed while their allocation is retained.
// QuotaRevision identifies the captured policy, independently of tree changes.
type VolumeSummaryResult struct {
	FileCount      uint64
	DirectoryCount uint64
	AllocationUsed int64
	QuotaBytes     int64
	QuotaRevision  uint64
}

// Check permits a lowered quota beneath existing allocation. Negative byte
// counts or an absent policy revision cannot represent an authoritative result.
func (s VolumeSummaryResult) Check() error {
	if s.AllocationUsed < 0 || s.QuotaBytes < 0 || s.QuotaRevision == 0 {
		return syscall.EINVAL
	}
	return nil
}
