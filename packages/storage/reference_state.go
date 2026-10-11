package storage

import "syscall"

func (p StateProjection) Check() error {
	if p != StateFull && p != StateScalars {
		return syscall.EINVAL
	}
	return nil
}

// CheckProjection validates query results. Mutation results can contain partial
// cleanup facts on failure and retain their separate validation contract.
func (r ReferenceState) CheckProjection(nodeID uint64, projection StateProjection) error {
	if err := projection.Check(); err != nil {
		return err
	}
	if nodeID == 0 || r.Attr.ID != nodeID || r.Attr.Kind.Check() != nil || r.Attr.Kind != NodeDirectory && r.Attr.Size < 0 {
		return syscall.EIO
	}
	if err := r.Attr.CheckAllocation(); err != nil {
		return err
	}
	if projection == StateScalars {
		if r.Attr.Metadata != nil || r.Attr.BirthTime != nil || r.Attr.ChangeTime != nil ||
			!r.Attr.AccessTime.IsZero() || !r.Attr.ModTime.IsZero() || r.LinkTarget != nil || r.PendingGeneration != nil {
			return syscall.EIO
		}
		return nil
	}
	if len(r.PendingGeneration) > MaxObservationTokenBytes || r.PendingUnlink && len(r.PendingGeneration) == 0 ||
		!r.PendingUnlink && len(r.PendingGeneration) != 0 {
		return syscall.EIO
	}
	if r.Attr.Kind == NodeSymlink {
		if len(r.LinkTarget) == 0 || len(r.LinkTarget) > MaxLinkTargetBytes || int64(len(r.LinkTarget)) != r.Attr.Size {
			return syscall.EIO
		}
	} else if len(r.LinkTarget) != 0 {
		return syscall.EIO
	}
	if err := CheckMetadata(r.Attr.Metadata); err != nil {
		return syscall.EIO
	}
	return nil
}
