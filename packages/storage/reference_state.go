package storage

import "syscall"

func (p StateProjection) Check() error {
	if p != StateFull && p != StateScalars {
		return syscall.EINVAL
	}
	return nil
}

// CheckProjection validates query identity and scalar bounds and privacy. Full
// payloads and mutation results retain their entry-specific validation contracts.
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
			!r.Attr.AccessTime.IsZero() || !r.Attr.ModTime.IsZero() || r.LinkTarget != nil ||
			len(r.PendingGeneration) > MaxObservationTokenBytes || r.PendingUnlink != (len(r.PendingGeneration) != 0) {
			return syscall.EIO
		}
		return nil
	}
	return nil
}
