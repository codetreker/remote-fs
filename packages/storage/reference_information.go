package storage

import (
	"context"
	"syscall"
)

// ReferenceInformationAccess captures scalar facts through the exact live
// reference in one authoritative snapshot. It grants no metadata permissions.
// Current authorization and reference lifetime apply; every error returns zero.
type ReferenceInformationAccess interface {
	CheckReferenceInformation() error
	Information(context.Context) (ReferenceInformation, error)
}

type ReferenceInformation struct {
	NodeID uint64
	Kind   NodeKind
	// Size is the content or symlink-target length; it is unspecified for directories.
	Size, AllocationSize    int64
	AllocationKnown         bool
	Detached, PendingUnlink bool
}

func (r ReferenceInformation) Check(nodeID uint64) error {
	if nodeID == 0 || r.NodeID != nodeID || r.Kind.Check() != nil || r.Kind != NodeDirectory && r.Size < 0 {
		return syscall.EIO
	}
	if r.AllocationSize < 0 || !r.AllocationKnown && r.AllocationSize != 0 {
		return syscall.EIO
	}
	return nil
}

// VolumeGeometryAccess reports an authoritative logical allocation geometry.
// Missing support and failed verification remain errors. The returned volume
// must match the owning session's BackendIdentity; host disk facts are unrelated.
type VolumeGeometryAccess interface {
	CheckVolumeGeometry() error
	VolumeGeometry(context.Context) (VolumeGeometry, error)
}

type VolumeGeometry struct {
	Volume                   VolumeID
	BytesPerSector           uint32
	SectorsPerAllocationUnit uint32
}

func (g VolumeGeometry) Check() error {
	if !canonicalFileIdentity(string(g.Volume)) || g.BytesPerSector == 0 || g.SectorsPerAllocationUnit == 0 {
		return syscall.EINVAL
	}
	return nil
}

func (g VolumeGeometry) CheckVolume(volume VolumeID) error {
	if err := g.Check(); err != nil {
		return err
	}
	if volume == "" || g.Volume != volume {
		return syscall.EIO
	}
	return nil
}

func (g VolumeGeometry) AllocationUnitBytes() (uint64, error) {
	if err := g.Check(); err != nil {
		return 0, err
	}
	return uint64(g.BytesPerSector) * uint64(g.SectorsPerAllocationUnit), nil
}
