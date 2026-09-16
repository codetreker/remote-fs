package storage

import "syscall"

type RangeOwnerID uint64
type RangeDomainID uint64
type RangeAcquisitionID uint64

type RangeOwner struct {
	Session string
	ID      RangeOwnerID
}
type RangeScope struct {
	Domain   RangeDomainID
	Enforced bool
}

func (s RangeScope) Check() error {
	if s.Enforced && s.Domain != 0 {
		return syscall.EINVAL
	}
	return nil
}

// Boundary at N intersects an ordinary inclusive [a,b] only when a < N && N <= b.
// Two boundaries never intersect; boundary zero is nonintersecting. Distinct
// acquisition IDs remain separate even when their regions are identical.
type RangeAcquisition struct {
	Boundary   bool
	ID         RangeAcquisitionID
	Start, End uint64
	Exclusive  bool
}

func (r RangeAcquisition) Check() error {
	if r.ID == 0 || (!r.Boundary && r.Start > r.End) || (r.Boundary && r.Start != r.End) {
		return syscall.EINVAL
	}
	return nil
}

type HeldRange struct {
	Owner RangeOwner
	Range RangeAcquisition
}

type RangeSnapshot struct {
	Revision       uint64
	Own            []RangeAcquisition
	Other          []HeldRange
	Available      int
	OwnerAvailable int
}

type RangeReplaceRequest struct {
	Owner            RangeOwnerID
	Scope            RangeScope
	ExpectedRevision uint64
	Ranges           []RangeAcquisition
}

type RangeWaitRequest struct {
	Owner            RangeOwnerID
	Scope            RangeScope
	ExpectedRevision uint64
	Ranges           []RangeAcquisition
	DetectDeadlock   bool
}

const MaxRangeAcquisitions = 65536

// MaxRangeSnapshotRanges bounds Own plus Other before an authority allocates
// a snapshot, independently of each replacement request's smaller bound.
const MaxRangeSnapshotRanges = 262144
const MaxFileSessionIDBytes = 128

func (r RangeReplaceRequest) Check() error {
	if err := r.Scope.Check(); err != nil {
		return err
	}
	if r.ExpectedRevision == 0 {
		return syscall.EINVAL
	}
	if len(r.Ranges) > MaxRangeAcquisitions {
		return syscall.ENOLCK
	}
	ids := make(map[RangeAcquisitionID]bool, len(r.Ranges))
	for _, item := range r.Ranges {
		if err := item.Check(); err != nil {
			return err
		}
		if ids[item.ID] {
			return syscall.EINVAL
		}
		ids[item.ID] = true
	}
	return nil
}

func (r RangeWaitRequest) Check() error {
	return (RangeReplaceRequest{Owner: r.Owner, Scope: r.Scope, ExpectedRevision: r.ExpectedRevision, Ranges: r.Ranges}).Check()
}
