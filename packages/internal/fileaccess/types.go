// Package fileaccess coordinates bounded access claims and owner-held ranges.
// Native callers must retain their publication ordering from an admission check
// through its effect. Session expiry and resource liveness are owned by that caller.
package fileaccess

import (
	"errors"
	"math"
)

var (
	ErrInvalid      = errors.New("invalid access coordination request")
	ErrConflict     = errors.New("access coordination conflict")
	ErrUnknownClaim = errors.New("unknown access claim")
	ErrAccess       = errors.New("access exceeds the retained claim")
	ErrCapacity     = errors.New("access coordination capacity exhausted")
	ErrRevision     = errors.New("access coordination revision changed")
	ErrDeadlock     = errors.New("access coordination dependency cycle")
	ErrClosed       = errors.New("access coordinator is closed")
	ErrExhausted    = errors.New("access coordination revision exhausted")
	ErrCanceled     = errors.New("access coordination wait canceled")
	ErrRetired      = errors.New("access coordination owner retired")
)

type Owner struct{ Session, ID uint64 }
type Claim struct{ Uses, Excludes uint64 }

// Guard revalidates a native caller's authority after admission work and before
// publication. It runs under the coordinator mutex and must not perform I/O or
// reenter the coordinator. Nil denotes a caller with no additional authority check.
type Guard func() error

func (g Guard) check() error {
	if g == nil {
		return nil
	}
	return g()
}

// Enforced ranges share one conflict domain. Advisory domains are opaque
// identifiers and do not participate in ordinary I/O admission.
type Scope struct {
	Resource, Domain uint64
	Enforced         bool
}

// Acquisition preserves one independently owned range, including duplicates of
// another acquisition's span. ID is unique within an owned set.
type Acquisition struct {
	ID, Start, End      uint64
	Boundary, Exclusive bool
}

type Held struct {
	Owner       Owner
	Acquisition Acquisition
}

type Snapshot struct {
	Revision                  uint64
	Own                       []Acquisition
	Other                     []Held
	Available, OwnerAvailable int
}

type Conflict struct{ Held Held }

func (c *Conflict) Error() string { return ErrConflict.Error() }
func (c *Conflict) Unwrap() error { return ErrConflict }

type Limits struct {
	MaxClaims, MaxOwners, MaxRanges, MaxOwnerRanges, MaxSetRanges        int
	MaxSnapshotRanges, MaxWaits, MaxWaitRanges, MaxDependencies, MaxWork int
}

func DefaultLimits() Limits {
	return Limits{MaxClaims: 65536, MaxOwners: 32768, MaxRanges: 262144,
		MaxOwnerRanges: 8192, MaxSetRanges: 8192, MaxSnapshotRanges: 262144,
		MaxWaits: 8192, MaxWaitRanges: 65536, MaxDependencies: 65536, MaxWork: 1 << 20}
}

func (l Limits) Check() error {
	for _, value := range []int{l.MaxClaims, l.MaxOwners, l.MaxRanges, l.MaxOwnerRanges, l.MaxSetRanges, l.MaxSnapshotRanges, l.MaxWaitRanges, l.MaxWork} {
		if value <= 0 || value == math.MaxInt {
			return ErrInvalid
		}
	}
	if l.MaxWaits < 0 || l.MaxWaits == math.MaxInt || l.MaxDependencies < 0 || l.MaxDependencies == math.MaxInt ||
		l.MaxOwnerRanges > l.MaxRanges || l.MaxSetRanges > l.MaxOwnerRanges {
		return ErrInvalid
	}
	return nil
}

func (o Owner) valid() bool { return o.Session != 0 }
func (s Scope) valid() bool { return s.Resource != 0 && (!s.Enforced || s.Domain == 0) }

// A boundary at N intersects an ordinary interval only when it starts below N
// and ends at or above N. Boundaries never intersect each other.
type Span struct {
	Start, End uint64
	Boundary   bool
}

func (s Span) valid() bool { return s.Start <= s.End && (!s.Boundary || s.Start == s.End) }
func overlap(a, b Acquisition) bool {
	if a.Boundary && b.Boundary {
		return false
	}
	if a.Boundary {
		return b.Start < a.Start && a.Start <= b.End
	}
	if b.Boundary {
		return a.Start < b.Start && b.Start <= a.End
	}
	return a.Start <= b.End && b.Start <= a.End
}
