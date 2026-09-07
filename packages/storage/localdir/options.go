package localdir

import (
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
)

// Config binds an ordinary namespace to its private lease evidence and staging area.
// Root and StateRoot must already exist as disjoint directories on the same mount.
// Init provisions a new binding; Open requires the matching existing binding.
type Config struct {
	Root      string
	StateRoot string
	Locks     locking.Options
	Limits    Limits
}

// Limits bounds native resources independently of the authority's control records.
// Zero fields select DefaultLimits. Snapshot bounds cover immutable directory metadata;
// caller result accounting runs after capture and retains its own independent bound.
type Limits struct {
	MaxOperations      int
	MaxWaiters         int
	MaxPinnedTargets   int
	MaxStagingBytes    int64
	MaxSnapshotBytes   int64
	MaxSnapshotEntries int
	MaxRecoveryEntries int
	MaxPathBytes       int
}

func DefaultLimits() Limits {
	return Limits{
		MaxOperations: 128, MaxWaiters: 256, MaxPinnedTargets: 4096,
		MaxStagingBytes: 1 << 30, MaxSnapshotBytes: 64 << 20,
		MaxSnapshotEntries: 100_000, MaxRecoveryEntries: 1024, MaxPathBytes: 4096,
	}
}

func (l Limits) Effective() (Limits, error) {
	d := DefaultLimits()
	for _, pair := range []struct{ value, fallback *int }{
		{&l.MaxOperations, &d.MaxOperations}, {&l.MaxWaiters, &d.MaxWaiters},
		{&l.MaxPinnedTargets, &d.MaxPinnedTargets}, {&l.MaxSnapshotEntries, &d.MaxSnapshotEntries},
		{&l.MaxRecoveryEntries, &d.MaxRecoveryEntries}, {&l.MaxPathBytes, &d.MaxPathBytes},
	} {
		if *pair.value == 0 {
			*pair.value = *pair.fallback
		}
		if *pair.value < 1 || *pair.value == math.MaxInt {
			return Limits{}, fmt.Errorf("local directory resource limits must be positive and finite: %w", syscall.EINVAL)
		}
	}
	for _, pair := range []struct{ value, fallback *int64 }{
		{&l.MaxStagingBytes, &d.MaxStagingBytes}, {&l.MaxSnapshotBytes, &d.MaxSnapshotBytes},
	} {
		if *pair.value == 0 {
			*pair.value = *pair.fallback
		}
		if *pair.value < 1 || *pair.value == math.MaxInt64 {
			return Limits{}, fmt.Errorf("local directory byte limits must be positive and finite: %w", syscall.EINVAL)
		}
	}
	return l, nil
}
