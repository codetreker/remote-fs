package sqlite

import (
	"fmt"
	"math"
	"syscall"
)

const (
	// DefaultMaxPendingObjects bounds reserved, unresolved, and garbage object records retained
	// by one namespace before new reservations wait for maintenance to make room.
	DefaultMaxPendingObjects int64 = 4096
	// DefaultMaxPendingBytes is the corresponding bound over their recorded payload sizes.
	DefaultMaxPendingBytes int64 = 8 * 1024 * 1024 * 1024
)

// ObjectLimits bounds the combined reserved, unresolved, and garbage object backlog for one
// Store. Reserve returns syscall.EFBIG when one requested object can never fit under
// MaxPendingBytes. A request that fits by itself returns syscall.EAGAIN when the existing
// backlog leaves insufficient room; cleanup may make that request succeed. Commits and
// namespace removal remain authoritative and may move an existing backlog over a bound; in
// that state cleanup remains available and ObjectStatus reports OverLimit.
//
// The limits are serving configuration rather than database state, so reopening may choose
// different values. Zero-valued fields select the corresponding exported default; there is no
// unbounded value.
type ObjectLimits struct {
	MaxPendingObjects int64
	MaxPendingBytes   int64
}

// DefaultObjectLimits returns the limits used by Open and OpenBound.
func DefaultObjectLimits() ObjectLimits {
	return ObjectLimits{
		MaxPendingObjects: DefaultMaxPendingObjects,
		MaxPendingBytes:   DefaultMaxPendingBytes,
	}
}

// Validate checks ObjectLimits without opening or modifying a database.
func (l ObjectLimits) Validate() error {
	_, err := l.Effective()
	return err
}

// Effective replaces zero-valued fields with their defaults and validates the result. It lets
// a composite validate relationships between these limits and its other configured bounds.
func (l ObjectLimits) Effective() (ObjectLimits, error) {
	if l.MaxPendingObjects == 0 {
		l.MaxPendingObjects = DefaultMaxPendingObjects
	}
	if l.MaxPendingBytes == 0 {
		l.MaxPendingBytes = DefaultMaxPendingBytes
	}
	if l.MaxPendingObjects < 0 || l.MaxPendingBytes < 0 {
		return ObjectLimits{}, fmt.Errorf("pending object limits must be positive: %w", syscall.EINVAL)
	}
	if l.MaxPendingObjects == math.MaxInt64 || l.MaxPendingBytes == math.MaxInt64 {
		return ObjectLimits{}, fmt.Errorf("pending object limits must be bounded below the largest integer: %w", syscall.EINVAL)
	}
	return l, nil
}
