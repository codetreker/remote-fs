package sqlite

import (
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

const (
	DefaultMaxRetainedFiles = 65536

	// DefaultMaxReaderConnections bounds the physical SQLite connections used by concurrent
	// volume reads when the caller supplies no limit of its own.
	DefaultMaxReaderConnections = 16

	// DefaultMaxSnapshotReaderConnections matches the default number of snapshot frames the
	// reference HTTP server can produce concurrently.
	DefaultMaxSnapshotReaderConnections = 16

	// DefaultMaxIntegrityRecords bounds the volume, node, object, entry, log, and change rows
	// examined by one integrity pass when the caller supplies no limit of its own.
	DefaultMaxIntegrityRecords int64 = 1_000_000

	// DefaultMaxIntegrityBytes bounds variable-length volume and log fields examined by
	// one full integrity pass.
	DefaultMaxIntegrityBytes int64 = 64 << 20

	// MinIntegrityRecords is the volume row, root node, and log row every usable volume
	// contains.
	MinIntegrityRecords int64 = 3
)

// Options configures the serving resources of one Store.
type Options struct {
	leaseRecoveryOwner    bool
	leaseOwner            *nativelease.Database
	requireExistingVolume bool
	Window                Window
	ObjectLimits          ObjectLimits

	// MaxRetainedFiles bounds native file references across this volume. Zero
	// selects DefaultMaxRetainedFiles; admission exhaustion returns EAGAIN.
	MaxRetainedFiles int
	// Advisory bounds volume-wide lock and materialization state. The zero
	// configuration selects advisory.DefaultConfig; shared opens must agree.
	Advisory advisory.Config

	// MaxReaderConnections bounds the physical SQLite connections used by ordinary volume
	// and log reads. Zero selects DefaultMaxReaderConnections. A read waits for a connection
	// when the pool is full and observes its context while waiting.
	MaxReaderConnections int

	// MaxSnapshotReaderConnections bounds the separate SQLite pool held by long-lived
	// snapshots. Zero selects DefaultMaxSnapshotReaderConnections.
	MaxSnapshotReaderConnections int

	// MaxIntegrityRecords bounds the volume, node, object, entry, log, and change rows
	// accepted by an integrity pass. A larger retained volume returns syscall.EFBIG before
	// recursive traversal or row validation. Zero selects DefaultMaxIntegrityRecords.
	MaxIntegrityRecords int64

	// MaxIntegrityBytes bounds the combined entry and retained-change name bytes examined by
	// a full integrity pass before content-sensitive validation. Zero selects
	// DefaultMaxIntegrityBytes.
	MaxIntegrityBytes int64
}

// DefaultOptions returns the default serving configuration.
func DefaultOptions() Options {
	return Options{
		Window:                       DefaultWindow(),
		ObjectLimits:                 DefaultObjectLimits(),
		MaxRetainedFiles:             DefaultMaxRetainedFiles,
		Advisory:                     advisory.DefaultConfig(),
		MaxReaderConnections:         DefaultMaxReaderConnections,
		MaxSnapshotReaderConnections: DefaultMaxSnapshotReaderConnections,
		MaxIntegrityRecords:          DefaultMaxIntegrityRecords,
		MaxIntegrityBytes:            DefaultMaxIntegrityBytes,
	}
}

// Effective resolves zero-valued defaults and validates Options without opening a database.
func (o Options) Effective() (Options, error) {
	if o.Advisory == (advisory.Config{}) {
		o.Advisory = advisory.DefaultConfig()
	}
	if err := o.Advisory.Check(); err != nil {
		return Options{}, err
	}
	if o.MaxRetainedFiles == 0 {
		o.MaxRetainedFiles = DefaultMaxRetainedFiles
	}
	if o.MaxRetainedFiles < 1 || o.MaxRetainedFiles == math.MaxInt {
		return Options{}, fmt.Errorf("the SQLite retained-file limit must be positive and bounded: %w", syscall.EINVAL)
	}
	if err := changes.CheckWindow(changes.Window(o.Window)); err != nil {
		return Options{}, err
	}
	objectLimits, err := o.ObjectLimits.Effective()
	if err != nil {
		return Options{}, err
	}
	maxReaders := o.MaxReaderConnections
	if maxReaders == 0 {
		maxReaders = DefaultMaxReaderConnections
	}
	if maxReaders < 1 {
		return Options{}, fmt.Errorf("the SQLite reader-connection limit must be positive: %w", syscall.EINVAL)
	}
	if maxReaders == math.MaxInt {
		return Options{}, fmt.Errorf("the SQLite reader-connection limit must be bounded below the largest integer: %w", syscall.EINVAL)
	}
	maxSnapshotReaders := o.MaxSnapshotReaderConnections
	if maxSnapshotReaders == 0 {
		maxSnapshotReaders = DefaultMaxSnapshotReaderConnections
	}
	if maxSnapshotReaders < 1 {
		return Options{}, fmt.Errorf("the SQLite snapshot reader-connection limit must be positive: %w", syscall.EINVAL)
	}
	if maxSnapshotReaders == math.MaxInt {
		return Options{}, fmt.Errorf("the SQLite snapshot reader-connection limit must be bounded below the largest integer: %w", syscall.EINVAL)
	}
	maxIntegrityRecords := o.MaxIntegrityRecords
	if maxIntegrityRecords == 0 {
		maxIntegrityRecords = DefaultMaxIntegrityRecords
	}
	if maxIntegrityRecords < MinIntegrityRecords {
		return Options{}, fmt.Errorf("the SQLite integrity work limit must be at least %d: %w",
			MinIntegrityRecords, syscall.EINVAL)
	}
	if maxIntegrityRecords == math.MaxInt64 {
		return Options{}, fmt.Errorf("the SQLite integrity work limit must be bounded below the largest integer: %w", syscall.EINVAL)
	}
	maxIntegrityBytes := o.MaxIntegrityBytes
	if maxIntegrityBytes == 0 {
		maxIntegrityBytes = DefaultMaxIntegrityBytes
	}
	if maxIntegrityBytes < 1 {
		return Options{}, fmt.Errorf("the SQLite integrity byte limit must be positive: %w", syscall.EINVAL)
	}
	if maxIntegrityBytes == math.MaxInt64 {
		return Options{}, fmt.Errorf("the SQLite integrity byte limit must be bounded below the largest integer: %w", syscall.EINVAL)
	}
	o.ObjectLimits = objectLimits
	o.MaxReaderConnections = maxReaders
	o.MaxSnapshotReaderConnections = maxSnapshotReaders
	o.MaxIntegrityRecords = maxIntegrityRecords
	o.MaxIntegrityBytes = maxIntegrityBytes
	return o, nil
}

const (
	// DefaultMaxPendingObjects bounds reserved, unresolved, and garbage object records retained
	// by one volume before new reservations wait for maintenance to make room.
	DefaultMaxPendingObjects int64 = 4096
	// DefaultMaxPendingBytes is the corresponding bound over their recorded payload sizes.
	DefaultMaxPendingBytes int64 = 8 * 1024 * 1024 * 1024
)

// ObjectLimits bounds the combined reserved, unresolved, and garbage object backlog for one
// Store. Reserve returns syscall.EFBIG when one requested object can never fit under
// MaxPendingBytes. A request that fits by itself returns syscall.EAGAIN when the existing
// backlog leaves insufficient room; cleanup may make that request succeed. Commits and
// volume removal remain authoritative and may move an existing backlog over a bound; in
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
