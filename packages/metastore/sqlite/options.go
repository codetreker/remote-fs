package sqlite

import (
	"fmt"
	"math"
	"syscall"
)

const (
	// DefaultMaxReaderConnections bounds the physical SQLite connections used by concurrent
	// namespace reads when the caller supplies no limit of its own.
	DefaultMaxReaderConnections = 16

	// DefaultMaxSnapshotReaderConnections matches the default number of snapshot frames the
	// reference HTTP server can produce concurrently.
	DefaultMaxSnapshotReaderConnections = 16

	// DefaultMaxIntegrityRecords bounds the namespace, node, object, entry, log, and change rows
	// examined by one integrity pass when the caller supplies no limit of its own.
	DefaultMaxIntegrityRecords int64 = 1_000_000

	// DefaultMaxIntegrityBytes bounds variable-length namespace and log fields examined by
	// one full integrity pass.
	DefaultMaxIntegrityBytes int64 = 64 << 20

	// MinIntegrityRecords is the namespace row, root node, and log row every usable namespace
	// contains.
	MinIntegrityRecords int64 = 3
)

// Options configures the serving resources of one Store.
type Options struct {
	leaseRecoveryOwner       bool
	leaseOwner               *leaseDatabaseFile
	requireExistingNamespace bool
	Window                   Window
	ObjectLimits             ObjectLimits

	// MaxReaderConnections bounds the physical SQLite connections used by ordinary namespace
	// and log reads. Zero selects DefaultMaxReaderConnections. A read waits for a connection
	// when the pool is full and observes its context while waiting.
	MaxReaderConnections int

	// MaxSnapshotReaderConnections bounds the separate SQLite pool held by long-lived
	// snapshots. Zero selects DefaultMaxSnapshotReaderConnections.
	MaxSnapshotReaderConnections int

	// MaxIntegrityRecords bounds the namespace, node, object, entry, log, and change rows
	// accepted by an integrity pass. A larger retained namespace returns syscall.EFBIG before
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
		MaxReaderConnections:         DefaultMaxReaderConnections,
		MaxSnapshotReaderConnections: DefaultMaxSnapshotReaderConnections,
		MaxIntegrityRecords:          DefaultMaxIntegrityRecords,
		MaxIntegrityBytes:            DefaultMaxIntegrityBytes,
	}
}

// Effective resolves zero-valued defaults and validates Options without opening a database.
func (o Options) Effective() (Options, error) {
	if err := o.Window.check(); err != nil {
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
