// Package limited enforces a byte allowance through native publication accounting
// over one bounded volume.
// Startup and explicit Recount use authoritative native usage, including detached
// retained files, or a bounded volume walk when retained files are unsupported.
// Subsequent publications and final-reference cleanup settle the count directly.
//
// Native publication accounting measures the actual target under the backend's final
// ordering, reserves growth before the volume effect, and releases shrinking bytes
// after an applied effect. Unknown outcomes and failed accounting settlements make the
// allowance unusable until reopening.
// The wrapper preserves the native lock service, mutation scope, and lifecycle.
//
// Out-of-band changes invalidate accounting. The zero floor only bounds reported figures;
// it cannot prevent overspending when drift has left the count below actual usage.
// Recount repairs a known count by measuring again; it cannot resolve uncertain native
// publication accounting.
package limited

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

// MinLimit is the smallest allowance a volume may be held under. It is one block of the
// 4096 bytes a mount reports space in, and an allowance below it would be reported as a
// filesystem of zero blocks — which reads as a disk with nothing left rather than as a
// volume with a little room, and is exactly the fabricated fact a measured count exists
// to avoid (R-ERR-2).
const MinLimit = 4096

const (
	// DefaultMaxDirectoryBytes bounds the entries and names retained for one directory
	// while the volume is measured.
	DefaultMaxDirectoryBytes int64 = 64 << 20

	// DefaultMaxFrontierBytes bounds the directory paths still to be visited, including
	// the directory currently being listed.
	DefaultMaxFrontierBytes int64 = 64 << 20
)

// MeasurementLimits bound the two variable-size structures retained while the volume
// is measured. They are independent of any transport limits: a measurement walks storage
// directly, and its retained representation is storage.Entry values and directory paths.
// A zero field selects its package default; there is no unbounded value.
type MeasurementLimits struct {
	MaxDirectoryBytes int64
	MaxFrontierBytes  int64
}

// DefaultMeasurementLimits returns the limits used by New.
func DefaultMeasurementLimits() MeasurementLimits {
	return MeasurementLimits{
		MaxDirectoryBytes: DefaultMaxDirectoryBytes,
		MaxFrontierBytes:  DefaultMaxFrontierBytes,
	}
}

// Effective fills zero fields with their defaults and rejects limits that cannot hold
// even the fixed part of the structure they govern.
func (l MeasurementLimits) Effective() (MeasurementLimits, error) {
	if l.MaxDirectoryBytes == 0 {
		l.MaxDirectoryBytes = DefaultMaxDirectoryBytes
	}
	if l.MaxFrontierBytes == 0 {
		l.MaxFrontierBytes = DefaultMaxFrontierBytes
	}
	if l.MaxDirectoryBytes < 0 {
		return MeasurementLimits{}, fmt.Errorf("the directory measurement bound cannot be negative: %w", syscall.EINVAL)
	}
	if l.MaxDirectoryBytes == 0 {
		return MeasurementLimits{}, fmt.Errorf("the directory measurement bound must be positive: %w", syscall.EINVAL)
	}
	if l.MaxDirectoryBytes == math.MaxInt64 {
		return MeasurementLimits{}, fmt.Errorf("the directory measurement bound must be below the largest byte count: %w", syscall.EINVAL)
	}
	if l.MaxFrontierBytes < measurementFrontierNodeBytes {
		return MeasurementLimits{}, fmt.Errorf(
			"the traversal-frontier bound is %d bytes, below the %d bytes needed to retain the root: %w",
			l.MaxFrontierBytes, measurementFrontierNodeBytes, syscall.EINVAL)
	}
	if l.MaxFrontierBytes == math.MaxInt64 {
		return MeasurementLimits{}, fmt.Errorf("the traversal-frontier bound must be below the largest byte count: %w", syscall.EINVAL)
	}
	return l, nil
}

// Validate reports whether the limits can govern a measurement.
func (l MeasurementLimits) Validate() error {
	_, err := l.Effective()
	return err
}

type publicationBackend interface {
	storage.BoundedStorage
	CheckPublicationAccounting() error
}

// Storage is a volume held under an allowance.
type Storage struct {
	backing     publicationBackend
	limit       int64
	measurement MeasurementLimits

	// Recount excludes volume requests and synchronous retained-file mutations.
	// Autonomous retirement remains independent and is detected through revision.
	gate sync.RWMutex

	// countMu guards arithmetic and the unknown-outcome fault, never backing I/O.
	countMu  sync.Mutex
	count    int64
	fault    error
	revision uint64
}

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// New holds the volume in backing under an allowance of limit bytes.
//
// backing must provide bounded storage and native final-publication accounting;
// an unsupported backend returns ENOSYS before usage is measured.
//
// Existing usage is measured through the native authority when available. A bounded
// tree walk is sufficient only for volumes without retained, detached files.
//
// A volume already holding more than limit is opened, not refused. An allowance lowered
// underneath content that is already written is an ordinary thing for an operator to do,
// and the answer to it is a volume that takes no new bytes until it has shed some — not
// one that cannot be served at all.
func New(ctx context.Context, backing storage.Storage, limit int64) (*Storage, error) {
	return NewWithLimits(ctx, backing, limit, DefaultMeasurementLimits())
}

// NewWithLimits holds the volume under limit. Measurement bounds govern a
// tree walk at startup and Recount. backing must implement BoundedStorage and native
// publication accounting. Missing capabilities return ENOSYS; a failed accounting
// check is returned before usage is measured. Retained-file backends additionally
// require authoritative usage before this wrapper can expose their file sessions.
func NewWithLimits(
	ctx context.Context,
	backing storage.Storage,
	limit int64,
	measurement MeasurementLimits,
) (*Storage, error) {
	if limit < MinLimit {
		return nil, fmt.Errorf("an allowance of %d bytes is below %d, the smallest a volume can be held under: %w",
			limit, MinLimit, syscall.EINVAL)
	}
	effective, err := measurement.Effective()
	if err != nil {
		return nil, err
	}
	native, ok := backing.(publicationBackend)
	if !ok {
		return nil, fmt.Errorf("an allowance requires native publication accounting and bounded storage: %w", syscall.ENOSYS)
	}
	if err := native.CheckPublicationAccounting(); err != nil {
		return nil, err
	}
	count, err := measureUsage(ctx, native, effective)
	if err != nil {
		return nil, err
	}
	return &Storage{backing: native, limit: limit, measurement: effective, count: count}, nil
}

// Recount measures the volume again and replaces the count with what it finds.
//
// Uncertain native publication accounting requires reopening the volume and cannot
// be cleared by recounting. In-band traffic does not repair out-of-band accounting drift.
//
// Synchronous mutations wait for measurement. Autonomous file retirement can proceed;
// a concurrent settlement causes another measurement, with EAGAIN after eight attempts.
// Backends without authoritative usage require a bounded tree walk.
func (s *Storage) Recount(ctx context.Context) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if err := s.healthy(); err != nil {
		return err
	}

	// Expiry cleanup runs independently of the request gate. Its accounting cannot
	// acquire that gate under native publication ordering, which Usage also needs.
	// A changed accounting revision requires a fresh authoritative measurement.
	for attempt := 0; attempt < 8; attempt++ {
		s.countMu.Lock()
		revision := s.revision
		s.countMu.Unlock()
		count, err := measureUsage(ctx, s.backing, s.measurement)
		if err != nil {
			return err
		}
		s.countMu.Lock()
		if s.fault != nil {
			err := s.fault
			s.countMu.Unlock()
			return err
		}
		if s.revision == revision {
			s.count = count
			s.revision++
			s.countMu.Unlock()
			return nil
		}
		s.countMu.Unlock()
	}
	return fmt.Errorf("retained-file cleanup changed usage during every recount attempt: %w", syscall.EAGAIN)
}

// measure walks the volume and sums what it holds.
//
// A directory contributes nothing: storage.Attr.Size is unspecified for one, so there is
// no figure there to add. A symbolic link contributes the length of the target it holds,
// which is what the link occupies and what a write of that link's own would have charged.
//
// Directory entries and the traversal frontier have independent limits. A complete
// directory remains retained only while its entries are charged and its child paths are
// added to the frontier. The frontier is a linked stack, so its allocation has no hidden
// slice capacity beyond the records charged to MaxFrontierBytes.
func measure(ctx context.Context, s storage.BoundedStorage, limits MeasurementLimits) (int64, error) {
	if err := s.CheckBounded(); err != nil {
		return 0, fmt.Errorf("measuring the volume requires bounded storage results: %w", err)
	}
	frontier, err := newMeasurementFrontier(limits.MaxFrontierBytes)
	if err != nil {
		return 0, err
	}
	var total int64
	for frontier.more() {
		if err := measurementCanceled(ctx); err != nil {
			return 0, err
		}
		current := frontier.take()
		dir := current.path

		result, err := storage.NewListResult(limits.MaxDirectoryBytes, 0, measurementEntryBytes)
		if err != nil {
			return 0, fmt.Errorf("measuring the volume, preparing to list %q: %w", dir, err)
		}
		if err := s.ListBounded(ctx, dir, result); err != nil {
			return 0, fmt.Errorf("measuring the volume, listing %q: %w", dir, err)
		}
		if err := measurementCanceled(ctx); err != nil {
			return 0, err
		}
		entries, err := result.Entries()
		if err != nil {
			return 0, fmt.Errorf("measuring the volume, completing the listing of %q: %w", dir, err)
		}
		if err := measurementCanceled(ctx); err != nil {
			return 0, err
		}
		for _, e := range entries {
			if err := measurementCanceled(ctx); err != nil {
				return 0, err
			}
			if e.Attr.IsDir() {
				if err := frontier.add(dir, e.Name); err != nil {
					return 0, err
				}
				continue
			}
			if _, err := measurementPathBytes(dir, e.Name); err != nil {
				return 0, err
			}
			// A negative size and a sum past what a byte count holds are both answers no
			// volume can give. Taking either would put an allowance in front of a
			// caller as a measured fact when it is a wrapped or a nonsensical figure, so
			// each is a failure to report rather than a number to repair (R-ERR-2).
			if e.Attr.Size < 0 {
				name, _ := measurementPath(dir, e.Name)
				return 0, fmt.Errorf("measuring the volume, %q holds %d bytes: %w",
					name, e.Attr.Size, syscall.EIO)
			}
			if e.Attr.Size > math.MaxInt64-total {
				name, _ := measurementPath(dir, e.Name)
				return 0, fmt.Errorf("measuring the volume, %q carries the total past what a byte count holds: %w",
					name, syscall.EOVERFLOW)
			}
			total += e.Attr.Size
		}
		frontier.release(current)
	}
	return total, nil
}

func measurementEntryBytes(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
	fixed := int64(unsafe.Sizeof(storage.Entry{}))
	if nameBytes > math.MaxInt64-fixed {
		return 0, fmt.Errorf("a directory entry is too large to measure: %w", syscall.EOVERFLOW)
	}
	return fixed + nameBytes, nil
}

type measurementFrontierNode struct {
	path string
	next *measurementFrontierNode
}

var measurementFrontierNodeBytes = int64(unsafe.Sizeof(measurementFrontierNode{}))

type measurementFrontier struct {
	head     *measurementFrontierNode
	used     int64
	maxBytes int64
}

func newMeasurementFrontier(maxBytes int64) (*measurementFrontier, error) {
	frontier := &measurementFrontier{maxBytes: maxBytes}
	if err := frontier.add("", ""); err != nil {
		return nil, err
	}
	return frontier, nil
}

func (f *measurementFrontier) more() bool { return f.head != nil }

func (f *measurementFrontier) take() *measurementFrontierNode {
	node := f.head
	f.head = node.next
	node.next = nil
	return node
}

func (f *measurementFrontier) release(node *measurementFrontierNode) {
	f.used -= measurementFrontierNodeBytes + int64(len(node.path))
}

func (f *measurementFrontier) add(dir, name string) error {
	joinedBytes, err := measurementPathBytes(dir, name)
	if err != nil {
		return err
	}
	if joinedBytes > math.MaxInt64-measurementFrontierNodeBytes {
		return fmt.Errorf("a volume path is too large to retain for measurement: %w", syscall.EOVERFLOW)
	}
	charge := measurementFrontierNodeBytes + joinedBytes
	if charge > f.maxBytes-f.used {
		return fmt.Errorf(
			"measuring the volume needs more than the configured %d-byte traversal frontier while retaining a child of %q: %w",
			f.maxBytes, dir, syscall.EIO)
	}
	joined, err := measurementPath(dir, name)
	if err != nil {
		return err
	}
	f.head = &measurementFrontierNode{path: joined, next: f.head}
	f.used += charge
	return nil
}

func measurementPathBytes(dir, name string) (int64, error) {
	if name == "" {
		if dir == "" {
			return 0, nil
		}
		return 0, fmt.Errorf("a directory listing returned an empty child name below %q: %w", dir, syscall.EIO)
	}
	if name == "." || name == ".." || strings.Contains(name, "/") {
		return 0, fmt.Errorf("a directory listing returned child name %q below %q: %w", name, dir, syscall.EIO)
	}
	separator := int64(0)
	if dir != "" {
		separator = 1
	}
	if int64(len(name)) > math.MaxInt64-int64(len(dir))-separator {
		return 0, fmt.Errorf("a volume path is too large to measure: %w", syscall.EOVERFLOW)
	}
	return int64(len(dir)) + separator + int64(len(name)), nil
}

func measurementPath(dir, name string) (string, error) {
	if _, err := measurementPathBytes(dir, name); err != nil {
		return "", err
	}
	if dir == "" {
		return name, nil
	}
	return dir + "/" + name, nil
}

func measurementCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("measuring the volume: %w", err)
	}
	return nil
}

// reserve atomically charges growth before publication. Shrinking writes retain their
// previous charge until successful publication; other writers cannot spend bytes that
// the volume still holds. The returned reservation is released if publication fails.
func (s *Storage) reserve(name string, delta int64) (int64, error) {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	if s.fault != nil {
		return 0, s.fault
	}

	// Written as a subtraction from the allowance rather than an addition to the count,
	// so that a volume holding close to what a byte count holds cannot wrap the sum
	// into a figure that passes.
	if delta > 0 && delta > s.limit-s.count {
		return 0, &os.PathError{Op: "write", Path: name, Err: fmt.Errorf(
			"%d more bytes would carry the volume past its allowance of %d bytes, of which %d are taken: %w",
			delta, s.limit, s.count, syscall.EDQUOT)}
	}
	charged := max(delta, 0)
	s.count += charged
	s.revision++
	return charged, nil
}

// release gives back bytes the volume no longer holds: a charge whose write failed, or
// the contents of something that has just been removed.
func (s *Storage) release(delta int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	s.count = floor(s.count - delta)
	s.revision++
}

func (s *Storage) taken() (int64, error) {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	return s.count, s.fault
}

// floor holds the count at or above zero, and it is applied where the count is stored
// rather than only where it is read.
//
// Out-of-band modification of the served volume is a non-goal, but when it happens a
// file added behind our back and removed in band credits bytes that were never charged,
// which drives the count down. A negative count reports an Avail larger than Total — room
// that exists nowhere, and an enormous positive once a kernel reply's unsigned field has
// it — so the count is held at zero instead.
//
// That is a bound on what we report and on nothing else. A floored count sits below what
// the volume holds, and writes are taken against the room it appears to have until
// Recount measures the volume again.
func floor(count int64) int64 { return max(count, 0) }

// Space reports the allowance, what is taken of it, and what may still be written.
//
// Avail is the smaller of what the allowance leaves and what the store underneath says is
// available, which is the reason the contract carries it separately from Total-Used. An
// allowance is a ceiling on what may be written rather than evidence the bytes will fit:
// where the disk under us is tighter than the allowance, reporting the allowance would
// promise room the machine cannot supply, and the tools that read this figure abort on it.
//
// A backing store answering ENOSYS has no room of its own to report, and the allowance is
// then the only measured fact anyone has.
func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	taken, err := s.taken()
	if err != nil {
		return storage.Space{}, err
	}
	space := storage.Space{Total: s.limit, Used: taken, Avail: max(s.limit-taken, 0)}

	beneath, err := s.backing.Space(ctx)
	switch {
	case errors.Is(err, syscall.ENOSYS):
		return space, nil
	case err != nil:
		return storage.Space{}, err
	}
	// Figures that cannot be true of anything would travel into ours through the minimum
	// below, and an Avail that went negative there would come back out of a kernel reply's
	// unsigned field as room no disk holds.
	if !beneath.Coherent() {
		return storage.Space{}, fmt.Errorf("the store beneath reports a total of %d bytes with %d used and %d available, which cannot be true of anything: %w",
			beneath.Total, beneath.Used, beneath.Avail, syscall.EIO)
	}
	space.Avail = min(space.Avail, beneath.Avail)
	return space, nil
}

// Write reserves growth from the actual sizes resolved under native publication
// ordering. The backend must reject an unpaid increase before changing the volume.
func (s *Storage) Write(ctx context.Context, name string, content []byte) error {
	if _, err := storage.CleanPath(name); err != nil {
		return &os.PathError{Op: "write", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Write(s.accountingContext(ctx, name), name, content))
}

// Remove credits bytes only when native publication releases their retention.
// An unlinked file with live references remains charged until final cleanup.
func (s *Storage) Remove(ctx context.Context, name string) error {
	if _, err := storage.CleanPath(name); err != nil {
		return &os.PathError{Op: "unlink", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Remove(s.accountingContext(ctx, name), name))
}

// Rename settles content reclaimed by replacement. A retained displaced file
// keeps its charge until the native owner completes final-reference cleanup.
func (s *Storage) Rename(ctx context.Context, from, to string) error {
	if _, err := storage.CleanPath(from); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	if _, err := storage.CleanPath(to); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Rename(s.accountingContext(ctx, to), from, to))
}

func (s *Storage) Stat(ctx context.Context, name string) (storage.Attr, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.Stat(ctx, name)
}

func (s *Storage) SetAttr(ctx context.Context, name string, change storage.AttrChange) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.SetAttr(s.accountingContext(ctx, name), name, change))
}

// CheckBounded refuses use as an embedded-server backend when the wrapped volume
// cannot enforce caller-owned result bounds before allocation.
func (s *Storage) CheckBounded() error {
	return s.backing.CheckBounded()
}

func (s *Storage) List(ctx context.Context, name string) ([]storage.Entry, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.List(ctx, name)
}

func (s *Storage) ListBounded(ctx context.Context, name string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.ListBounded(ctx, name, result)
}

func (s *Storage) Read(ctx context.Context, name string) ([]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.Read(ctx, name)
}

func (s *Storage) ReadBounded(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.ReadBounded(ctx, name, maxBytes)
}

func (s *Storage) Create(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Create(s.accountingContext(ctx, name), name))
}

func (s *Storage) Mkdir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Mkdir(s.accountingContext(ctx, name), name))
}

func (s *Storage) RemoveDir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.RemoveDir(s.accountingContext(ctx, name), name))
}
