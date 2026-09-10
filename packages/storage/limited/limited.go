// Package limited enforces a byte allowance over one bounded volume.
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
// Other bounded backends use per-path size sampling, which does not coordinate ancestor
// renames. They cannot expose a lock service through this wrapper. Enumeration always
// requires storage.BoundedStorage so measurement stops before excess allocation.
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
	"hash/fnv"
	"io/fs"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/locking"
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

// The fallback path exclusions have fixed capacity. Native backends provide their own
// final target ordering and do not hold these stripes while staging content.
const stripeCount = 256

// Storage is a volume held under an allowance.
type Storage struct {
	backing     storage.BoundedStorage
	limit       int64
	measurement MeasurementLimits
	accounted   bool

	// Recount excludes volume requests and synchronous retained-file mutations.
	// Autonomous retirement remains independent and is detected through revision.
	gate sync.RWMutex

	// countMu guards arithmetic and the unknown-outcome fault, never backing I/O.
	countMu  sync.Mutex
	count    int64
	fault    error
	revision uint64

	// Only non-native backends use sampled sizes protected by these path stripes.
	stripes [stripeCount]sync.Mutex
}

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// New holds the volume in backing under an allowance of limit bytes.
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
// fallback tree walk at startup and Recount. backing must implement BoundedStorage;
// retained-file backends additionally require authoritative usage and publication
// accounting before this wrapper can expose their file sessions.
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
	bounded, ok := backing.(storage.BoundedStorage)
	if !ok {
		return nil, fmt.Errorf("measuring an allowance requires backing storage with bounded listings: %w", syscall.ENOSYS)
	}
	accounted := false
	if native, ok := bounded.(interface{ CheckPublicationAccounting() error }); ok {
		switch err := native.CheckPublicationAccounting(); err {
		case nil:
			accounted = true
		case syscall.ENOSYS:
		default:
			return nil, err
		}
	}
	if source, ok := bounded.(interface{ LockService() locking.Service }); ok && source.LockService() != nil && !accounted {
		return nil, fmt.Errorf("a lock-enabled volume requires native publication accounting for its allowance: %w", syscall.ENOSYS)
	}
	count, err := measureUsage(ctx, bounded, effective)
	if err != nil {
		return nil, err
	}
	return &Storage{backing: bounded, limit: limit, measurement: effective, count: count, accounted: accounted}, nil
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

// stripeOf picks the exclusion a path falls under. The path is the cleaned one, so that
// the several ways of naming one node all arrive at one stripe.
func stripeOf(cleaned string) uint32 {
	h := fnv.New32a()
	// hash.Hash's Write never returns an error.
	_, _ = h.Write([]byte(cleaned))
	return h.Sum32() % stripeCount
}

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

// Write charges the difference between what the file will hold and what it holds now, and
// refuses with EDQUOT when the allowance cannot pay for it.
//
// Native backends determine the sizes and settle the charge under final publication
// ordering. Other backends sample the size under a path stripe, reserve growth before
// Write, refund on failure, and release shrinking bytes only after success.
func (s *Storage) Write(ctx context.Context, name string, content []byte) error {
	cleaned, err := storage.CleanPath(name)
	if err != nil {
		return &os.PathError{Op: "write", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	if s.accounted {
		return s.publicationError(s.backing.Write(s.accountingContext(ctx, name), name, content))
	}
	stripe := &s.stripes[stripeOf(cleaned)]
	stripe.Lock()
	defer stripe.Unlock()

	var held int64
	switch attr, err := s.backing.Stat(ctx, name); {
	case errors.Is(err, syscall.ENOENT):
		// The write makes the file, so nothing is charged for the name yet. Whether it can
		// be made where it was asked for is the write's own question.
	case err != nil:
		// The bytes cannot be charged against a size nobody could read, and a write that
		// landed uncharged is the one direction the count may not err in.
		return err
	case attr.IsDir() || attr.Mode.Type() == fs.ModeSymlink:
		// Neither is a node whose contents a write replaces — the contract answers EISDIR
		// for one and ELOOP for the other, and a directory has no size to charge against
		// in any case. Charging first would answer EDQUOT in place of the errno that says
		// what is actually at the name.
		return s.publicationError(s.backing.Write(ctx, name, content))
	default:
		held = attr.Size
	}

	delta := int64(len(content)) - held
	charged, err := s.reserve(name, delta)
	if err != nil {
		return err
	}
	if err := s.backing.Write(ctx, name, content); err != nil {
		if storage.IsPublicationAccountingUncertain(err) {
			return s.publicationError(err)
		}
		s.release(charged)
		return err
	}
	if delta < 0 {
		s.release(-delta)
	}
	return nil
}

// Remove credits the file's contents back, and only once the file is gone. Crediting first
// would hand out room the volume has not released.
func (s *Storage) Remove(ctx context.Context, name string) error {
	cleaned, err := storage.CleanPath(name)
	if err != nil {
		return &os.PathError{Op: "unlink", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	if s.accounted {
		return s.publicationError(s.backing.Remove(s.accountingContext(ctx, name), name))
	}
	stripe := &s.stripes[stripeOf(cleaned)]
	stripe.Lock()
	defer stripe.Unlock()

	// A node that could not be stat-ed, and a directory whose size means nothing, are both
	// removed without a credit. Nothing is destroyed uncounted by that: removing a
	// directory is EISDIR, and a removal that fails releases nothing. Where it does leave
	// the count high — a file whose size the store could not report — high is the direction
	// the count is allowed to err in.
	attr, err := s.backing.Stat(ctx, name)
	if err != nil || attr.IsDir() {
		return s.publicationError(s.backing.Remove(ctx, name))
	}
	if err := s.backing.Remove(ctx, name); err != nil {
		return s.publicationError(err)
	}
	s.release(attr.Size)
	return nil
}

// Rename credits back whatever the move destroys. The bytes moved are charged already and
// stay charged wherever they land, so a move over nothing, and a move onto the node
// itself, both cost nothing.
func (s *Storage) Rename(ctx context.Context, from, to string) error {
	cleanFrom, err := storage.CleanPath(from)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	cleanTo, err := storage.CleanPath(to)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	if s.accounted {
		return s.publicationError(s.backing.Rename(s.accountingContext(ctx, to), from, to))
	}

	// Both stripes, in index order. Two renames in opposite directions between the same
	// pair of paths would otherwise each hold the one the other is waiting for.
	first, second := stripeOf(cleanFrom), stripeOf(cleanTo)
	if first > second {
		first, second = second, first
	}
	s.stripes[first].Lock()
	defer s.stripes[first].Unlock()
	if second != first {
		s.stripes[second].Lock()
		defer s.stripes[second].Unlock()
	}

	// A move onto the node itself replaces nothing, so there is nothing to credit: POSIX
	// has rename(2) "return successfully and perform no other action" when both names
	// resolve to one directory entry, and the bytes at the destination are the same bytes
	// that are still there afterwards. Crediting them would hand out room the volume
	// never released, once for every time the move is repeated. Both names fall on one
	// stripe here, which the ordering above already allows for.
	//
	// The move is still carried out beneath, because whether it is permitted at all is not
	// ours to answer: naming the root either way is EBUSY, and a source that is not there
	// is ENOENT.
	if cleanFrom == cleanTo {
		return s.publicationError(s.backing.Rename(ctx, from, to))
	}

	// As in Remove: a destination that could not be stat-ed and a destination that is a
	// directory are both replaced without a credit, which can only leave the count high.
	var replaced int64
	if attr, err := s.backing.Stat(ctx, to); err == nil && !attr.IsDir() {
		replaced = attr.Size
	}
	if err := s.backing.Rename(ctx, from, to); err != nil {
		return s.publicationError(err)
	}
	s.release(replaced)
	return nil
}

// The operations below move the count by nothing, and Create is the one worth saying so
// about: it makes an empty file, which costs no bytes and is therefore never refused. The
// bytes arrive later, through Write, and are charged there.
//
// They hold the gate for the reason every operation does, and they need no stripe, because
// none of them reads a size and then charges against it.

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
	return s.publicationError(s.backing.SetAttr(s.mutationContext(ctx, name), name, change))
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
	return s.publicationError(s.backing.Create(s.mutationContext(ctx, name), name))
}

func (s *Storage) Mkdir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.Mkdir(s.mutationContext(ctx, name), name))
}

func (s *Storage) RemoveDir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	return s.publicationError(s.backing.RemoveDir(s.mutationContext(ctx, name), name))
}
