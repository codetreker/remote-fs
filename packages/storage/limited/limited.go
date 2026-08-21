// Package limited holds a namespace under a byte allowance and refuses the write that
// would carry it past one.
//
// The count and the refusal are ours. They have to be: the store a namespace is held in
// need have no notion of an allowance at all — a plain directory has none — and the
// allowance belongs to the workspace rather than to the disk underneath it. So this wraps
// any storage.Storage. What the namespace holds is measured once, when it is opened, and
// moved afterwards by every mutation that passes through.
//
// The count is exact for everything that goes through here and for nothing else. The
// served namespace being modified behind the server's back is a stated non-goal, and no
// amount of in-band traffic repairs the drift it causes: a file deleted out of band keeps
// its bytes charged for good, and one added out of band is credited when it is removed in
// band, which drives the count down.
//
// The count is held at or above zero, and what that bounds is what we report and nothing
// more. The room named to a caller never exceeds what the allowance leaves and never
// arrives in a kernel reply's unsigned field as an enormous positive, which is the
// fabricated fact R-ERR-2 forbids. It does not bound what is true: a count that drift has
// left below what the namespace holds accepts writes that carry the namespace past its
// allowance, and the refusal R-WS-5 asks for is then not made at all. Only a fresh
// measurement repairs that, and Recount is the one way to it.
package limited

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"math"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

// MinLimit is the smallest allowance a namespace may be held under. It is one block of the
// 4096 bytes a mount reports space in, and an allowance below it would be reported as a
// filesystem of zero blocks — which reads as a disk with nothing left rather than as a
// workspace with a little room, and is exactly the fabricated fact a measured count exists
// to avoid (R-ERR-2).
const MinLimit = 4096

// stripeCount is how many path exclusions there are. The set is fixed rather than grown
// per path because R-INT-3 forbids anything that accumulates without a bound, and one
// mutex per path in a namespace is that. Two distinct paths that fall on one stripe
// serialise with each other and with nothing else, which is a bounded cost against work
// that is one stat and one write of the whole file.
const stripeCount = 256

// Storage is a namespace held under an allowance.
type Storage struct {
	backing storage.Storage
	limit   int64

	// gate is closed by Recount and held open by every operation, the read-only ones
	// included. Only a mutation can spoil the walk Recount makes, but a rule with
	// exceptions in it is one a later mutating operation gets added outside of.
	gate sync.RWMutex

	// countMu guards count alone rather than the storage as a whole. R-CC-2 requires
	// writes to different files not to interfere, and one lock held across an operation
	// is that interference.
	countMu sync.Mutex
	count   int64

	// stripes exclude two operations on one path, so that the size a write reads before
	// it charges cannot change under it.
	stripes [stripeCount]sync.Mutex
}

var _ storage.Storage = (*Storage)(nil)

// New holds the namespace in backing under an allowance of limit bytes.
//
// What the namespace already holds is measured here, by walking it. That walk is the only
// unconditional traversal in the life of the storage: everything afterwards moves the
// figure by what passes through, so asking how full the namespace is costs nothing.
//
// A namespace already holding more than limit is opened, not refused. An allowance lowered
// underneath content that is already written is an ordinary thing for an operator to do,
// and the answer to it is a namespace that takes no new bytes until it has shed some — not
// one that cannot be served at all.
func New(ctx context.Context, backing storage.Storage, limit int64) (*Storage, error) {
	if limit < MinLimit {
		return nil, fmt.Errorf("an allowance of %d bytes is below %d, the smallest a namespace can be held under: %w",
			limit, MinLimit, syscall.EINVAL)
	}
	count, err := measure(ctx, backing)
	if err != nil {
		return nil, err
	}
	return &Storage{backing: backing, limit: limit, count: count}, nil
}

// Recount measures the namespace again and replaces the count with what it finds.
//
// It is the way back from drift, and the only one: everything passing through this storage
// is counted exactly, so no amount of in-band traffic corrects a figure that out-of-band
// work moved.
//
// Nothing mutates while it walks. Every caller therefore waits for the length of a tree
// traversal, which is a price an operator may choose to pay and not one anything may
// impose unasked — which is why nothing here schedules it.
func (s *Storage) Recount(ctx context.Context) error {
	s.gate.Lock()
	defer s.gate.Unlock()

	count, err := measure(ctx, s.backing)
	if err != nil {
		return err
	}
	s.countMu.Lock()
	defer s.countMu.Unlock()
	s.count = count
	return nil
}

// measure walks the namespace and sums what it holds.
//
// A directory contributes nothing: storage.Attr.Size is unspecified for one, so there is
// no figure there to add. A symbolic link contributes the length of the target it holds,
// which is what the link occupies and what a write of that link's own would have charged.
//
// The walk is iterative because a namespace's depth is not ours to bound.
func measure(ctx context.Context, s storage.Storage) (int64, error) {
	var total int64
	pending := []string{""}
	for len(pending) > 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		entries, err := s.List(ctx, dir)
		if err != nil {
			return 0, fmt.Errorf("measuring the namespace, listing %q: %w", dir, err)
		}
		for _, e := range entries {
			if e.Attr.IsDir() {
				pending = append(pending, path.Join(dir, e.Name))
				continue
			}
			// A negative size and a sum past what a byte count holds are both answers no
			// namespace can give. Taking either would put an allowance in front of a
			// caller as a measured fact when it is a wrapped or a nonsensical figure, so
			// each is a failure to report rather than a number to repair (R-ERR-2).
			if e.Attr.Size < 0 {
				return 0, fmt.Errorf("measuring the namespace, %q holds %d bytes: %w",
					path.Join(dir, e.Name), e.Attr.Size, syscall.EIO)
			}
			if e.Attr.Size > math.MaxInt64-total {
				return 0, fmt.Errorf("measuring the namespace, %q carries the total past what a byte count holds: %w",
					path.Join(dir, e.Name), syscall.EOVERFLOW)
			}
			total += e.Attr.Size
		}
	}
	return total, nil
}

// reserve charges delta against the allowance, refusing what the allowance cannot pay for,
// and reports how far the count actually moved.
//
// The check and the charge are one step, and both happen before the write that spends
// them. Two writers to different paths that each checked before either charged would both
// pass a check only one of them could satisfy; the caller hands the charge back if the
// write it covered fails.
//
// What is handed back is the figure returned here rather than delta, because the two part
// company where the floor bites: a count of 5 charged -10 lands at 0, having moved by 5,
// and giving 10 back would leave the count above where the write found it. The pair has to
// be invertible, since a write that did not happen may not move the count at all.
//
// A delta that shrinks the namespace is never refused, not even from over the allowance.
// Refusing it would leave a workspace that is over its limit with no way back under it.
func (s *Storage) reserve(name string, delta int64) (int64, error) {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	// Written as a subtraction from the allowance rather than an addition to the count,
	// so that a namespace holding close to what a byte count holds cannot wrap the sum
	// into a figure that passes.
	if delta > 0 && delta > s.limit-s.count {
		return 0, &os.PathError{Op: "write", Path: name, Err: fmt.Errorf(
			"%d more bytes would carry the namespace past its allowance of %d bytes, of which %d are taken: %w",
			delta, s.limit, s.count, syscall.EDQUOT)}
	}
	before := s.count
	s.count = floor(s.count + delta)
	return s.count - before, nil
}

// release gives back bytes the namespace no longer holds: a charge whose write failed, or
// the contents of something that has just been removed.
func (s *Storage) release(delta int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	s.count = floor(s.count - delta)
}

func (s *Storage) taken() int64 {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	return s.count
}

// floor holds the count at or above zero, and it is applied where the count is stored
// rather than only where it is read.
//
// Out-of-band modification of the served namespace is a non-goal, but when it happens a
// file added behind our back and removed in band credits bytes that were never charged,
// which drives the count down. A negative count reports an Avail larger than Total — room
// that exists nowhere, and an enormous positive once a kernel reply's unsigned field has
// it — so the count is held at zero instead.
//
// That is a bound on what we report and on nothing else. A floored count sits below what
// the namespace holds, and writes are taken against the room it appears to have until
// Recount measures the namespace again.
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

	taken := s.taken()
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
// The charge happens before the write and is given back if the write fails, which is what
// keeps two writers to different paths from both spending the same last bytes. The stripe
// keeps the size read here from moving between the reading and the charging.
func (s *Storage) Write(ctx context.Context, name string, content []byte) error {
	cleaned, err := storage.CleanPath(name)
	if err != nil {
		return &os.PathError{Op: "write", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
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
		return s.backing.Write(ctx, name, content)
	default:
		held = attr.Size
	}

	delta := int64(len(content)) - held
	charged, err := s.reserve(name, delta)
	if err != nil {
		return err
	}
	if err := s.backing.Write(ctx, name, content); err != nil {
		s.release(charged)
		return err
	}
	return nil
}

// Remove credits the file's contents back, and only once the file is gone. Crediting first
// would hand out room the namespace has not released.
func (s *Storage) Remove(ctx context.Context, name string) error {
	cleaned, err := storage.CleanPath(name)
	if err != nil {
		return &os.PathError{Op: "unlink", Path: name, Err: err}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
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
		return s.backing.Remove(ctx, name)
	}
	if err := s.backing.Remove(ctx, name); err != nil {
		return err
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
	// that are still there afterwards. Crediting them would hand out room the namespace
	// never released, once for every time the move is repeated. Both names fall on one
	// stripe here, which the ordering above already allows for.
	//
	// The move is still carried out beneath, because whether it is permitted at all is not
	// ours to answer: naming the root either way is EBUSY, and a source that is not there
	// is ENOENT.
	if cleanFrom == cleanTo {
		return s.backing.Rename(ctx, from, to)
	}

	// As in Remove: a destination that could not be stat-ed and a destination that is a
	// directory are both replaced without a credit, which can only leave the count high.
	var replaced int64
	if attr, err := s.backing.Stat(ctx, to); err == nil && !attr.IsDir() {
		replaced = attr.Size
	}
	if err := s.backing.Rename(ctx, from, to); err != nil {
		return err
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
	return s.backing.SetAttr(ctx, name, change)
}

func (s *Storage) List(ctx context.Context, name string) ([]storage.Entry, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.List(ctx, name)
}

func (s *Storage) Read(ctx context.Context, name string) ([]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.Read(ctx, name)
}

func (s *Storage) Create(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.Create(ctx, name)
}

func (s *Storage) Mkdir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.Mkdir(ctx, name)
}

func (s *Storage) RemoveDir(ctx context.Context, name string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.backing.RemoveDir(ctx, name)
}
