package fuse

import (
	"context"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// reportedBlockSize is the unit this mount reports space in. Nothing here holds anything
// in blocks — the namespace charges bytes — so it is a denomination rather than a
// property of any storage, and 4096 is what the filesystems a caller compares against use.
const reportedBlockSize = 4096

// maxNameLength is the longest name this mount will say a directory can hold. It is the
// shortest limit any Linux filesystem a namespace can be held in imposes, and understating
// it is the safe direction: a caller checking whether a name will fit is told no about a
// name that would have fitted, rather than yes about one that will not.
const maxNameLength = 255

// spaceDeadline bounds one question to the namespace about its room, instead of letting
// that question run for as long as the mount's own operation timeout allows.
//
// df touches every mount on the machine, so a server that is unreachable would otherwise
// make a df anywhere on the host stall for that whole timeout — thirty seconds by default
// — where a mount that answers statfs with ENOSYS returns instantly. Two seconds is well
// above the round trip a working link takes and well below the timeout, so a server that
// is merely slow still answers, and one that is not there costs a pause rather than a hang.
const spaceDeadline = 2 * time.Second

// Statfs describes the room the namespace has, in the blocks the kernel asks for.
//
// The figures are the namespace's own, and there is no other source for them: the FUSE
// library's default for a filesystem that does not answer is a zeroed reply, which reads
// as a disk with no space left, and a program that checks for room before writing acts on
// it (R-ERR-2). A namespace with no room of its own to report therefore answers ENOSYS and
// that reaches the caller as it stands — df says "Function not implemented", which is true
// — rather than being turned into numbers nobody measured.
func (n *node) Statfs(ctx context.Context, out *gofuse.StatfsOut) syscall.Errno {
	ask, cancel := context.WithTimeout(ctx, spaceDeadline)
	defer cancel()

	space, err := n.ns.storage.Space(ask)
	if err != nil {
		return errnoOf(err)
	}
	// An answer that cannot be true of anything is a failure to report rather than a set
	// of numbers to repair. These fields cross into the kernel unsigned, so a negative
	// would arrive as an enormous positive offering room no disk anywhere holds.
	if !space.Coherent() {
		return syscall.EIO
	}

	out.Bsize, out.Frsize = reportedBlockSize, reportedBlockSize
	// Floor throughout: a block that cannot be filled is not offered. Floor division
	// preserves the order Coherent establishes — Avail is at most what Total leaves, which
	// is at most Total — so Bavail cannot come out above Bfree, nor Bfree above Blocks.
	out.Blocks = uint64(space.Total) / reportedBlockSize
	out.Bfree = uint64(max(space.Total-space.Used, 0)) / reportedBlockSize
	out.Bavail = uint64(space.Avail) / reportedBlockSize
	// Nothing here counts inodes: the namespace charges bytes, and a figure in these
	// fields would have to be invented. Zero is how a filesystem with no inode table
	// reports having none, and df prints it as no figure rather than as none left.
	out.Files, out.Ffree = 0, 0
	out.NameLen = maxNameLength
	return 0
}

// roomWindow is how long one measurement of the room left in the namespace is used before
// the namespace is asked again.
//
// write(2) is a filesystem's hottest path, and a round trip on each one would cost far
// more than the refusal it pays for. One figure is therefore shared by every handle in the
// mount and refreshed after roomWindow. A completed query or an independent failure
// starts that window; an interrupted query can be retried immediately. A second is short
// enough that the figure describes the workspace a caller is working in and long enough
// that a program writing a file byte by byte usually pays for it once.
//
// The write that finds the figure aged is the one that waits for its replacement, for at
// most spaceDeadline. Every other write in flight meanwhile goes through on the figure
// that is already there.
const roomWindow = time.Second

// roomGauge is what this mount last heard about the room left in the namespace.
//
// Everything a write is checked against here may be that old, and both directions of the
// staleness are accounted for. A figure larger than the truth lets through a write that
// will not fit, which the commit refuses; the commit is the authority, and this is only
// what makes the refusal reach the program at the write(2) that caused it. A figure
// smaller than the truth refuses a write that would have fitted, which the next attempt
// a moment later accepts.
type roomGauge struct {
	mu sync.Mutex

	// asked is when the namespace last answered or failed independently of caller
	// cancellation. A failed measurement shares the answer's cooldown; a withdrawn
	// query leaves the previous timestamp intact so its retry can ask again.
	asked time.Time

	// asking says a question is in flight. Whoever finds one uses the figure that is
	// already there rather than waiting behind it: two programs writing two different
	// files may not be made to wait on each other (R-CC-2).
	asking bool

	// avail is the last figure, and measured says there is one at all. A question that
	// failed leaves the previous figure standing rather than discarding it — it is still
	// the last thing anybody measured — and until the first one succeeds there is no
	// figure and nothing to check against.
	avail    int64
	measured bool

	// absent records a namespace that has no room of its own to report. The contract
	// makes that a standing property of the implementation rather than a condition of the
	// call, so the question is never put again and no write is ever checked.
	absent bool
}

// remaining reports what the namespace last said may still be written to it, and whether
// there is such a figure at all. The namespace is asked only when the standing figure has
// aged past roomWindow and nobody else is already asking. An honored caller interruption
// returns its cause without changing the previous measurement or its timestamp.
func (g *roomGauge) remaining(ctx context.Context, s storage.Storage) (int64, bool, error) {
	g.mu.Lock()
	due := !g.absent && !g.asking && time.Since(g.asked) >= roomWindow
	if due {
		g.asking = true
	}
	standing, measured := g.avail, g.measured
	g.mu.Unlock()

	if !due {
		return standing, measured, nil
	}

	ask, cancel := context.WithTimeout(ctx, spaceDeadline)
	defer cancel()
	space, err := s.Space(ask)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.asking = false
	// A withdrawn query neither measures space nor starts the failure cooldown. Its
	// caller must leave the buffer untouched, and an immediate retry needs a fresh query.
	if errnoOf(err) == syscall.EINTR {
		return g.avail, g.measured, err
	}
	g.asked = time.Now()
	switch {
	case errnoOf(err) == syscall.ENOSYS:
		g.absent = true
	case err == nil && space.Coherent():
		g.avail, g.measured = space.Avail, true
	}
	return g.avail, g.measured, nil
}
