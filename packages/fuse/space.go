package fuse

import (
	"context"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
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
