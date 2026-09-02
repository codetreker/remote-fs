package fuse

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// handle is one open file. It holds the file's whole contents, because the namespace
// reads and writes whole files and the kernel reads and writes pieces of them: without a
// buffer in between, reading a file once would cost one namespace read per piece.
//
// The buffer belongs to the handle rather than to the node, so two programs that have
// the same file open do not write into each other's copy. The one that commits last wins,
// which is the concurrency the namespace offers.
//
// Nothing here may grow the buffer past the mount's ceiling on a single file's size, which
// is why a write and a resize alike report an errno rather than simply doing what they
// were asked. Both are weighed against the room the namespace says it has left as well, so
// that a workspace at its limit refuses at the call that asked rather than leaving the
// commit to discover it at the close(2).
type handle struct {
	node *node

	mu       sync.Mutex
	contents []byte
	// dirty says the buffer holds something the namespace does not have yet.
	dirty bool
	// stored is how many of this file's bytes the namespace already holds, so that a
	// change is weighed against the room its growth needs rather than against the file's
	// whole length: appending to a large file in a nearly full workspace costs what it
	// appends.
	//
	// A handle opened with O_TRUNC leaves it at zero although the namespace still holds
	// the contents that are about to be replaced, because nothing on that path asked how
	// long they were. The replacement is then charged in full, which is the direction that
	// refuses a write that would have fitted rather than accepting one that will not.
	stored int64
	// changed is when the buffer last changed, reported while it is uncommitted so that
	// a program which writes a file and stats it does not see the previous time.
	changed time.Time

	// opened is the node the namespace had at this name when the buffer was filled, and
	// it is what tells a descriptor held across a replacement from one whose file is
	// still there. Zero for a handle that never read a file — one opened with O_TRUNC,
	// or made by a creation — whose buffer is the file until it is committed and is the
	// file afterwards, so neither needs telling apart from anything.
	opened uint64
}

// The two states a fresh buffer can be in. A handle opened with O_TRUNC starts empty and
// uncommitted, because truncating a file is a change even when nothing is written after
// it; every other handle starts holding what the namespace holds.
const (
	committed   = false
	uncommitted = true
)

var (
	_ fs.FileReader   = (*handle)(nil)
	_ fs.FileWriter   = (*handle)(nil)
	_ fs.FileFlusher  = (*handle)(nil)
	_ fs.FileFsyncer  = (*handle)(nil)
	_ fs.FileReleaser = (*handle)(nil)
)

func newHandle(n *node, contents []byte, dirty bool, opened uint64) *handle {
	h := &handle{node: n, contents: contents, dirty: dirty, changed: time.Now(), opened: opened}
	if !dirty {
		h.stored = int64(len(contents))
	}
	n.track(h)
	return h
}

func (h *handle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off >= int64(len(h.contents)) {
		return gofuse.ReadResultData(nil), 0
	}
	return gofuse.ReadResultData(dest[:copy(dest, h.contents[off:])]), 0
}

func (h *handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// off is wherever the caller seeked to, and the kernel lets it reach the top of
	// int64, so the ceiling is subtracted from rather than the sum being formed and
	// compared: off+len(data) wraps to a negative number up there, and a negative end is
	// below every ceiling.
	//
	// The whole write is refused rather than clamped to what fits, which is what a local
	// filesystem does at its own maximum. Clamping reports a short write, and a short
	// write says how much arrived without saying why the rest did not; EFBIG says why.
	if off > h.node.ns.maxFileSize-int64(len(data)) {
		return 0, syscall.EFBIG
	}

	end := off + int64(len(data))
	if errno := h.weigh(ctx, max(end, int64(len(h.contents)))); errno != 0 {
		return 0, errno
	}

	if end > int64(len(h.contents)) {
		h.contents = resized(h.contents, end)
	}
	copy(h.contents[off:], data)
	h.dirty = true
	h.changed = time.Now()
	return uint32(len(data)), 0
}

func (h *handle) resize(ctx context.Context, size int64) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.node.ns.holds(size) {
		return syscall.EFBIG
	}
	if errno := h.weigh(ctx, size); errno != 0 {
		return errno
	}
	h.contents = resized(h.contents, size)
	h.dirty = true
	h.changed = time.Now()
	return 0
}

// weigh answers whether the buffer may take on a given length, against the room the
// namespace last said it had left. Called with h.mu held.
//
// What is weighed is the growth: the length the namespace would end up holding for this
// file beyond what it holds for it now. A change that leaves the file no longer than the
// namespace already has it therefore costs nothing and is never refused, whatever the
// figure says — a workspace past its allowance has to have a way back under it, and
// shortening a file is that way.
//
// The namespace's own limit is weighed here as well as at the commit, and for the same
// reason the ceiling on a single file is weighed before the buffer grows: the commit
// happens at close(2), and a large share of programs never look at what close(2) returned,
// so a refusal discovered only there loses the bytes in silence (R-WS-5). EDQUOT rather
// than ENOSPC — no disk is full, an allowance is spent.
//
// The figure may be as old as roomWindow, so a change that no longer fits can still be
// accepted here; the commit refuses it and remains the authority. This is what carries that
// answer back to the program that caused it, at the call that caused it.
func (h *handle) weigh(ctx context.Context, length int64) syscall.Errno {
	grown := length - h.stored
	if grown <= 0 {
		return 0
	}
	if avail, measured := h.node.ns.room.remaining(ctx, h.node.ns.storage); measured && grown > avail {
		return syscall.EDQUOT
	}
	return 0
}

// Flush commits. The kernel sends it for every close, which is the last moment a failure
// can still be reported to whoever caused it; Release, which comes afterwards, has
// nowhere to report anything.
func (h *handle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.commit(ctx)
}

// Fsync commits as well. Asking for durability and being told it was achieved, while the
// contents sat in this process's memory, would be exactly the false report this
// filesystem exists to avoid.
func (h *handle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.commit(ctx)
}

func (h *handle) Release(ctx context.Context) syscall.Errno {
	h.node.forget(h)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.contents = nil
	return 0
}

func (h *handle) commit(ctx context.Context) syscall.Errno {
	if !h.dirty {
		return 0
	}
	// The path is read now rather than remembered from the open, because the file may
	// have been renamed since, and the contents belong to the file rather than the name.
	if err := h.node.ns.storage.Write(ctx, h.node.path(), h.contents); err != nil {
		return errnoOf(err)
	}
	h.dirty = false
	h.stored = int64(len(h.contents))
	return 0
}

// describe overrides what the namespace reports with what this handle holds, where the two
// are about different things. at is the node the namespace has at the name now.
//
// An uncommitted buffer is the file: a program that writes and then stats must see what it
// wrote, size and time both, before the commit has happened (R-CON-4).
//
// A committed buffer is overridden only when the name no longer holds the node the buffer
// was filled from. Then this descriptor and that name are two different files, and the
// length has to be the one this descriptor will serve: the kernel will not ask for a byte
// past the length it was told, so a length taken from the name clips the read at whatever
// is there now — which is how a rename over the name by another mount turned a held
// descriptor into a reader of the old contents cut to the new file's length (R-FS-5,
// R-CON-3).
//
// Overriding unconditionally instead is what this must not do, and it is not hypothetical:
// the kernel sends no file handle with a GETATTR for a path, and go-fuse fills that in from
// whichever descriptor happens to be open on the node (fs/bridge.go, "the linux kernel
// doesnt pass along the file descriptor, so we have to fake it here"). An unconditional
// override therefore answers an ordinary stat from an unrelated reader's buffer: a file
// rewritten while somebody holds it open reported the holder's length to every program on
// the machine until that descriptor closed — 6 where the file was 55, from stat(1), du(1)
// and os.Stat alike, while a read of the same path returned all 55 bytes.
//
// The times are not overridden for a committed buffer, so a descriptor held across a
// replacement reports the length of what it will serve with the time of what replaced it.
// That misdescribes the file, where the other one hands over another file's bytes; it is
// not the same kind of wrong and it is left.
func (h *handle) describe(out *gofuse.Attr, at uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.dirty {
		out.Size = uint64(len(h.contents))
		out.Mtime, out.Ctime = uint64(h.changed.Unix()), uint64(h.changed.Unix())
		out.Mtimensec = uint32(h.changed.Nanosecond())
		out.Ctimensec = out.Mtimensec
		return
	}
	if h.opened != 0 && at != h.opened {
		out.Size = uint64(len(h.contents))
	}
}

// resized returns body at exactly size bytes, padded with zeroes when it grows. The
// padding is what makes a write past the end of a file behave the way it does on a
// local one.
func resized(body []byte, size int64) []byte {
	if size <= int64(len(body)) {
		return body[:size]
	}
	grown := make([]byte, size)
	copy(grown, body)
	return grown
}
