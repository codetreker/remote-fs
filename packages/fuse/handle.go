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
// Nothing here may grow the buffer past the mount's ceiling on a single file's size. That
// is the only reason a write or a resize is ever refused, and it is why both report an
// errno rather than simply doing what they were asked.
type handle struct {
	node *node

	mu       sync.Mutex
	contents []byte
	// dirty says the buffer holds something the namespace does not have yet.
	dirty bool
	// changed is when the buffer last changed, reported while it is uncommitted so that
	// a program which writes a file and stats it does not see the previous time.
	changed time.Time
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

func newHandle(n *node, contents []byte, dirty bool) *handle {
	h := &handle{node: n, contents: contents, dirty: dirty, changed: time.Now()}
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

	if end := off + int64(len(data)); end > int64(len(h.contents)) {
		h.contents = resized(h.contents, end)
	}
	copy(h.contents[off:], data)
	h.dirty = true
	h.changed = time.Now()
	return uint32(len(data)), 0
}

func (h *handle) resize(size int64) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.node.ns.holds(size) {
		return syscall.EFBIG
	}
	h.contents = resized(h.contents, size)
	h.dirty = true
	h.changed = time.Now()
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
	return 0
}

// describeUncommitted overrides what the namespace reports about a file with what this
// handle holds, while the two differ.
func (h *handle) describeUncommitted(out *gofuse.Attr) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		return
	}
	out.Size = uint64(len(h.contents))
	out.Mtime, out.Ctime = uint64(h.changed.Unix()), uint64(h.changed.Unix())
	out.Mtimensec = uint32(h.changed.Nanosecond())
	out.Ctimensec = out.Mtimensec
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
