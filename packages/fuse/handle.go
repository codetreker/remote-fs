package fuse

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// A handle retains one object across namespace changes. No contents or pathname are
// retained here; every operation observes the authoritative object state.
type handle struct {
	node     *node
	file     storage.File
	readable bool
	writable bool

	closeMu   sync.Mutex
	closeDone chan struct{}
	closeErr  error
}

var (
	_ fs.FileReader  = (*handle)(nil)
	_ fs.FileWriter  = (*handle)(nil)
	_ fs.FileFsyncer = (*handle)(nil)
)

func newHandle(n *node, file storage.File, readable, writable bool) *handle {
	return &handle{node: n, file: file, readable: readable, writable: writable}
}

func (h *handle) check() error {
	if err := h.node.ns.check(); err != nil {
		return err
	}
	h.closeMu.Lock()
	closed := h.closeDone != nil
	h.closeMu.Unlock()
	if closed {
		return syscall.EBADF
	}
	return nil
}

func (h *handle) stat(ctx context.Context) (storage.Attr, error) {
	if err := h.check(); err != nil {
		return storage.Attr{}, err
	}
	attr, err := h.file.Stat(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	if err := h.node.checkAttr(attr); err != nil {
		return storage.Attr{}, err
	}
	return attr, nil
}

func (h *handle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	if err := h.check(); err != nil {
		return nil, errnoOf(err)
	}
	if !h.readable {
		return nil, syscall.EBADF
	}
	if off < 0 {
		return nil, syscall.EINVAL
	}
	read, err := h.file.ReadAt(ctx, off, len(dest))
	if err != nil {
		return nil, errnoOf(err)
	}
	if err := h.node.checkAttr(read.Attr); err != nil {
		return nil, errnoOf(err)
	}
	if !h.node.ns.holds(read.Attr.Size) {
		return nil, syscall.EFBIG
	}
	if len(read.Data) > len(dest) || int64(len(read.Data)) > max(read.Attr.Size-off, 0) {
		return nil, syscall.EIO
	}
	if len(dest) != 0 && off < read.Attr.Size && len(read.Data) == 0 {
		return nil, syscall.EIO
	}
	return gofuse.ReadResultData(dest[:copy(dest, read.Data)]), 0
}

func (h *handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if err := h.check(); err != nil {
		return 0, errnoOf(err)
	}
	if !h.writable {
		return 0, syscall.EBADF
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	// Subtraction avoids overflowing a caller-controlled offset near MaxInt64.
	if off > h.node.ns.maxFileSize-int64(len(data)) {
		return 0, syscall.EFBIG
	}
	current, err := h.stat(ctx)
	if err != nil {
		return 0, errnoOf(err)
	}
	if !h.node.ns.holds(current.Size) {
		return 0, syscall.EFBIG
	}
	attr, err := h.file.WriteAt(ctx, off, data)
	if err != nil {
		return 0, errnoOf(err)
	}
	if err := h.node.checkAttr(attr); err != nil {
		return 0, errnoOf(err)
	}
	return uint32(len(data)), 0
}

func (h *handle) resize(ctx context.Context, size int64) error {
	if err := h.check(); err != nil {
		return err
	}
	if !h.writable {
		return syscall.EBADF
	}
	if size < 0 {
		return syscall.EINVAL
	}
	if !h.node.ns.holds(size) {
		return syscall.EFBIG
	}
	if size != 0 {
		current, err := h.stat(ctx)
		if err != nil {
			return err
		}
		if !h.node.ns.holds(current.Size) {
			return syscall.EFBIG
		}
	}
	attr, err := h.file.Truncate(ctx, size)
	if err != nil {
		return err
	}
	return h.node.checkAttr(attr)
}

func (h *handle) setAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	if err := h.check(); err != nil {
		return storage.Attr{}, err
	}
	attr, err := h.file.SetAttr(ctx, change)
	if err != nil {
		return storage.Attr{}, err
	}
	if err := h.node.checkAttr(attr); err != nil {
		return storage.Attr{}, err
	}
	return attr, nil
}

func (h *handle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if err := h.check(); err != nil {
		if logger := h.node.ns.logger; logger != nil {
			logger.Printf("sync node %d (handle): %v; request context: %v", h.node.id.node, err, ctx.Err())
		}
		return errnoOf(err)
	}
	err := h.file.Sync(ctx)
	if err != nil {
		if logger := h.node.ns.logger; logger != nil {
			logger.Printf("sync node %d (file): %v; request context: %v", h.node.id.node, err, ctx.Err())
		}
	}
	return errnoOf(err)
}

// Reference retirement belongs to Close even after the session is fenced. Concurrent
// releases share one result; a waiting caller can stop waiting without repeating it.
func (h *handle) closeFile(ctx context.Context) error {
	h.closeMu.Lock()
	if done := h.closeDone; done != nil {
		h.closeMu.Unlock()
		select {
		case <-done:
			h.closeMu.Lock()
			err := h.closeErr
			h.closeMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.closeDone = make(chan struct{})
	h.closeMu.Unlock()

	err := h.node.ns.closeError(h.file.Close(ctx))
	h.closeMu.Lock()
	h.closeErr = err
	close(h.closeDone)
	h.closeMu.Unlock()
	return err
}
