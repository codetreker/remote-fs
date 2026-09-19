package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type directoryHandle struct {
	node      *node
	reference storage.NodeReference
	scope     storage.UseScope
	mu        sync.Mutex
	stream    fs.DirStream
	closed    bool
}

var (
	_ fs.NodeOpendirHandler = (*node)(nil)
	_ fs.FileReaddirenter   = (*directoryHandle)(nil)
	_ fs.FileLookuper       = (*directoryHandle)(nil)
	_ fs.FileSeekdirer      = (*directoryHandle)(nil)
	_ fs.FileReleasedirer   = (*directoryHandle)(nil)
)

func (n *node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, 0, errnoOf(err)
	}
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EISDIR
	}
	references, ok := n.volume.files.(storage.NodeReferences)
	if !ok {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if err := references.CheckNodeReferences(); err != nil {
		return nil, 0, errnoOf(err)
	}
	result, err := references.OpenNodeRef(ctx, n.id.node, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: n.id.node}, Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata})
	if result.Reference == nil {
		if err == nil {
			err = syscall.EIO
		}
		return nil, 0, errnoOf(err)
	}
	cleanup := func(cause error) error {
		call, cancel := n.volume.cleanupContext(ctx)
		defer cancel()
		return errors.Join(cause, n.volume.closeError(result.Reference.Close(call)))
	}
	if result.Outcome != storage.Opened {
		err = errors.Join(syscall.EIO, err)
	}
	if err == nil {
		err = n.checkAttr(result.Attr)
	}
	scoped, ok := result.Reference.(storage.ScopedReference)
	if err == nil && !ok {
		err = syscall.EOPNOTSUPP
	}
	if err == nil {
		err = scoped.CheckScopedReference()
	}
	var scope storage.UseScope
	if err == nil {
		scope, err = scoped.Scope(ctx)
	}
	if err == nil {
		err = scope.Check()
	}
	if err != nil {
		return nil, 0, errnoOf(cleanup(err))
	}
	return &directoryHandle{node: n, reference: result.Reference, scope: scope}, 0, 0
}

func (d *directoryHandle) check() error {
	if err := d.node.volume.check(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return syscall.EBADF
	}
	return nil
}

func (d *directoryHandle) load(ctx context.Context) error {
	if err := d.check(); err != nil {
		return err
	}
	d.mu.Lock()
	loaded := d.stream != nil
	d.mu.Unlock()
	if loaded {
		return nil
	}
	stream, errno := d.node.readdir(ctx, d.scope)
	if errno != 0 {
		return errno
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		stream.Close()
		return syscall.EBADF
	}
	if d.stream != nil {
		stream.Close()
	} else {
		d.stream = stream
	}
	return nil
}

func (d *directoryHandle) Readdirent(ctx context.Context) (*gofuse.DirEntry, syscall.Errno) {
	if err := d.load(ctx); err != nil {
		return nil, errnoOf(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, syscall.EBADF
	}
	if !d.stream.HasNext() {
		return nil, 0
	}
	entry, errno := d.stream.Next()
	return &entry, errno
}

func (d *directoryHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	if err := d.load(ctx); err != nil {
		return errnoOf(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return syscall.EBADF
	}
	seek, ok := d.stream.(fs.FileSeekdirer)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return seek.Seekdir(ctx, off)
}

func (d *directoryHandle) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := d.check(); err != nil {
		return nil, errnoOf(err)
	}
	return d.node.lookup(ctx, name, out, &d.scope)
}

func (d *directoryHandle) stat(ctx context.Context) (storage.Attr, error) {
	if err := d.check(); err != nil {
		return storage.Attr{}, err
	}
	attr, err := d.reference.Stat(ctx)
	if err == nil {
		err = d.node.checkAttr(attr)
	}
	return attr, err
}

func (d *directoryHandle) setAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkMutationLocked(ctx); err != nil {
		return storage.Attr{}, err
	}
	attr, err := d.node.volume.files.SetNodeAttr(ctx, d.node.id.node, change)
	if err == nil {
		err = d.node.checkAttr(attr)
	}
	return attr, err
}

// The handle owns its reference exclusively. Holding mu through the identity
// mutation prevents Releasedir from retiring its pin; session expiry remains
// subject to the backend's final publication guard.
func (d *directoryHandle) checkMutationLocked(ctx context.Context) error {
	if err := d.node.volume.check(); err != nil {
		return err
	}
	if d.closed {
		return syscall.EBADF
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := d.reference.(storage.ScopedReference).Scope(ctx)
	if err != nil {
		return err
	}
	if scope != d.scope {
		return storage.ErrInvalidScope
	}
	return nil
}

func (d *directoryHandle) Releasedir(ctx context.Context, _ uint32) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	if d.stream != nil {
		d.stream.Close()
		d.stream = nil
	}
	d.mu.Unlock()
	cleanup, cancel := d.node.volume.cleanupContext(ctx)
	defer cancel()
	_ = d.node.volume.closeError(d.reference.Close(cleanup))
}
