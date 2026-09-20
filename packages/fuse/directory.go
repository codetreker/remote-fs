package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// directoryHandle retains the opened directory identity for lookup, enumeration,
// and descriptor metadata.
type directoryHandle struct {
	node      *node
	reference storage.NodeReference
	scope     storage.UseScope

	mu     sync.Mutex
	stream fs.DirStream
	closed bool
}

type capturedDirectory struct {
	entries []capturedDirectoryEntry
	present map[string]struct{}
	before  uint64
}

type capturedDirectoryEntry struct {
	name   string
	mode   uint32
	nodeID uint64
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
	action, err := n.volume.newFileAction(ctx)
	if err != nil {
		return nil, 0, errnoOf(err)
	}
	result, err := n.openNodeReference(ctx, n.id.node, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: n.id.node},
		Action: action, Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata,
	})
	unknown := result.Outcome != storage.Opened
	if result.Reference == nil {
		if err == nil {
			err = syscall.EIO
		}
		return nil, 0, errnoOf(afterMutation(result.Outcome != 0, err))
	}
	cleanup := func(cause error) error {
		return n.volume.closeUnreturnedReference(ctx, result.Reference, cause, unknown)
	}
	if result.Outcome != storage.Opened {
		err = errors.Join(syscall.EIO, err)
	}
	if err == nil {
		err = n.checkAttr(result.Attr)
	}
	if err == nil {
		err = result.Reference.CheckScopedReference()
	}
	var scope storage.UseScope
	if err == nil {
		scope, err = result.Reference.Scope(ctx)
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
	if err := d.node.volume.check(); err != nil {
		return err
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return syscall.EBADF
	}
	if d.stream != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	captured, err := d.capture(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return syscall.EBADF
	}
	if d.stream == nil {
		d.stream = captured.install(d.node.id)
	}
	return nil
}

func (d *directoryHandle) capture(ctx context.Context) (*capturedDirectory, error) {
	access, err := d.node.volume.namespace()
	if err != nil {
		return nil, err
	}
	result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0,
		func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			return storage.ObservedEntryBytes(nameBytes, metadataBytes)
		})
	if err != nil {
		return nil, err
	}
	target := storage.DirectoryTarget{NodeID: d.node.id.node, Scope: &d.scope}
	// Membership learned after this capture starts may describe a later authority
	// state and must not be discarded merely because this capture does not contain it.
	before := d.node.id.given()
	observation, err := access.ReadDirNodeBounded(ctx, target, result)
	if err != nil {
		return nil, err
	}
	entries, err := result.Entries()
	if err != nil {
		return nil, errors.Join(syscall.EIO, err)
	}
	observed := storage.ObservedDirectory{
		Observation: observation,
		Entries:     make([]storage.ObservedEntry, len(entries)),
	}
	for i, entry := range entries {
		observed.Entries[i] = storage.ObservedEntry{RawLeaf: []byte(entry.Name), Attr: entry.Attr}
	}
	if observation.ParentID != target.NodeID {
		return nil, syscall.EIO
	}
	if err := observed.Check(); err != nil {
		return nil, errors.Join(syscall.EIO, err)
	}

	captured := &capturedDirectory{
		entries: make([]capturedDirectoryEntry, len(observed.Entries)),
		present: make(map[string]struct{}, len(observed.Entries)),
		before:  before,
	}
	for i, entry := range observed.Entries {
		mode, errno := attributeMode(entry.Attr)
		if errno != 0 {
			return nil, errno
		}
		name := string(entry.RawLeaf)
		captured.entries[i] = capturedDirectoryEntry{name: name, mode: mode, nodeID: entry.Attr.ID}
		captured.present[name] = struct{}{}
	}
	return captured, nil
}

func (c *capturedDirectory) install(parent *identity) fs.DirStream {
	listing := make([]gofuse.DirEntry, len(c.entries))
	for i, entry := range c.entries {
		listing[i] = gofuse.DirEntry{
			Name: entry.name,
			Mode: entry.mode,
			Ino:  parent.child(entry.name, entry.mode, entry.nodeID).ino,
		}
	}
	parent.keepOnly(c.present, c.before)
	return fs.NewListDirStream(listing)
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
	scope, err := d.reference.Scope(ctx)
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
