package fuse

import (
	"context"
	iofs "io/fs"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type nodeReferenceFixture struct {
	attr                   storage.Attr
	scope                  storage.UseScope
	link                   []byte
	closes, stats, updates int
	clean                  bool
}

func (r *nodeReferenceFixture) Stat(context.Context) (storage.Attr, error) {
	r.stats++
	if r.closes != 0 {
		return storage.Attr{}, syscall.EBADF
	}
	return r.attr, nil
}
func (r *nodeReferenceFixture) SetAttr(_ context.Context, change storage.AttrChange) (storage.Attr, error) {
	if change.ModTime != nil {
		r.attr.ModTime = *change.ModTime
	}
	r.updates++
	return r.attr, nil
}
func (r *nodeReferenceFixture) Close(ctx context.Context) error {
	r.closes++
	_, deadline := ctx.Deadline()
	r.clean = ctx.Err() == nil && deadline
	return nil
}
func (r *nodeReferenceFixture) CheckScopedReference() error { return nil }
func (r *nodeReferenceFixture) Scope(context.Context) (storage.UseScope, error) {
	return r.scope, nil
}
func (r *nodeReferenceFixture) CheckReferenceState() error { return nil }
func (r *nodeReferenceFixture) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: r.attr, LinkTarget: append([]byte(nil), r.link...)}, nil
}

type directorySessionFixture struct {
	*namespaceFixture
	reference      *nodeReferenceFixture
	openOptions    storage.NodeRefOptions
	openID         uint64
	opens, updates int
}

func (s *directorySessionFixture) CheckNodeReferences() error { return nil }
func (s *directorySessionFixture) OpenNodeRef(_ context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens++
	s.openID = id
	s.openOptions = options
	return storage.NodeOpenResult{Reference: s.reference, Attr: s.reference.attr, Outcome: storage.Opened}, nil
}
func (s *directorySessionFixture) OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	panic("known inode was reopened through a parent name")
}
func (s *directorySessionFixture) CheckMetadataAccess() error { return nil }
func (s *directorySessionFixture) SetMetadata(_ context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	if id != s.reference.attr.ID || namespace != posix.Namespace {
		return storage.OpaquePayload{}, syscall.EINVAL
	}
	s.updates++
	payload := storage.OpaquePayload{Version: []byte{byte(s.updates)}, Data: append([]byte(nil), data...)}
	if s.reference.attr.Metadata == nil {
		s.reference.attr.Metadata = make(map[string]storage.OpaquePayload)
	}
	s.reference.attr.Metadata[namespace] = payload
	return payload, nil
}
func (s *directorySessionFixture) SetNodeAttr(_ context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	if id != s.reference.attr.ID {
		return storage.Attr{}, syscall.ESTALE
	}
	return s.reference.SetAttr(context.Background(), change)
}

func TestReadOnlyDirectoryHandleRetainsIdentityWithoutRequestingWriteAccess(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 7, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "opened-directory"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	namespace.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 7 || name.Parent.Scope == nil || *name.Parent.Scope != reference.scope {
			t.Fatalf("scoped lookup = %+v", name)
		}
		return storage.Attr{ID: 8, Kind: storage.NodeRegular}, nil
	}

	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY|syscall.O_DIRECTORY)
	if errno != 0 {
		t.Fatal(errno)
	}
	if session.openID != 7 || session.openOptions.Use != (storage.UseClaim{Uses: storage.ReadEntries}) || session.openOptions.MetadataAccess != storage.ReadMetadata {
		t.Fatalf("read-only directory options = %+v", session.openOptions)
	}
	if epoch, err := session.openOptions.Action.Epoch(); err != nil || epoch != 7 {
		t.Fatalf("directory action = %q epoch=%d err=%v", session.openOptions.Action, epoch, err)
	}
	handle := opened.(*directoryHandle)
	if handle.scope != reference.scope {
		t.Fatalf("retained scope = %+v", handle.scope)
	}
	if _, errno := handle.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	var out gofuse.AttrOut
	if errno := root.Getattr(t.Context(), handle, &out); errno != 0 {
		t.Fatal(errno)
	}
	if errno := root.Setattr(t.Context(), handle, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MODE, Mode: 0700}}, &out); errno != 0 {
		t.Fatal(errno)
	}
	mode, err := permissions(reference.attr)
	if err != nil || mode.Perm() != iofs.FileMode(0700) || session.updates != 1 {
		t.Fatalf("directory mode=%v updates=%d err=%v", mode, session.updates, err)
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	handle.Releasedir(canceled, 0)
	handle.Releasedir(canceled, 0)
	if reference.closes != 1 || !reference.clean || session.opens != 1 {
		t.Fatalf("closes=%d clean=%v opens=%d", reference.closes, reference.clean, session.opens)
	}
}

func TestReadlinkUsesRetainedSymlinkIdentityAndClosesIt(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 9, Kind: storage.NodeSymlink, Size: 9},
		link: []byte("../target"), scope: storage.UseScope{Token: "symlink"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	link := root.child(t.Context(), "link", syscall.S_IFLNK|0777, 9).Operations().(*node)

	target, errno := link.Readlink(t.Context())
	if errno != 0 || string(target) != "../target" {
		t.Fatalf("readlink=%q errno=%v", target, errno)
	}
	if session.openID != 9 || session.openOptions.Kind != storage.NodeSymlink || session.openOptions.Target.State != storage.SameNode || session.openOptions.Target.NodeID != 9 || session.openOptions.MetadataAccess != storage.ReadMetadata {
		t.Fatalf("readlink options = %+v", session.openOptions)
	}
	if reference.closes != 1 || !reference.clean {
		t.Fatalf("closes=%d clean=%v", reference.closes, reference.clean)
	}
}

func TestReadlinkRejectsEmptyAuthorityTargetAndClosesReference(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 9, Kind: storage.NodeSymlink},
		scope: storage.UseScope{Token: "empty-symlink"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	link := root.child(t.Context(), "link", syscall.S_IFLNK|0777, 9).Operations().(*node)

	if target, errno := link.Readlink(t.Context()); errno != syscall.EIO || target != nil {
		t.Fatalf("readlink=%q errno=%v", target, errno)
	}
	if reference.closes != 1 || !reference.clean {
		t.Fatalf("closes=%d clean=%v", reference.closes, reference.clean)
	}
}

func TestDirectoryHandleLookupKeepsScopeAfterParentRenameAndNameReuse(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 8, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "renamed-directory"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	old := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 8)
	if !root.AddChild("parent", old, false) {
		t.Fatal("attaching parent")
	}
	handleValue, _, errno := old.Operations().(*node).OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	handle := handleValue.(*directoryHandle)
	t.Cleanup(func() { handle.Releasedir(context.Background(), 0) })
	if !root.MvChild("parent", &root.Inode, "moved", true) {
		t.Fatal("moving parent")
	}
	root.id.move("parent", root.id, "moved")
	replacement := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 10)
	if !root.AddChild("parent", replacement, false) {
		t.Fatal("attaching replacement parent")
	}
	namespace.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 8 || name.Parent.Scope == nil || *name.Parent.Scope != reference.scope {
			t.Fatalf("lookup rebound to replacement: %+v", name)
		}
		return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
	}
	if _, errno := handle.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
}

// A retained directory protects descriptor operations, but the path-based listing can
// observe a replacement directory. The test keeps that boundary explicit.
func TestDirectoryEnumerationRemainsPathBasedAcrossExternalParentReplacement(t *testing.T) {
	_, backing := memoryfixture.New(t, "directory-path-list", 0, locking.DefaultOptions())
	if err := backing.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Write(t.Context(), "parent/original", []byte("old")); err != nil {
		t.Fatal(err)
	}
	parentAttr, err := backing.Stat(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	session, err := backing.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	v := activeTestVolume(backing, 1024)
	v.files = session
	rootAttr, err := backing.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	root := &node{volume: v, id: rootIdentity(rootAttr.ID)}
	fs.NewNodeFS(root, &fs.Options{})
	parent := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, parentAttr.ID)
	if !root.AddChild("parent", parent, false) {
		t.Fatal("attaching parent")
	}
	handle, _, errno := parent.Operations().(*node).OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	directory := handle.(*directoryHandle)
	t.Cleanup(func() { directory.Releasedir(context.Background(), 0) })

	if err := backing.Rename(t.Context(), "parent", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Write(t.Context(), "parent/replacement", []byte("new")); err != nil {
		t.Fatal(err)
	}
	entry, errno := directory.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "replacement" {
		t.Fatalf("path-based enumeration = %+v, %v", entry, errno)
	}
}
