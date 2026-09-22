package smb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type namespaceBackend struct {
	storage.FileStorage
	root  storage.Attr
	err   error
	loads int
}

func (b *namespaceBackend) Stat(ctx context.Context, name string) (storage.Attr, error) {
	if name != "" {
		return storage.Attr{}, errors.New("path stat used below root")
	}
	if b.err != nil {
		return storage.Attr{}, b.err
	}
	scalar := b.root
	scalar.Metadata = nil
	metadataBytes, err := storage.MetadataSize(b.root.Metadata)
	if err != nil {
		return storage.Attr{}, err
	}
	if err := storage.CheckAttrResultBudget(ctx, scalar, int64(metadataBytes)); err != nil {
		return storage.Attr{}, err
	}
	b.loads++
	return b.root.Clone(), nil
}

type namespaceView struct {
	revision []byte
	entries  []storage.Entry
	err      error
}

type namespaceReference struct {
	id       uint64
	scope    storage.UseScope
	closed   int
	closeFn  func(int) error
	checkErr error
}

func (r *namespaceReference) ReferenceNodeID() (uint64, error) { return r.id, nil }
func (*namespaceReference) CheckScopedReference() error        { return nil }
func (r *namespaceReference) Scope(context.Context) (storage.UseScope, error) {
	return r.scope, nil
}
func (*namespaceReference) CheckReferenceState() error { return nil }
func (*namespaceReference) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{}, syscall.EBADF
}
func (*namespaceReference) CheckReferenceNameObservation() error { return nil }
func (*namespaceReference) ObserveName(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error) {
	return storage.NameObservation{}, syscall.EBADF
}
func (*namespaceReference) CheckMetadataAccess() error { return nil }
func (*namespaceReference) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{}, syscall.EBADF
}
func (*namespaceReference) CheckDeleteIntent() error { return nil }
func (*namespaceReference) SetPendingUnlink(context.Context, storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return storage.ReferenceState{}, syscall.EBADF
}
func (*namespaceReference) ClearPendingUnlink(context.Context, storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return storage.ReferenceState{}, syscall.EBADF
}
func (r *namespaceReference) CheckConditionalFileMutation() error { return r.checkErr }
func (*namespaceReference) MutateFile(context.Context, storage.FileMutation) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*namespaceReference) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*namespaceReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (r *namespaceReference) Close(context.Context) error {
	r.closed++
	if r.closeFn != nil {
		return r.closeFn(r.closed)
	}
	return nil
}

type namespaceSession struct {
	storage.FileSession
	views       map[uint64]*namespaceView
	reads       []storage.DirectoryTarget
	opens       []storage.ChildSelection
	references  []*namespaceReference
	openOptions []storage.NodeRefOptions
	beforeRead  func(uint64)
	beforeOpen  func(storage.ChildSelection)
	loadedNames int
}

func (*namespaceSession) CheckDirectoryRead() error                { return nil }
func (*namespaceSession) CheckNodeReferences() error               { return nil }
func (*namespaceSession) CheckDirectoryMetadataObservation() error { return nil }

func (s *namespaceSession) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{}, syscall.EOPNOTSUPP
}

func (s *namespaceSession) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	return s.readDirectory(target, result, nil)
}

func (s *namespaceSession) ObserveDirectoryMetadata(_ context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	observation, err := s.readDirectory(target, result, options.Guards)
	return storage.DirectoryMetadataObservation{Observation: observation}, err
}

func (s *namespaceSession) readDirectory(target storage.DirectoryTarget, result *storage.ListResult, guards *storage.NamespaceGuards) (storage.DirectoryObservation, error) {
	s.reads = append(s.reads, target)
	if s.beforeRead != nil {
		s.beforeRead(target.NodeID)
	}
	if guards != nil {
		if err := checkNamespaceTestGuards(s.views, guards); err != nil {
			return storage.DirectoryObservation{}, err
		}
	}
	view := s.views[target.NodeID]
	if view == nil {
		return storage.DirectoryObservation{}, syscall.ENOENT
	}
	observation := storage.DirectoryObservation{ParentID: target.NodeID, Revision: bytes.Clone(view.revision)}
	for _, entry := range view.entries {
		metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
		if err != nil {
			return storage.DirectoryObservation{}, err
		}
		scalar := entry.Attr
		scalar.Metadata = nil
		reserved, err := result.Reserve(int64(len(entry.Name)), int64(metadataBytes), scalar)
		if err != nil {
			return storage.DirectoryObservation{}, err
		}
		s.loadedNames++
		if err := reserved.Commit(entry.Name, entry.Attr.Metadata); err != nil {
			return storage.DirectoryObservation{}, err
		}
	}
	return observation, view.err
}

func (s *namespaceSession) OpenNodeRef(_ context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.openOptions = append(s.openOptions, options)
	if id != 1 || options.Kind != storage.NodeDirectory || options.Target.NodeID != id {
		return storage.NodeOpenResult{}, syscall.EINVAL
	}
	ref := &namespaceReference{id: id, scope: storage.UseScope{Token: "root-scope"}}
	s.references = append(s.references, ref)
	return storage.NodeOpenResult{Reference: ref, Attr: storage.Attr{ID: id, Kind: storage.NodeDirectory}, Outcome: storage.Opened}, nil
}

func (s *namespaceSession) OpenChildRef(_ context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens = append(s.opens, selection.Clone())
	s.openOptions = append(s.openOptions, options)
	if s.beforeOpen != nil {
		s.beforeOpen(selection)
	}
	if err := checkNamespaceTestGuards(s.views, selection.Guards); err != nil {
		return storage.NodeOpenResult{}, err
	}
	id := options.Target.NodeID
	ref := &namespaceReference{id: id, scope: storage.UseScope{Token: "scope-" + string(selection.Name.RawLeaf)}}
	s.references = append(s.references, ref)
	return storage.NodeOpenResult{Reference: ref, Attr: storage.Attr{ID: id, Kind: storage.NodeDirectory}, Outcome: storage.Opened}, nil
}

func checkNamespaceTestGuards(views map[uint64]*namespaceView, guards *storage.NamespaceGuards) error {
	if guards == nil || guards.RootID != 1 {
		return storage.ErrConditionConflict
	}
	for _, directory := range guards.Directories {
		view := views[directory.ParentID]
		if view == nil || !bytes.Equal(view.revision, directory.Revision) {
			return storage.ErrConditionConflict
		}
	}
	for _, edge := range guards.Edges {
		found := false
		for _, entry := range views[edge.ParentID].entries {
			if entry.Name == string(edge.RawLeaf) && entry.Attr.ID == edge.ChildID {
				found = true
			}
		}
		if !found {
			return storage.ErrConditionConflict
		}
	}
	return nil
}

func namespaceTree(t *testing.T) (*tree, *namespaceBackend, *namespaceSession) {
	t.Helper()
	registry := newHandleTestRegistry(t, 16)
	backend := &namespaceBackend{root: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}
	session := &namespaceSession{views: map[uint64]*namespaceView{
		1: {revision: []byte("root-1"), entries: []storage.Entry{{Name: "Folder", Attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory}}}},
		2: {revision: []byte("folder-1"), entries: []storage.Entry{{Name: "Actual", Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular, Size: 7}}}},
	}}
	registry.tree.export.share = endpointShare("data", "volume", backend)
	registry.tree.authority.raw = session
	registry.tree.authority.actionEpoch = 1
	return registry.tree, backend, session
}

func testNamespaceAction(t *testing.T) namespaceActionFactory {
	t.Helper()
	return func() (storage.FileActionID, error) { return storage.NewFileActionID(1) }
}

func TestNamespaceResolverRetainsEveryDirectoryAndBuildsCompleteGuards(t *testing.T) {
	tree, backend, session := namespaceTree(t)
	var retained []handleReference
	var operations []storage.Operation
	resolved, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"folder", "ACTUAL"}},
		func(reference handleReference) { retained = append(retained, reference) },
		func(_ context.Context, request authz.AccessRequest) error {
			operations = append(operations, request.Operation)
			return nil
		},
		testNamespaceAction(t), testNameCompare)
	if err != nil {
		t.Fatal(err)
	}
	if backend.loads != 1 || len(retained) != 2 || len(session.reads) != 2 || len(session.opens) != 1 {
		t.Fatalf("calls: root=%d retained=%d reads=%d child-opens=%d", backend.loads, len(retained), len(session.reads), len(session.opens))
	}
	if session.reads[0].NodeID != 1 || session.reads[0].Scope == nil || session.reads[0].Scope.Token != "root-scope" ||
		session.reads[1].NodeID != 2 || session.reads[1].Scope == nil || session.reads[1].Scope.Token != "scope-Folder" {
		t.Fatalf("unscoped traversal: %+v", session.reads)
	}
	if resolved.selection.Name.Parent.NodeID != 2 || resolved.selection.Name.Parent.Scope == nil ||
		resolved.selection.Name.Parent.Scope.Token != "scope-Folder" || string(resolved.selection.Name.RawLeaf) != "Actual" ||
		resolved.condition.State != storage.SameNode || resolved.condition.NodeID != 3 || len(resolved.condition.ExpectedMetadata) != 0 ||
		resolved.attr == nil || resolved.attr.ID != 3 {
		t.Fatalf("resolution = %+v", resolved)
	}
	guards := resolved.selection.Guards
	if guards == nil || guards.RootID != 1 || len(guards.Directories) != 2 || len(guards.Edges) != 2 ||
		string(guards.Edges[0].RawLeaf) != "Folder" || string(guards.Edges[1].RawLeaf) != "Actual" {
		t.Fatalf("guards = %+v", guards)
	}
	wantOperations := []storage.Operation{
		storage.OpVolumeStat, storage.OpFileOpenNodeRef, storage.OpFileScope, storage.OpFileObserveDirectoryMetadata,
		storage.OpFileOpenChildRef, storage.OpFileScope, storage.OpFileObserveDirectoryMetadata,
	}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("operations = %v", operations)
	}
	for _, options := range session.openOptions {
		if options.Use != (storage.UseClaim{}) {
			t.Fatalf("resolver directory reference carries application use claim: %+v", options.Use)
		}
	}
	status := Status{}
	tree.files.addStatus(&status)
	if status.DirectoryEntries != 1 || status.DirectoryBytes <= 256 {
		t.Fatalf("selected result is not retained under a bound: %+v", status)
	}
	resolved.releaseCapture()
	status = Status{}
	tree.files.addStatus(&status)
	if status.DirectoryEntries != 0 || status.DirectoryBytes != 0 {
		t.Fatalf("released directory capture remains charged: %+v", status)
	}
}

func TestNamespaceResolverValidatesAllSiblingsBeforeSelectingLeaf(t *testing.T) {
	for _, invalid := range []storage.Entry{
		{Name: "bad.", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}},
		{Name: "ACTUAL", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}},
	} {
		tree, _, session := namespaceTree(t)
		session.views[2].entries = append(session.views[2].entries, invalid)
		_, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder", "Actual"}},
			func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare)
		if err == nil {
			t.Fatalf("invalid sibling set accepted: %+v", invalid)
		}
	}
}

func TestNamespaceResolverDistinguishesFinalAbsenceAndUnknownState(t *testing.T) {
	tree, backend, session := namespaceTree(t)
	resolved, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder", "New"}},
		func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare)
	if err != nil || resolved.condition.State != storage.Absent || string(resolved.selection.Name.RawLeaf) != "New" ||
		len(resolved.selection.Guards.Directories) != 2 || len(resolved.selection.Guards.Edges) != 1 {
		t.Fatalf("final absence = %+v, %v", resolved, err)
	}
	if _, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Missing", "New"}},
		func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare); !errors.Is(err, errPathMissing) {
		t.Fatalf("intermediate absence = %v", err)
	}
	backend.err = syscall.ENOENT
	if _, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"x"}},
		func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare); !errors.Is(err, syscall.ENOENT) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("unknown root became absence: %v", err)
	}
	session.views[1].err = syscall.ENOENT
	backend.err = nil
	if _, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"x"}},
		func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare); !errors.Is(err, syscall.ENOENT) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("unknown directory result became absence: %v", err)
	}
}

func TestNamespaceResolverBoundsPayloadBeforeLoading(t *testing.T) {
	tree, _, session := namespaceTree(t)
	tree.files.limits.MaxDirectoryBytes = 1
	_, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder"}},
		func(handleReference) {}, func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare)
	if !errors.Is(err, syscall.ENOMEM) || session.loadedNames != 0 {
		t.Fatalf("over-budget capture = %v, loaded names=%d", err, session.loadedNames)
	}
	status := Status{}
	tree.files.addStatus(&status)
	if status.DirectoryEntries != 0 || status.DirectoryBytes != 0 {
		t.Fatalf("failed capture leaked charge: %+v", status)
	}

	tree, _, _ = namespaceTree(t)
	tree.files.limits.MaxDirectoryBytes = 256
	first, err := tree.files.reserveDirectoryCapture(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.files.reserveDirectoryCapture(t.Context(), 1); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("concurrent capture above aggregate bound = %v", err)
	}
	first.release()
	second, err := tree.files.reserveDirectoryCapture(t.Context(), 1)
	if err != nil {
		t.Fatalf("released capture did not restore capacity: %v", err)
	}
	second.release()
}

func TestNamespaceResolverRejectsPrefixMutationAtRetainedChildOpen(t *testing.T) {
	tree, _, session := namespaceTree(t)
	session.beforeOpen = func(storage.ChildSelection) {
		session.views[1].revision = []byte("root-2")
	}
	var retained []handleReference
	_, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder", "Actual"}},
		func(reference handleReference) { retained = append(retained, reference) },
		func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare)
	if !errors.Is(err, storage.ErrConditionConflict) || len(retained) != 1 {
		t.Fatalf("prefix mutation = %v retained=%d", err, len(retained))
	}
}

func TestNamespaceResolverAuthorizesScopeBeforeRetainingRoot(t *testing.T) {
	tree, _, session := namespaceTree(t)
	_, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder"}}, func(handleReference) {},
		func(_ context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpFileScope {
				return authz.ErrDenied
			}
			return nil
		}, testNamespaceAction(t), testNameCompare)
	if !errors.Is(err, authz.ErrDenied) || len(session.references) != 0 {
		t.Fatalf("scope denial = %v, retained=%d", err, len(session.references))
	}
}

func TestNamespaceResolverPreservesExplicitDenialJoinedWithENOENT(t *testing.T) {
	for _, denied := range []storage.Operation{storage.OpVolumeStat, storage.OpFileObserveDirectoryMetadata} {
		tree, _, session := namespaceTree(t)
		connection := &connection{server: tree.export.server}
		tree.export.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
			if request.Operation == denied {
				return errors.Join(authz.ErrDenied, syscall.ENOENT)
			}
			return nil
		})
		var retained []handleReference
		_, err := resolveNameWithComparer(t.Context(), tree, smbPath{components: []string{"Folder"}},
			func(reference handleReference) { retained = append(retained, reference) },
			func(ctx context.Context, request authz.AccessRequest) error {
				return connection.authorizeFileAccess(ctx, tree, request)
			},
			testNamespaceAction(t), testNameCompare)
		if !errors.Is(err, authz.ErrDenied) || storage.ErrnoOf(err) != syscall.EACCES || namespaceStatus(err) != statusDenied {
			t.Fatalf("%s denial = %v, errno=%v, status=%#x", denied, err, storage.ErrnoOf(err), namespaceStatus(err))
		}
		for _, reference := range retained {
			if closeErr := reference.Close(t.Context()); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if denied == storage.OpFileObserveDirectoryMetadata && len(session.references) != 1 {
			t.Fatalf("observation denial retained %d root references", len(session.references))
		}
	}
}

func TestNamespaceResolverRootDoesNotCreateTemporaryReference(t *testing.T) {
	tree, _, _ := namespaceTree(t)
	retained := 0
	resolved, err := resolveNameWithComparer(t.Context(), tree, smbPath{}, func(handleReference) { retained++ },
		func(context.Context, authz.AccessRequest) error { return nil }, testNamespaceAction(t), testNameCompare)
	if err != nil || !resolved.root || resolved.rootID != 1 || retained != 0 || resolved.attr == nil || resolved.attr.ID != 1 {
		t.Fatalf("root resolution = %+v, %v, retained=%d", resolved, err, retained)
	}
	resolved.releaseCapture()
}
