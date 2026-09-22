package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type guardedCreateFile struct {
	*namespaceReference
	attr storage.Attr
}

func (f *guardedCreateFile) Stat(context.Context) (storage.Attr, error) { return f.attr.Clone(), nil }
func (*guardedCreateFile) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	return storage.FileRead{}, syscall.EBADF
}
func (*guardedCreateFile) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*guardedCreateFile) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*guardedCreateFile) Sync(context.Context) error { return nil }

type guardedCreateSession struct {
	*namespaceSession
	attr       storage.Attr
	attempts   int
	selections []storage.ChildSelection
	options    []storage.OpenAtOptions
	file       *guardedCreateFile
	open       func(context.Context, storage.ChildSelection, storage.OpenAtOptions) (storage.OpenResult, error)
}

func (*guardedCreateSession) CheckAtomicFileOpen() error { return nil }

func (s *guardedCreateSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	s.attempts++
	s.selections = append(s.selections, selection.Clone())
	s.options = append(s.options, options)
	if s.open != nil {
		return s.open(ctx, selection, options)
	}
	if s.attempts == 1 {
		s.views[1].entries = append(s.views[1].entries, storage.Entry{Name: "NEW", Attr: s.attr.Clone()})
		s.views[1].revision = []byte("root-2")
	}
	if err := checkNamespaceTestGuards(s.views, selection.Guards); err != nil {
		return storage.OpenResult{}, err
	}
	scalar := s.attr
	scalar.Metadata = nil
	metadataBytes, err := storage.MetadataSize(s.attr.Metadata)
	if err != nil {
		return storage.OpenResult{}, err
	}
	if err := storage.CheckAttrResultBudget(ctx, scalar, int64(metadataBytes)); err != nil {
		return storage.OpenResult{}, err
	}
	return storage.OpenResult{File: s.file, Attr: s.attr.Clone(), Outcome: storage.Opened}, nil
}

func TestCreateRetainsPartialFinalOwnerUntilCleanupRetry(t *testing.T) {
	smbTree, _, namespace := namespaceTree(t)
	attr := createTestAttr(t, 5, storage.NodeRegular, dosArchive)
	cleanupFailure := errors.New("reference cleanup failed")
	reference := &namespaceReference{id: attr.ID, scope: storage.UseScope{Token: "file-scope"}, closeFn: func(attempt int) error {
		if attempt == 1 {
			return cleanupFailure
		}
		return nil
	}}
	file := &guardedCreateFile{namespaceReference: reference, attr: attr}
	session := &guardedCreateSession{namespaceSession: namespace}
	session.open = func(ctx context.Context, _ storage.ChildSelection, _ storage.OpenAtOptions) (storage.OpenResult, error) {
		scalar := attr
		scalar.Metadata = nil
		metadataBytes, err := storage.MetadataSize(attr.Metadata)
		if err != nil {
			return storage.OpenResult{}, err
		}
		if err := storage.CheckAttrResultBudget(ctx, scalar, int64(metadataBytes)); err != nil {
			return storage.OpenResult{}, err
		}
		return storage.OpenResult{File: file, Attr: attr.Clone(), Outcome: storage.Created}, syscall.EIO
	}
	smbTree.authority.raw = session
	smbTree.export.share.DeleteIntentOwner = createTestOwner(t)
	connection := &connection{server: smbTree.export.server}
	request := createTestRequest(2)
	request.Name = "new"
	resolver := func(context.Context, *tree, string, namespaceReferenceRetainer, namespaceAuthorizer, namespaceActionFactory) (resolvedName, error) {
		return createTestResolved(nil), nil
	}
	body, err := connection.createOpenWithResolver(t.Context(), smbTree, request, resolver)
	if body != nil || !errors.Is(err, syscall.EIO) || !errors.Is(err, cleanupFailure) || reference.closed != 1 {
		t.Fatalf("partial result = %x, %v, close calls=%d", body, err, reference.closed)
	}
	smbTree.files.mu.Lock()
	reservations := len(smbTree.files.reservations)
	smbTree.files.mu.Unlock()
	if reservations != 1 {
		t.Fatalf("partial owner reservations = %d", reservations)
	}
	if err := smbTree.files.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if reference.closed != 2 {
		t.Fatalf("cleanup retry calls = %d", reference.closed)
	}
}

func TestCreateReservesFileIDAndSlotBeforeNamespaceAuthority(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	_, _ = reserveFileHandle(t, registry, &handleReferenceProbe{}, 6)
	connection := &connection{server: registry.tree.export.server}
	request := createTestRequest(3)
	request.Name = "new"
	resolverCalls := 0
	resolver := func(context.Context, *tree, string, namespaceReferenceRetainer, namespaceAuthorizer, namespaceActionFactory) (resolvedName, error) {
		resolverCalls++
		return createTestResolved(nil), nil
	}
	body, err := connection.createOpenWithResolver(t.Context(), registry.tree, request, resolver)
	if body != nil || !errors.Is(err, syscall.ENOMEM) || resolverCalls != 0 {
		t.Fatalf("pre-reservation = %x, %v, resolver calls=%d", body, err, resolverCalls)
	}
}

func TestCreateAuthorizesScopeBeforeFinalOpenEffect(t *testing.T) {
	smbTree, _, namespace := namespaceTree(t)
	session := &guardedCreateSession{namespaceSession: namespace}
	smbTree.authority.raw = session
	smbTree.export.share.DeleteIntentOwner = createTestOwner(t)
	smbTree.export.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpFileScope {
			return authz.ErrDenied
		}
		return nil
	})
	connection := &connection{server: smbTree.export.server}
	request := createTestRequest(2)
	request.Name = "new"
	resolver := func(context.Context, *tree, string, namespaceReferenceRetainer, namespaceAuthorizer, namespaceActionFactory) (resolvedName, error) {
		return createTestResolved(nil), nil
	}
	body, err := connection.createOpenWithResolver(t.Context(), smbTree, request, resolver)
	if body != nil || !errors.Is(err, authz.ErrDenied) || session.attempts != 0 {
		t.Fatalf("scope denial = %x, %v, open attempts=%d", body, err, session.attempts)
	}
}

func TestCreateClassifiesAuthorizationInabilityAsIO(t *testing.T) {
	smbTree, _, _ := namespaceTree(t)
	smbTree.export.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		return fmt.Errorf("policy backend unavailable: %w", syscall.ENOENT)
	})
	connection := &connection{server: smbTree.export.server}
	resolver := func(ctx context.Context, tree *tree, _ string, _ namespaceReferenceRetainer, authorize namespaceAuthorizer, _ namespaceActionFactory) (resolvedName, error) {
		err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: storage.OpVolumeStat})
		return resolvedName{}, err
	}
	_, err := connection.createOpenWithResolver(t.Context(), smbTree, createTestRequest(1), resolver)
	if !errors.Is(err, syscall.ENOENT) || storage.ErrnoOf(err) != syscall.EIO || createStatus(err) != statusIO {
		t.Fatalf("authorization inability = %v, errno=%v, status=%#x", err, storage.ErrnoOf(err), createStatus(err))
	}
}

func TestCreateDoesNotRetryPartialConditionConflict(t *testing.T) {
	smbTree, _, namespace := namespaceTree(t)
	attr := createTestAttr(t, 10, storage.NodeRegular, dosArchive)
	reference := &namespaceReference{id: attr.ID, scope: storage.UseScope{Token: "file-scope"}}
	file := &guardedCreateFile{namespaceReference: reference, attr: attr}
	session := &guardedCreateSession{namespaceSession: namespace}
	session.open = func(ctx context.Context, _ storage.ChildSelection, _ storage.OpenAtOptions) (storage.OpenResult, error) {
		scalar := attr
		scalar.Metadata = nil
		metadataBytes, err := storage.MetadataSize(attr.Metadata)
		if err != nil {
			return storage.OpenResult{}, err
		}
		if err := storage.CheckAttrResultBudget(ctx, scalar, int64(metadataBytes)); err != nil {
			return storage.OpenResult{}, err
		}
		return storage.OpenResult{File: file, Attr: attr.Clone(), Outcome: storage.Created}, storage.ErrConditionConflict
	}
	smbTree.authority.raw = session
	smbTree.export.share.DeleteIntentOwner = createTestOwner(t)
	connection := &connection{server: smbTree.export.server}
	request := createTestRequest(2)
	request.Name = "new"
	resolverCalls := 0
	resolver := func(context.Context, *tree, string, namespaceReferenceRetainer, namespaceAuthorizer, namespaceActionFactory) (resolvedName, error) {
		resolverCalls++
		return createTestResolved(nil), nil
	}
	body, err := connection.createOpenWithResolver(t.Context(), smbTree, request, resolver)
	if body != nil || !errors.Is(err, storage.ErrConditionConflict) || storage.ErrnoOf(err) != syscall.EIO ||
		createStatus(err) != statusIO || resolverCalls != 1 || session.attempts != 1 || reference.closed != 1 {
		t.Fatalf("partial conflict = %x, %v, errno=%v, resolves=%d opens=%d closes=%d",
			body, err, storage.ErrnoOf(err), resolverCalls, session.attempts, reference.closed)
	}
}

func TestCreateRetriesCaseEquivalentInsertionAtFinalAuthorityOperation(t *testing.T) {
	smbTree, backend, namespace := namespaceTree(t)
	attr := createTestAttr(t, 4, storage.NodeRegular, dosArchive)
	file := &guardedCreateFile{namespaceReference: &namespaceReference{id: attr.ID, scope: storage.UseScope{Token: "file-scope"}}, attr: attr}
	session := &guardedCreateSession{namespaceSession: namespace, attr: attr, file: file}
	smbTree.authority.raw = session
	smbTree.export.share.Backend = backend
	smbTree.export.share.DeleteIntentOwner = createTestOwner(t)
	connection := &connection{server: smbTree.export.server}

	request := createTestRequest(3)
	request.Name = "new"
	request.DesiredAccess = fileReadData | fileReadAttributes
	request.Options = createWriteThrough
	resolver := func(ctx context.Context, tree *tree, name string, retain namespaceReferenceRetainer, authorize namespaceAuthorizer, action namespaceActionFactory) (resolvedName, error) {
		path, err := parseSMBPath(name)
		if err != nil {
			return resolvedName{}, err
		}
		return resolveNameWithComparer(ctx, tree, path, retain, authorize, action, testNameCompare)
	}
	body, err := connection.createOpenWithResolver(t.Context(), smbTree, request, resolver)
	if err != nil {
		t.Fatalf("CREATE: %v (attempts=%d refs=%d selections=%+v)", err, session.attempts, len(namespace.references), session.selections)
	}
	if session.attempts != 2 || len(session.selections) != 2 {
		t.Fatalf("guarded opens = %d", session.attempts)
	}
	if session.selections[0].Guards == nil || string(session.selections[0].Guards.Directories[0].Revision) != "root-1" ||
		session.selections[1].Guards == nil || string(session.selections[1].Guards.Directories[0].Revision) != "root-2" ||
		string(session.selections[1].Name.RawLeaf) != "NEW" || session.options[1].Target.NodeID != attr.ID || !session.options[1].Create {
		t.Fatalf("retry did not select the authority spelling: selections=%+v options=%+v", session.selections, session.options)
	}
	if len(namespace.references) != 2 || namespace.references[0].closed != 1 || namespace.references[1].closed != 1 {
		t.Fatalf("temporary root references = %+v", namespace.references)
	}
	if len(body) < 80 || binary.LittleEndian.Uint32(body[4:8]) != 1 {
		t.Fatalf("CREATE response = %x", body)
	}
	var id wire.FileID
	copy(id[:], body[64:80])
	handle := smbTree.files.get(id)
	if handle == nil || handle.file != file || handle.nodeID != attr.ID || !handle.writeThrough {
		t.Fatalf("installed handle = %+v", handle)
	}
	if err := smbTree.files.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
}
