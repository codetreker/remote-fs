package smb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type namespaceTestKey struct{}
type namespaceTestBackend struct {
	storage.FileStorage
	root          storage.Attr
	err           error
	calls, loads  int
	metadataBytes int64
	contextValue  any
}

func (b *namespaceTestBackend) Stat(ctx context.Context, path string) (storage.Attr, error) {
	b.calls++
	b.contextValue = ctx.Value(namespaceTestKey{})
	if path != "" {
		return storage.Attr{}, errors.New("resolver performed path Stat")
	}
	if b.err != nil {
		return storage.Attr{}, b.err
	}
	scalar := b.root
	scalar.Metadata = nil
	metadataBytes := b.metadataBytes
	if metadataBytes == 0 {
		metadataBytes = 6
	}
	if err := storage.CheckAttrResultBudget(ctx, scalar, metadataBytes); err != nil {
		return storage.Attr{}, err
	}
	b.loads++
	return b.root.Clone(), nil
}

type namespaceTestView struct {
	revision []byte
	entries  []storage.Entry
	err      error
}
type namespaceTestCall struct {
	target  storage.DirectoryTarget
	options storage.DirectoryMetadataOptions
	value   any
}
type namespaceTestSession struct {
	storage.FileSession
	views               map[uint64]*namespaceTestView
	calls               []namespaceTestCall
	loads               int
	checkErr            error
	before              func(*namespaceTestSession, storage.DirectoryTarget)
	headerMetadataBytes int64
}

func (s *namespaceTestSession) CheckDirectoryMetadataObservation() error { return s.checkErr }
func (s *namespaceTestSession) checkGuards(guards *storage.NamespaceGuards) error {
	if guards.RootID != 1 {
		return storage.ErrConditionConflict
	}
	for _, directory := range guards.Directories {
		view := s.views[directory.ParentID]
		if view == nil || !bytes.Equal(view.revision, directory.Revision) {
			return storage.ErrConditionConflict
		}
	}
	for _, edge := range guards.Edges {
		view := s.views[edge.ParentID]
		found := false
		if view != nil {
			for _, entry := range view.entries {
				if entry.Name == string(edge.RawLeaf) && entry.Attr.ID == edge.ChildID {
					found = true
				}
			}
		}
		if !found {
			return storage.ErrConditionConflict
		}
	}
	return nil
}
func (s *namespaceTestSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	s.calls = append(s.calls, namespaceTestCall{target, storage.DirectoryMetadataOptions{Guards: cloneNamespaceGuards(*options.Guards), IncludeName: options.IncludeName}, ctx.Value(namespaceTestKey{})})
	if s.before != nil {
		s.before(s, target)
	}
	if err := s.checkGuards(options.Guards); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	view := s.views[target.NodeID]
	if view == nil {
		return storage.DirectoryMetadataObservation{}, syscall.ENOENT
	}
	observation := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: view.revision}}
	for _, entry := range view.entries {
		metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
		if err != nil {
			return observation, err
		}
		if s.headerMetadataBytes != 0 {
			metadataBytes = int(s.headerMetadataBytes)
		}
		scalar := entry.Attr
		scalar.Metadata = nil
		reserved, err := result.Reserve(int64(len(entry.Name)), int64(metadataBytes), scalar)
		if err != nil {
			return observation, err
		}
		s.loads++
		if err := reserved.Commit(entry.Name, entry.Attr.Metadata); err != nil {
			return observation, err
		}
	}
	return observation, view.err
}

func namespaceFixture() (*namespaceTestBackend, *namespaceTestSession) {
	backend := &namespaceTestBackend{root: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}
	session := &namespaceTestSession{views: map[uint64]*namespaceTestView{
		1: {revision: []byte("root-one"), entries: []storage.Entry{{Name: "Folder", Attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory}}}},
		2: {revision: []byte("folder-one"), entries: []storage.Entry{{Name: "Actual", Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular, Size: 7, Metadata: map[string]storage.OpaquePayload{"foreign.namespace": {Version: []byte{1}, Data: []byte{0xff}}}}}}},
	}}
	return backend, session
}
func resolveTestName(ctx context.Context, backend *namespaceTestBackend, session *namespaceTestSession, name string, limits Limits, authorize func(context.Context, storage.Operation) error) (resolvedName, error) {
	path, err := parseSMBPath(name)
	if err != nil {
		return resolvedName{}, err
	}
	return resolveNameWithComparer(ctx, backend, session, path, limits, authorize, testNameCompare)
}
func allowNamespace(context.Context, storage.Operation) error { return nil }

func TestSMBNamespacePreservesRawIdentityGuardsAndAuthorization(t *testing.T) {
	backend, session := namespaceFixture()
	ctx := context.WithValue(t.Context(), namespaceTestKey{}, "principal")
	var operations []storage.Operation
	authorize := func(got context.Context, op storage.Operation) error {
		if got.Value(namespaceTestKey{}) != "principal" {
			t.Fatal("request context lost")
		}
		operations = append(operations, op)
		return nil
	}
	result, err := resolveTestName(ctx, backend, session, `folder\ACTUAL`, DefaultLimits(), authorize)
	if err != nil || result.Root || result.RootID != 1 || result.Target.Parent.NodeID != 2 || string(result.Target.RawLeaf) != "Actual" || result.Condition != (storage.ChildCondition{State: storage.SameNode, NodeID: 3}) || result.Attr == nil || result.Attr.ID != 3 {
		t.Fatalf("resolution: %+v %v", result, err)
	}
	if !reflect.DeepEqual(operations, []storage.Operation{storage.OpVolumeStat, storage.OpReplicationSnapshot, storage.OpReplicationSnapshot}) {
		t.Fatalf("wrong permissions: %v", operations)
	}
	if backend.calls != 1 || backend.contextValue != "principal" || len(session.calls) != 2 {
		t.Fatalf("root/observer calls: %d %+v", backend.calls, session.calls)
	}
	if err := result.Guards.Check(); err != nil {
		t.Fatal(err)
	}
	if len(result.Guards.Directories) != 2 || len(result.Guards.Edges) != 2 || string(result.Guards.Edges[0].RawLeaf) != "Folder" {
		t.Fatalf("incomplete guards: %+v", result.Guards)
	}
	second := session.calls[1]
	if second.options.IncludeName || second.value != "principal" || len(second.options.Guards.Directories) != 1 || len(second.options.Guards.Edges) != 1 || second.options.Guards.RootID != 1 {
		t.Fatalf("prefix not validated at next parent: %+v", second)
	}
	session.views[1].revision[0] = 'x'
	session.views[2].entries[0].Attr.Metadata["foreign.namespace"].Data[0] = 0
	result.Target.RawLeaf[0] = 'z'
	if string(result.Guards.Edges[1].RawLeaf) != "Actual" || string(result.Guards.Directories[0].Revision) != "root-one" || result.Attr.Metadata["foreign.namespace"].Data[0] != 0xff {
		t.Fatal("result retained mutable source/target aliases")
	}
}

func TestSMBNamespaceSeparatesRootFinalAndIntermediateAbsence(t *testing.T) {
	for _, name := range []string{"", `\`} {
		backend, session := namespaceFixture()
		result, err := resolveTestName(t.Context(), backend, session, name, DefaultLimits(), allowNamespace)
		if err != nil || !result.Root || result.Attr == nil || result.Attr.ID != 1 || !result.DirectoryRequired || result.Condition.State != storage.SameNode || len(session.calls) != 0 {
			t.Fatalf("root acquired/listed a parent: %+v %v calls=%d", result, err, len(session.calls))
		}
	}
	backend, session := namespaceFixture()
	result, err := resolveTestName(t.Context(), backend, session, `folder\New`, DefaultLimits(), allowNamespace)
	if err != nil || result.Condition.State != storage.Absent || result.Attr != nil || string(result.Target.RawLeaf) != "New" || len(result.Guards.Directories) != 2 || len(result.Guards.Edges) != 1 {
		t.Fatalf("unproven absence: %+v %v", result, err)
	}
	result, err = resolveTestName(t.Context(), backend, session, `missing\new`, DefaultLimits(), allowNamespace)
	if !errors.Is(err, errPathMissing) || namespaceStatus(err) != 0xc000003a || !reflect.DeepEqual(result, resolvedName{}) {
		t.Fatalf("intermediate absence became final: %+v %v", result, err)
	}
	result, err = resolveTestName(t.Context(), backend, session, `folder\Actual\child`, DefaultLimits(), allowNamespace)
	if !errors.Is(err, syscall.ENOTDIR) || !reflect.DeepEqual(result, resolvedName{}) {
		t.Fatalf("non-directory prefix: %+v %v", result, err)
	}
	if _, err = resolveTestName(t.Context(), backend, session, `folder\Actual\`, DefaultLimits(), allowNamespace); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("lost directory suffix: %v", err)
	}
	result, err = resolveTestName(t.Context(), backend, session, `folder\`, DefaultLimits(), allowNamespace)
	if err != nil || !result.DirectoryRequired || result.Attr == nil || !result.Attr.IsDir() {
		t.Fatalf("directory suffix: %+v %v", result, err)
	}
}

func TestSMBNamespacePreservesFailuresWithoutAbsenceOrRetry(t *testing.T) {
	for _, failure := range []error{syscall.ENOENT, syscall.EIO, syscall.EACCES, syscall.EAGAIN, storage.ErrConditionConflict, storage.ErrInvalidScope, context.Canceled} {
		backend, session := namespaceFixture()
		session.views[1].err = failure
		result, err := resolveTestName(t.Context(), backend, session, "Folder", DefaultLimits(), allowNamespace)
		if !errors.Is(err, failure) || !reflect.DeepEqual(result, resolvedName{}) || len(session.calls) != 1 || backend.calls != 1 {
			t.Fatalf("failure retried/hidden: %+v %v calls=%d/%d", result, err, backend.calls, len(session.calls))
		}
		if errors.Is(failure, syscall.ENOENT) && (!strings.Contains(err.Error(), failure.Error()) || !strings.Contains(err.Error(), "namespace observation failed") || storage.ErrnoOf(err) != syscall.EIO || namespaceStatus(err) != statusIO) {
			t.Fatalf("backend ENOENT became final absence: %v status=%x", err, namespaceStatus(err))
		}
	}
	backend, session := namespaceFixture()
	backend.err = syscall.ENOENT
	if _, err := resolveTestName(t.Context(), backend, session, "x", DefaultLimits(), allowNamespace); !errors.Is(err, syscall.ENOENT) || storage.ErrnoOf(err) != syscall.EIO || len(session.calls) != 0 {
		t.Fatalf("root ENOENT: %v", err)
	}
	for _, denied := range []storage.Operation{storage.OpVolumeStat, storage.OpReplicationSnapshot} {
		backend, session := namespaceFixture()
		_, err := resolveTestName(t.Context(), backend, session, "Folder", DefaultLimits(), func(_ context.Context, op storage.Operation) error {
			if op == denied {
				return authz.ErrDenied
			}
			return nil
		})
		if !errors.Is(err, authz.ErrDenied) || namespaceStatus(err) != statusDenied || len(session.calls) != 0 || denied == storage.OpVolumeStat && backend.calls != 0 {
			t.Fatalf("unauthorized capture: %v root=%d observer=%d", err, backend.calls, len(session.calls))
		}
	}
	backend, session = namespaceFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := resolveTestName(ctx, backend, session, "Folder", DefaultLimits(), allowNamespace); !errors.Is(err, context.Canceled) || backend.calls != 0 {
		t.Fatalf("cancellation reached authority: %v", err)
	}
	backend, session = namespaceFixture()
	session.checkErr = syscall.EOPNOTSUPP
	if _, err := resolveTestName(t.Context(), backend, session, "Folder", DefaultLimits(), allowNamespace); !errors.Is(err, syscall.EOPNOTSUPP) || len(session.calls) != 0 {
		t.Fatalf("unsupported observer: %v", err)
	}
}

func TestSMBNamespaceGuardsRejectChangedPrefixesAndFinalFacts(t *testing.T) {
	backend, session := namespaceFixture()
	session.before = func(s *namespaceTestSession, target storage.DirectoryTarget) {
		if target.NodeID == 2 {
			s.views[1].revision = []byte("changed")
		}
	}
	result, err := resolveTestName(t.Context(), backend, session, `Folder\new`, DefaultLimits(), allowNamespace)
	if !errors.Is(err, storage.ErrConditionConflict) || !reflect.DeepEqual(result, resolvedName{}) || len(session.calls) != 2 {
		t.Fatalf("stale prefix supplied absence: %+v %v", result, err)
	}
	session.before = nil
	result, err = resolveTestName(t.Context(), backend, session, `Folder\new`, DefaultLimits(), allowNamespace)
	if err != nil || result.Condition.State != storage.Absent || string(result.Guards.Directories[0].Revision) != "changed" || backend.calls != 2 {
		t.Fatalf("fresh traversal reused old prefix: %+v %v", result, err)
	}
	session.views[2].entries = append(session.views[2].entries, storage.Entry{Name: "NEW", Attr: storage.Attr{ID: 4, Kind: storage.NodeRegular}})
	session.views[2].revision = []byte("new-alias")
	if err := session.checkGuards(&result.Guards); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatal("final absent proof survived a case alias creation")
	}
	present, err := resolveTestName(t.Context(), backend, session, `Folder\Actual`, DefaultLimits(), allowNamespace)
	if err != nil {
		t.Fatal(err)
	}
	session.views[2].entries[0].Attr.ID = 5
	session.views[2].revision = []byte("replacement")
	if err := session.checkGuards(&present.Guards); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatal("present proof survived replacement")
	}
}

func TestSMBNamespaceRefusesUnrelatedInvalidOrAmbiguousNames(t *testing.T) {
	for _, entries := range [][]storage.Entry{
		{{Name: "invalid.", Attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}},
		{{Name: "Peer", Attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}, {Name: "PEER", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}}},
	} {
		backend, session := namespaceFixture()
		session.views[2].entries = append(session.views[2].entries, entries...)
		result, err := resolveTestName(t.Context(), backend, session, `folder\Actual`, DefaultLimits(), allowNamespace)
		if err == nil || !reflect.DeepEqual(result, resolvedName{}) {
			t.Fatalf("filtered directory yielded success: %+v %v", result, err)
		}
	}
}

func TestSMBNamespaceBudgetsBeforePayloadAndProjection(t *testing.T) {
	backend, session := namespaceFixture()
	limits := DefaultLimits()
	limits.MaxDirectoryBytes = 2048
	session.headerMetadataBytes = storage.MaxMetadataBytes
	result, err := resolveTestName(t.Context(), backend, session, "Folder", limits, allowNamespace)
	if err == nil || session.loads != 0 || !reflect.DeepEqual(result, resolvedName{}) {
		t.Fatalf("metadata loaded before reserve: %+v %v loads=%d", result, err, session.loads)
	}
	backend, session = namespaceFixture()
	backend.metadataBytes = storage.MaxMetadataBytes
	if _, err := resolveTestName(t.Context(), backend, session, "Folder", limits, allowNamespace); !errors.Is(err, syscall.EFBIG) || backend.loads != 0 || len(session.calls) != 0 {
		t.Fatalf("root metadata loaded before budget: %v loads=%d", err, backend.loads)
	}
	backend, session = namespaceFixture()
	limits.MaxFrameBytes = 2
	if _, err := resolveTestName(t.Context(), backend, session, "Folder", limits, allowNamespace); !errors.Is(err, syscall.EFBIG) || backend.calls != 0 {
		t.Fatalf("input exceeded frame budget: %v", err)
	}
	for _, root := range []storage.Attr{{Kind: storage.NodeDirectory}, {ID: 1, Kind: storage.NodeRegular}, {ID: 1, Kind: storage.NodeDirectory, Size: -1}} {
		backend, session = namespaceFixture()
		backend.root = root
		if _, err := resolveTestName(t.Context(), backend, session, "", DefaultLimits(), allowNamespace); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid root accepted: %+v %v", root, err)
		}
	}
}
