package sqlite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func namespaceName(parent int64, name string) storage.ChildName {
	return storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(parent)}, RawLeaf: []byte(name)}
}

func namespaceCreate(t *testing.T, s *Store, parent int64, name string, kind storage.NameOperation) storage.Attr {
	t.Helper()
	result, err := s.MutateName(t.Context(), storage.NameCommand{Kind: kind, Name: namespaceName(parent, name), Target: storage.ChildCondition{State: storage.Absent}})
	if err != nil || result.Attr == nil {
		t.Fatalf("create %q: %+v, %v", name, result, err)
	}
	return *result.Attr
}

func namespaceRename(parent int64, from string, source uint64, observed string, displaced uint64, output string) storage.NameCommand {
	expected := storage.ChildCondition{State: storage.Absent}
	if displaced != 0 {
		expected = storage.ChildCondition{State: storage.SameNode, NodeID: displaced}
	}
	return storage.NameCommand{
		Kind: storage.NameRename, Name: namespaceName(parent, from), Target: storage.ChildCondition{State: storage.SameNode, NodeID: source},
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: uint64(parent)}, ObservedLeaf: []byte(observed), Expected: expected, OutputLeaf: []byte(output)},
	}
}

func TestNamespaceMutationUsesRetainedParentIdentityAfterRenameAndReuse(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	parent, err := s.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := parent.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := parent.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MutateName(t.Context(), namespaceRename(s.root, "dir", directory.ID, "moved", 0, "moved")); err != nil {
		t.Fatal(err)
	}
	replacement := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	if replacement.ID == directory.ID {
		t.Fatal("name reuse reused the old parent identity")
	}
	command := storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(int64(directory.ID), "child"), Target: storage.ChildCondition{State: storage.Absent}}
	command.Name.Parent.Scope = &scope
	created, err := s.MutateName(t.Context(), command)
	if err != nil || created.Attr == nil {
		t.Fatalf("retained parent creation=%+v error=%v", created, err)
	}
	if child, err := s.Stat(t.Context(), "moved/child"); err != nil || uint64(child.ID) != created.Attr.ID {
		t.Fatalf("old parent child=%+v error=%v", child, err)
	}
	if _, err := s.Stat(t.Context(), "dir/child"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("replacement parent received old request=%v", err)
	}
	command.Name.RawLeaf = []byte("sibling")
	if _, err := s.MutateName(t.Context(), command); err != nil {
		t.Fatalf("unrelated sibling mutation required a directory guard: %v", err)
	}
	if err := parent.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	command.Name.RawLeaf = []byte("stale")
	if _, err := s.MutateName(t.Context(), command); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("closed parent scope=%v", err)
	}
}

func TestNamespaceMutationChecksDirectoryObservationBeforeEffects(t *testing.T) {
	s := pendingUnlinkStore(t)
	view, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	namespaceCreate(t, s.Store, s.root, "sibling", storage.NameCreate)
	_, err = s.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameCreate, Name: namespaceName(s.root, "guarded"), Target: storage.ChildCondition{State: storage.Absent},
		Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{view.Observation}},
	})
	if !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale directory observation=%v", err)
	}
	if _, err := s.Stat(t.Context(), "guarded"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("stale observation left a name=%v", err)
	}
	if children, err := s.List(t.Context(), ""); err != nil || len(children) != 1 || !bytes.Equal(children[0].Name, []byte("sibling")) {
		t.Fatalf("guarded directory=%+v error=%v", children, err)
	}
}

func TestNamespaceRenameSpellingCannotRemoveAnUnobservedOccupant(t *testing.T) {
	s := pendingUnlinkStore(t)
	source := namespaceCreate(t, s.Store, s.root, "source", storage.NameCreate)
	displaced := namespaceCreate(t, s.Store, s.root, "bar", storage.NameCreate)
	third := namespaceCreate(t, s.Store, s.root, "BAR", storage.NameCreate)
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	fixture.put(t, t.Context(), "source", 3)
	fixture.put(t, t.Context(), "bar", 5)
	old, err := s.OpenFile(t.Context(), "bar", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	before, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := namespaceRename(s.root, "source", source.ID, "bar", displaced.ID, "BAR")
	if _, err := s.MutateName(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("third output occupant=%v", err)
	}
	after, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected rename changed directory: before=%+v after=%+v error=%v", before, after, err)
	}
	afterPosition, err := s.CommittedPosition(t.Context())
	if err != nil || afterPosition != position {
		t.Fatalf("rejected rename changed history=%v error=%v", afterPosition, err)
	}
	if _, err := s.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "BAR"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: third.ID}}); err != nil {
		t.Fatal(err)
	}
	result, err := s.MutateName(t.Context(), command)
	if err != nil || result.Attr == nil || result.Attr.ID != source.ID {
		t.Fatalf("spelling replacement=%+v error=%v", result, err)
	}
	for _, path := range []string{"source", "bar"} {
		if _, err := s.Stat(t.Context(), path); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("removed slot %q=%v", path, err)
		}
	}
	if state, err := old.Node(t.Context()); err != nil || uint64(state.ID) != displaced.ID || !state.Detached {
		t.Fatalf("replaced reference=%+v error=%v", state, err)
	}
	if node, err := s.Stat(t.Context(), "BAR"); err != nil || uint64(node.ID) != source.ID {
		t.Fatalf("renamed source=%+v error=%v", node, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("retained replacement usage=%d error=%v", used, err)
	}
	if err := old.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 3 {
		t.Fatalf("released replacement usage=%d error=%v", used, err)
	}
}

func TestNamespaceRenameCanKeepSourceSlotWhileRemovingObservedDestination(t *testing.T) {
	s := pendingUnlinkStore(t)
	source := namespaceCreate(t, s.Store, s.root, "source", storage.NameCreate)
	target := namespaceCreate(t, s.Store, s.root, "target", storage.NameCreate)
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.MutateName(t.Context(), namespaceRename(s.root, "source", source.ID, "target", target.ID, "source"))
	if err != nil || result.Attr == nil || result.Attr.ID != source.ID {
		t.Fatalf("retained source slot=%+v error=%v", result, err)
	}
	if node, err := s.Stat(t.Context(), "source"); err != nil || uint64(node.ID) != source.ID {
		t.Fatalf("source slot=%+v error=%v", node, err)
	}
	if _, err := s.Stat(t.Context(), "target"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("observed destination was not removed=%v", err)
	}
	changes := pendingChangesAfter(t, s.Store, position)
	if len(changes) != 3 || changes[0].Kind != metastore.Removed || changes[1].Kind != metastore.Modified || changes[2].Kind != metastore.Modified || changes[1].Node == nil || changes[2].Node == nil || changes[1].Node.ChangeTime == nil || changes[2].Node.ChangeTime == nil || !changes[1].Node.ChangeTime.Equal(*changes[2].Node.ChangeTime) {
		t.Fatalf("same source slot emitted an invalid move or mismatched operation times: %+v", changes)
	}
}

func TestNamespaceRenameNoOpStillChecksStrongWithoutChangingMetadata(t *testing.T) {
	s := pendingUnlinkStore(t)
	source := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	command := namespaceRename(s.root, "file", source.ID, "file", source.ID, "file")
	before, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Exclusive)
	_, err = s.MutateName(t.Context(), command)
	requirePublicationCode(t, err, locking.Conflict)
	result, err := s.MutateName(publicationScope(t.Context(), owner, grant), command)
	if err != nil || result.Attr == nil || !reflect.DeepEqual(*result.Attr, before.Entries[0].Attr) {
		t.Fatalf("authorized no-op=%+v error=%v", result, err)
	}
	after, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("no-op changed directory=%+v error=%v", after, err)
	}
	afterPosition, err := s.CommittedPosition(t.Context())
	if err != nil || position != afterPosition {
		t.Fatalf("no-op changed history=%v error=%v", afterPosition, err)
	}
}

func TestNamespaceSymlinkBytesShareAtomicQuotaAndReferenceRetention(t *testing.T) {
	config := lockingTestConfig(t)
	config.Allowance = 4
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	target := []byte{0xff, '/', 'a', 'b'}
	command := storage.NameCommand{Kind: storage.NameSymlink, Name: namespaceName(s.root, "link"), Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: target}}
	result, err := s.MutateName(t.Context(), command)
	if err != nil || result.Attr == nil || result.Attr.Kind != storage.NodeSymlink || result.Attr.Size != 4 {
		t.Fatalf("symlink=%+v error=%v", result, err)
	}
	if node, err := s.Stat(t.Context(), "link"); err != nil || !bytes.Equal(node.LinkTarget, target) {
		t.Fatalf("symlink target=%+v error=%v", node, err)
	}
	command.Name.RawLeaf = []byte("over-quota")
	if _, err := s.MutateName(t.Context(), command); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("over-quota symlink=%v", err)
	}
	if _, err := s.Stat(t.Context(), "over-quota"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("quota rejection left a name=%v", err)
	}
	opened, err := s.OpenNodeRef(t.Context(), result.Attr.ID, storage.NodeRefOptions{Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "link"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: result.Attr.ID}}); err != nil {
		t.Fatal(err)
	}
	if state, err := opened.Reference.Node(t.Context()); err != nil || !state.Detached || !bytes.Equal(state.LinkTarget, target) {
		t.Fatalf("detached symlink=%+v error=%v", state, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("retained symlink usage=%d error=%v", used, err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("released symlink usage=%d error=%v", used, err)
	}
}

func TestNamespaceRenameProtectsBothSourceAndDisplacedIdentity(t *testing.T) {
	for _, protected := range []string{"source", "target"} {
		t.Run(protected, func(t *testing.T) {
			s := pendingUnlinkStore(t)
			source := namespaceCreate(t, s.Store, s.root, "source", storage.NameCreate)
			target := namespaceCreate(t, s.Store, s.root, "target", storage.NameCreate)
			command := namespaceRename(s.root, "source", source.ID, "target", target.ID, "target")
			fixture := publicationFixture{store: s.Store, service: s.LockService()}
			owner := fixture.owner(t)
			grant := fixture.grant(t, owner, protected, locking.Exclusive)
			_, err := s.MutateName(t.Context(), command)
			requirePublicationCode(t, err, locking.Conflict)
			result, err := s.MutateName(publicationScope(t.Context(), owner, grant), command)
			if err != nil || result.Attr == nil || result.Attr.ID != source.ID {
				t.Fatalf("protected rename=%+v error=%v", result, err)
			}
		})
	}
}

func TestNamespaceMutationRejectsCyclesTypesAndNonemptyDirectories(t *testing.T) {
	s := pendingUnlinkStore(t)
	file := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	child := namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameMkdir)
	other := namespaceCreate(t, s.Store, s.root, "other", storage.NameMkdir)
	namespaceCreate(t, s.Store, int64(other.ID), "nested", storage.NameCreate)
	cycle := namespaceRename(s.root, "dir", directory.ID, "moved", 0, "moved")
	cycle.Destination.Parent.NodeID = child.ID
	for _, test := range []struct {
		name    string
		command storage.NameCommand
		want    error
	}{
		{"file onto directory", namespaceRename(s.root, "file", file.ID, "dir", directory.ID, "dir"), syscall.EISDIR},
		{"directory onto file", namespaceRename(s.root, "dir", directory.ID, "file", file.ID, "file"), syscall.ENOTDIR},
		{"directory cycle", cycle, syscall.EINVAL},
		{"nonempty replacement", namespaceRename(s.root, "dir", directory.ID, "other", other.ID, "other"), syscall.ENOTEMPTY},
		{"unlink directory", storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "dir"), Target: storage.ChildCondition{State: storage.Any}}, syscall.EISDIR},
		{"rmdir file", storage.NameCommand{Kind: storage.NameRemoveDir, Name: namespaceName(s.root, "file"), Target: storage.ChildCondition{State: storage.Any}}, syscall.ENOTDIR},
		{"rmdir nonempty", storage.NameCommand{Kind: storage.NameRemoveDir, Name: namespaceName(s.root, "dir"), Target: storage.ChildCondition{State: storage.Any}}, syscall.ENOTEMPTY},
		{"missing source", storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "missing"), Target: storage.ChildCondition{State: storage.Any}}, syscall.ENOENT},
		{"existing create", storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(s.root, "file"), Target: storage.ChildCondition{State: storage.Any}}, syscall.EEXIST},
		{"file parent", storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(int64(file.ID), "child"), Target: storage.ChildCondition{State: storage.Absent}}, syscall.ENOTDIR},
		{"root spelling", storage.NameCommand{Kind: storage.NameRemoveDir, Name: namespaceName(s.root, "."), Target: storage.ChildCondition{State: storage.Any}}, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.MutateName(t.Context(), test.command); !errors.Is(err, test.want) {
				t.Fatalf("mutation=%v want=%v", err, test.want)
			}
			after, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected mutation changed directory=%+v error=%v", after, err)
			}
		})
	}
}

func TestNamespaceMutationDoesNotInferParentReadEntriesUse(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	opened, err := s.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata,
		Use: storage.UseClaim{Uses: storage.ReadEntries, Deny: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("directory claim did not protect enumeration: %v", err)
	}
	namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameCreate)
	scope, err := opened.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameCreate, Name: namespaceName(int64(directory.ID), "foreign"), Target: storage.ChildCondition{State: storage.Absent},
		Uses: []storage.TargetUse{{NodeID: directory.ID, Scope: scope}},
	})
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unused parent target scope=%v", err)
	}
}

func TestNamespaceRemovalChecksActualDeleteUseAndRetainsDetachedDirectory(t *testing.T) {
	s := pendingUnlinkStore(t)
	file := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	opened, err := s.OpenAt(t.Context(), namespaceName(s.root, "file"), storage.OpenAtOptions{
		Read: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID},
		Use: storage.UseClaim{Uses: storage.ReadData, Deny: storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.File.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	command := storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "file"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID}}
	if _, err := s.MutateName(t.Context(), command); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous deletion bypassed actual file claim: %v", err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MutateName(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	retained, err := s.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := retained.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemoveDir, Name: namespaceName(s.root, "dir"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}}); err != nil {
		t.Fatal(err)
	}
	if state, err := retained.Reference.Node(t.Context()); err != nil || !state.Detached {
		t.Fatalf("retained directory=%+v error=%v", state, err)
	}
	if _, err := s.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(int64(directory.ID), "child"), Target: storage.ChildCondition{State: storage.Absent}}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached parent accepted a child: %v", err)
	}
}

func TestNamespaceResultBudgetChecksCreatedAndFinalRenamedAttributesOnly(t *testing.T) {
	s := pendingUnlinkStore(t)
	source := namespaceCreate(t, s.Store, s.root, "source", storage.NameCreate)
	target := namespaceCreate(t, s.Store, s.root, "target", storage.NameCreate)
	refusal := errors.New("namespace result does not fit")
	reject := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, metadataBytes int64) error {
		if attr.Metadata != nil || metadataBytes < 6 {
			t.Fatalf("creation budget received loaded attributes: %+v bytes=%d", attr, metadataBytes)
		}
		return refusal
	})
	if _, err := s.MutateName(reject, storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(s.root, "refused"), Target: storage.ChildCondition{State: storage.Absent}}); !errors.Is(err, refusal) {
		t.Fatalf("creation result budget=%v", err)
	}
	if _, err := s.Stat(t.Context(), "refused"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused result left a created node=%v", err)
	}
	old := time.Unix(100, 0)
	if err := s.SetAttr(t.Context(), "source", storage.AttrChange{ChangeTime: &old}); err != nil {
		t.Fatal(err)
	}
	before, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, metadataBytes int64) error {
		calls++
		if attr.ID != source.ID || attr.Metadata != nil || metadataBytes < 6 || attr.ChangeTime == nil {
			t.Fatalf("rename budget received internal or loaded attributes: %+v bytes=%d", attr, metadataBytes)
		}
		if !attr.ChangeTime.Equal(old) {
			return refusal
		}
		return nil
	})
	if _, err := s.MutateName(ctx, namespaceRename(s.root, "source", source.ID, "target", target.ID, "target")); !errors.Is(err, refusal) {
		t.Fatalf("final rename result budget=%v", err)
	}
	after, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("final result refusal published rename effects: %+v error=%v", after, err)
	}
	beforeRemoval := calls
	result, err := s.MutateName(ctx, storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "source"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: source.ID}})
	if err != nil || result.Attr != nil || calls != beforeRemoval {
		t.Fatalf("remove admitted attributes it does not return: %+v error=%v calls=%d", result, err, calls)
	}
}
