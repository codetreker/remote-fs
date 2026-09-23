package sqlite

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openIdentityTestStore(t *testing.T) (*LockingStore, metastore.Node) {
	t.Helper()
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	return store, root
}

func mutateName(t *testing.T, store *LockingStore, command storage.NameCommand) storage.NameResult {
	t.Helper()
	command.Action = fileAction(t)
	result, err := store.MutateName(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func absentName(kind storage.NameOperation, parent storage.DirectoryTarget, leaf string) storage.NameCommand {
	return storage.NameCommand{
		Kind:   kind,
		Name:   storage.ChildName{Parent: parent, RawLeaf: []byte(leaf)},
		Target: storage.ChildCondition{State: storage.Absent},
	}
}

func TestAtomicOpenResetsReplacesAndRejectsKindMismatch(t *testing.T) {
	store, root := openIdentityTestStore(t)
	if err := store.CheckAtomicFileOpen(); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckNodeReferences(); err != nil {
		t.Fatal(err)
	}

	rootTarget := directoryTarget(root)
	createdAt := time.Unix(123, 0)
	name := storage.ChildName{Parent: rootTarget, RawLeaf: []byte("file")}
	created, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Exclusive: true,
		Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Existing: storage.Keep,
		Initial: storage.InitialState{OnCreate: storage.InitialFields{
			Attr:     storage.AttrChange{AccessTime: &createdAt},
			Metadata: map[string][]byte{"test.open": []byte("created")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Outcome != storage.Created || created.State.AccessTime != createdAt {
		t.Fatalf("created open = %+v", created)
	}
	createdID := created.State.ID
	if err := created.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	resetAt := time.Unix(456, 0)
	reset, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
		Read: true, Write: true,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(createdID)}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Existing: storage.ResetContent,
		Initial: storage.InitialState{OnReset: storage.InitialFields{
			Attr:     storage.AttrChange{AccessTime: &resetAt},
			Metadata: map[string][]byte{"test.open": []byte("reset")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reset.Outcome != storage.Reset || reset.State.ID != createdID || reset.State.AccessTime != resetAt ||
		!bytes.Equal(reset.State.Metadata["test.open"].Data, []byte("reset")) {
		t.Fatalf("reset open = %+v", reset)
	}
	if err := reset.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	replaceAt := time.Unix(789, 0)
	replaced, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
		Read: true, Write: true, Create: true,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(createdID)}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}, Existing: storage.ReplaceNode,
		Initial: storage.InitialState{OnReplace: storage.InitialFields{
			Attr:     storage.AttrChange{AccessTime: &replaceAt},
			Metadata: map[string][]byte{"test.open": []byte("replacement")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Outcome != storage.Replaced || replaced.State.ID == createdID || replaced.State.AccessTime != replaceAt {
		t.Fatalf("replacement open = %+v", replaced)
	}
	if err := replaced.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	directory := mutateName(t, store, absentName(storage.NameMkdir, rootTarget, "directory"))
	link := absentName(storage.NameSymlink, rootTarget, "link")
	link.Initial.LinkTarget = []byte("target")
	mutateName(t, store, link)
	for _, test := range []struct {
		name string
		leaf string
		kind storage.NodeKind
		want error
	}{
		{"directory as file", "directory", storage.NodeRegular, syscall.EISDIR},
		{"symlink as file", "link", storage.NodeRegular, syscall.ELOOP},
		{"file as directory", "file", storage.NodeDirectory, syscall.ENOTDIR},
		{"file as symlink", "file", storage.NodeSymlink, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: rootTarget, RawLeaf: []byte(test.leaf)}), storage.NodeRefOptions{
				Kind: test.kind, Target: storage.ChildCondition{State: storage.Any}, Action: fileAction(t),
				MetadataAccess: storage.ReadMetadata,
			})
			if opened.Reference != nil || !errors.Is(err, test.want) {
				t.Fatalf("kind mismatch opened=%v err=%v", opened.Reference != nil, err)
			}
		})
	}
	if directory.Attr == nil || directory.Attr.Kind != storage.NodeDirectory {
		t.Fatalf("directory creation = %+v", directory)
	}
}

func TestNamespaceMutationPreservesIdentityAndRejectsInvalidRemoval(t *testing.T) {
	store, root := openIdentityTestStore(t)
	rootTarget := directoryTarget(root)
	directory := mutateName(t, store, absentName(storage.NameMkdir, rootTarget, "directory"))
	directoryTarget := storage.DirectoryTarget{NodeID: directory.Attr.ID}
	childDirectory := mutateName(t, store, absentName(storage.NameMkdir, directoryTarget, "child"))
	mutateName(t, store, absentName(storage.NameCreate, rootTarget, "source"))
	mutateName(t, store, absentName(storage.NameCreate, rootTarget, "destination"))
	link := absentName(storage.NameSymlink, rootTarget, "link")
	link.Initial.LinkTarget = []byte("target")
	mutateName(t, store, link)

	source, err := store.Stat(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	destination, err := store.Stat(t.Context(), "destination")
	if err != nil {
		t.Fatal(err)
	}
	replaced := mutateName(t, store, storage.NameCommand{
		Kind:   storage.NameRename,
		Name:   storage.ChildName{Parent: rootTarget, RawLeaf: []byte("source")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(source.ID)},
		Destination: &storage.RenameTarget{
			Parent: rootTarget, ObservedLeaf: []byte("destination"), OutputLeaf: []byte("destination"),
			Expected: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(destination.ID)},
		},
	})
	if replaced.Attr == nil || replaced.Attr.ID != uint64(source.ID) {
		t.Fatalf("replace rename = %+v", replaced)
	}
	if _, err := store.StatNode(t.Context(), uint64(destination.ID)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("displaced identity = %v", err)
	}

	noOp := mutateName(t, store, storage.NameCommand{
		Kind:   storage.NameRename,
		Name:   storage.ChildName{Parent: rootTarget, RawLeaf: []byte("destination")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(source.ID)},
		Destination: &storage.RenameTarget{
			Parent: rootTarget, ObservedLeaf: []byte("destination"), OutputLeaf: []byte("destination"),
			Expected: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(source.ID)},
		},
	})
	if noOp.Attr == nil || noOp.Attr.ID != uint64(source.ID) {
		t.Fatalf("no-op rename = %+v", noOp)
	}

	cycle := storage.NameCommand{
		Kind: storage.NameRename, Action: fileAction(t),
		Name:   storage.ChildName{Parent: rootTarget, RawLeaf: []byte("directory")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.Attr.ID},
		Destination: &storage.RenameTarget{
			Parent: storage.DirectoryTarget{NodeID: childDirectory.Attr.ID}, ObservedLeaf: []byte("moved"),
			OutputLeaf: []byte("moved"), Expected: storage.ChildCondition{State: storage.Absent},
		},
	}
	if _, err := store.MutateName(t.Context(), cycle); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("directory cycle = %v", err)
	}

	for _, test := range []struct {
		name    string
		command storage.NameCommand
		want    error
	}{
		{"remove directory as file", storage.NameCommand{Kind: storage.NameRemove, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("directory")}, Target: storage.ChildCondition{State: storage.Any}}, syscall.EISDIR},
		{"remove file as directory", storage.NameCommand{Kind: storage.NameRemoveDir, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("destination")}, Target: storage.ChildCondition{State: storage.Any}}, syscall.ENOTDIR},
		{"remove nonempty directory", storage.NameCommand{Kind: storage.NameRemoveDir, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("directory")}, Target: storage.ChildCondition{State: storage.Any}}, syscall.ENOTEMPTY},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.command.Action = fileAction(t)
			if _, err := store.MutateName(t.Context(), test.command); !errors.Is(err, test.want) {
				t.Fatalf("mutation = %v", err)
			}
		})
	}

	mutateName(t, store, storage.NameCommand{Kind: storage.NameRemove, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("link")}, Target: storage.ChildCondition{State: storage.Any}})
	mutateName(t, store, storage.NameCommand{Kind: storage.NameRemove, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("destination")}, Target: storage.ChildCondition{State: storage.Any}})
	mutateName(t, store, storage.NameCommand{Kind: storage.NameRemoveDir, Name: storage.ChildName{Parent: directoryTarget, RawLeaf: []byte("child")}, Target: storage.ChildCondition{State: storage.Any}})
	mutateName(t, store, storage.NameCommand{Kind: storage.NameRemoveDir, Name: storage.ChildName{Parent: rootTarget, RawLeaf: []byte("directory")}, Target: storage.ChildCondition{State: storage.Any}})
}

func TestNodeReferenceDelegatesMetadataMutationPendingDeleteAndLifecycle(t *testing.T) {
	store, root := openIdentityTestStore(t)
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}
	opened, err := store.OpenChildRef(t.Context(), selectChild(name), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.Absent},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.DeleteName},
		MetadataAccess: storage.ReadMetadata | storage.WriteMetadata, Create: true, Exclusive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := opened.Reference
	deletion := ref.(metastore.DeleteIntent)
	mutation := ref.(metastore.ConditionalFileMutation)
	for name, check := range map[string]func() error{
		"scoped reference":     ref.CheckScopedReference,
		"metadata access":      ref.CheckMetadataAccess,
		"reference state":      ref.CheckReferenceState,
		"delete intent":        deletion.CheckDeleteIntent,
		"conditional mutation": mutation.CheckConditionalFileMutation,
	} {
		if err := check(); err != nil {
			t.Fatalf("%s capability = %v", name, err)
		}
	}
	scope, err := ref.Scope(t.Context())
	if err != nil || scope.Token == "" {
		t.Fatalf("scope = %+v, %v", scope, err)
	}
	ordered := false
	if err := ref.Order(t.Context(), func() error { ordered = true; return nil }); err != nil || !ordered {
		t.Fatalf("ordered=%v err=%v", ordered, err)
	}

	metadata, err := ref.SetMetadata(t.Context(), "test.reference", nil, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	accessTime := time.Unix(321, 0)
	state, err := ref.SetAttr(t.Context(), storage.AttrChange{AccessTime: &accessTime})
	if err != nil || state.AccessTime != accessTime {
		t.Fatalf("set attr = %+v, %v", state, err)
	}
	modTime := time.Unix(654, 0)
	state, err = mutation.MutateFile(t.Context(), storage.FileMutation{
		Action: fileAction(t), Kind: storage.MutateAttributes,
		Attr:     storage.AttrChange{ModTime: &modTime},
		Metadata: map[string]storage.OpaquePayload{"test.reference": {Version: metadata.Version, Data: []byte("next")}},
	})
	if err != nil || state.ModTime != modTime || !bytes.Equal(state.Metadata["test.reference"].Data, []byte("next")) {
		t.Fatalf("attribute mutation = %+v, %v", state, err)
	}
	if _, err := mutation.CommitMutation(t.Context(), storage.FileMutation{
		Action: fileAction(t), Kind: storage.MutateTruncate, Size: 0,
	}, state.Revision, metastore.Object{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only reference committed content = %v", err)
	}

	pending, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
		ExpectedMetadata: map[string][]byte{"test.reference": state.Metadata["test.reference"].Version},
	})
	if err != nil || !pending.PendingUnlink || len(pending.PendingGeneration) != 8 {
		t.Fatalf("pending state = %+v, %v", pending, err)
	}
	again, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
	})
	if err != nil || !bytes.Equal(again.PendingGeneration, pending.PendingGeneration) {
		t.Fatalf("repeated pending state = %+v, %v; want generation %x", again, err, pending.PendingGeneration)
	}
	if result, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.Any}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	}); result.File != nil || !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("open pending child = file %v, err %v", result.File != nil, err)
	}
	pendingRename := storage.NameCommand{
		Kind: storage.NameRename, Action: fileAction(t), Name: name,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.State.Attr().ID},
		Destination: &storage.RenameTarget{
			Parent: name.Parent, ObservedLeaf: name.RawLeaf, OutputLeaf: name.RawLeaf,
			Expected: storage.ChildCondition{State: storage.SameNode, NodeID: opened.State.Attr().ID},
		},
	}
	if _, err := store.MutateName(t.Context(), pendingRename); !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("pending name without its live use = %v", err)
	}
	pendingRename.Action = fileAction(t)
	pendingRename.Uses = []storage.TargetUse{{NodeID: opened.State.Attr().ID, Scope: scope}}
	if _, err := store.MutateName(t.Context(), pendingRename); err != nil {
		t.Fatalf("pending name with its live use = %v", err)
	}
	if _, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{
		Action: fileAction(t), Generation: pending.PendingGeneration,
		Uses: []storage.TargetUse{{NodeID: opened.State.Attr().ID + 1, Scope: scope}},
	}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("clear with unrelated use = %v", err)
	}
	wrongGeneration := bytes.Clone(pending.PendingGeneration)
	wrongGeneration[len(wrongGeneration)-1] ^= 1
	if _, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{
		Action: fileAction(t), Generation: wrongGeneration,
	}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("clear with stale generation = %v", err)
	}
	cleared, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{
		Action: fileAction(t), Generation: pending.PendingGeneration,
	})
	if err != nil || cleared.PendingUnlink || len(cleared.PendingGeneration) != 0 {
		t.Fatalf("cleared state = %+v, %v", cleared, err)
	}
	read, err := ref.Node(t.Context())
	if err != nil || read.ID != int64(opened.State.Attr().ID) {
		t.Fatalf("reference node = %+v, %v", read, err)
	}
	readState, err := ref.State(t.Context())
	if err != nil || readState.PendingUnlink {
		t.Fatalf("reference state after clear = %+v, %v", readState, err)
	}
	direct, err := store.OpenNodeRef(t.Context(), opened.State.Attr().ID, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.Any},
		Action: fileAction(t), MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	if direct.State.ID != int64(opened.State.Attr().ID) || direct.Outcome != storage.Opened {
		t.Fatalf("direct identity open = %+v", direct)
	}
	if err := direct.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ref.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ref.DropUse(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ref.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPendingDeleteRequiresDeleteUseAndValidMetadata(t *testing.T) {
	store, root := openIdentityTestStore(t)
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}
	opened, err := store.OpenChildRef(t.Context(), selectChild(name), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.Absent},
		Action: fileAction(t), MetadataAccess: storage.ReadMetadata, Create: true, Exclusive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	deletion := opened.Reference.(metastore.DeleteIntent)
	if _, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
	}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("pending delete without DeleteName use = %v", err)
	}
	if _, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
		ExpectedMetadata: map[string][]byte{"test.absent": []byte("stale")},
	}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("access must precede metadata predicate = %v", err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceAccountingBindsUsageAfterValidation(t *testing.T) {
	store, _ := openIdentityTestStore(t)
	if err := store.CheckMaintenanceAccounting(); err != nil {
		t.Fatal(err)
	}
	if err := store.BindMaintenanceAccounting(t.Context(), storage.PublicationAccountingChain{}, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil maintenance initializer = %v", err)
	}
	initialized := int64(-1)
	chain := storage.PublicationAccountingChain{}.With(func(int64, int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error { return nil }, nil
	})
	if err := store.BindMaintenanceAccounting(t.Context(), chain, func(used int64) { initialized = used }); err != nil {
		t.Fatal(err)
	}
	if initialized != 0 || store.fileDomain.maintenanceAccounting.Empty() {
		t.Fatalf("maintenance accounting initialized=%d empty=%v", initialized, store.fileDomain.maintenanceAccounting.Empty())
	}
}

func TestPendingRecoveryDeferralRequiresOnlyRecoverableLockFailures(t *testing.T) {
	conflict := locking.Wrap(locking.Conflict, "held", nil)
	recovering := locking.Wrap(locking.Recovering, "lease recovery", nil)
	if !pendingRecoveryDeferred(conflict) || !pendingRecoveryDeferred(errors.Join(conflict, recovering)) {
		t.Fatal("recoverable lock failures were not deferred")
	}
	if pendingRecoveryDeferred(nil) || pendingRecoveryDeferred(errors.Join(conflict, syscall.EIO)) || pendingRecoveryDeferred(errors.Join()) {
		t.Fatal("non-recoverable failure was deferred")
	}
}
