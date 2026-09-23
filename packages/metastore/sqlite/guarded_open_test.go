package sqlite

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func guardedFileFixture(t *testing.T) (*LockingStore, metastore.Node, metastore.Node, storage.ChildSelection) {
	t.Helper()
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	retained, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := retained.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := retained.Reserve(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Commit(t.Context(), state.Revision, metastore.Object{Key: key, Size: 4, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := retained.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file, err := store.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	observed := observedDirectory(t, store.Store, uint64(root.ID))
	selection := storage.ChildSelection{
		Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")},
		Guards: &storage.NamespaceGuards{
			RootID:      uint64(root.ID),
			Directories: []storage.DirectoryObservation{observed.Observation},
		},
	}
	return store, root, file, selection
}

func requireRejectedGuardedOpen(t *testing.T, result metastore.OpenResult, err error) {
	t.Helper()
	if !errors.Is(err, storage.ErrConditionConflict) || result.File != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("stale guarded open = %+v, %v", result, err)
	}
}

func requireRejectedGuardedReference(t *testing.T, result metastore.NodeOpenResult, err error) {
	t.Helper()
	if !errors.Is(err, storage.ErrConditionConflict) || result.Reference != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("stale guarded child reference = %+v, %v", result, err)
	}
}

func TestGuardedChildSelectionAcceptsFreshDirectoryAndEdgeEvidence(t *testing.T) {
	store, root, file, selection := guardedFileFixture(t)
	accessTime := time.Now().Add(-time.Hour)
	if _, err := store.SetNodeAttr(t.Context(), uint64(file.ID), storage.AttrChange{AccessTime: &accessTime}); err != nil {
		t.Fatal(err)
	}

	opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if err != nil || opened.File == nil || opened.State.ID != file.ID || opened.Outcome != storage.Opened {
		t.Fatalf("fresh guarded file open = %+v, %v", opened, err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	edgeSelection := storage.ChildSelection{
		Name: selection.Name,
		Guards: &storage.NamespaceGuards{
			RootID: uint64(root.ID),
			Edges: []storage.ObservedEdge{{
				ParentID: uint64(root.ID), RawLeaf: []byte("file"), ChildID: uint64(file.ID),
			}},
		},
	}
	reference, err := store.OpenChildRef(t.Context(), edgeSelection, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil || reference.Reference == nil || reference.State.ID != file.ID || reference.Outcome != storage.Opened {
		t.Fatalf("fresh guarded child reference = %+v, %v", reference, err)
	}
	if err := reference.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedResetRejectsCaseEquivalentSiblingWithoutPartialEffects(t *testing.T) {
	store, _, file, selection := guardedFileFixture(t)
	if err := store.Create(t.Context(), "FILE"); err != nil {
		t.Fatal(err)
	}
	intent := storage.DeleteIntentID("11111111111111111111111111111111")
	result, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Write: true,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)}, Action: fileAction(t),
		Use:      storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName, Deny: storage.ReadData},
		Existing: storage.ResetContent,
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner,
			ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile,
		},
	})
	requireRejectedGuardedOpen(t, result, err)

	current, err := store.Stat(t.Context(), "file")
	if err != nil || current.ID != file.ID || current.Size != 4 {
		t.Fatalf("failed guarded reset changed the selected node: %+v, %v", current, err)
	}
	if status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent); err != nil || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("failed guarded reset armed close intent: %+v, %v", status, err)
	}
	probe, err := store.OpenAt(t.Context(), selectChild(selection.Name), storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if err != nil {
		t.Fatalf("failed guarded reset retained a use claim: %v", err)
	}
	if err := probe.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedCreateRejectsCaseEquivalentSiblingWithoutCreatingTheSelectedName(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	observed := observedDirectory(t, store.Store, uint64(root.ID))
	selection := storage.ChildSelection{
		Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("new")},
		Guards: &storage.NamespaceGuards{
			RootID:      uint64(root.ID),
			Directories: []storage.DirectoryObservation{observed.Observation},
		},
	}
	if err := store.Create(t.Context(), "NEW"); err != nil {
		t.Fatal(err)
	}

	result, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	requireRejectedGuardedOpen(t, result, err)
	if _, err := store.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed guarded create left the selected child: %v", err)
	}
	if sibling, err := store.Stat(t.Context(), "NEW"); err != nil || sibling.ID == 0 {
		t.Fatalf("failed guarded create changed the case-equivalent sibling: %+v, %v", sibling, err)
	}
}

func TestGuardedReplaceRejectsCaseEquivalentSiblingWithoutReplacingTheSelectedNode(t *testing.T) {
	store, _, file, selection := guardedFileFixture(t)
	if err := store.Create(t.Context(), "FILE"); err != nil {
		t.Fatal(err)
	}

	result, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Write: true, Create: true,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}, Existing: storage.ReplaceNode,
		Initial: storage.InitialState{OnReplace: storage.InitialFields{
			Metadata: map[string][]byte{"test.replacement": []byte("unexpected")},
		}},
	})
	requireRejectedGuardedOpen(t, result, err)

	current, err := store.Stat(t.Context(), "file")
	if err != nil || current.ID != file.ID {
		t.Fatalf("failed guarded replacement changed identity: %+v, %v", current, err)
	}
	if current.Size != 4 {
		t.Fatalf("failed guarded replacement changed size: %+v", current)
	}
}

func TestGuardedChildReferenceRejectsAStaleEdgeWithoutPartialEffects(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	selection := storage.ChildSelection{
		Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("directory")},
		Guards: &storage.NamespaceGuards{
			RootID: uint64(root.ID),
			Edges: []storage.ObservedEdge{{
				ParentID: uint64(root.ID), RawLeaf: []byte("directory"), ChildID: uint64(directory.ID),
			}},
		},
	}
	if err := store.Rename(t.Context(), "directory", "renamed"); err != nil {
		t.Fatal(err)
	}
	intent := storage.DeleteIntentID("22222222222222222222222222222222")
	result, err := store.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{
		Kind:   storage.NodeDirectory,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)}, Action: fileAction(t),
		Use:            storage.UseClaim{Uses: storage.ReadEntries | storage.DeleteName, Deny: storage.ReadEntries},
		MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner,
			ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty,
		},
	})
	requireRejectedGuardedReference(t, result, err)
	if status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent); err != nil || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("failed guarded child reference armed close intent: %+v, %v", status, err)
	}

	probe, err := store.OpenNodeRef(t.Context(), uint64(directory.ID), storage.NodeRefOptions{
		Kind:   storage.NodeDirectory,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatalf("failed guarded child reference retained a use claim: %v", err)
	}
	if err := probe.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedChildSelectionReportsInvalidParentScopeBeforeStaleGuards(t *testing.T) {
	store, root, file, selection := guardedFileFixture(t)
	parent, err := store.OpenNodeRef(t.Context(), uint64(root.ID), storage.NodeRefOptions{
		Kind:   storage.NodeDirectory,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(root.ID)}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := parent.Reference.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	selection.Name.Parent.Scope = &scope
	if err := store.Create(t.Context(), "FILE"); err != nil {
		t.Fatal(err)
	}

	result, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if !errors.Is(err, storage.ErrInvalidScope) || errors.Is(err, storage.ErrConditionConflict) || result.File != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("invalid parent scope was masked by stale guards: %+v, %v", result, err)
	}
}

func TestStaleGuardPrecedesChildSelectionFailures(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file, err := store.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	observed := observedDirectory(t, store.Store, uint64(root.ID))
	if err := store.Create(t.Context(), "SIBLING"); err != nil {
		t.Fatal(err)
	}
	selection := func(leaf string) storage.ChildSelection {
		return storage.ChildSelection{
			Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte(leaf)},
			Guards: &storage.NamespaceGuards{
				RootID:      uint64(root.ID),
				Directories: []storage.DirectoryObservation{observed.Observation},
			},
		}
	}

	tests := []struct {
		name      string
		selection storage.ChildSelection
		options   storage.OpenAtOptions
	}{
		{
			name:      "absent child",
			selection: selection("missing"),
			options: storage.OpenAtOptions{
				Read: true, Target: storage.ChildCondition{State: storage.Any},
				Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
			},
		},
		{
			name:      "exclusive existing child",
			selection: selection("file"),
			options: storage.OpenAtOptions{
				Read: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
				Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
			},
		},
		{
			name:      "wrong child kind",
			selection: selection("directory"),
			options: storage.OpenAtOptions{
				Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
				Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
			},
		},
		{
			name:      "wrong child identity",
			selection: selection("file"),
			options: storage.OpenAtOptions{
				Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
				Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.options.Action = fileAction(t)
			result, err := store.OpenAt(t.Context(), test.selection, test.options)
			requireRejectedGuardedOpen(t, result, err)
		})
	}

	if current, err := store.Stat(t.Context(), "file"); err != nil || current.ID != file.ID {
		t.Fatalf("failed selections changed file: %+v, %v", current, err)
	}
	if current, err := store.Stat(t.Context(), "directory"); err != nil || current.ID != directory.ID {
		t.Fatalf("failed selections changed directory: %+v, %v", current, err)
	}
	if _, err := store.Stat(t.Context(), "missing"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed absent selection created a child: %v", err)
	}
}

func TestAnyTargetResetAndReplaceAccountForTheSelectedNode(t *testing.T) {
	tests := []struct {
		name     string
		existing storage.ExistingEffect
		outcome  storage.OpenOutcome
	}{
		{name: "reset", existing: storage.ResetContent, outcome: storage.Reset},
		{name: "replace", existing: storage.ReplaceNode, outcome: storage.Replaced},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, file, selection := guardedFileFixture(t)
			var accounting [][2]int64
			uses := storage.ReadData | storage.WriteData
			if test.existing == storage.ReplaceNode {
				uses |= storage.DeleteName
			}
			result, err := store.OpenAt(accountingContext(t, &accounting), selection, storage.OpenAtOptions{
				Read: true, Write: true, Create: test.existing == storage.ReplaceNode,
				Target: storage.ChildCondition{State: storage.Any}, Action: fileAction(t),
				Use: storage.UseClaim{Uses: uses}, Existing: test.existing,
			})
			if err != nil || result.File == nil || result.Outcome != test.outcome {
				t.Fatalf("Target=Any %s = %+v, %v", test.name, result, err)
			}
			if test.existing == storage.ResetContent && result.State.ID != file.ID {
				t.Fatalf("reset selected another identity: got %d want %d", result.State.ID, file.ID)
			}
			if test.existing == storage.ReplaceNode && result.State.ID == file.ID {
				t.Fatalf("replace retained the old identity: %+v", result.State)
			}
			if len(accounting) != 1 || accounting[0] != [2]int64{4, 0} {
				t.Fatalf("Target=Any %s accounting = %+v", test.name, accounting)
			}
			if err := result.File.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if current, err := store.Stat(t.Context(), "file"); err != nil || current.ID != result.State.ID || current.Size != 0 {
				t.Fatalf("Target=Any %s published state = %+v, %v", test.name, current, err)
			}
		})
	}
}

func TestAnyTargetResetAndReplaceHonorTheSelectedNodeLock(t *testing.T) {
	for _, test := range []struct {
		name     string
		existing storage.ExistingEffect
	}{{name: "reset", existing: storage.ResetContent}, {name: "replace", existing: storage.ReplaceNode}} {
		t.Run(test.name, func(t *testing.T) {
			store, root, file, _ := guardedFileFixture(t)
			fixture := publicationFixture{store: store.Store, service: store.LockService()}
			fixture.grant(t, fixture.owner(t), "file", locking.Shared)
			uses := storage.ReadData | storage.WriteData
			if test.existing == storage.ReplaceNode {
				uses |= storage.DeleteName
			}
			result, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{
				Parent: directoryTarget(root), RawLeaf: []byte("file"),
			}), storage.OpenAtOptions{
				Read: true, Write: true, Create: test.existing == storage.ReplaceNode,
				Target: storage.ChildCondition{State: storage.Any}, Action: fileAction(t),
				Use: storage.UseClaim{Uses: uses}, Existing: test.existing,
			})
			if result.File != nil || result.State.ID != 0 || result.Outcome != 0 {
				t.Fatalf("locked Target=Any open returned a partial result: %+v, %v", result, err)
			}
			requirePublicationCode(t, err, locking.Conflict)
			if current := fixture.node(t, "file"); current.ID != file.ID || current.Size != 4 {
				t.Fatalf("locked Target=Any open changed the selected node: %+v", current)
			}
		})
	}
}

func TestKeepWithCloseIntentDoesNotPublishBeforeTheCloseTransition(t *testing.T) {
	store, _, file, selection := guardedFileFixture(t)
	fixture := publicationFixture{store: store.Store, service: store.LockService()}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Shared)
	intent := storage.DeleteIntentID("33333333333333333333333333333333")
	openContext := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		t.Fatal("arming a close intent attempted early publication accounting")
		return nil, syscall.EIO
	})
	result, err := store.OpenAt(openContext, selection, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName}, Existing: storage.Keep,
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner,
			ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile,
		},
	})
	if err != nil || result.File == nil || result.State.ID != file.ID || result.Outcome != storage.Opened {
		t.Fatalf("keep with close intent under shared grant = %+v, %v", result, err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent)
	if err != nil || status.Outcome != storage.DeleteIntentArmed || status.NodeID != uint64(file.ID) {
		t.Fatalf("armed close intent = %+v, %v", status, err)
	}
	if _, err := fixture.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := result.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("close intent did not remove the original name: %v", err)
	}
}
