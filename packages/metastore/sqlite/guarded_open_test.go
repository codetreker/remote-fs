package sqlite

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestGuardedChildOpenRejectsAChangedDirectoryWithoutPartialEffects(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Create(t.Context(), "file"); err != nil {
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
			Directories: []storage.DirectoryObservation{observed.Observation},
		},
	}
	changed := root.Attr().ModTime
	if _, err := store.SetNodeAttr(t.Context(), uint64(file.ID), storage.AttrChange{AccessTime: &changed}); err != nil {
		t.Fatal(err)
	}
	positive, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if err != nil {
		t.Fatalf("content-only change invalidated the namespace guard: %v", err)
	}
	if err := positive.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "FILE"); err != nil {
		t.Fatal(err)
	}

	intent := storage.DeleteIntentID("33333333333333333333333333333333")
	result, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName, Deny: storage.ReadData}, Existing: storage.ResetContent,
		CloseIntent: &storage.CloseIntent{ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if !errors.Is(err, storage.ErrConditionConflict) || result.File != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("stale guarded open = %+v, %v", result, err)
	}
	if current, err := store.Stat(t.Context(), "file"); err != nil || current.ID != file.ID {
		t.Fatalf("failed guarded open changed the selected file: %+v, %v", current, err)
	}
	if status, err := store.QueryDeleteIntent(t.Context(), intent); err != nil || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("failed guarded open armed a close intent: %+v, %v", status, err)
	}
	probe, err := store.OpenAt(t.Context(), storage.ChildSelection{Name: selection.Name}, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if err != nil {
		t.Fatalf("failed guarded open retained a use claim: %v", err)
	}
	if err := probe.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedChildReferenceRejectsStaleEdgeAndCreateRevision(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
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
	edgeSelection := storage.ChildSelection{
		Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("directory")},
		Guards: &storage.NamespaceGuards{RootID: uint64(root.ID), Edges: []storage.ObservedEdge{{
			ParentID: uint64(root.ID), RawLeaf: []byte("directory"), ChildID: uint64(directory.ID),
		}}},
	}
	opened, err := store.OpenChildRef(t.Context(), edgeSelection, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatalf("fresh guarded child reference = %v", err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "directory", "renamed"); err != nil {
		t.Fatal(err)
	}
	if result, err := store.OpenChildRef(t.Context(), edgeSelection, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), MetadataAccess: storage.ReadMetadata,
	}); !errors.Is(err, storage.ErrConditionConflict) || result.Reference != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("stale guarded child reference = %+v, %v", result, err)
	}

	beforeCreate := observedDirectory(t, store.Store, uint64(root.ID))
	if err := store.Create(t.Context(), "NEW"); err != nil {
		t.Fatal(err)
	}
	createSelection := storage.ChildSelection{
		Name:   storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("new")},
		Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{beforeCreate.Observation}},
	}
	result, err := store.OpenChildRef(t.Context(), createSelection, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Create: true, Exclusive: true, MetadataAccess: storage.ReadMetadata,
	})
	if !errors.Is(err, storage.ErrConditionConflict) || result.Reference != nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("stale guarded create = %+v, %v", result, err)
	}
	if _, err := store.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("stale guarded create left a child: %v", err)
	}
}
