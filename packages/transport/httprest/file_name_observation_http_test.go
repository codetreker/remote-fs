package httprest

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestNameObservationNativeHTTPKeepsMetadataAndEnumerationSeparate(t *testing.T) {
	meta, backend := memoryfixture.New(t, "name-observation", 1<<20, locking.DefaultOptions())
	if err := backend.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Create(t.Context(), "directory/child"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Create(t.Context(), "original"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, meta)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := backend.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	held, err := session.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, Use: storage.UseClaim{Deny: storage.ReadEntries}})
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	otherSession := other.(*remoteFileSession)
	scope, err := held.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	scopedTarget := storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}
	foreignResult := observationResult(t, 4096)
	foreign, err := otherSession.ObserveDirectoryMetadata(t.Context(), scopedTarget, storage.DirectoryMetadataOptions{}, foreignResult)
	_, foreignErr := foreignResult.Entries()
	if !errors.Is(err, storage.ErrInvalidScope) || foreignErr == nil || foreign.Observation.ParentID != 0 {
		t.Fatalf("foreign directory scope disclosed metadata: %+v %v/%v", foreign, err, foreignErr)
	}
	ownResult := observationResult(t, 4096)
	if _, err := session.ObserveDirectoryMetadata(t.Context(), scopedTarget, storage.DirectoryMetadataOptions{}, ownResult); err != nil {
		t.Fatalf("own live metadata scope: %v", err)
	}
	if _, err := otherSession.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("ReadEntries exclusion did not hold: %v", err)
	}
	before, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
	if err != nil {
		t.Fatal(err)
	}
	result := observationResult(t, 4096)
	observation, err := otherSession.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{IncludeName: true, Guards: &storage.NamespaceGuards{RootID: root.ID}}, result)
	entries, readErr := result.Entries()
	if err != nil || readErr != nil || observation.Name == nil || string(observation.Name.RawLeaf) != "directory" || len(entries) != 1 || entries[0].Name != "child" {
		t.Fatalf("metadata under enumeration denial: %+v %+v %v/%v", observation, entries, err, readErr)
	}
	after, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
	if err != nil || before.Position != after.Position {
		t.Fatalf("metadata observation published a mutation: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := backend.Create(t.Context(), "directory/later"); err != nil {
		t.Fatal(err)
	}
	stale := observationResult(t, 4096)
	value, err := otherSession.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{observation.Observation}}}, stale)
	_, staleErr := stale.Entries()
	if !errors.Is(err, storage.ErrConditionConflict) || staleErr == nil || value.Observation.ParentID != 0 {
		t.Fatalf("stale directory guard supplied an absence view: %+v %v/%v", value, err, staleErr)
	}
	if err := held.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := otherSession.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}); err != nil {
		t.Fatalf("ReadEntries did not recover after release: %v", err)
	}

	original, err := backend.Stat(t.Context(), "original")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := session.OpenNodeRef(t.Context(), original.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	reference := retained.Reference.(storage.ReferenceNameObserver)
	if _, err := retained.Reference.Stat(t.Context()); err == nil {
		t.Fatal("DELETE-only reference gained attribute permission")
	}
	if _, err := retained.Reference.(storage.ReferenceStateAccess).State(t.Context()); err == nil {
		t.Fatal("DELETE-only reference gained state permission")
	}
	name, err := reference.ObserveName(t.Context(), nil)
	if err != nil || name.NodeID != original.ID || string(name.RawLeaf) != "original" {
		t.Fatalf("DELETE-only binding: %+v %v", name, err)
	}
	if err := backend.Rename(t.Context(), "original", "moved"); err != nil {
		t.Fatal(err)
	}
	name, err = reference.ObserveName(t.Context(), nil)
	if err != nil || name.NodeID != original.ID || string(name.RawLeaf) != "moved" {
		t.Fatalf("current binding after another client's rename: %+v %v", name, err)
	}
	if err := backend.Create(t.Context(), "original"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	name, err = reference.ObserveName(t.Context(), nil)
	if err != nil || name.NodeID != original.ID || name.State != storage.NameDetached || name.RawLeaf != nil || name.ParentID != 0 {
		t.Fatalf("detached retained binding: %+v %v", name, err)
	}
}
