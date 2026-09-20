package localstore_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestStorePreservesNameObservationCapabilities(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.Write(t.Context(), "file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	directory, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		t.Fatal("store hid directory metadata observation")
	}
	if err := directory.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	name, ok := file.(storage.ReferenceNameObserver)
	if !ok {
		t.Fatal("store hid retained name observation")
	}
	if err := name.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	before, err := name.ObserveName(t.Context(), nil)
	if err != nil || before.State != storage.NameLinked || before.ParentID != root.ID || !bytes.Equal(before.RawLeaf, []byte("file")) {
		t.Fatalf("initial name = %+v, %v", before, err)
	}
	id, err := storage.ReferenceNodeID(file)
	if err != nil || id != before.NodeID {
		t.Fatalf("reference identity = %d, %v", id, err)
	}
	if err := store.Rename(t.Context(), "file", "renamed"); err != nil {
		t.Fatal(err)
	}
	after, err := name.ObserveName(t.Context(), nil)
	if err != nil || after.NodeID != before.NodeID || after.State != storage.NameLinked || after.ParentID != root.ID || !bytes.Equal(after.RawLeaf, []byte("renamed")) {
		t.Fatalf("current name = %+v, %v", after, err)
	}
	result, err := storage.NewListResult(65536, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return 256 + nameBytes + 4*metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := directory.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: root.ID}, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Name == nil || observed.Name.State != storage.NameRoot || observed.Name.NodeID != root.ID || observed.Observation.ParentID != root.ID {
		t.Fatalf("root observation = %+v", observed)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "renamed" || entries[0].Attr.ID != before.NodeID {
		t.Fatalf("directory metadata = %+v, %v", entries, err)
	}
}
