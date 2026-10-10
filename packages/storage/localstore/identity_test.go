package localstore_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestStorePreservesBackendAndSessionIdentity(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.CheckBackendIdentity(); err != nil {
		t.Fatal(err)
	}
	identity, err := store.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil || identity.RootNodeID != root.ID {
		t.Fatalf("root identity = %+v; root=%+v, %v", identity, root, err)
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
	for _, check := range []func() error{
		session.(storage.FileSessionIdentity).CheckFileSessionIdentity,
		session.(storage.StableReferenceIdentity).CheckStableReferenceIdentity,
		session.(storage.OpenMetadataAccess).CheckOpenMetadataAccess,
	} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	bound, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if bound.Backend != identity || bound.SessionEpoch != status.Epoch {
		t.Fatalf("session identity = %+v; backend=%+v, epoch=%q", bound, identity, status.Epoch)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if result, err := store.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || err == nil {
		t.Fatalf("closed backend identity = %+v, %v", result, err)
	}
}

func TestStoreAtomicOpenPreservesIndependentMetadataPermissions(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.Write(t.Context(), "file", []byte("bytes")); err != nil {
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
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{
		Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")},
	}, storage.OpenAtOptions{
		Read: true, Action: action, Target: storage.ChildCondition{State: storage.Any},
		Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.WriteMetadata, Existing: storage.Keep,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id, err := storage.ReferenceNodeID(result.File); id != result.Attr.ID || err != nil {
		t.Fatalf("opened identity = %d, %v; Attr=%+v", id, err, result.Attr)
	}
	if _, err := result.File.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata write permission granted Stat: %v", err)
	}
	changedAt := time.Unix(1720000000, 0)
	if attr, err := result.File.SetAttr(t.Context(), storage.AttrChange{ModTime: &changedAt}); err != nil || !attr.ModTime.Equal(changedAt) {
		t.Fatalf("metadata write = %+v, %v", attr, err)
	}
	if data, err := result.File.ReadAt(t.Context(), 0, 5); err != nil || string(data.Data) != "bytes" {
		t.Fatalf("byte read = %+v, %v", data, err)
	}
	if _, err := result.File.WriteAt(t.Context(), 0, []byte("other")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata write permission granted byte write: %v", err)
	}
	if err := result.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if id, err := storage.ReferenceNodeID(result.File); id != result.Attr.ID || err != nil {
		t.Fatalf("retired reference identity = %d, %v", id, err)
	}
}
