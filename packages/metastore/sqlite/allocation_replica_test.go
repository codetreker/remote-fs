package sqlite

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReplicaPreservesUnknownAndOtherClusterAllocationsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.db")
	replica, err := OpenReplica(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := replica.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rows := []metastore.Row{
		{Node: metastore.Node{ID: 1, Kind: storage.NodeDirectory, DirectoryRevision: []byte("source-root")}},
		{Parent: 1, Name: []byte("unknown"), Node: metastore.Node{ID: 2, Kind: storage.NodeRegular, Size: 1}},
		{Parent: 1, Name: []byte("known"), Node: metastore.Node{ID: 3, Kind: storage.NodeRegular, Size: 1, AllocationKnown: true, AllocationSize: 512}},
	}
	if err := seed.Add(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := seed.Complete(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(wantKnown bool, wantSize int64, name string) {
		t.Helper()
		node, err := replica.Stat(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if node.AllocationKnown != wantKnown || node.AllocationSize != wantSize || node.Attr().AllocationKnown != wantKnown {
			t.Fatalf("%s allocation = %+v", name, node)
		}
	}
	check(false, 0, "unknown")
	check(true, 512, "known")
	if _, err := replica.store.Space(t.Context()); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("replica offered a local space estimate: %v", err)
	}
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	replica, err = OpenReplica(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := replica.Close(); err != nil {
			t.Error(err)
		}
	})
	check(false, 0, "unknown")
	check(true, 512, "known")
	modified := rows[1].Node
	modified.AllocationKnown, modified.AllocationSize = true, 512
	if applied, err := replica.Apply(t.Context(), metastore.Change{Position: 1, Kind: metastore.Modified, Parent: 1, Name: []byte("unknown"), Node: &modified}); err != nil || !applied {
		t.Fatalf("known allocation update applied=%t error=%v", applied, err)
	}
	check(true, 512, "unknown")
	modified.AllocationKnown, modified.AllocationSize = false, 0
	if applied, err := replica.Apply(t.Context(), metastore.Change{Position: 2, Kind: metastore.Modified, Parent: 1, Name: []byte("unknown"), Node: &modified}); err != nil || !applied {
		t.Fatalf("unknown allocation update applied=%t error=%v", applied, err)
	}
	check(false, 0, "unknown")
}
