package fuse

import (
	"path/filepath"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestPermissionProjectionSurvivesNativeReopenAndReplicaReplay(t *testing.T) {
	database := filepath.Join(t.TempDir(), "permissions.db")
	first, err := sqlite.Open(t.Context(), database, "permissions", 4096, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := first.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := first.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	metadata := initialMetadata(0640)
	if err := first.SetAttr(t.Context(), "file", storage.AttrChange{ExpectedRevision: node.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := sqlite.Open(t.Context(), database, "permissions", 4096, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Error(err)
		}
	})
	node, err = second.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	v := &volume{}
	var attr gofuse.Attr
	if errno := v.fillAttr(&attr, node.Attr()); errno != 0 || attr.Mode&07777 != 0640 {
		t.Fatalf("reopened mode=%o/%v", attr.Mode, errno)
	}

	replica, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := replica.Close(); err != nil {
			t.Error(err)
		}
	})
	snap, position, err := second.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	rows, err := metastore.NewRowResult(16<<10, 0, func(_ int, _ metastore.Row, l metastore.RowPayloadLengths) (int64, error) {
		return 256 + l.Name + l.Content + l.Metadata + l.Target, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	done, err := snap.Next(t.Context(), 8, rows)
	if err != nil || !done {
		t.Fatalf("small snapshot complete=%v/%v", done, err)
	}
	values, err := rows.Rows()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := replica.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	if err := seed.Add(t.Context(), values); err != nil {
		t.Fatal(err)
	}
	if err := seed.Complete(t.Context(), position); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	metadata = initialMetadata(0600)
	if err := second.SetAttr(t.Context(), "file", storage.AttrChange{ExpectedRevision: node.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
	changes, err := metastore.NewChangeResult(16<<10, 0, func(_ int, _ metastore.Change, l metastore.ChangePayloadLengths) (int64, error) {
		return 256 + l.Name + l.FromName + l.Content + l.Metadata + l.Target + l.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Since(t.Context(), position, 8, changes); err != nil {
		t.Fatal(err)
	}
	events, err := changes.Changes()
	if err != nil || len(events) != 1 {
		t.Fatalf("chmod history=%d/%v", len(events), err)
	}
	for _, event := range events {
		if applied, err := replica.Apply(t.Context(), event); err != nil || !applied {
			t.Fatalf("chmod replay=%v/%v", applied, err)
		}
	}
	node, err = replica.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	if errno := v.fillAttr(&attr, node.Attr()); errno != 0 || attr.Mode&07777 != 0600 {
		t.Fatalf("replayed mode=%o/%v", attr.Mode, errno)
	}
}
