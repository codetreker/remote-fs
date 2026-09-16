package schema

import (
	"errors"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHistoryRejectsDiscontinuousOrMalformedChanges(t *testing.T) {
	for _, test := range []struct{ name, mutation, diagnostic string }{
		{"tail ahead", `UPDATE logs SET committed_position=50`, "invalid volume logs"},
		{"trim beyond tail", `UPDATE logs SET trimmed_through=50`, "invalid volume logs"},
		{"missing predecessor", `UPDATE changes SET previous_position=1 WHERE position=1`, "invalid volume logs"},
		{"invalid kind", `UPDATE changes SET kind=99`, "kind 99"},
		{"invalid name", `UPDATE changes SET name=X'2f'`, "invalid destination name"},
		{"removed node payload", `UPDATE changes SET node=1`, "node field node"},
		{"rename without source", `UPDATE changes SET kind=3`, "source location"},
		{"change outside volume", `UPDATE changes SET volume=999`, "invalid change rows"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "workspace")
			testChange(t, db, id, root, "old")
			testChange(t, db, id, root, "another")
			for _, scope := range []*int64{nil, &id} {
				if err := validateLogIntegrity(t.Context(), db, scope, 1000, 8<<20); err != nil {
					t.Fatal(err)
				}
			}
			execute(t, db, test.mutation)
			err := validateLogIntegrity(t.Context(), db, nil, 1000, 8<<20)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
				t.Fatalf("corrupt history accepted: %v", err)
			}
		})
	}
}

func TestHistoricalLogValidatesShapeWithoutPredecessorColumn(t *testing.T) {
	db := testDatabase(t, 2)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'legacy',1,0)`)
	execute(t, db, `INSERT INTO logs VALUES(1,'old-history',4,0,0)`)
	execute(t, db, `INSERT INTO changes (position,volume,kind,parent,name,recorded_sec,recorded_nsec)
		VALUES(4,1,1,1,X'66',0,0)`)
	id := int64(1)
	for _, scope := range []*int64{nil, &id} {
		if err := validateVersionTwoLogIntegrity(t.Context(), db, scope); err != nil {
			t.Fatalf("valid sparse historical log refused: %v", err)
		}
	}
	execute(t, db, `UPDATE changes SET recorded_nsec='bad'`)
	if err := validateVersionTwoLogIntegrity(t.Context(), db, &id); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "invalid SQLite storage classes") {
		t.Fatalf("historical scalar was coerced: %v", err)
	}
}

func TestHistoryAcceptsSymlinkFactsAndRejectsObjectContent(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "links")
	node, _ := testFile(t, db, volume, root, "link", 0, false)
	tx := testTransaction(t, db)
	var entry storage.EntryID
	if err := tx.QueryRow(`SELECT id FROM entries WHERE node=?`, node).Scan(&entry); err != nil {
		t.Fatal(err)
	}
	link := &metastore.Node{ID: node, Kind: storage.NodeSymlink, Size: 6, MetadataRevision: 1, LinkTarget: []byte("target")}
	change := metastore.Change{Kind: metastore.Created, Parent: root, Name: []byte("link"), Node: link,
		Notification: &metastore.Notification{SubjectID: node, SubjectKind: storage.NodeSymlink, ChangeMask: metastore.ChangeName,
			After: &metastore.EventImage{Attr: link.Attr(), LinkTarget: []byte("target"),
				Location: storage.EntryLocation{State: storage.LocationLinked, RootNodeID: uint64(root), NodeID: uint64(node),
					Ancestors: []storage.EntryCondition{{ParentID: uint64(root), DirectoryRevision: 1, EntryID: entry, NodeID: uint64(node), Name: []byte("link")}}}}}}
	if err := changes.Record(t.Context(), tx, volume, change); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := validateLogIntegrity(t.Context(), db, &volume, 1000, 8<<20); err != nil {
		t.Fatalf("valid symlink history: %v", err)
	}
	execute(t, db, `UPDATE changes SET content='object'`)
	if err := validateLogIntegrity(t.Context(), db, &volume, 1000, 8<<20); !errors.Is(err, syscall.EIO) {
		t.Fatalf("symlink with object: %v", err)
	}
}
