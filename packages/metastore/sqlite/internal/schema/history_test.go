package schema

import (
	"errors"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"io/fs"
	"strings"
	"syscall"
	"testing"
)

func TestHistoryRejectsDiscontinuousOrMalformedChanges(t *testing.T) {
	for _, test := range []struct{ name, mutation string }{
		{"tail ahead", `UPDATE logs SET committed_position=50`},
		{"trim beyond tail", `UPDATE logs SET trimmed_through=50`},
		{"missing predecessor", `UPDATE changes SET previous_position=1 WHERE position=1`},
		{"invalid kind", `UPDATE changes SET kind=99`},
		{"invalid name", `UPDATE changes SET name=X'2f'`},
		{"removed node payload", `UPDATE changes SET node=1`},
		{"rename without source", `UPDATE changes SET kind=3`},
		{"change outside volume", `UPDATE changes SET volume=999`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "workspace")
			testChange(t, db, id, root, "old")
			testChange(t, db, id, root, "another")
			for _, scope := range []*int64{nil, &id} {
				if err := validateLogIntegrity(t.Context(), db, scope); err != nil {
					t.Fatal(err)
				}
			}
			execute(t, db, test.mutation)
			err := validateLogIntegrity(t.Context(), db, nil)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "invalid change rows") {
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
	change := metastore.Change{Kind: metastore.Created, Parent: root, Name: []byte("link"), Node: &metastore.Node{ID: node, Mode: fs.ModeSymlink | 0777, Size: 6}, Notification: &metastore.Notification{SubjectID: node, SubjectKind: fs.ModeSymlink, ChangeMask: metastore.ChangeName, After: &metastore.LocationFacts{Ancestors: []metastore.DirectoryAncestor{{DirectoryID: root}}, LeafName: []byte("link")}}}
	if err := changes.Record(t.Context(), tx, volume, change); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := validateLogIntegrity(t.Context(), db, &volume); err != nil {
		t.Fatalf("valid symlink history: %v", err)
	}
	execute(t, db, `UPDATE changes SET content='object'`)
	if err := validateLogIntegrity(t.Context(), db, &volume); !errors.Is(err, syscall.EIO) {
		t.Fatalf("symlink with object: %v", err)
	}
}
