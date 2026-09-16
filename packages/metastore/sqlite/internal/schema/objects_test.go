package schema

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestObjectRelationshipsRequireUniqueSameVolumeOwnership(t *testing.T) {
	for _, mutation := range []string{
		`DELETE FROM objects`,
		`UPDATE objects SET volume=99`,
		`UPDATE objects SET state=2`,
		`UPDATE objects SET size=9`,
		`UPDATE nodes SET content=NULL WHERE id=2`,
		`UPDATE nodes SET content='absent' WHERE id=2`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "workspace")
			testFile(t, db, id, root, "file", 3, false)
			execute(t, db, mutation)
			for _, scope := range []*int64{nil, &id} {
				err := validateObjectRelationships(t.Context(), db, scope)
				if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "object relationships") {
					t.Fatalf("unsafe object ownership accepted: %v", err)
				}
			}
		})
	}
}

func TestLegacyObjectsCannotInventDeletionAuthority(t *testing.T) {
	for _, test := range []struct{ name, mutation, diagnostic string }{
		{"reserved", `UPDATE objects SET state=0`, "ownership and payload size cannot be proven"},
		{"negative size", `UPDATE objects SET size=-1`, "invalid size"},
		{"missing file", `DELETE FROM nodes WHERE id=2`, "referenced objects without exactly one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 2)
			execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'legacy',1,3)`)
			execute(t, db, `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec) VALUES('data',1,1,3,0,0)`)
			execute(t, db, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
				VALUES(1,1,2147484141,0,0,0,0,0,NULL),(2,1,420,3,0,0,0,0,'data')`)
			execute(t, db, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,X'66696c65',2)`)
			if err := validateLegacyObjectIntegrity(t.Context(), db, 2); err != nil {
				t.Fatal(err)
			}
			execute(t, db, test.mutation)
			err := validateLegacyObjectIntegrity(t.Context(), db, 2)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
				t.Fatalf("got %v, want EIO describing %q", err, test.diagnostic)
			}
		})
	}
}

func TestObjectRelationshipsAllowInlineSymlinkBytes(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "links")
	node, key := testFile(t, db, volume, root, "link", 0, false)
	execute(t, db, `UPDATE nodes SET kind=3,size=6,content=NULL,link_target=X'746172676574' WHERE id=?`, node)
	execute(t, db, `DELETE FROM objects WHERE key=?`, key)
	if err := validateObjectRelationships(t.Context(), db, &volume); err != nil {
		t.Fatalf("symlink has no object: %v", err)
	}
	execute(t, db, `UPDATE nodes SET kind=1,link_target=X'' WHERE id=?`, node)
	if err := validateObjectRelationships(t.Context(), db, &volume); !errors.Is(err, syscall.EIO) {
		t.Fatalf("regular file bytes have no object: %v", err)
	}
}
