package schema

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"
)

func TestNodeRelationshipsRejectBrokenRootsAndCycles(t *testing.T) {
	for _, test := range []struct{ name, mutation, diagnostic string }{
		{"missing root", `UPDATE namespaces SET root=900`, "invalid namespace roots"},
		{"two names", `INSERT INTO entries VALUES(1,1,X'616c696173',2)`, "entry cardinality"},
		{"file parent", `UPDATE entries SET parent=2`, "invalid relationship"},
		{"detached with name", `UPDATE nodes SET detached=1 WHERE id=2`, "entry cardinality"},
		{"cross namespace", `UPDATE entries SET namespace=20`, "invalid relationship"},
		{"unreachable cycle", fmt.Sprintf(`UPDATE nodes SET mode=%d,size=0,content=NULL WHERE id=2; UPDATE entries SET parent=2`, fs.ModeDir|0o755), "outside their namespace root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
			testFile(t, db, id, root, "file", 3, false)
			execute(t, db, test.mutation)
			for _, scope := range []*int64{nil, &id} {
				err := validateNodeRelationships(t.Context(), db, scope)
				if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
					t.Fatalf("got %v, want EIO describing %q", err, test.diagnostic)
				}
			}
		})
	}
}

func TestVersionOneGraphRejectsDamageBeforeEntryRebuild(t *testing.T) {
	for _, test := range []struct{ name, mutation, diagnostic string }{
		{"missing endpoint", `UPDATE entries SET node=999`, "invalid endpoints"},
		{"missing root", `UPDATE namespaces SET root=999`, "invalid namespace roots"},
		{"unreachable cycle", fmt.Sprintf(`UPDATE nodes SET mode=%d WHERE id=2; UPDATE entries SET parent=2`, fs.ModeDir|0o755), "outside their namespace root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 1)
			execute(t, db, `INSERT INTO namespaces VALUES(1,'legacy',1,0)`)
			execute(t, db, `INSERT INTO nodes (id,namespace,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec)
				VALUES(1,1,?,0,0,0,0,0),(2,1,420,0,0,0,0,0)`, int64(fs.ModeDir|0o755))
			execute(t, db, `INSERT INTO entries VALUES(1,X'66',2)`)
			if err := validateVersionOneNodeRelationships(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			execute(t, db, test.mutation)
			err := validateVersionOneNodeRelationships(t.Context(), db)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
				t.Fatalf("got %v, want EIO describing %q", err, test.diagnostic)
			}
		})
	}
}

func TestUsedAccountingRejectsOverflowAndInvalidScalars(t *testing.T) {
	for _, test := range []struct{ name, mutation, diagnostic string }{
		{"counter text", `UPDATE namespaces SET used='bad'`, "1 namespaces with an invalid used counter"},
		{"negative counter", `UPDATE namespaces SET used=-1`, "1 namespaces with an invalid used counter"},
		{"size text", `UPDATE nodes SET size='bad' WHERE id=2`, "1 nodes with invalid accounting values"},
		{"mode text", `UPDATE nodes SET mode='bad' WHERE id=2`, "1 nodes with invalid accounting values"},
		{"mismatched total", `UPDATE namespaces SET used=0`, "1 namespaces whose used counter disagrees"},
		{"overflow", `UPDATE nodes SET size=9223372036854775807 WHERE id=2`, "1 namespaces whose file sizes overflow"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
			testFile(t, db, id, root, "first", 3, false)
			testFile(t, db, id, root, "second", 5, false)
			if err := validateUsedAccounting(t.Context(), db, &id); err != nil {
				t.Fatal(err)
			}
			execute(t, db, test.mutation)
			err := validateUsedAccounting(t.Context(), db, &id)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
				t.Fatalf("got %v, want EIO describing %q", err, test.diagnostic)
			}
		})
	}
}
