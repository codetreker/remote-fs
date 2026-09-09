package schema

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestObjectRelationshipsRequireUniqueSameNamespaceOwnership(t *testing.T) {
	for _, mutation := range []string{
		`DELETE FROM objects`,
		`UPDATE objects SET namespace=99`,
		`UPDATE objects SET state=2`,
		`UPDATE objects SET size=9`,
		`UPDATE nodes SET content=NULL WHERE id=2`,
		`UPDATE nodes SET content='absent' WHERE id=2`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
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
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
			testFile(t, db, id, root, "file", 3, false)
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
