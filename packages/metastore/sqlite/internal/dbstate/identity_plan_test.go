package dbstate

import (
	"database/sql"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	_ "modernc.org/sqlite"
	"os"
	"strings"
	"testing"
)

func TestGlobalIdentityBoundsUseExpressionIndexSearches(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/metastore.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	migrations := sqliteschema.MustLoad(os.DirFS("../schema"), "migrations")
	if err := migrations.Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	plan := func(query string) string {
		rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(details, "\n")
	}

	combined := plan(globalNodeIdentityBoundsQuery) + "\n" + plan(globalChangeIdentityBoundsQuery)
	if strings.Contains(combined, "USE TEMP B-TREE") {
		t.Fatalf("global identity bounds build a temporary ordering:\n%s", combined)
	}
	for _, index := range []string{
		"namespaces_by_root_identity",
		"entries_by_node_identity",
		"changes_by_node_identity",
		"changes_by_position_identity",
		"logs_by_change_identity",
	} {
		if count := strings.Count(combined, index); count != 2 {
			t.Fatalf("global identity bounds use %s %d times, want invalid and maximum searches:\n%s",
				index, count, combined)
		}
	}
	for _, table := range []string{"namespaces", "entries", "changes", "logs"} {
		if strings.Contains(combined, "SCAN "+table) {
			t.Fatalf("global identity bounds scan %s:\n%s", table, combined)
		}
	}
}
