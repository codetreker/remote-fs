package schema

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestColumnLayoutRejectsMissingExtraAndMistypedSourceFields(t *testing.T) {
	for _, test := range []struct{ name, ddl string }{
		{"missing table", ""},
		{"view", `CREATE TABLE actual(id INTEGER); CREATE VIEW sample AS SELECT id FROM actual`},
		{"wrong type", `CREATE TABLE sample(id TEXT)`},
		{"extra field", `CREATE TABLE sample(id INTEGER, extra INTEGER)`},
		{"missing field", `CREATE TABLE sample(other INTEGER)`},
		{"oversized field", `CREATE TABLE sample("` + strings.Repeat("x", 65) + `" INTEGER)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			if test.ddl != "" {
				execute(t, db, test.ddl)
			}
			if err := validateTableColumns(t.Context(), db, "sample", []string{"id"}, 5); !errors.Is(err, syscall.EIO) {
				t.Fatalf("unknown source layout accepted: %v", err)
			}
		})
	}
	db := testDatabase(t, 0)
	execute(t, db, `CREATE TABLE sample(id INTEGER)`)
	if err := validateTableColumns(t.Context(), db, "sample", []string{"id"}, 5); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := validateTableColumns(ctx, db, "sample", []string{"id"}, 5); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled layout read lost its cause: %v", err)
	}
}
