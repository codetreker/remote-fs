package dbstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

func TestInspectValidatesRecordedDurableSourceVersions(t *testing.T) {
	files, err := filepath.Glob("../schema/migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{3, 4, 5, 6} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			selected := fstest.MapFS{}
			for _, name := range files[:version] {
				data, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				selected["migrations/"+filepath.Base(name)] = &fstest.MapFile{Data: data}
			}
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			tx := stateTransaction(t, db)
			if err := sqliteschema.MustLoad(selected, "migrations").Reach(t.Context(), tx); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			state, err := Inspect(t.Context(), db)
			if err != nil || state.DatabaseID == "" || state.Generation != 0 || state.NodeHighWater != 0 || state.ChangeHighWater != 0 {
				t.Fatalf("recorded source inspection=%+v, %v", state, err)
			}
		})
	}
}

func TestInspectRejectsInvalidVersionsAndPreservesReadFailures(t *testing.T) {
	for _, version := range []any{int64(0), int64(2), int64(7), "invalid"} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, `UPDATE schema_version SET version=?`, version)
			if _, err := Inspect(t.Context(), db); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid recorded version=%v", err)
			}
		})
	}
	db, _ := stateFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Inspect(ctx, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled inspection=%v", err)
	}
	execState(t, db, `DROP TABLE schema_version`)
	if _, err := Inspect(t.Context(), db); err == nil {
		t.Fatal("missing version table accepted")
	}
}
