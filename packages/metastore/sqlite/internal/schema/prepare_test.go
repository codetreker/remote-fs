package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	_ "modernc.org/sqlite"
)

func testDatabase(t *testing.T, version int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if version == 0 {
		return db
	}
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	selected := fstest.MapFS{}
	for _, name := range files[:version] {
		body, err := migrationFiles.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		selected[name] = &fstest.MapFile{Data: body}
	}
	tx := testTransaction(t, db)
	if err := sqliteschema.MustLoad(selected, "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func testTransaction(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func execute(t *testing.T, db interface {
	Exec(string, ...any) (sql.Result, error)
}, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("fixture statement %q: %v", query, err)
	}
}

func testNamespace(t *testing.T, db *sql.DB, name string) (int64, int64) {
	t.Helper()
	id, root, err := Prepare(t.Context(), db, name, "", changes.DefaultWindow(), 1000, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return id, root
}

func testFile(t *testing.T, db *sql.DB, namespace, parent int64, name string, size int64, detached bool) (int64, string) {
	t.Helper()
	tx := testTransaction(t, db)
	id, err := dbstate.AllocateNodeID(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("content-%d", id)
	execute(t, tx, `INSERT INTO objects (key, namespace, state, size, created_sec, created_nsec)
		VALUES (?, ?, ?, ?, 0, 0)`, key, namespace, StateReferenced, size)
	execute(t, tx, `INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, detached)
		VALUES (?, ?, 420, ?, 0, 0, 0, 0, ?, ?)`, id, namespace, size, key, detached)
	if !detached {
		execute(t, tx, `INSERT INTO entries (namespace, parent, name, node) VALUES (?, ?, ?, ?)`, namespace, parent, []byte(name), id)
	}
	execute(t, tx, `UPDATE namespaces SET used = used + ? WHERE id = ?`, size, namespace)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return id, key
}

func testChange(t *testing.T, db *sql.DB, namespace, parent int64, name string) {
	t.Helper()
	tx := testTransaction(t, db)
	if err := changes.Record(t.Context(), tx, namespace, metastore.Change{
		Kind:   metastore.Removed,
		Parent: parent,
		Name:   []byte(name),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparePreservesNamespaceAndAdvancesDurableState(t *testing.T) {
	db := testDatabase(t, 0)
	id, root, state, err := PrepareConfigured(t.Context(), db, "workspace", "store-a", changes.DefaultWindow(), 1000, 1<<20,
		&DurableOpen{Mode: CreateNamespaceIfMissing, Witnessed: true})
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 || root <= 0 || state.Generation != 1 || state.NodeHighWater != root || len(state.DatabaseID) != 32 {
		t.Fatalf("unexpected prepared identity: namespace=%d root=%d state=%+v", id, root, state)
	}
	var mode, size, used int64
	var incarnation string
	if err := db.QueryRow(`SELECT n.mode, n.size, ns.used, l.incarnation FROM namespaces ns
		JOIN nodes n ON n.id = ns.root JOIN logs l ON l.namespace = ns.id WHERE ns.id = ?`, id).
		Scan(&mode, &size, &used, &incarnation); err != nil {
		t.Fatal(err)
	}
	if mode != int64(fs.ModeDir|0o755) || size != 0 || used != 0 || incarnation == "" {
		t.Fatalf("invalid root: mode=%o size=%d used=%d log=%q", mode, size, used, incarnation)
	}
	gotID, gotRoot, next, err := PrepareConfigured(t.Context(), db, "workspace", "store-a", changes.DefaultWindow(), 1000, 1<<20,
		&DurableOpen{Mode: RequireExistingNamespace, Witnessed: true, Startup: dbstate.Startup{
			Accepted: state, CheckpointedGeneration: state.Generation,
		}})
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id || gotRoot != root || next.DatabaseID != state.DatabaseID || next.Generation != state.Generation+1 {
		t.Fatalf("reopening changed identity or failed to advance: %d %d %+v", gotID, gotRoot, next)
	}
}

func TestPrepareRefusalRollsBackMigrationAndNamespaceCreation(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    int
		mode       *DurableOpen
		maxRecords int64
		want       error
	}{
		{"missing required namespace", 0, &DurableOpen{Mode: RequireExistingNamespace}, 1000, syscall.EIO},
		{"insufficient integrity budget", 0, nil, 2, syscall.EFBIG},
		{"unwitnessed existing layout", 5, &DurableOpen{Mode: CreateNamespaceIfMissing, Witnessed: true}, 1000, syscall.EIO},
		{"accepted identity without schema", 0, &DurableOpen{Mode: RequireExistingNamespace, Startup: dbstate.Startup{Accepted: dbstate.State{DatabaseID: strings.Repeat("a", 32)}}}, 1000, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, test.version)
			_, _, _, err := PrepareConfigured(t.Context(), db, "missing", "store", changes.DefaultWindow(), test.maxRecords, 1<<20, test.mode)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			var tables int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='namespaces'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if test.version == 0 && tables != 0 {
				t.Fatal("a refused opening committed its migration")
			}
			if tables != 0 {
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM namespaces`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("refused opening retained namespace: count=%d error=%v", count, err)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := Prepare(ctx, testDatabase(t, 0), "x", "", changes.DefaultWindow(), 1000, 1<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled preparation lost its cause: %v", err)
	}
}

func TestPrepareMigratesPopulatedHistoricalNamespaces(t *testing.T) {
	for _, version := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db := testDatabase(t, version)
			execute(t, db, `INSERT INTO namespaces (id,name,root,used) VALUES (1,'legacy',1,3)`)
			execute(t, db, `INSERT INTO nodes (id,namespace,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
				VALUES (1,1,?,0,0,0,0,0,NULL),(2,1,420,3,0,0,0,0,'data')`, int64(fs.ModeDir|0o755))
			execute(t, db, `INSERT INTO objects (key,namespace,state,size,created_sec,created_nsec) VALUES ('data',1,1,3,0,0)`)
			if version == 1 {
				execute(t, db, `INSERT INTO entries (parent,name,node) VALUES (1,X'66696c65',2)`)
			} else {
				execute(t, db, `INSERT INTO entries (namespace,parent,name,node) VALUES (1,1,X'66696c65',2)`)
				execute(t, db, `INSERT INTO logs (namespace,incarnation,committed_position,trimmed_through,trimmed_by_age)
					VALUES (1,'historical',0,0,0)`)
			}
			if version >= 3 {
				execute(t, db, `UPDATE database_state SET node_high_water=2`)
			}
			id, root, err := Prepare(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 1<<20)
			if err != nil || id != 1 || root != 1 {
				t.Fatalf("migrating version %d: id=%d root=%d error=%v", version, id, root, err)
			}
			var stored, revision int
			var size int64
			if err := db.QueryRow(`SELECT (SELECT version FROM schema_version),content_revision,size FROM nodes WHERE id=2`).Scan(&stored, &revision, &size); err != nil {
				t.Fatal(err)
			}
			if stored != 5 || revision != 1 || size != 3 {
				t.Fatalf("migration changed data: version=%d revision=%d size=%d", stored, revision, size)
			}
		})
	}
}

func TestBackingStoreBindingRejectsDifferentOrPopulatedStores(t *testing.T) {
	for _, test := range []struct {
		name, setup, requested string
		want                   error
	}{
		{"unbound", "", "", nil},
		{"first binding", "", "store-a", nil},
		{"matching", `INSERT INTO backing_store VALUES(1,'store-a')`, "store-a", nil},
		{"unbound opener", `INSERT INTO backing_store VALUES(1,'store-a')`, "", syscall.EINVAL},
		{"different store", `INSERT INTO backing_store VALUES(1,'store-a')`, "store-b", syscall.EINVAL},
		{"populated unbound", `INSERT INTO namespaces VALUES(1,'existing',1,0)`, "store-a", syscall.EINVAL},
		{"invalid binding", `PRAGMA ignore_check_constraints=ON; INSERT INTO backing_store VALUES(1,'')`, "store-a", syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 5)
			if test.setup != "" {
				execute(t, db, test.setup)
			}
			tx := testTransaction(t, db)
			err := bindBackingStore(t.Context(), tx, test.requested)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			if err == nil && test.requested != "" {
				var stored string
				if err := tx.QueryRow(`SELECT store_id FROM backing_store`).Scan(&stored); err != nil || stored != test.requested {
					t.Fatalf("binding = %q, error=%v", stored, err)
				}
			}
		})
	}
}

func TestRecordedSchemaVersionRejectsCoercedScalars(t *testing.T) {
	for _, test := range []struct {
		name, setup string
		version     int
		recorded    bool
		want        error
	}{
		{"absent", "", 0, false, nil},
		{"empty", `CREATE TABLE schema_version(version)`, 0, false, nil},
		{"integer", `CREATE TABLE schema_version(version); INSERT INTO schema_version VALUES(5)`, 5, true, nil},
		{"text", `CREATE TABLE schema_version(version); INSERT INTO schema_version VALUES('5')`, 0, false, syscall.EIO},
		{"negative", `CREATE TABLE schema_version(version); INSERT INTO schema_version VALUES(-1)`, 0, false, syscall.EIO},
		{"blob", `CREATE TABLE schema_version(version); INSERT INTO schema_version VALUES(X'35')`, 0, false, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			if test.setup != "" {
				execute(t, db, test.setup)
			}
			got, recorded, err := recordedSchemaVersion(t.Context(), testTransaction(t, db))
			if got != test.version || recorded != test.recorded || !errors.Is(err, test.want) {
				t.Fatalf("version=%d recorded=%t error=%v", got, recorded, err)
			}
		})
	}
}
