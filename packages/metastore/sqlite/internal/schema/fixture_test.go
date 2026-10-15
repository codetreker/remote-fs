package schema

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	"github.com/codetreker/remote-fs/packages/storage"
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
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
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

func testVolume(t *testing.T, db *sql.DB, name string) (int64, int64) {
	t.Helper()
	id, root, err := Prepare(t.Context(), db, name, "", changes.DefaultWindow(), 1000, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return id, root
}

func testFile(t *testing.T, db *sql.DB, volume, parent int64, name string, size int64, detached bool) (int64, string) {
	t.Helper()
	tx := testTransaction(t, db)
	id, err := dbstate.AllocateNodeID(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("content-%d", id)
	execute(t, tx, `INSERT INTO objects (key, volume, state, size, created_sec, created_nsec)
		VALUES (?, ?, ?, ?, 0, 0)`, key, volume, StateReferenced, size)
	allocation, err := expectedAllocation(1, size)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, tx, `INSERT INTO nodes (id,volume,kind,size,allocation_size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,detached)
		VALUES (?,?,1,?,?,0,0,0,0,?,?)`, id, volume, size, allocation, key, detached)
	if !detached {
		execute(t, tx, `INSERT INTO entries (volume, parent, name, node) VALUES (?, ?, ?, ?)`, volume, parent, []byte(name), id)
	}
	execute(t, tx, `UPDATE volumes SET used = used + ? WHERE id = ?`, size, volume)
	execute(t, tx, `UPDATE volumes SET allocated_used = allocated_used + ? WHERE id = ?`, allocation, volume)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return id, key
}

func testChange(t *testing.T, db *sql.DB, volume, parent int64, name string) {
	t.Helper()
	tx := testTransaction(t, db)
	if err := changes.Record(t.Context(), tx, volume, metastore.Change{
		Kind: metastore.Removed, Parent: parent, Name: []byte(name),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

type validationFixture struct {
	db     *sql.DB
	ctx    context.Context
	before validationSnapshot
}

type validationSnapshot struct {
	constraints int
	schema      [][]any
	tables      map[string][][]any
}

func newValidationFixture(t *testing.T, db *sql.DB) *validationFixture {
	t.Helper()
	db.SetMaxIdleConns(1)
	fixture := &validationFixture{db: db, ctx: context.WithoutCancel(t.Context())}
	fixture.before = fixture.snapshot(t)
	return fixture
}

func (f *validationFixture) transaction(t *testing.T) *sql.Tx {
	t.Helper()
	f.assertIdle(t)
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("rolling back validation fixture: %v", err)
			return
		}
		f.assertIdle(t)
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf("PRAGMA ignore_check_constraints=%d", f.before.constraints)); err != nil {
			t.Errorf("restoring validation fixture constraints: %v", err)
			return
		}
		if after := f.snapshot(t); !reflect.DeepEqual(after, f.before) {
			t.Error("validation fixture retained changed data, schema, or constraints")
		}
		f.assertIdle(t)
	})
	return tx
}

func (f *validationFixture) assertIdle(t *testing.T) {
	t.Helper()
	if count := f.db.Stats().InUse; count != 0 {
		t.Fatalf("validation fixture retains %d connections", count)
	}
}

func (f *validationFixture) snapshot(t *testing.T) validationSnapshot {
	t.Helper()
	f.assertIdle(t)
	state := validationSnapshot{tables: make(map[string][][]any)}
	if err := f.db.QueryRowContext(f.ctx, `PRAGMA ignore_check_constraints`).Scan(&state.constraints); err != nil {
		t.Fatal(err)
	}
	state.schema = validationRows(t, f.ctx, f.db, `SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema`)
	for _, object := range state.schema {
		if object[0] == "table" {
			name := object[1].(string)
			state.tables[name] = validationRows(t, f.ctx, f.db, `SELECT * FROM "`+strings.ReplaceAll(name, `"`, `""`)+`"`)
		}
	}
	return state
}

func validationRows(t *testing.T, ctx context.Context, db *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var captured [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range pointers {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if payload, ok := value.([]byte); ok {
				values[i] = bytes.Clone(payload)
			}
		}
		captured = append(captured, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(captured, func(left, right []any) int {
		return strings.Compare(validationRowKey(left), validationRowKey(right))
	})
	return captured
}

func validationRowKey(row []any) string {
	var key strings.Builder
	for _, value := range row {
		fmt.Fprintf(&key, "%T:%#v;", value, value)
	}
	return key.String()
}

func TestValidationTransactionsRestoreMetadataTriggersAndConstraints(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "workspace")
	fixture := newValidationFixture(t, db)
	if !t.Run("damaged transaction", func(t *testing.T) {
		tx := fixture.transaction(t)
		execute(t, tx, `PRAGMA ignore_check_constraints=ON`)
		execute(t, tx, `UPDATE volumes SET metadata_used='bad' WHERE id=?`, volume)
		execute(t, tx, `DROP TRIGGER nodes_metadata_update`)
		execute(t, tx, `CREATE TRIGGER reject_reused_metadata BEFORE UPDATE OF metadata ON nodes
			BEGIN SELECT RAISE(ABORT,'metadata trigger escaped its transaction'); END`)
		if err := validateMetadataIntegrity(t.Context(), tx, &volume, math.MaxInt64, false, schema.Version()); !errors.Is(err, syscall.EIO) {
			t.Fatalf("corrupt metadata accounting returned %v, want EIO", err)
		}
	}) {
		return
	}
	t.Run("healthy successor", func(t *testing.T) {
		tx := fixture.transaction(t)
		encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{
			"client.isolation": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte("value")},
		})
		if err != nil {
			t.Fatal(err)
		}
		execute(t, tx, `UPDATE nodes SET metadata=? WHERE id=?`, encoded, root)
		if err := validateIntegrity(t.Context(), tx, &volume, 1000, 1<<20, schema.Version()); err != nil {
			t.Fatalf("metadata update after corruption cleanup: %v", err)
		}
		var got []byte
		if err := tx.QueryRowContext(t.Context(), `SELECT metadata FROM nodes WHERE id=?`, root).Scan(&got); err != nil || !bytes.Equal(got, encoded) {
			t.Fatalf("metadata update after corruption cleanup = %x, %v; want %x", got, err, encoded)
		}
	})
}
