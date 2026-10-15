package dbstate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

type stateExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func stateFixture(t *testing.T) (*sql.DB, State) {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/state.db")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx := stateTransaction(t, db)
	if err := sqliteschema.MustLoad(os.DirFS("../schema"), "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	execState(t, db, `INSERT INTO volumes (id, name, root, used) VALUES (1, 'workspace', 1, 0)`)
	execState(t, db, `INSERT INTO nodes
		(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec)
		VALUES (1,1,2,0,0,0,0,0),(2,1,1,0,0,0,0,0)`)
	execState(t, db, `INSERT INTO entries (volume, parent, name, node) VALUES (1, 1, x'66', 2)`)
	execState(t, db, `INSERT INTO logs VALUES (1, '0123456789abcdef0123456789abcdef', 2, 0, 0)`)
	execState(t, db, `INSERT INTO changes
		(position, previous_position, volume, kind, parent, name, node,
		 node_kind, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, recorded_sec, recorded_nsec, metadata)
		VALUES (2,0,1,0,1,x'66',2,1,0,0,0,0,0,0,0,x'52464d010000')`)
	state := State{DatabaseID: "0123456789abcdef0123456789abcdef", Generation: 7, NodeHighWater: 4, ChangeHighWater: 6}
	setFixtureState(t, db, state)
	return db, state
}

func stateTransaction(t *testing.T, db *sql.DB) *sql.Tx {
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

func execState(t *testing.T, db stateExecutor, statement string, arguments ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), statement, arguments...); err != nil {
		t.Fatalf("fixture statement %q: %v", statement, err)
	}
}

func setFixtureState(t *testing.T, db stateExecutor, state State) {
	t.Helper()
	execState(t, db, `UPDATE database_state SET database_id=?, generation=?, node_high_water=?, change_high_water=?`,
		state.DatabaseID, state.Generation, state.NodeHighWater, state.ChangeHighWater)
	execState(t, db, `UPDATE sqlite_sequence SET seq=? WHERE name='nodes'`, state.NodeHighWater)
	execState(t, db, `UPDATE sqlite_sequence SET seq=? WHERE name='changes'`, state.ChangeHighWater)
}

type stateSQLSnapshot struct {
	query string
	rows  [][]any
}

type stateMatrix struct {
	db                     *sql.DB
	before                 State
	ctx                    context.Context
	ignoreCheckConstraints int
	snapshots              []stateSQLSnapshot
}

func preparedState(t *testing.T) *stateMatrix {
	t.Helper()
	db, before := stateFixture(t)
	f := &stateMatrix{db: db, before: before, ctx: context.WithoutCancel(t.Context())}
	const schemaQuery = `SELECT type, name, sql FROM sqlite_schema ORDER BY type, name`
	schema := stateRows(t, f.ctx, db, schemaQuery)
	f.snapshots = append(f.snapshots, stateSQLSnapshot{schemaQuery, schema})
	for _, object := range schema {
		if object[0] != "table" {
			continue
		}
		name := `"` + strings.ReplaceAll(object[1].(string), `"`, `""`) + `"`
		rows, err := db.QueryContext(f.ctx, `SELECT * FROM `+name+` LIMIT 0`)
		if err != nil {
			t.Fatal(err)
		}
		columns, columnErr := rows.Columns()
		closeErr := rows.Close()
		if columnErr != nil || closeErr != nil {
			t.Fatal(errors.Join(columnErr, closeErr))
		}
		order := make([]string, len(columns))
		for i := range columns {
			order[i] = fmt.Sprint(i + 1)
		}
		query := `SELECT * FROM ` + name + ` ORDER BY ` + strings.Join(order, ",")
		f.snapshots = append(f.snapshots, stateSQLSnapshot{query, stateRows(t, f.ctx, db, query)})
	}
	if err := db.QueryRowContext(f.ctx, `PRAGMA ignore_check_constraints`).Scan(&f.ignoreCheckConstraints); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.assertClean(t) })
	return f
}

func (f *stateMatrix) transaction(t *testing.T) *sql.Tx {
	t.Helper()
	return f.begin(t, false)
}

func (f *stateMatrix) committableTransaction(t *testing.T) *sql.Tx {
	t.Helper()
	return f.begin(t, true)
}

func (f *stateMatrix) begin(t *testing.T, commit bool) *sql.Tx {
	t.Helper()
	f.assertClean(t)
	// Child context cancellation must not race rollback and connection-setting restoration.
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !(commit && errors.Is(err, sql.ErrTxDone)) {
			t.Errorf("release matrix transaction: %v", err)
		}
		f.assertReleased(t)
		if _, err := f.db.ExecContext(f.ctx, fmt.Sprintf(`PRAGMA ignore_check_constraints=%d`, f.ignoreCheckConstraints)); err != nil {
			t.Errorf("restore matrix check constraints: %v", err)
		}
		f.assertClean(t)
	})
	return tx
}

func (f *stateMatrix) assertReleased(t *testing.T) {
	t.Helper()
	if inUse := f.db.Stats().InUse; inUse != 0 {
		t.Fatalf("matrix retains %d database connections", inUse)
	}
}

func (f *stateMatrix) assertClean(t *testing.T) {
	t.Helper()
	f.assertReleased(t)
	var ignore int
	if err := f.db.QueryRowContext(f.ctx, `PRAGMA ignore_check_constraints`).Scan(&ignore); err != nil {
		t.Fatal(err)
	}
	if ignore != f.ignoreCheckConstraints {
		t.Fatalf("matrix check constraints=%d, want %d", ignore, f.ignoreCheckConstraints)
	}
	got, err := Validate(f.ctx, f.db)
	if err != nil || got != f.before {
		t.Fatalf("matrix retained state %+v, %v; want %+v", got, err, f.before)
	}
	for _, snapshot := range f.snapshots {
		if got := stateRows(t, f.ctx, f.db, snapshot.query); !reflect.DeepEqual(got, snapshot.rows) {
			t.Fatalf("matrix retained changes for %q: got %#v, want %#v", snapshot.query, got, snapshot.rows)
		}
	}
	f.assertReleased(t)
}

func stateRows(t *testing.T, ctx context.Context, db *sql.DB, query string) [][]any {
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
	var snapshot [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if blob, ok := value.([]byte); ok {
				values[i] = bytes.Clone(blob)
			}
		}
		snapshot = append(snapshot, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestPreparedStateTransactionsRestoreDataTriggersAndConstraints(t *testing.T) {
	f := preparedState(t)
	if !t.Run("corrupt data and abort writes", func(t *testing.T) {
		tx := f.transaction(t)
		execState(t, tx, `PRAGMA ignore_check_constraints=ON`)
		execState(t, tx, `UPDATE database_state SET generation=-1`)
		execState(t, tx, `CREATE TRIGGER reject_identity BEFORE UPDATE ON database_state BEGIN SELECT RAISE(ABORT, 'identity write refused'); END`)
		if got, err := Read(t.Context(), tx); got != (State{}) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("corrupt state returned %+v, %v", got, err)
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE database_state SET node_high_water=5`); err == nil || !strings.Contains(err.Error(), "identity write refused") {
			t.Fatalf("abort trigger returned %v", err)
		}
	}) {
		return
	}
	t.Run("next transaction retains valid allocation", func(t *testing.T) {
		tx := f.transaction(t)
		if got, err := Validate(t.Context(), tx); err != nil || got != f.before {
			t.Fatalf("next transaction inherited state %+v, %v", got, err)
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE database_state SET generation=-1`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("restored check constraint returned %v", err)
		}
		if id, err := AllocateNodeID(t.Context(), tx); err != nil || id != f.before.NodeHighWater+1 {
			t.Fatalf("next allocation=%d, %v", id, err)
		}
	})
}
