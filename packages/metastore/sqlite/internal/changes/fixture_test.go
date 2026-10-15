package changes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

func prepareLogFixture(t *testing.T, count int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/log.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	if err := sqliteschema.MustLoad(os.DirFS("../schema"), "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	execLogSQL(t, tx, `INSERT INTO volumes(id,name,root,used) VALUES(1,'one',1,0),(2,'two',3,0)`)
	execLogSQL(t, tx, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,directory_revision)
		VALUES(1,1,2,0,0,0,0,0,X'0000000000000001'),(2,1,1,0,0,0,0,0,X''),(3,2,2,0,0,0,0,0,X'0000000000000001'),(4,2,1,0,0,0,0,0,X'')`)
	execLogSQL(t, tx, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,x'66696c65',2),(2,3,x'66696c65',4)`)
	execLogSQL(t, tx, `UPDATE database_state SET node_high_water=4`)
	for _, volume := range []int64{1, 2} {
		if err := CreateLog(t.Context(), tx, volume); err != nil {
			t.Fatal(err)
		}
	}
	appendLog(t, tx, count)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func logFixture(t *testing.T) (*sql.DB, *sql.Tx) {
	t.Helper()
	db := prepareLogFixture(t, 0)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	return db, tx
}

var logFixturePragmas = [...]string{"query_only", "foreign_keys", "ignore_check_constraints", "recursive_triggers"}

type logMatrixFixture struct {
	db       *sql.DB
	baseline map[string][]string
	pragmas  [len(logFixturePragmas)]int
}

func newLogMatrixFixture(t *testing.T, count int) *logMatrixFixture {
	t.Helper()
	db := prepareLogFixture(t, count)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	pragmas, err := readLogFixturePragmas(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	return &logMatrixFixture{db: db, baseline: logFixtureSnapshot(t, db), pragmas: pragmas}
}

func (fixture *logMatrixFixture) transaction(t *testing.T) *sql.Tx {
	t.Helper()
	if inUse := fixture.db.Stats().InUse; inUse != 0 {
		t.Fatalf("log matrix retained %d connections before transaction", inUse)
	}
	conn, err := fixture.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup owns rollback; testing cancels the child context before running it.
	ctx := context.WithoutCancel(t.Context())
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(errors.Join(err, conn.Close()))
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("rollback log matrix transaction: %v", err)
		}
		for i, name := range logFixturePragmas {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA %s=%d", name, fixture.pragmas[i])); err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
		}
		pragmas, err := readLogFixturePragmas(ctx, conn)
		if err != nil {
			t.Errorf("read restored log matrix PRAGMAs: %v", err)
		} else if pragmas != fixture.pragmas {
			t.Errorf("restored log matrix PRAGMAs = %v, want %v", pragmas, fixture.pragmas)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("release log matrix connection: %v", err)
		}
		if inUse := fixture.db.Stats().InUse; inUse != 0 {
			t.Errorf("log matrix retained %d connections after transaction", inUse)
			return
		}
		if got := logFixtureSnapshot(t, fixture.db); !reflect.DeepEqual(got, fixture.baseline) {
			t.Errorf("log matrix transaction changed committed schema or rows: got %v, want %v", got, fixture.baseline)
		}
	})
	return tx
}

func readLogFixturePragmas(ctx context.Context, queryer sqlvalue.Queryer) ([len(logFixturePragmas)]int, error) {
	var values [len(logFixturePragmas)]int
	for i, name := range logFixturePragmas {
		if err := queryer.QueryRowContext(ctx, "PRAGMA "+name).Scan(&values[i]); err != nil {
			return values, fmt.Errorf("read %s: %w", name, err)
		}
	}
	return values, nil
}

func logFixtureSnapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	snapshot := map[string][]string{
		"sqlite_schema": logFixtureRows(t, db, `SELECT type,name,tbl_name,sql FROM sqlite_schema`),
	}
	rows, err := db.QueryContext(context.WithoutCancel(t.Context()), `SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
	for _, name := range tables {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		snapshot[name] = logFixtureRows(t, db, "SELECT * FROM "+quoted)
	}
	return snapshot
}

func logFixtureRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.WithoutCancel(t.Context()), query)
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
	var result []string
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		var encoded strings.Builder
		for _, value := range values {
			fmt.Fprintf(&encoded, "%T:%#v;", value, value)
		}
		result = append(result, encoded.String())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(result)
	return result
}

func TestLogMatrixTransactionsRestoreDatabaseAndConnectionState(t *testing.T) {
	fixture := newLogMatrixFixture(t, 0)
	if !t.Run("damaged transaction", func(t *testing.T) {
		tx := fixture.transaction(t)
		appendLog(t, tx, 2)
		execLogSQL(t, tx, `DELETE FROM entries WHERE volume=1`)
		execLogSQL(t, tx, `UPDATE nodes SET size=8 WHERE id=2`)
		execLogSQL(t, tx, `UPDATE logs SET trimmed_through=1 WHERE volume=1`)
		execLogSQL(t, tx, `DROP TRIGGER changes_allocation_insert`)
		execLogSQL(t, tx, `CREATE TRIGGER fail_insert BEFORE INSERT ON changes BEGIN SELECT RAISE(ABORT,'matrix insert refused'); END`)
		if err := Record(t.Context(), tx, 1, fileChange(metastore.Created)); err == nil || !strings.Contains(err.Error(), "matrix insert refused") {
			t.Fatalf("temporary trigger Record error = %v", err)
		}
		execLogSQL(t, tx, `PRAGMA ignore_check_constraints=ON`)
		execLogSQL(t, tx, `PRAGMA recursive_triggers=ON`)
		execLogSQL(t, tx, `UPDATE database_state SET generation=-1,node_high_water=44`)
		execLogSQL(t, tx, `UPDATE volumes SET used=-8 WHERE id=1`)
		execLogSQL(t, tx, `PRAGMA query_only=ON`)
		pragmas, err := readLogFixturePragmas(t.Context(), tx)
		want := fixture.pragmas
		want[0], want[2], want[3] = 1, 1, 1
		if err != nil || pragmas != want {
			t.Fatalf("damaged connection PRAGMAs = %v, %v; want %v", pragmas, err, want)
		}
	}) {
		return
	}
	t.Run("healthy successor", func(t *testing.T) {
		tx := fixture.transaction(t)
		pragmas, err := readLogFixturePragmas(t.Context(), tx)
		if err != nil || pragmas != fixture.pragmas {
			t.Fatalf("successor PRAGMAs = %v, %v; want %v", pragmas, err, fixture.pragmas)
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE database_state SET generation=-1`); err == nil {
			t.Fatal("successor transaction accepted invalid generation")
		}
		page := changeResult(t, 10)
		retention, err := ReadPage(t.Context(), tx, 1, 100, 0, 10, page)
		if err != nil || retention != (metastore.Retention{}) {
			t.Fatalf("successor retained damaged history = %+v, %v", retention, err)
		}
		if err := Record(t.Context(), tx, 1, fileChange(metastore.Created)); err != nil {
			t.Fatalf("successor Record: %v", err)
		}
		page = changeResult(t, 10)
		retention, err = ReadPage(t.Context(), tx, 1, 100, 0, 10, page)
		if err != nil {
			t.Fatal(err)
		}
		changes, err := page.Changes()
		if err != nil || retention.Tail != 1 || len(changes) != 1 || changes[0].Position != 1 || changes[0].Node == nil || changes[0].Node.AllocationSize != 4096 {
			t.Fatalf("successor history = %+v, %+v, %v", changes, retention, err)
		}
		var changeHighWater, nodeHighWater, generation, sequence int64
		if err := tx.QueryRowContext(t.Context(), `SELECT change_high_water,node_high_water,generation,
			(SELECT seq FROM sqlite_sequence WHERE name='changes') FROM database_state`).Scan(&changeHighWater, &nodeHighWater, &generation, &sequence); err != nil {
			t.Fatal(err)
		}
		if changeHighWater != 1 || nodeHighWater != 4 || generation != 0 || sequence != 1 {
			t.Fatalf("successor allocation = change %d, node %d, generation %d, sequence %d", changeHighWater, nodeHighWater, generation, sequence)
		}
	})
}
