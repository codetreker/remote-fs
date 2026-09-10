package changes

import (
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

func logFixture(t *testing.T) (*sql.DB, *sql.Tx) {
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
	setup := tx
	t.Cleanup(func() {
		if err := setup.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	if err := sqliteschema.MustLoad(os.DirFS("../schema"), "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	execLogSQL(t, tx, `INSERT INTO volumes(id,name,root,used) VALUES(1,'one',1,0),(2,'two',3,0)`)
	execLogSQL(t, tx, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec)
		VALUES(1,1,?,0,0,0,0,0),(2,1,420,0,0,0,0,0),(3,2,?,0,0,0,0,0),(4,2,420,0,0,0,0,0)`,
		int64(fs.ModeDir|0755), int64(fs.ModeDir|0755))
	execLogSQL(t, tx, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,x'66696c65',2),(2,3,x'66696c65',4)`)
	execLogSQL(t, tx, `UPDATE database_state SET node_high_water=4`)
	for _, volume := range []int64{1, 2} {
		if err := CreateLog(t.Context(), tx, volume); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(t.Context(), nil)
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

func execLogSQL(t *testing.T, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("SQL %q: %v", query, err)
	}
}

func fileChange(kind metastore.ChangeKind) metastore.Change {
	change := metastore.Change{Kind: kind, Parent: 1, Name: []byte("file")}
	if kind != metastore.Removed {
		change.Node = &metastore.Node{ID: 2, Mode: 0644, Size: 4, Content: "body",
			AccessTime: time.Unix(-100, 123).UTC(), ModTime: time.Unix(100, 456).UTC()}
	}
	if kind == metastore.Renamed {
		change.From = &metastore.Location{Parent: 1, Name: []byte("old")}
	}
	return change
}

func appendLog(t *testing.T, tx *sql.Tx, count int) {
	t.Helper()
	for range count {
		if err := Record(t.Context(), tx, 1, fileChange(metastore.Created)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecordedKindsKeepTheirStorageValues(t *testing.T) {
	for _, test := range []struct {
		kind   metastore.ChangeKind
		stored int64
	}{{metastore.Created, 0}, {metastore.Removed, 1}, {metastore.Modified, 2}, {metastore.Renamed, 3}} {
		stored, err := storedKind(test.kind)
		if err != nil || stored != test.stored {
			t.Errorf("encode %v = %d, %v", test.kind, stored, err)
		}
		kind, err := loadedKind(test.stored)
		if err != nil || kind != test.kind {
			t.Errorf("decode %d = %v, %v", test.stored, kind, err)
		}
	}
	if _, err := storedKind(metastore.ChangeKind(99)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown change kind: %v", err)
	}
	if _, err := loadedKind(99); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown stored kind: %v", err)
	}
}

func TestRecordStoresChangesAndTailInTheCallerTransaction(t *testing.T) {
	db, tx := logFixture(t)
	for i, kind := range []metastore.ChangeKind{metastore.Created, metastore.Modified, metastore.Renamed, metastore.Removed} {
		want := fileChange(kind)
		if err := Record(t.Context(), tx, 1, want); err != nil {
			t.Fatal(err)
		}
		page := changeResult(t, 100)
		retention, err := ReadPage(t.Context(), tx, 1, 100, metastore.Position(i), 1, page)
		if err != nil {
			t.Fatal(err)
		}
		got, err := page.Changes()
		want.Position = metastore.Position(i + 1)
		if err != nil || !reflect.DeepEqual(got, []metastore.Change{want}) || retention.Tail != want.Position {
			t.Fatalf("record round trip = %+v, %+v, %v; want %+v", got, retention, err, want)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count, tail, highWater int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM changes),
		(SELECT committed_position FROM logs WHERE volume=1), change_high_water FROM database_state`).Scan(&count, &tail, &highWater); err != nil {
		t.Fatal(err)
	}
	if count != 0 || tail != 0 || highWater != 0 {
		t.Fatalf("rolled-back record escaped: count=%d tail=%d high-water=%d", count, tail, highWater)
	}
}

func TestRecordRefusesInvalidTailAndPreservesSQLFailures(t *testing.T) {
	for _, test := range []struct{ name, setup, want string }{
		{"missing log", `DELETE FROM logs WHERE volume=1`, "no rows"},
		{"text tail", `UPDATE logs SET committed_position='bad' WHERE volume=1`, "invalid committed position"},
		{"negative tail", `UPDATE logs SET committed_position=-1 WHERE volume=1`, "invalid committed position"},
		{"future tail", `UPDATE logs SET committed_position=9 WHERE volume=1`, "does not precede"},
		{"missing state", `DELETE FROM database_state`, "durable-state rows"},
		{"insert failure", `CREATE TRIGGER fail_insert BEFORE INSERT ON changes BEGIN SELECT RAISE(ABORT,'record insert failed'); END`, "record insert failed"},
		{"tail failure", `CREATE TRIGGER fail_tail BEFORE UPDATE ON logs BEGIN SELECT RAISE(ABORT,'record tail failed'); END`, "record tail failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, tx := logFixture(t)
			execLogSQL(t, tx, test.setup)
			if err := Record(t.Context(), tx, 1, fileChange(metastore.Created)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Record error = %v, want %q", err, test.want)
			}
		})
	}
	_, tx := logFixture(t)
	if err := Record(t.Context(), tx, 1, fileChange(metastore.ChangeKind(99))); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown kind = %v", err)
	}
}

func TestCreateLogAssignsDistinctEmptyHistories(t *testing.T) {
	_, tx := logFixture(t)
	first, err := ReadLogBarrier(t.Context(), tx, 1, 32, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ReadLogBarrier(t.Context(), tx, 2, 32, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Position != 0 || second.Position != 0 || first.Incarnation == second.Incarnation ||
		len(first.Incarnation) != 32 || strings.Trim(string(first.Incarnation), "0123456789abcdef") != "" {
		t.Fatalf("empty histories = %+v, %+v", first, second)
	}
	if err := CreateLog(t.Context(), tx, 1); err == nil {
		t.Fatal("duplicate log creation succeeded")
	}
}
