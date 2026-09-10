package changes

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestTrimHonorsVolumeAgeFloorAndVolume(t *testing.T) {
	for _, test := range []struct {
		name   string
		window Window
		old    int
		want   []int64
		cut    int64
		age    bool
	}{
		{"no trim", Window{2, 10, time.Hour}, 0, []int64{1, 2, 3, 4, 5}, 0, false},
		{"volume", Window{1, 3, time.Hour}, 0, []int64{3, 4, 5}, 2, false},
		{"age floor", Window{2, 10, time.Hour}, 5, []int64{4, 5}, 3, true},
		{"volume tie", Window{2, 2, time.Hour}, 5, []int64{4, 5}, 3, false},
		{"age boundary", Window{1, 10, time.Hour}, 3, []int64{4, 5}, 3, true},
		{"floor keeps all", Window{5, 10, time.Hour}, 5, []int64{1, 2, 3, 4, 5}, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 5)
			foreignChange := fileChange(metastore.Created)
			foreignChange.Parent = 3
			foreignChange.Node.ID = 4
			if err := Record(t.Context(), tx, 2, foreignChange); err != nil {
				t.Fatal(err)
			}
			execLogSQL(t, tx, `UPDATE changes SET recorded_sec=?, recorded_nsec=0 WHERE volume=1 AND position<=?`, time.Now().Add(-48*time.Hour).Unix(), test.old)
			if err := Trim(t.Context(), tx, 1, test.window); err != nil {
				t.Fatal(err)
			}
			rows, err := tx.QueryContext(t.Context(), `SELECT position FROM changes WHERE volume=1 ORDER BY position`)
			if err != nil {
				t.Fatal(err)
			}
			var got []int64
			for rows.Next() {
				var p int64
				if err := rows.Scan(&p); err != nil {
					t.Fatal(err)
				}
				got = append(got, p)
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				t.Fatal(err)
			}
			var cut, tail, foreign int64
			var age bool
			if err := tx.QueryRowContext(t.Context(), `SELECT trimmed_through,trimmed_by_age,committed_position FROM logs WHERE volume=1`).Scan(&cut, &age, &tail); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRowContext(t.Context(), `SELECT count(*) FROM changes WHERE volume=2`).Scan(&foreign); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) || cut != test.cut || age != test.age || tail != 5 || foreign != 1 {
				t.Fatalf("retained=%v cut=%d age=%v tail=%d foreign=%d", got, cut, age, tail, foreign)
			}
		})
	}
}

func TestRetentionQueriesHandleEmptyAndClosedTransactions(t *testing.T) {
	_, tx := logFixture(t)
	if err := Trim(t.Context(), tx, 1, DefaultWindow()); err != nil {
		t.Fatal(err)
	}
	oldest, at, err := oldestEntry(t.Context(), tx, 1)
	if err != nil || oldest != 0 || !at.IsZero() {
		t.Fatalf("empty oldest=%d %v %v", oldest, at, err)
	}
	newest, err := newestEntry(t.Context(), tx, 1)
	if err != nil || newest != 0 {
		t.Fatalf("empty newest=%d %v", newest, err)
	}
	for _, n := range []int64{-1, 0} {
		p, err := nthOldest(t.Context(), tx, 1, n)
		if err != nil || p != 0 {
			t.Fatalf("nth %d=%d %v", n, p, err)
		}
	}
	if _, err := nthOldest(t.Context(), tx, 1, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing nth=%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, _, oldErr := oldestEntry(t.Context(), tx, 1)
	_, newErr := newestEntry(t.Context(), tx, 1)
	_, nthErr := nthOldest(t.Context(), tx, 1, 1)
	_, lastErr := lastBefore(t.Context(), tx, 1, time.Now())
	for _, err := range []error{oldErr, newErr, nthErr, lastErr, Trim(t.Context(), tx, 1, DefaultWindow())} {
		if !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("closed transaction = %v", err)
		}
	}
}

func TestTrimPropagatesDeletionAndAnchorFailures(t *testing.T) {
	for _, trigger := range []string{
		`CREATE TRIGGER fail_trim BEFORE DELETE ON changes BEGIN SELECT RAISE(ABORT,'trim refused'); END`,
		`CREATE TRIGGER fail_anchor BEFORE UPDATE ON logs BEGIN SELECT RAISE(ABORT,'trim refused'); END`,
	} {
		t.Run(trigger, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 3)
			execLogSQL(t, tx, trigger)
			if err := Trim(t.Context(), tx, 1, Window{1, 1, time.Hour}); err == nil || !strings.Contains(err.Error(), "trim refused") {
				t.Fatalf("trim failure = %v", err)
			}
		})
	}
}
