package changes

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func changeResult(t *testing.T, capacity int64) *metastore.ChangeResult {
	t.Helper()
	result, err := metastore.NewChangeResult(capacity, 0, func(int, metastore.Change, metastore.ChangePayloadLengths) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestReadPageReportsEmptyHistoryAndResumesAtExactBounds(t *testing.T) {
	_, tx := logFixture(t)
	result := changeResult(t, 1)
	retention, err := ReadPage(t.Context(), tx, 1, 1, 0, 100, result)
	if err != nil || retention != (metastore.Retention{}) {
		t.Fatalf("empty history=%+v %v", retention, err)
	}
	appendLog(t, tx, 5)
	for _, test := range []struct {
		name        string
		after       metastore.Position
		limit       int
		work, bytes int64
		want        []metastore.Position
	}{
		{"bounded by count", 0, 2, 100, 100, []metastore.Position{1, 2}},
		{"bounded by bytes", 0, 5, 100, 2, []metastore.Position{1, 2}},
		{"bounded by work", 0, 5, 5, 100, []metastore.Position{1, 2}},
		{"resume", 2, 10, 100, 100, []metastore.Position{3, 4, 5}},
		{"caught up", 5, 10, 100, 100, nil},
		{"past tail", 9, 10, 100, 100, nil},
		{"zero count", 0, 0, 3, 0, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := changeResult(t, test.bytes)
			retention, err := ReadPage(t.Context(), tx, 1, test.work, test.after, test.limit, result)
			if err != nil {
				t.Fatal(err)
			}
			got, err := result.Changes()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(test.want) || retention.Oldest != 1 || retention.Tail != 5 || retention.TrimmedThrough != 0 {
				t.Fatalf("page=%+v retention=%+v", got, retention)
			}
			for i, want := range test.want {
				if got[i].Position != want {
					t.Errorf("position[%d]=%d want %d", i, got[i].Position, want)
				}
			}
		})
	}
	for _, test := range []struct{ work, bytes int64 }{{2, 100}, {3, 100}, {100, 0}} {
		if _, err := ReadPage(t.Context(), tx, 1, test.work, 0, 1, changeResult(t, test.bytes)); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("work=%d bytes=%d returned %v", test.work, test.bytes, err)
		}
	}
}

func TestPageAnchorsRejectBrokenRetainedChains(t *testing.T) {
	for _, test := range []struct{ name, setup, want string }{
		{"missing middle", `DELETE FROM changes WHERE position=2`, "follows position"},
		{"missing tail", `DELETE FROM changes WHERE position=3`, "committed tail"},
		{"wrong oldest predecessor", `UPDATE changes SET previous_position=1 WHERE position=1`, "invalid position/predecessor"},
		{"wrong trim anchor", `UPDATE logs SET trimmed_through=1 WHERE volume=1`, "trim anchor"},
		{"untrimmed empty", `DELETE FROM changes`, "empty retained log"},
		{"invalid metadata", `UPDATE changes SET mode=-1 WHERE position=2`, "invalid node metadata"},
		{"invalid payload", `UPDATE changes SET name=x'2e2e' WHERE position=2`, "invalid destination"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 3)
			execLogSQL(t, tx, test.setup)
			result := changeResult(t, 10)
			_, err := ReadPage(t.Context(), tx, 1, 100, 0, 10, result)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("broken history returned %v, want %q", err, test.want)
			}
			result.Fail(err)
			if _, got := result.Changes(); !errors.Is(got, err) {
				t.Fatalf("failed result exposed prefix: %v", got)
			}
		})
	}
	_, tx := logFixture(t)
	appendLog(t, tx, 3)
	execLogSQL(t, tx, `DELETE FROM changes WHERE position<=2`)
	execLogSQL(t, tx, `UPDATE logs SET trimmed_through=2 WHERE volume=1`)
	retention, err := ReadPage(t.Context(), tx, 1, 100, 0, 10, changeResult(t, 10))
	if err != nil || retention.TrimmedThrough != 2 || retention.Oldest != 3 || retention.Tail != 3 {
		t.Fatalf("trimmed page=%+v %v", retention, err)
	}
	execLogSQL(t, tx, `DELETE FROM changes`)
	execLogSQL(t, tx, `UPDATE logs SET trimmed_through=3 WHERE volume=1`)
	retention, err = ReadPage(t.Context(), tx, 1, 2, 0, 10, changeResult(t, 10))
	if err != nil || retention.Oldest != 0 || retention.Tail != 3 || retention.TrimmedThrough != 3 {
		t.Fatalf("fully trimmed page=%+v %v", retention, err)
	}
}

func TestPageReservesBeforeReadingPayloadAndPreservesChargeFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    string
		chargeErr error
		want      error
	}{
		{"charge error", "", syscall.ENOMEM, syscall.ENOMEM},
		{"payload no longer matches reservation", `UPDATE changes SET name=x'6c6f6e676572' WHERE position=1`, nil, syscall.EIO},
		{"payload disappears", `DELETE FROM changes WHERE position=1`, nil, sql.ErrNoRows},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 1)
			charged := false
			result, err := metastore.NewChangeResult(10, 0, func(index int, change metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
				charged = true
				if index != 0 || change.Position != 1 || len(change.Name) != 0 || change.Node.Content != "" || lengths.Name != 4 || lengths.Content != 4 {
					t.Fatalf("charge already holds payload or incorrect metadata: %+v %+v", change, lengths)
				}
				if test.mutate != "" {
					execLogSQL(t, tx, test.mutate)
				}
				return 1, test.chargeErr
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = ReadPage(t.Context(), tx, 1, 100, 0, 1, result)
			if !charged || !errors.Is(err, test.want) {
				t.Fatalf("charged=%v error=%v, want %v", charged, err, test.want)
			}
		})
	}
	_, tx := logFixture(t)
	appendLog(t, tx, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := metastore.NewChangeResult(10, 0, func(int, metastore.Change, metastore.ChangePayloadLengths) (int64, error) { cancel(); return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPage(ctx, tx, 1, 100, 0, 1, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled payload = %v", err)
	}
}

func TestBarrierValidatesIncarnationAndDurablePosition(t *testing.T) {
	_, tx := logFixture(t)
	appendLog(t, tx, 2)
	full, err := ReadLogBarrier(t.Context(), tx, 1, 32, true)
	if err != nil {
		t.Fatal(err)
	}
	position, err := ReadLogBarrier(t.Context(), tx, 1, 0, false)
	if err != nil || position.Position != 2 || position.Incarnation != "" || full.Position != 2 || len(full.Incarnation) != 32 {
		t.Fatalf("barriers=%+v %+v %v", full, position, err)
	}
	if _, err := ReadLogBarrier(t.Context(), tx, 1, 31, true); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("short incarnation bound=%v", err)
	}
	for _, test := range []struct{ name, mutation string }{
		{"bad incarnation length", `incarnation='short'`},
		{"bad incarnation alphabet", `incarnation='zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'`},
		{"blob incarnation", `incarnation=zeroblob(32)`},
		{"negative tail", `committed_position=-1`},
		{"negative trim", `trimmed_through=-1`},
		{"trim beyond tail", `trimmed_through=3`},
		{"nonboolean age", `trimmed_by_age=2`},
		{"text age", `trimmed_by_age='invalid'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 2)
			execLogSQL(t, tx, `UPDATE logs SET `+test.mutation+` WHERE volume=1`)
			if _, err := ReadLogBarrier(t.Context(), tx, 1, 32, true); !errors.Is(err, syscall.EIO) {
				t.Errorf("invalid barrier = %v", err)
			}
			if _, _, _, _, err := logPageState(t.Context(), tx, 1, 0); !errors.Is(err, syscall.EIO) {
				t.Errorf("invalid page anchors = %v", err)
			}
		})
	}
}

func TestLogQueriesPreserveMissingAndClosedTransactionErrors(t *testing.T) {
	_, tx := logFixture(t)
	if _, err := ReadLogBarrier(t.Context(), tx, 9, 32, true); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing barrier=%v", err)
	}
	if _, err := ReadPage(t.Context(), tx, 9, 10, 0, 1, changeResult(t, 10)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing page=%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, barrierErr := ReadLogBarrier(t.Context(), tx, 1, 32, true)
	_, pageErr := ReadPage(t.Context(), tx, 1, 10, 0, 1, changeResult(t, 10))
	for _, err := range []error{barrierErr, pageErr, Record(t.Context(), tx, 1, fileChange(metastore.Created)), CreateLog(t.Context(), tx, 9)} {
		if !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("closed transaction returned %v", err)
		}
	}
}

func TestStoredPositionsRejectInvalidScalarsAndOrdering(t *testing.T) {
	db, tx := logFixture(t)
	for _, value := range []any{"bad", int64(0), int64(-1)} {
		if _, err := storedPosition(tx.QueryRowContext(t.Context(), `SELECT ?,typeof(?)`, value, value)); !errors.Is(err, syscall.EIO) {
			t.Errorf("invalid position %v = %v", value, err)
		}
	}
	for _, pair := range [][2]int64{{0, 0}, {2, -1}, {2, 2}, {2, 3}} {
		if _, _, _, err := storedPositionPair(tx.QueryRowContext(t.Context(), `SELECT ?,typeof(?),?,typeof(?)`, pair[0], pair[0], pair[1], pair[1])); !errors.Is(err, syscall.EIO) {
			t.Errorf("invalid pair %v = %v", pair, err)
		}
	}
	if _, _, found, err := storedPositionPair(tx.QueryRowContext(t.Context(), `SELECT 1,'integer',0,'integer' WHERE 0`)); err != nil || found {
		t.Fatalf("empty pair found=%v error=%v", found, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storedPosition(db.QueryRow(`SELECT 1,'integer'`)); err == nil {
		t.Fatal("closed database returned position")
	}
	if _, _, _, err := storedPositionPair(db.QueryRow(`SELECT 1,'integer',0,'integer'`)); err == nil {
		t.Fatal("closed database returned position pair")
	}
}
