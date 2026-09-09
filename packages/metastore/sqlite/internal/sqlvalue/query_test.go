package sqlvalue

import (
	"database/sql"
	"errors"
	"strings"
	"syscall"
	"testing"

	_ "modernc.org/sqlite"
)

func TestExactlyOneRejectsMissingAndMultipleRows(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/rows.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER);
		INSERT INTO items VALUES (1, 0), (2, 0)`); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		where string
		valid bool
	}{
		{"missing row", "WHERE id = 3", false},
		{"one row", "WHERE id = 1", true},
		{"multiple rows", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := db.ExecContext(t.Context(), "UPDATE items SET value = value + 1 "+test.where)
			if err != nil {
				t.Fatal(err)
			}
			err = ExactlyOne(result, "the requested item")
			if test.valid {
				if err != nil {
					t.Fatalf("one affected row returned %v", err)
				}
			} else if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "the requested item") {
				t.Fatalf("invalid row count returned %v, want EIO naming the requested item", err)
			}
		})
	}
}

type rowsAffectedFailure struct {
	sql.Result
	cause error
}

func (r rowsAffectedFailure) RowsAffected() (int64, error) { return 0, r.cause }

func TestExactlyOnePreservesRowsAffectedFailure(t *testing.T) {
	cause := errors.New("row count unavailable")
	if got := ExactlyOne(rowsAffectedFailure{cause: cause}, "the requested item"); got != cause {
		t.Fatalf("row count failure returned %v, want the original %v", got, cause)
	}
}
