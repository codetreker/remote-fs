package sqlerr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"modernc.org/sqlite"
)

type sqliteCodeError int

func (e sqliteCodeError) Error() string { return fmt.Sprintf("SQLite code %d", e) }

func (e sqliteCodeError) Code() int { return int(e) }

func TestFailurePreservesCancellationAndIndependentFailures(t *testing.T) {
	fault := errors.New("database unavailable")
	for _, test := range []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"cancellation", context.Canceled, syscall.EINTR},
		{"wrapped cancellation", fmt.Errorf("read: %w", context.Canceled), syscall.EINTR},
		{"joined cancellation", errors.Join(context.Canceled, syscall.EINTR), syscall.EINTR},
		{"deadline", context.DeadlineExceeded, syscall.EIO},
		{"unknown", fault, syscall.EIO},
		{"unknown write interruption", sqliteCodeError(9), syscall.EIO},
		{"cancellation before fault", errors.Join(context.Canceled, fault), syscall.EIO},
		{"fault before cancellation", errors.Join(fault, context.Canceled), syscall.EIO},
		{"cancellation before errno", errors.Join(context.Canceled, syscall.ENOENT), syscall.ENOENT},
		{"errno before cancellation", errors.Join(syscall.ENOENT, context.Canceled), syscall.ENOENT},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := Failure(test.err)
			if got := storage.ErrnoOf(err); got != test.want || !errors.Is(err, test.err) {
				t.Fatalf("Failure(%v) = %v (%v), want %v retaining its cause", test.err, err, got, test.want)
			}
			if (test.want == syscall.EIO) != errors.Is(err, syscall.EIO) {
				t.Fatalf("Failure(%v) has incorrect EIO decoration: %v", test.err, err)
			}
		})
	}
}

func TestInterruptedReadRequiresCancellationAndPreservesFaults(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	deadline, finish := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer finish()
	fault := errors.New("database corrupt")
	interrupt := sqliteCodeError(9)
	for _, test := range []struct {
		name string
		ctx  context.Context
		err  error
		want syscall.Errno
	}{
		{"active context", t.Context(), interrupt, syscall.EIO},
		{"completed transaction with active context", t.Context(), sql.ErrTxDone, syscall.EIO},
		{"automatically rolled back transaction", ctx, sql.ErrTxDone, syscall.EINTR},
		{"wrapped transaction completion", ctx, fmt.Errorf("query: %w", sql.ErrTxDone), syscall.EIO},
		{"canceled read", ctx, interrupt, syscall.EINTR},
		{"wrapped interrupted read", ctx, fmt.Errorf("query: %w", interrupt), syscall.EINTR},
		{"driver before cancellation", ctx, errors.Join(interrupt, context.Canceled), syscall.EINTR},
		{"cancellation before driver", ctx, errors.Join(context.Canceled, interrupt), syscall.EINTR},
		{"expired read", deadline, interrupt, syscall.EIO},
		{"another SQLite error", ctx, sqliteCodeError(11), syscall.EIO},
		{"canceled context with fault", ctx, fault, syscall.EIO},
		{"driver before fault", ctx, errors.Join(interrupt, fault), syscall.EIO},
		{"fault before driver", ctx, errors.Join(fault, interrupt), syscall.EIO},
		{"uncertain commit", ctx, &UncertainCommitError{err: interrupt}, syscall.EIO},
		{"durability failure", ctx, &DurabilityFailure{err: interrupt}, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ReadFailure(test.ctx, test.err)
			if got := storage.ErrnoOf(err); got != test.want || !errors.Is(err, test.err) {
				t.Fatalf("ReadFailure(%v) = %v (%v), want %v retaining its cause", test.err, err, got, test.want)
			}
			if test.want == syscall.EINTR && (!errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO)) {
				t.Fatalf("interrupted read lost cancellation or gained EIO: %v", err)
			}
		})
	}
}

func TestUniqueViolationClassifiesRealSQLiteConstraintErrors(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/constraints.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE items (
		id INTEGER PRIMARY KEY, name TEXT UNIQUE, size INTEGER CHECK(size >= 0));
		INSERT INTO items VALUES (1, 'original', 0)`); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		sql    string
		code   int
		unique bool
	}{
		{"primary key", `INSERT INTO items VALUES (1, 'another', 0)`, 1555, true},
		{"unique value", `INSERT INTO items VALUES (2, 'original', 0)`, 2067, true},
		{"check constraint", `INSERT INTO items VALUES (2, 'another', -1)`, 275, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, cause := db.ExecContext(t.Context(), test.sql)
			var driverErr *sqlite.Error
			if !errors.As(cause, &driverErr) || driverErr.Code() != test.code {
				t.Fatalf("statement returned %v, want SQLite code %d", cause, test.code)
			}
			for _, err := range []error{cause, fmt.Errorf("mutation: %w", cause), errors.Join(context.Canceled, cause)} {
				if got := IsUniqueViolation(err); got != test.unique {
					t.Fatalf("IsUniqueViolation(%v) = %v, want %v", err, got, test.unique)
				}
			}
		})
	}
	for _, err := range []error{nil, errors.New("not a driver error"), sqliteCodeError(1555)} {
		if IsUniqueViolation(err) {
			t.Fatalf("a non-SQLite error is a uniqueness violation: %v", err)
		}
	}
}

func TestReadCancellationRetainsDriverCauseAndContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cause := sqliteCodeError(9)
	err := ReadFailure(ctx, cause)
	if err.Error() != cause.Error() || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation lost a cause: %v", err)
	}
	var canceled *readCancellationError
	if !errors.As(err, &canceled) || canceled.Classification() != context.Canceled {
		t.Fatalf("read interruption lacks its cancellation classification: %v", err)
	}
	if storage.ErrnoOf(err) != syscall.EINTR {
		t.Fatalf("read interruption returned %v, want EINTR", storage.ErrnoOf(err))
	}
	if err := ReadFailure(ctx, nil); err != nil {
		t.Fatalf("a successful read became a failure after cancellation: %v", err)
	}
	if err := Failure(nil); err != nil {
		t.Fatalf("a nil failure became %v", err)
	}
}

type emptyErrorGroup struct{}

func (emptyErrorGroup) Error() string   { return "an error group without causes" }
func (emptyErrorGroup) Unwrap() []error { return nil }

func TestEmptyErrorGroupDoesNotProveReadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cause := emptyErrorGroup{}
	err := ReadFailure(ctx, cause)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) || errors.Is(err, context.Canceled) {
		t.Fatalf("empty error group was reclassified as cancellation: %v", err)
	}
}
