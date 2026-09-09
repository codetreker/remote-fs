package sqlerr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"testing"
	"time"
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
