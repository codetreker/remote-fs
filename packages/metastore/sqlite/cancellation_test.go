package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type sqliteCodeError int

func (e sqliteCodeError) Error() string { return fmt.Sprintf("SQLite code %d", e) }
func (e sqliteCodeError) Code() int     { return int(e) }

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
			err := failure(test.err)
			if got := storage.ErrnoOf(err); got != test.want || !errors.Is(err, test.err) {
				t.Fatalf("failure(%v) = %v (%v), want %v retaining its cause", test.err, err, got, test.want)
			}
			if (test.want == syscall.EIO) != errors.Is(err, syscall.EIO) {
				t.Fatalf("failure(%v) has incorrect EIO decoration: %v", test.err, err)
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
		{"uncertain commit", ctx, &uncertainCommitError{err: interrupt}, syscall.EIO},
		{"durability failure", ctx, &durabilityFailure{err: interrupt}, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := readFailure(test.ctx, test.err)
			if got := storage.ErrnoOf(err); got != test.want || !errors.Is(err, test.err) {
				t.Fatalf("readFailure(%v) = %v (%v), want %v retaining its cause", test.err, err, got, test.want)
			}
			if test.want == syscall.EINTR && (!errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO)) {
				t.Fatalf("interrupted read lost cancellation or gained EIO: %v", err)
			}
		})
	}
}

func TestReadTransactionDistinguishesAutomaticRollbackFromCleanupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	deadline, finish := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer finish()
	cleanup := errors.New("rollback disk failure")
	for _, test := range []struct {
		name     string
		ctx      context.Context
		primary  error
		rollback error
		want     syscall.Errno
	}{
		{"canceled work already rolled back", ctx, context.Canceled, sql.ErrTxDone, syscall.EINTR},
		{"query observes automatic rollback", ctx, sql.ErrTxDone, sql.ErrTxDone, syscall.EINTR},
		{"query completed before successful cleanup", ctx, sql.ErrTxDone, nil, syscall.EINTR},
		{"query cancellation with cleanup fault", ctx, sql.ErrTxDone, cleanup, syscall.EIO},
		{"expired query observes automatic rollback", deadline, sql.ErrTxDone, sql.ErrTxDone, syscall.EIO},
		{"healthy query observes completed transaction", t.Context(), sql.ErrTxDone, sql.ErrTxDone, syscall.EIO},
		{"wrapped query completion remains a fault", ctx, fmt.Errorf("query: %w", sql.ErrTxDone), sql.ErrTxDone, syscall.EIO},
		{"query completion before independent fault", ctx, errors.Join(sql.ErrTxDone, cleanup), sql.ErrTxDone, syscall.EIO},
		{"independent fault before query completion", ctx, errors.Join(cleanup, sql.ErrTxDone), sql.ErrTxDone, syscall.EIO},
		{"successful work then automatic rollback", ctx, nil, sql.ErrTxDone, syscall.EINTR},
		{"deadline automatic rollback", deadline, nil, sql.ErrTxDone, syscall.EIO},
		{"healthy already completed transaction", t.Context(), nil, sql.ErrTxDone, syscall.EIO},
		{"wrapped completion is a cleanup fault", ctx, context.Canceled, fmt.Errorf("rollback: %w", sql.ErrTxDone), syscall.EIO},
		{"cancellation with cleanup fault", ctx, context.Canceled, cleanup, syscall.EIO},
		{"rollback cancellation is a cleanup fault", ctx, context.Canceled, context.Canceled, syscall.EIO},
		{"rollback errno is a cleanup fault", ctx, context.Canceled, syscall.ENOSPC, syscall.EIO},
		{"primary fault with automatic rollback", ctx, cleanup, sql.ErrTxDone, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &rollbackFailure{err: test.rollback}
			err := finishReadTransaction(test.ctx, "reader transaction", tx, test.primary)
			if got := storage.ErrnoOf(err); got != test.want || tx.calls != 1 {
				t.Fatalf("finishing read = %v (%v) after %d rollbacks, want %v", err, got, tx.calls, test.want)
			}
			if test.primary != nil && !errors.Is(err, test.primary) {
				t.Fatalf("read lost primary error %v: %v", test.primary, err)
			}
			if test.rollback != nil && test.rollback != sql.ErrTxDone && !errors.Is(err, test.rollback) {
				t.Fatalf("read lost rollback error %v: %v", test.rollback, err)
			}
			if test.primary == sql.ErrTxDone && test.ctx.Err() != nil && !errors.Is(err, test.ctx.Err()) {
				t.Fatalf("completed read lost its owning context's cancellation: %v", err)
			}
			if test.want == syscall.EINTR && (!errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO)) {
				t.Fatalf("automatic rollback changed cancellation into a fault: %v", err)
			}
		})
	}
}

func TestLabeledReadCancellationSurvivesTransactionCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	primary := fmt.Errorf("validating namespace integrity: %w", readFailure(ctx, sql.ErrTxDone))
	tx := &rollbackFailure{err: sql.ErrTxDone}
	err := finishReadTransaction(ctx, "object status transaction", tx, primary)
	if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, sql.ErrTxDone) ||
		!errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO) {
		t.Fatalf("labeled query cancellation = %v, want interruption retaining query and context causes", err)
	}
}

func TestReadContextAutomaticRollbackPreservesTheOperationResult(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	for _, successful := range []bool{false, true} {
		t.Run(fmt.Sprintf("successful callback %t", successful), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			err := store.inspect(ctx, func(tx *sql.Tx) error {
				var one int
				if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
					return err
				}
				cancel()
				waitForReadRollback(t, store.read)
				if successful {
					return nil
				}
				return ctx.Err()
			})
			if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO) {
				t.Fatalf("read after database/sql automatic rollback = %v, want cancellation without EIO", err)
			}
			if _, err := store.Stat(t.Context(), "."); err != nil {
				t.Fatalf("canceled read poisoned subsequent reads: %v", err)
			}
		})
	}
}

func TestCanceledReadOperationsReturnInterruption(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"stat", func() error { _, err := store.Stat(ctx, "."); return err }},
		{"list", func() error { _, err := store.List(ctx, "."); return err }},
		{"space", func() error { _, err := store.Space(ctx); return err }},
		{"object status", func() error { _, err := store.ObjectStatus(ctx); return err }},
		{"snapshot", func() error { _, _, err := store.Snapshot(ctx); return err }},
		{"position", func() error { _, err := store.CommittedPosition(ctx); return err }},
		{"durable state", func() error { _, err := store.DurableState(ctx); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.run()
			if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO) {
				t.Fatalf("canceled %s = %v, want interruption without EIO", operation.name, err)
			}
		})
	}
}

func TestSnapshotAfterAutomaticRollbackReturnsCancellation(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snap, _, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitForReadRollback(t, store.snapshotRead)
	result, err := metastore.NewRowResult(1024, 0,
		func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
			return 192 + lengths.Name + lengths.Content, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snap.Next(t.Context(), 1, result); storage.ErrnoOf(err) != syscall.EINTR ||
		!errors.Is(err, context.Canceled) || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, syscall.EIO) {
		t.Fatalf("reading automatically rolled back snapshot with a new context = %v, want interruption", err)
	}
	if rows, err := result.Rows(); rows != nil || storage.ErrnoOf(err) != syscall.EINTR {
		t.Fatalf("canceled snapshot exposed rows %v with error %v", rows, err)
	}
	if err := snap.Close(); storage.ErrnoOf(err) != syscall.EINTR || errors.Is(err, syscall.EIO) {
		t.Fatalf("closing automatically rolled back snapshot = %v, want interruption", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("closing snapshot twice: %v", err)
	}
}

func TestSnapshotCancellationDoesNotHideIndependentFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	picture := &snapshot{ctx: ctx}
	fault := errors.New("query failed")
	for _, cause := range []error{
		errors.Join(sql.ErrTxDone, fault),
		errors.Join(fault, sql.ErrTxDone),
		fmt.Errorf("query: %w", sql.ErrTxDone),
	} {
		err := picture.readFailure(t.Context(), cause)
		if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
			t.Fatalf("snapshot failure %v = %v, want EIO retaining original failure", cause, err)
		}
	}
	activePicture := &snapshot{ctx: t.Context()}
	err := activePicture.readFailure(ctx, sql.ErrTxDone)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
		t.Fatalf("canceled page hid an unexpectedly completed live transaction: %v", err)
	}
}

func waitForReadRollback(t *testing.T, pool *sql.DB) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for pool.Stats().InUse != 0 {
		select {
		case <-deadline.C:
			t.Fatal("database/sql did not release the canceled transaction's connection")
		default:
			runtime.Gosched()
		}
	}
}
