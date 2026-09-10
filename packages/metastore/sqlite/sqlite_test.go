package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
	_ "modernc.org/sqlite"
)

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
	primary := fmt.Errorf("validating volume integrity: %w", sqlerr.ReadFailure(ctx, sql.ErrTxDone))
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

func TestEveryWriterConnectionUsesDurablePragmas(t *testing.T) {
	db, err := openPool(t.Context(), t.TempDir()+"/metastore.db", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// No idle connection is retained, so each query below is made through a newly opened
	// physical connection and exercises the writer DSN again.
	db.SetMaxIdleConns(0)
	for attempt := range 3 {
		var effective int
		if err := db.QueryRowContext(t.Context(), `PRAGMA synchronous`).Scan(&effective); err != nil {
			t.Fatalf("reading connection %d's synchronous setting: %v", attempt+1, err)
		}
		if effective != 2 {
			t.Fatalf("writer connection %d uses synchronous level %d, want FULL (2)", attempt+1, effective)
		}
		var autocheckpoint int
		if err := db.QueryRowContext(t.Context(), `PRAGMA wal_autocheckpoint`).Scan(&autocheckpoint); err != nil {
			t.Fatalf("reading connection %d's WAL autocheckpoint setting: %v", attempt+1, err)
		}
		if autocheckpoint != 0 {
			t.Fatalf("writer connection %d checkpoints automatically after %d pages, want disabled",
				attempt+1, autocheckpoint)
		}
	}
}

func TestReaderPoolHonorsItsConnectionBoundAndCancellation(t *testing.T) {
	const limit = 2
	store, err := OpenWithOptions(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, Options{
			Window:               DefaultWindow(),
			MaxReaderConnections: limit,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first, err := store.read.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	second, err := store.read.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()

	waitContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		tx, err := store.read.BeginTx(waitContext, &sql.TxOptions{ReadOnly: true})
		if tx != nil {
			err = errors.Join(err, tx.Rollback())
		}
		result <- err
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for store.read.Stats().WaitCount == 0 {
		select {
		case <-deadline.C:
			t.Fatal("a third read transaction did not wait for the bounded reader pool")
		default:
			runtime.Gosched()
		}
	}
	stats := store.read.Stats()
	if stats.MaxOpenConnections != limit || stats.OpenConnections > limit || stats.InUse != limit {
		t.Fatalf("the saturated reader pool reports %+v, want at most %d open and %d in use", stats, limit, limit)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling a snapshot waiting for a reader connection returned %v, want context.Canceled", err)
	}
	if err := first.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, err := store.read.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("opening a read transaction after releasing a reader connection: %v", err)
	}
	if err := after.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestAWriterSettingOtherThanFullIsRefused(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/metastore.db?_pragma=synchronous(OFF)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = requireFullSynchronous(t.Context(), db)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("verifying a writer with synchronous disabled: %v, want EIO", err)
	}
}

func TestWriterCloseFailureIsJoinedWhenReaderPoolOpenFails(t *testing.T) {
	primary := errors.New("reader pool refused")
	closeFailure := errors.New("writer close failed")
	writer, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	closeCalls := 0
	store, err := openWithHooks(
		t.Context(), "unused", "workspace", "", 0, Options{Window: DefaultWindow()},
		storeOpenHooks{
			openPool: func(_ context.Context, _ string, isWriter bool, _ int) (*sql.DB, error) {
				if isWriter {
					return writer, nil
				}
				return nil, primary
			},
			prepare: func(context.Context, *sql.DB, string, string, Window, int64, int64) (int64, int64, error) {
				t.Fatal("prepare ran after reader pool open failed")
				return 0, 0, nil
			},
			closePool: func(db *sql.DB) error {
				closeCalls++
				return errors.Join(db.Close(), closeFailure)
			},
		},
	)
	if store != nil {
		store.Close()
		t.Fatal("reader pool failure returned a Store")
	}
	if !errors.Is(err, primary) || !errors.Is(err, closeFailure) {
		t.Fatalf("reader pool failure returned %v, want primary and writer close failures", err)
	}
	if closeCalls != 1 || !strings.Contains(err.Error(), "writer pool") {
		t.Fatalf("reader pool failure closed %d pools and returned %v", closeCalls, err)
	}
}

func TestBothPoolCloseFailuresAreJoinedWhenPrepareFails(t *testing.T) {
	primary := errors.New("prepare failed")
	readerClose := errors.New("reader close failed")
	snapshotClose := errors.New("snapshot reader close failed")
	writerClose := errors.New("writer close failed")
	writer, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	snapshot, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		writer.Close()
		reader.Close()
		t.Fatal(err)
	}
	readPools := 0
	store, err := openWithHooks(
		t.Context(), "database", "workspace", "", 0, Options{Window: DefaultWindow()},
		storeOpenHooks{
			openPool: func(_ context.Context, _ string, isWriter bool, _ int) (*sql.DB, error) {
				if isWriter {
					return writer, nil
				}
				readPools++
				if readPools == 1 {
					return reader, nil
				}
				return snapshot, nil
			},
			prepare: func(context.Context, *sql.DB, string, string, Window, int64, int64) (int64, int64, error) {
				return 0, 0, primary
			},
			closePool: func(db *sql.DB) error {
				actual := db.Close()
				if db == reader {
					return errors.Join(actual, readerClose)
				}
				if db == snapshot {
					return errors.Join(actual, snapshotClose)
				}
				return errors.Join(actual, writerClose)
			},
		},
	)
	if store != nil {
		store.Close()
		t.Fatal("prepare failure returned a Store")
	}
	for _, want := range []error{primary, readerClose, snapshotClose, writerClose} {
		if !errors.Is(err, want) {
			t.Fatalf("prepare failure returned %v, missing %v", err, want)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("prepare failure returned %v, want EIO classification", err)
	}
	if !strings.Contains(err.Error(), "reader pool") ||
		!strings.Contains(err.Error(), "snapshot reader pool") ||
		!strings.Contains(err.Error(), "writer pool") {
		t.Fatalf("prepare failure does not label every pool cleanup failure: %v", err)
	}
}

type rollbackFailure struct {
	err   error
	calls int
}

func (r *rollbackFailure) Rollback() error {
	r.calls++
	return r.err
}

func TestReadTransactionRollbackFailureIsReportedAfterSuccessfulWork(t *testing.T) {
	closeFailure := errors.New("rollback failed")
	tx := &rollbackFailure{err: closeFailure}
	err := finishReadTransaction(t.Context(), "reader transaction", tx, nil)
	if tx.calls != 1 || !errors.Is(err, closeFailure) || !errors.Is(err, syscall.EIO) ||
		!strings.Contains(err.Error(), "reader transaction") {
		t.Fatalf("successful read with rollback failure returned %v after %d calls", err, tx.calls)
	}
}

func TestReadTransactionRollbackFailureIsJoinedWithPrimaryFailure(t *testing.T) {
	primary := context.Canceled
	closeFailure := errors.New("rollback failed")
	tx := &rollbackFailure{err: closeFailure}
	err := finishReadTransaction(t.Context(), "snapshot transaction", tx, primary)
	if tx.calls != 1 || !errors.Is(err, primary) || !errors.Is(err, closeFailure) ||
		!errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "snapshot transaction") {
		t.Fatalf("failed read with rollback failure returned %v after %d calls", err, tx.calls)
	}
}

func TestReaderPoolCloseFailuresAreJoinedWhenSnapshotPoolOpenFails(t *testing.T) {
	primary := errors.New("snapshot pool refused")
	readerClose := errors.New("reader close failed")
	writerClose := errors.New("writer close failed")
	writer, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	readPools := 0
	store, err := openWithHooks(
		t.Context(), "database", "workspace", "", 0, Options{Window: DefaultWindow()},
		storeOpenHooks{
			openPool: func(_ context.Context, _ string, isWriter bool, _ int) (*sql.DB, error) {
				if isWriter {
					return writer, nil
				}
				readPools++
				if readPools == 1 {
					return reader, nil
				}
				return nil, primary
			},
			prepare: func(context.Context, *sql.DB, string, string, Window, int64, int64) (int64, int64, error) {
				t.Fatal("prepare ran after snapshot reader pool open failed")
				return 0, 0, nil
			},
			closePool: func(db *sql.DB) error {
				actual := db.Close()
				if db == reader {
					return errors.Join(actual, readerClose)
				}
				return errors.Join(actual, writerClose)
			},
		},
	)
	if store != nil {
		store.Close()
		t.Fatal("snapshot pool failure returned a Store")
	}
	for _, want := range []error{primary, readerClose, writerClose} {
		if !errors.Is(err, want) {
			t.Fatalf("snapshot pool failure returned %v, missing %v", err, want)
		}
	}
	if !strings.Contains(err.Error(), "reader pool") || !strings.Contains(err.Error(), "writer pool") {
		t.Fatalf("snapshot pool failure does not label cleanup failures: %v", err)
	}
}

func TestWriterCloseFailureIsJoinedWhenSynchronousVerificationFails(t *testing.T) {
	primary := errors.New("synchronous verification failed")
	closeFailure := errors.New("writer close failed")
	db, err := openPoolWith(
		t.Context(), t.TempDir()+"/metastore.db", true, 1,
		func(context.Context, *sql.DB) error { return primary },
		func(db *sql.DB) error { return errors.Join(db.Close(), closeFailure) },
	)
	if db != nil {
		db.Close()
		t.Fatal("failed synchronous verification returned a pool")
	}
	if !errors.Is(err, primary) || !errors.Is(err, closeFailure) ||
		!strings.Contains(err.Error(), "writer pool") {
		t.Fatalf("synchronous verification failure returned %v, want labeled primary and close failures", err)
	}
}

type acceptingCommitWitness struct{}

func (acceptingCommitWitness) Accept(DurableState) error     { return nil }
func (acceptingCommitWitness) Checkpoint(DurableState) error { return nil }

func TestOpenCleanupFailureRetainsDatabaseOwnership(t *testing.T) {
	for _, stage := range []string{"writer", "reader", "snapshot reader", "prepare"} {
		t.Run(stage, func(t *testing.T) {
			path := t.TempDir() + "/metastore.db"
			primary := errors.New(stage + " open failed")
			closeFailure := errors.New(stage + " cleanup left a native handle open")
			var pools []*sql.DB
			newPool := func(database string, writer bool) (*sql.DB, error) {
				var db *sql.DB
				var err error
				if stage == "prepare" {
					db, err = openPool(t.Context(), database, writer, 1)
				} else {
					db, err = sql.Open("sqlite", ":memory:")
				}
				if err == nil {
					pools = append(pools, db)
				}
				return db, err
			}
			t.Cleanup(func() {
				for _, db := range pools {
					db.Close()
				}
			})

			var cleanupTarget *sql.DB
			readOpens := 0
			hooks := storeOpenHooks{
				openDurableWriter: func(_ context.Context, database string, _ int) (*sql.DB, error) {
					db, err := newPool(database, true)
					if err != nil {
						return nil, err
					}
					if stage == "writer" {
						cleanupTarget = db
						return nil, errors.Join(primary, poolCloseFailure("writer pool", closeFailure))
					}
					if stage == "reader" {
						cleanupTarget = db
					}
					return db, nil
				},
				openPool: func(_ context.Context, database string, writer bool, _ int) (*sql.DB, error) {
					if writer {
						t.Fatal("durable open used the ordinary writer hook")
					}
					readOpens++
					if stage == "reader" && readOpens == 1 || stage == "snapshot reader" && readOpens == 2 {
						return nil, primary
					}
					db, err := newPool(database, false)
					if err == nil && stage == "snapshot reader" && readOpens == 1 {
						cleanupTarget = db
					}
					if err == nil && stage == "prepare" && readOpens == 2 {
						cleanupTarget = db
					}
					return db, err
				},
				closePool: func(db *sql.DB) error {
					if db == cleanupTarget {
						return closeFailure
					}
					return db.Close()
				},
			}
			durable := &durableOpen{
				mode: RequireExistingVolume, witness: acceptingCommitWitness{},
			}
			store, err := openConfiguredWithHooks(
				t.Context(), path, "workspace", "store", 0, DefaultOptions(), durable, hooks,
			)
			if store != nil {
				store.Abort()
				t.Fatal("failed open returned a Store")
			}
			if stage != "prepare" && !errors.Is(err, primary) {
				t.Fatalf("%s failure returned %v, missing primary error", stage, err)
			}
			if !errors.Is(err, closeFailure) || !OpenFailureRetainsOwnership(err) {
				t.Fatalf("%s cleanup returned %v without ownership-retention classification", stage, err)
			}
			if _, err := acquireCoordinator(path, true); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("durable reopen after %s cleanup returned %v, want EBUSY", stage, err)
			}
		})
	}
}

func TestTerminalPoolCloseReportingFailuresAreCached(t *testing.T) {
	writer, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	snapshot, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		writer.Close()
		reader.Close()
		t.Fatal(err)
	}
	readPools := 0
	writerReporting := errors.New("writer close reporting failed after the pool closed")
	readerReporting := errors.New("reader close reporting failed after the pool closed")
	snapshotReporting := errors.New("snapshot close reporting failed after the pool closed")
	store, err := openWithHooks(
		t.Context(), "terminal-close", "workspace", "", 0, Options{Window: DefaultWindow()},
		storeOpenHooks{
			openPool: func(_ context.Context, _ string, isWriter bool, _ int) (*sql.DB, error) {
				if isWriter {
					return writer, nil
				}
				readPools++
				if readPools == 1 {
					return reader, nil
				}
				return snapshot, nil
			},
			prepare: prepare,
			closePool: func(db *sql.DB) error {
				closed := db.Close()
				switch db {
				case writer:
					return errors.Join(closed, writerReporting)
				case reader:
					return errors.Join(closed, readerReporting)
				case snapshot:
					return errors.Join(closed, snapshotReporting)
				}
				return closed
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	closeErr := store.Close()
	for _, want := range []error{writerReporting, readerReporting, snapshotReporting} {
		if !errors.Is(closeErr, want) {
			t.Fatalf("terminal pool close returned %v, missing %v", closeErr, want)
		}
	}
	if !store.Terminal() {
		t.Fatal("Store does not report terminal closure after writer Close returned an error")
	}
	if err := store.Close(); !errors.Is(err, writerReporting) ||
		!errors.Is(err, readerReporting) || !errors.Is(err, snapshotReporting) {
		t.Fatalf("rechecking an already terminal Store returned %v, want the original failure", err)
	}
}

type reportedMutationRollback struct {
	tx      *sql.Tx
	entered chan struct{}
	release <-chan struct{}
	fault   error
}

func (r reportedMutationRollback) Rollback() error {
	close(r.entered)
	<-r.release
	return errors.Join(r.tx.Rollback(), r.fault)
}

func TestSQLiteMutationRollbackFailureFencesBeforeFreshReadAdmission(t *testing.T) {
	for _, observationHeld := range []bool{false, true} {
		t.Run(fmt.Sprintf("observation already held %t", observationHeld), func(t *testing.T) {
			f := newPublicationFixture(t)
			f.put(t, t.Context(), "file", 3)
			before := f.node(t, "file")
			if err := f.store.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer f.store.coordinator.commit.release()
			tx, err := f.store.write.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(t.Context(), `UPDATE nodes SET size = 7 WHERE id = ?`, before.ID); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("SQLite rollback could not be confirmed")
			*f.authorityFence = fault
			entered, proceed := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			finished := make(chan error, 1)
			consumed := false
			defer func() {
				release()
				if !consumed {
					<-finished
				}
			}()
			if observationHeld {
				f.store.coordinator.health.Lock()
			}
			go func() {
				finished <- f.store.finishMutationTransaction(reportedMutationRollback{
					tx: tx, entered: entered, release: proceed, fault: fault,
				}, syscall.EDQUOT, observationHeld)
			}()
			<-entered
			read := make(chan error, 1)
			go func() {
				_, err := f.store.Stat(t.Context(), "file")
				read <- err
			}()
			waitForMutationRollbackRead(t, read)
			release()
			err = <-finished
			consumed = true
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, syscall.EDQUOT) || !errors.Is(err, fault) {
				t.Fatalf("rollback failure = %v, want EIO with primary and cleanup causes", err)
			}
			if err := <-read; storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, fault) {
				t.Fatalf("fresh view crossed unconfirmed cleanup: %v", err)
			}
			status, err := f.store.locks.Status(t.Context())
			if err != nil || !status.Unavailable {
				t.Fatalf("authority stayed available after rollback failure: %+v, %v", status, err)
			}
			var size int64
			if err := f.store.read.QueryRowContext(t.Context(), `SELECT size FROM nodes WHERE id = ?`, before.ID).Scan(&size); err != nil || size != 3 {
				t.Fatalf("real rollback left size %d with error %v", size, err)
			}
		})
	}
}

func waitForMutationRollbackRead(t *testing.T, read <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	stacks := make([]byte, 1<<20)
	for {
		select {
		case err := <-read:
			t.Fatalf("fresh read completed before rollback outcome: %v", err)
		default:
		}
		for _, stack := range strings.Split(string(stacks[:runtime.Stack(stacks, true)]), "\n\n") {
			if strings.Contains(stack, "TestSQLiteMutationRollbackFailureFencesBeforeFreshReadAdmission.func") &&
				strings.Contains(stack, "(*databaseCoordinator).beginHealthyRead") &&
				strings.Contains(stack, "(*Store).Stat") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("fresh read never reached rollback observation admission")
		}
		runtime.Gosched()
	}
}

func TestSQLiteMutationRollbackDistinguishesFinalizationFromFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		fenced bool
	}{
		{name: "successful rollback"},
		{name: "commit or automatic rollback completed", err: sql.ErrTxDone},
		{name: "wrapped completion failure", err: fmt.Errorf("cleanup: %w", sql.ErrTxDone), fenced: true},
		{name: "independent joined completion failure", err: errors.Join(sql.ErrTxDone, context.Canceled), fenced: true},
		{name: "cancellation reported by rollback", err: context.Canceled, fenced: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			if test.fenced {
				*f.authorityFence = test.err
			}
			if err := f.store.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			tx := &rollbackFailure{err: test.err}
			err := f.store.finishMutationTransaction(tx, syscall.EDQUOT, false)
			f.store.coordinator.commit.release()
			want := syscall.EDQUOT
			if test.fenced {
				want = syscall.EIO
			}
			if storage.ErrnoOf(err) != want || !errors.Is(err, syscall.EDQUOT) || tx.calls != 1 {
				t.Fatalf("cleanup = %v, calls=%d, want %v retaining primary", err, tx.calls, want)
			}
			if test.fenced && !errors.Is(err, test.err) {
				t.Fatalf("cleanup lost the rollback error: %v", err)
			}
			status, statusErr := f.store.locks.Status(t.Context())
			if statusErr != nil || status.Unavailable != test.fenced {
				t.Fatalf("cleanup left authority status %+v, error %v", status, statusErr)
			}
		})
	}
}

func TestBoundOpenersPreserveIdentityAndEnforceConfiguredBacklog(t *testing.T) {
	for _, api := range []string{"default", "object limits", "options"} {
		t.Run(api, func(t *testing.T) {
			path := t.TempDir() + "/bound.db"
			openBound := func(identity string) (*Store, error) {
				switch api {
				case "default":
					return OpenBound(t.Context(), path, "workspace", identity, 100, DefaultWindow())
				case "object limits":
					return OpenBoundWithObjectLimits(t.Context(), path, "workspace", identity, 100, DefaultWindow(), ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 8})
				default:
					options := DefaultOptions()
					options.ObjectLimits = ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 8}
					options.MaxReaderConnections = 1
					return OpenBoundWithOptions(t.Context(), path, "workspace", identity, 100, options)
				}
			}
			if s, err := openBound(""); s != nil || !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("empty identity = %v, %v", s, err)
			}
			s, err := openBound("objects-one")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if s != nil {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if err := s.Create(t.Context(), "kept"); err != nil {
				t.Fatal(err)
			}
			before, err := s.Stat(t.Context(), "kept")
			if err != nil {
				t.Fatal(err)
			}
			if api != "default" {
				key, err := s.Reserve(t.Context(), "kept", 8)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.Reserve(t.Context(), "kept", 1); !errors.Is(err, syscall.EAGAIN) {
					t.Fatalf("full backlog = %v", err)
				}
				if err := s.Abandon(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if other, err := openBound("objects-two"); other != nil || !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("different backing store = %v, %v", other, err)
			}
			if other, err := Open(t.Context(), path, "workspace", 100, DefaultWindow()); other != nil || !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("unbound bypass = %v, %v", other, err)
			}
			s, err = openBound("objects-one")
			if err != nil {
				t.Fatal(err)
			}
			if after, err := s.Stat(t.Context(), "kept"); err != nil || after != before {
				t.Fatalf("reopened node = %+v, %v; want %+v", after, err, before)
			}
		})
	}
}
