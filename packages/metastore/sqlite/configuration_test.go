package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestEveryWriterConnectionUsesFullSynchronous(t *testing.T) {
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
	}
}

func TestReaderConnectionOptionsAreBoundedAndValidatedBeforeOpening(t *testing.T) {
	defaults := DefaultOptions()
	if defaults.MaxReaderConnections != DefaultMaxReaderConnections || defaults.MaxReaderConnections < 1 ||
		defaults.MaxSnapshotReaderConnections != DefaultMaxSnapshotReaderConnections ||
		defaults.MaxSnapshotReaderConnections < 1 ||
		defaults.MaxIntegrityRecords != DefaultMaxIntegrityRecords || defaults.MaxIntegrityRecords < 1 {
		t.Fatalf("default SQLite options are %+v", defaults)
	}
	effective, err := (Options{Window: DefaultWindow()}).Effective()
	if err != nil {
		t.Fatal(err)
	}
	if effective != defaults {
		t.Fatalf("zero-valued resource options resolve to %+v, want %+v", effective, defaults)
	}
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if got := store.read.Stats().MaxOpenConnections; got != DefaultMaxReaderConnections {
		store.Close()
		t.Fatalf("Open configured %d reader connections, want default %d", got, DefaultMaxReaderConnections)
	}
	if got := store.snapshotRead.Stats().MaxOpenConnections; got != DefaultMaxSnapshotReaderConnections {
		store.Close()
		t.Fatalf("Open configured %d snapshot reader connections, want default %d",
			got, DefaultMaxSnapshotReaderConnections)
	}
	if store.maxIntegrityRecords != DefaultMaxIntegrityRecords {
		store.Close()
		t.Fatalf("Open configured an integrity record limit of %d, want default %d",
			store.maxIntegrityRecords, DefaultMaxIntegrityRecords)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	minimum, err := OpenWithOptions(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0,
		Options{Window: DefaultWindow(), MaxIntegrityRecords: MinIntegrityRecords},
	)
	if err != nil {
		t.Fatalf("opening an empty namespace at the minimum integrity record limit: %v", err)
	}
	if err := minimum.Close(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name  string
		limit int
	}{
		{"negative", -1},
		{"unbounded", math.MaxInt},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:               DefaultWindow(),
				MaxReaderConnections: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite reader-connection limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with reader-connection limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
	}

	for _, test := range []struct {
		name  string
		limit int
	}{
		{"negative snapshot readers", -1},
		{"unbounded snapshot readers", math.MaxInt},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:                       DefaultWindow(),
				MaxSnapshotReaderConnections: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite snapshot reader limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with snapshot reader limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
	}

	for _, test := range []struct {
		name  string
		limit int64
	}{
		{"negative integrity records", -1},
		{"below an empty namespace", MinIntegrityRecords - 1},
		{"unbounded integrity records", math.MaxInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:              DefaultWindow(),
				MaxIntegrityRecords: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite integrity record limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with integrity record limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
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

func TestSnapshotPoolDoesNotBlockGeneralReaderPool(t *testing.T) {
	store, err := OpenWithOptions(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, Options{
			Window:                       DefaultWindow(),
			MaxReaderConnections:         1,
			MaxSnapshotReaderConnections: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	picture, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer picture.Close()

	waitContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		blocked, _, err := store.Snapshot(waitContext)
		if blocked != nil {
			err = errors.Join(err, blocked.Close())
		}
		result <- err
	}()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for store.snapshotRead.Stats().WaitCount == 0 {
		select {
		case <-deadline.C:
			t.Fatal("a second snapshot did not wait for the dedicated snapshot pool")
		default:
			runtime.Gosched()
		}
	}
	if _, err := store.Incarnation(t.Context(), 1024); err != nil {
		t.Fatalf("a saturated snapshot pool blocked a general log read: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling a snapshot pool waiter returned %v, want context.Canceled", err)
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
			prepare: func(context.Context, *sql.DB, string, string, Window, int64) (int64, int64, error) {
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
			prepare: func(context.Context, *sql.DB, string, string, Window, int64) (int64, int64, error) {
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
	err := finishReadTransaction("reader transaction", tx, nil)
	if tx.calls != 1 || !errors.Is(err, closeFailure) || !errors.Is(err, syscall.EIO) ||
		!strings.Contains(err.Error(), "reader transaction") {
		t.Fatalf("successful read with rollback failure returned %v after %d calls", err, tx.calls)
	}
}

func TestReadTransactionRollbackFailureIsJoinedWithPrimaryFailure(t *testing.T) {
	primary := context.Canceled
	closeFailure := errors.New("rollback failed")
	tx := &rollbackFailure{err: closeFailure}
	err := finishReadTransaction("snapshot transaction", tx, primary)
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
			prepare: func(context.Context, *sql.DB, string, string, Window, int64) (int64, int64, error) {
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

func TestPendingAdmissionSeeksPastALargeReferencedSet(t *testing.T) {
	store, err := OpenWithObjectLimits(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, DefaultWindow(), ObjectLimits{
			MaxPendingObjects: 1,
			MaxPendingBytes:   1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	tx, err := store.write.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(t.Context(), `
		INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec)
		VALUES (?, ?, ?, 1, NULL, 0, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20_000 {
		if _, err := statement.ExecContext(t.Context(), fmt.Sprintf("referenced-%05d", i), store.namespace, stateReferenced); err != nil {
			t.Fatal(err)
		}
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := store.write.QueryContext(t.Context(), `EXPLAIN QUERY PLAN `+pendingObjectStatusQuery,
		stateReserved, stateReserved, stateUnresolved, stateUnresolved,
		stateGarbage, stateGarbage,
		store.namespace, stateReserved, stateUnresolved, stateGarbage)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	whole := strings.Join(plan, "; ")
	if strings.Contains(whole, "SCAN objects") || !strings.Contains(whole, "objects_by_state") {
		t.Fatalf("pending admission does not seek by namespace and pending state; plan: %s", whole)
	}
	if _, err := store.Reserve(t.Context(), "pending", 1); err != nil {
		t.Fatalf("reserving beside 20,000 referenced objects: %v", err)
	}
}

func TestGarbageSelectionUsesTheBoundedStateIndex(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	rows, err := store.read.QueryContext(t.Context(), `EXPLAIN QUERY PLAN `+garbageQuery,
		store.namespace, stateGarbage, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	whole := strings.Join(plan, "; ")
	if strings.Contains(whole, "SCAN objects") || strings.Contains(whole, "TEMP B-TREE") ||
		!strings.Contains(whole, "objects_by_state") {
		t.Fatalf("garbage selection does not remain a bounded state-index scan: %s", whole)
	}
}
