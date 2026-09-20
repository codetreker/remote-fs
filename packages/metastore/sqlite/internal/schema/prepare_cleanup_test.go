package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
	sqliteDriver "modernc.org/sqlite"
)

type preparationDriverGate struct {
	cancel              context.CancelFunc
	rollbackEntered     chan struct{}
	rollbackRelease     chan struct{}
	beginFailure        error
	connections, closes atomic.Int32
}

type preparationConnector struct {
	dsn  string
	gate *preparationDriverGate
}

func (c preparationConnector) Driver() driver.Driver { return &sqliteDriver.Driver{} }

func (c preparationConnector) Connect(context.Context) (driver.Conn, error) {
	raw, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	c.gate.connections.Add(1)
	return &preparationConnection{Conn: raw, gate: c.gate}, nil
}

type preparationConnection struct {
	driver.Conn
	gate *preparationDriverGate
}

func (c *preparationConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if c.gate.beginFailure != nil {
		return nil, c.gate.beginFailure
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.gate.cancel()
	return preparationDriverTx{Tx: tx, gate: c.gate}, nil
}

func (c *preparationConnection) Close() error {
	c.gate.closes.Add(1)
	return c.Conn.Close()
}

func (c *preparationConnection) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *preparationConnection) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

type preparationDriverTx struct {
	driver.Tx
	gate *preparationDriverGate
}

func (tx preparationDriverTx) Rollback() error {
	close(tx.gate.rollbackEntered)
	<-tx.gate.rollbackRelease
	return tx.Tx.Rollback()
}

func openPreparationDriverDatabase(t *testing.T, gate *preparationDriverGate) *sql.DB {
	t.Helper()
	db := sql.OpenDB(preparationConnector{dsn: t.TempDir() + "/prepare.db", gate: gate})
	db.SetMaxIdleConns(0)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestPrepareConfiguredJoinsAutomaticRollbackBeforeReturning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		gate := &preparationDriverGate{
			cancel:          cancel,
			rollbackEntered: make(chan struct{}),
			rollbackRelease: make(chan struct{}),
		}
		release := sync.OnceFunc(func() { close(gate.rollbackRelease) })
		defer release()
		db := openPreparationDriverDatabase(t, gate)
		finished := make(chan error, 1)
		go func() {
			_, _, _, err := PrepareConfigured(ctx, db, "workspace", "store", changes.DefaultWindow(), 1000, 1<<20, nil)
			finished <- err
		}()

		<-gate.rollbackEntered
		select {
		case err := <-finished:
			t.Fatalf("preparation returned while driver rollback held its connection: %v", err)
		default:
		}
		if got := db.Stats().InUse; got != 1 {
			t.Fatalf("held preparation owns %d connections, want 1", got)
		}

		release()
		if err := <-finished; err != context.Canceled {
			t.Fatalf("preparation cleanup changed cancellation: %v", err)
		}
		if got := db.Stats().InUse; got != 0 {
			t.Fatalf("preparation returned with %d connections in use", got)
		}
		if got := gate.closes.Load(); got != 1 {
			t.Fatalf("returned preparation closed %d driver connections, want 1", got)
		}
	})
}

func TestPrepareConfiguredBeginFailureClosesConnectionOnce(t *testing.T) {
	fault := errors.New("begin failed")
	gate := &preparationDriverGate{beginFailure: fault}
	db := openPreparationDriverDatabase(t, gate)

	_, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "store", changes.DefaultWindow(), 1000, 1<<20, nil)
	if err != fault {
		t.Fatalf("preparation begin failure = %v, want original failure", err)
	}
	if got := gate.connections.Load(); got != 1 {
		t.Fatalf("preparation acquired %d driver connections, want 1", got)
	}
	if got := db.Stats().InUse; got != 0 {
		t.Fatalf("failed preparation retained %d connections", got)
	}
	if got := gate.closes.Load(); got != 1 {
		t.Fatalf("failed preparation closed its driver connection %d times, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := gate.closes.Load(); got != 1 {
		t.Fatalf("database close changed driver connection closes to %d", got)
	}
}

func TestPreparationCleanupJoinsAutomaticRollback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, err := sql.Open("sqlite", t.TempDir()+"/prepare.db")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), "CREATE TABLE rollback_probe(value INTEGER)"); err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		if err := conn.Raw(func(raw any) error {
			raw.(interface {
				RegisterRollbackHook(sqliteDriver.RollbackHookFn)
			}).RegisterRollbackHook(func() { close(entered); <-release })
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO rollback_probe VALUES(1)"); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-entered
		closing := make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			finished <- finishPreparation(tx, func() error { close(closing); return conn.Close() }, ctx.Err())
		}()
		select {
		case <-closing:
		case err := <-finished:
			t.Fatalf("preparation returned without joining connection: %v", err)
		}
		select {
		case err := <-finished:
			t.Fatalf("preparation returned before driverrollback completed: %v", err)
		default:
		}
		if got := db.Stats().InUse; got != 1 {
			t.Fatalf("held preparation owns %d connections", got)
		}
		unblock()
		if err := <-finished; err != context.Canceled {
			t.Fatalf("preparation cleanup changed cancellation: %v", err)
		}
		if got := db.Stats().InUse; got != 0 {
			t.Fatalf("preparation returned with %d connections inuse", got)
		}
	})
}

type preparationRollbackResult struct{ err error }

func (r preparationRollbackResult) Rollback() error { return r.err }

func TestPreparationCleanupPreservesCommitAndCleanupFailures(t *testing.T) {
	primary := sqlerr.NewUncertainCommit(errors.New("commit outcome unknown"))
	fault := errors.New("cleanup failed")
	for _, test := range []struct {
		name            string
		rollback, close error
		bad             bool
	}{
		{"clean", nil, nil, false},
		{"committed", sql.ErrTxDone, nil, false},
		{"rollback", fault, nil, true},
		{"wrapped transaction done", errors.Join(sql.ErrTxDone), nil, true},
		{"close", nil, fault, true},
		{"automatic close", sql.ErrTxDone, fault, true},
		{"canceled close", sql.ErrTxDone, context.Canceled, true},
		{"closed connection", sql.ErrTxDone, sql.ErrConnDone, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := finishPreparation(preparationRollbackResult{test.rollback}, func() error { calls++; return test.close }, primary)
			if calls != 1 || !errors.Is(err, primary) || !sqlerr.IsUncertainCommit(err) {
				t.Fatalf("cleanup lost original commit outcome: %v, calls%d", err, calls)
			}
			if !test.bad && err != primary {
				t.Fatalf("successful release wrapped original result: %v", err)
			}
			if test.bad {
				if storage.ErrnoOf(err) != syscall.EIO || test.rollback != nil && !errors.Is(err, test.rollback) || test.close != nil && !errors.Is(err, test.close) {
					t.Fatalf("cleanup failure not preserved: %v", err)
				}
			}
		})
	}
	for _, primary := range []error{nil, context.Canceled} {
		for _, closeFault := range []error{context.Canceled, sql.ErrTxDone, sql.ErrConnDone} {
			err := finishPreparation(preparationRollbackResult{sql.ErrTxDone}, func() error { return closeFault }, primary)
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, closeFault) || primary != nil && !errors.Is(err, primary) {
				t.Fatalf("independent close failure became cancellation: primary=%v close=%v result=%v", primary, closeFault, err)
			}
		}
	}
	calls := 0
	if err := finishPreparation(nil, func() error { calls++; return nil }, context.Canceled); err != context.Canceled || calls != 1 {
		t.Fatalf("failed begin cleanup: %v,calls%d", err, calls)
	}
}
