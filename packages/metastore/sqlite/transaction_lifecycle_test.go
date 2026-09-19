package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
	"github.com/codetreker/remote-fs/packages/storage"
	sqliteDriver "modernc.org/sqlite"
)

type transactionRollbackGate struct {
	armed                      atomic.Bool
	entered, release           chan struct{}
	beginFailure, queryFailure error
}

type transactionRollbackConnector struct {
	base *sqliteDriver.Driver
	dsn  string
	gate *transactionRollbackGate
}

func (c transactionRollbackConnector) Driver() driver.Driver { return c.base }
func (c transactionRollbackConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &transactionRollbackConnection{Conn: conn, gate: c.gate}, nil
}

type transactionRollbackConnection struct {
	driver.Conn
	gate *transactionRollbackGate
}

func (c *transactionRollbackConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if c.gate.beginFailure != nil {
		return nil, c.gate.beginFailure
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return transactionRollbackDriverTx{Tx: tx, gate: c.gate}, nil
}
func (c *transactionRollbackConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.gate.queryFailure != nil {
		return nil, c.gate.queryFailure
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *transactionRollbackConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *transactionRollbackConnection) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *transactionRollbackConnection) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

type transactionRollbackDriverTx struct {
	driver.Tx
	gate *transactionRollbackGate
}

func (tx transactionRollbackDriverTx) Rollback() error {
	if tx.gate.armed.CompareAndSwap(true, false) {
		close(tx.gate.entered)
		<-tx.gate.release
	}
	return tx.Tx.Rollback()
}

func TestCanceledReadJoinsAutomaticRollbackBeforeReturning(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := &transactionRollbackGate{entered: make(chan struct{}), release: make(chan struct{})}
				release := sync.OnceFunc(func() { close(gate.release) })
				defer release()
				store := openReadRollbackStore(t, gate)
				ctx, cancel := context.WithCancel(t.Context())
				interrupt := cancel
				wantCause, wantErrno := error(context.Canceled), syscall.EINTR
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), time.Second)
					interrupt = func() { time.Sleep(time.Second) }
					wantCause, wantErrno = context.DeadlineExceeded, syscall.EIO
				}
				defer cancel()
				tx, err := store.beginReadSnapshot(ctx, store.read)
				if err != nil {
					t.Fatal(err)
				}
				var one int
				if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
					t.Fatal(err)
				}
				gate.armed.Store(true)
				interrupt()
				<-gate.entered
				if got := store.read.Stats().InUse; got != 1 {
					t.Fatalf("held rollback owns %d connections, want 1", got)
				}
				closeEntered := make(chan struct{})
				realClose := tx.closeConnection
				tx.closeConnection = func() error {
					close(closeEntered)
					return realClose()
				}
				finished := make(chan error, 1)
				go func() { finished <- finishReadTransaction(ctx, "reader transaction", tx, ctx.Err()) }()
				returned := false
				var readErr error
				select {
				case <-closeEntered:
					select {
					case readErr = <-finished:
						returned = true
						t.Error("read returned while driver rollback still owns its connection")
					default:
					}
				case readErr = <-finished:
					returned = true
					t.Error("read returned without joining its owned connection")
				}
				release()
				if !returned {
					readErr = <-finished
				} else {
					if err := realClose(); err != nil {
						t.Errorf("releasing failed test connection: %v", err)
					}
				}

				if storage.ErrnoOf(readErr) != wantErrno || !errors.Is(readErr, wantCause) {
					t.Fatalf("canceled read = %v, want %v retaining %v", readErr, wantErrno, wantCause)
				}
				if got := store.read.Stats().InUse; got != 0 {
					t.Fatalf("returned read still owns %d connections", got)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func openReadRollbackStore(t *testing.T, gate *transactionRollbackGate) *Store {
	t.Helper()
	path := t.TempDir() + "/read.db"
	store, err := OpenBoundDurableWithOptions(t.Context(), path, "volume", "objects", 4096,
		DefaultOptions(), CreateVolumeIfMissing, DurableStartup{}, acceptingCommitWitness{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.read.Close(); err != nil {
		t.Fatal(err)
	}
	store.read = sql.OpenDB(transactionRollbackConnector{base: &sqliteDriver.Driver{}, dsn: poolDataSource(path, false), gate: gate})
	store.read.SetMaxOpenConns(1)
	return store
}

func TestReadTransactionSetupFailureReleasesItsOwnedConnection(t *testing.T) {
	for _, pin := range []bool{false, true} {
		name := "begin"
		if pin {
			name = "snapshot pin"
		}
		t.Run(name, func(t *testing.T) {
			gate := &transactionRollbackGate{}
			store := openReadRollbackStore(t, gate)
			fault := errors.New("reader setup failed")
			if pin {
				gate.queryFailure = fault
			} else {
				gate.beginFailure = fault
			}
			if tx, err := store.beginReadSnapshot(t.Context(), store.read); tx != nil || storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, fault) {
				t.Fatalf("reader setup = %v, %v; want EIO retaining setup failure", tx, err)
			}
			if got := store.read.Stats().InUse; got != 0 {
				t.Fatalf("failed setup still owns %d connections", got)
			}
			gate.beginFailure, gate.queryFailure = nil, nil
			if _, err := store.Stat(t.Context(), ""); err != nil {
				t.Fatalf("reader setup failure poisoned later reads: %v", err)
			}
		})
	}
}

func TestReadTransactionConnectionCloseFailuresRemainIOFailures(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		for _, fault := range []error{errors.New("connection close failed"), context.Canceled, sql.ErrTxDone, sql.ErrConnDone} {
			name := "explicit/" + fault.Error()
			if automatic {
				name = "automatic/" + fault.Error()
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					store := openReadRollbackStore(t, &transactionRollbackGate{})
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					tx, err := store.beginReadSnapshot(ctx, store.read)
					if err != nil {
						t.Fatal(err)
					}
					if automatic {
						cancel()
						synctest.Wait()
					}
					actualClose := tx.closeConnection
					tx.closeConnection = func() error { return errors.Join(actualClose(), fault) }
					err = finishReadTransaction(ctx, "reader transaction", tx, ctx.Err())
					if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, fault) || automatic && !errors.Is(err, context.Canceled) {
						t.Fatalf("connection close failure = %v, want EIO retaining %v", err, fault)
					}
					if got := store.read.Stats().InUse; got != 0 {
						t.Fatalf("reported close failure still owns %d connections", got)
					}
				})
			})
		}
	}
}

func TestCanceledWriterCleanupRetainsNativeOwnershipUntilRollback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := t.TempDir() + "/writer.db"
		store, err := Open(t.Context(), path, "volume", 4096, DefaultWindow())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		gate, release := installWriterRollbackGate(t, store, path)
		defer release()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := store.coordinator.commit.acquire(ctx); err != nil {
			t.Fatal(err)
		}
		releaseCommit := sync.OnceFunc(store.coordinator.commit.release)
		defer releaseCommit()
		tx, err := beginOwnedTransaction(ctx, store.write, nil)
		if err != nil {
			releaseCommit()
			t.Fatal(err)
		}
		defer func() {
			release()
			if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				t.Error(err)
			}
		}()
		if _, err := tx.ExecContext(ctx, "UPDATE volumes SET used=used WHERE id=?", store.volume); err != nil {
			t.Fatal(err)
		}
		gate.armed.Store(true)
		cancel()
		<-gate.entered
		entered := observeConnectionRelease(tx)
		finished := make(chan error, 1)
		go func() {
			err := store.finishMutationTransaction(tx, ctx.Err(), false)
			releaseCommit()
			finished <- err
		}()
		assertCleanupWaiting(t, entered, finished, store, path)
		release()
		if err := <-finished; !errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO) {
			t.Fatalf("mutation cleanup = %v", err)
		}
		assertCleanWriterClose(t, store, path)
	})
}

func TestCanceledReplicaSeedJoinsRollbackBeforeSettling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := t.TempDir() + "/replica.db"
		replica, err := OpenReplica(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := replica.Close(); err != nil {
				t.Error(err)
			}
		})
		gate, release := installWriterRollbackGate(t, replica.store, path)
		defer release()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		seed, err := replica.Reseed(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			release()
			if err := seed.Close(); err != nil {
				t.Error(err)
			}
		}()
		gate.armed.Store(true)
		cancel()
		<-gate.entered
		entered := observeConnectionRelease(seed.tx)
		finished := make(chan error, 1)
		go func() { finished <- seed.Close() }()
		assertCleanupWaiting(t, entered, finished, replica.store, path)
		release()
		if err := <-finished; !errors.Is(err, sql.ErrTxDone) || storage.ErrnoOf(err) != syscall.EIO {
			t.Fatalf("seed cleanup = %v", err)
		}
		assertCleanWriterClose(t, replica.store, path)
	})
}

func installWriterRollbackGate(t *testing.T, store *Store, path string) (*transactionRollbackGate, func()) {
	t.Helper()
	if err := store.write.Close(); err != nil {
		t.Fatal(err)
	}
	gate := &transactionRollbackGate{entered: make(chan struct{}), release: make(chan struct{})}
	store.write = sql.OpenDB(transactionRollbackConnector{base: &sqliteDriver.Driver{}, dsn: poolDataSource(path, true), gate: gate})
	store.write.SetMaxOpenConns(1)
	return gate, sync.OnceFunc(func() { close(gate.release) })
}

func observeConnectionRelease(tx *ownedTransaction) <-chan struct{} {
	entered := make(chan struct{})
	actualClose := tx.closeConnection
	tx.closeConnection = func() error { close(entered); return actualClose() }
	return entered
}

func assertCleanupWaiting(t *testing.T, entered <-chan struct{}, finished <-chan error, store *Store, path string) {
	t.Helper()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("cleanup returned before joining connection: %v", err)
	}
	select {
	case err := <-finished:
		t.Fatalf("cleanup returned while driver rollback held: %v", err)
	default:
	}
	if got := store.write.Stats().InUse; got != 1 {
		t.Fatalf("held writer count = %d", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("close bypassed retained commit gate: %v", err)
	}
	owner, err := nativelease.AcquireDatabase(path, true, false)
	if owner != nil {
		if closeErr := owner.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("native ownership released during rollback: %v", err)
	}
}

func assertCleanWriterClose(t *testing.T, store *Store, path string) {
	t.Helper()
	if got := store.write.Stats().InUse; got != 0 {
		t.Fatalf("returned cleanup still owns %d writer connections", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err := nativelease.AcquireDatabase(path, true, false)
	if err != nil {
		t.Fatalf("completed close retained native ownership: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedTransactionReleasesOnceAfterCommitOrRollback(t *testing.T) {
	for _, commit := range []bool{false, true} {
		for _, reportFailure := range []bool{false, true} {
			name := fmt.Sprintf("committed=%t/close_failure=%t", commit, reportFailure)
			t.Run(name, func(t *testing.T) {
				store, err := Open(t.Context(), t.TempDir()+"/owned.db", "volume", 4096, DefaultWindow())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				})
				tx, err := beginOwnedTransaction(t.Context(), store.write, nil)
				if err != nil {
					t.Fatal(err)
				}
				actualClose, calls := tx.closeConnection, 0
				fault := errors.New("connection release reporting failed")
				tx.closeConnection = func() error {
					calls++
					err := actualClose()
					if reportFailure {
						return errors.Join(err, fault)
					}
					return err
				}
				if commit {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				first, second := tx.Rollback(), tx.Rollback()
				if calls != 1 || store.write.Stats().InUse != 0 {
					t.Fatalf("release calls=%d writer count=%d", calls, store.write.Stats().InUse)
				}
				if reportFailure {
					if !errors.Is(first, fault) || !errors.Is(second, fault) || !errors.Is(second, sql.ErrTxDone) {
						t.Fatalf("release failure was not retained: first=%v second=%v", first, second)
					}
				} else if second != sql.ErrTxDone || commit && first != sql.ErrTxDone || !commit && first != nil {
					t.Fatalf("rollback results first=%v second=%v committed=%t", first, second, commit)
				}
			})
		}
	}
}
