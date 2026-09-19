package schema

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
	sqliteDriver "modernc.org/sqlite"
)

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
