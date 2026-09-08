package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestForgetOwnsCancellationDuringSQLiteMutation(t *testing.T) {
	for _, target := range []struct {
		table string
		op    int32
	}{
		{table: "objects", op: sqlite3.SQLITE_DELETE},
		{table: "database_state", op: sqlite3.SQLITE_UPDATE},
	} {
		t.Run(target.table, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newPublicationFixture(t)
				f.put(t, t.Context(), "file", 3)
				f.put(t, t.Context(), "file", 7)
				garbage, err := f.store.Garbage(t.Context(), 1)
				if err != nil || len(garbage) != 1 {
					t.Fatalf("garbage=%v, error=%v", garbage, err)
				}
				before, err := f.store.DurableState(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				operation := "DELETE"
				if target.op == sqlite3.SQLITE_UPDATE {
					operation = "UPDATE"
				}
				// A short single-row opcode can finish before SQLite checks its interrupt
				// flag. This bounded trigger forces that check inside the same statement.
				_, err = f.store.write.ExecContext(t.Context(), fmt.Sprintf(`
					CREATE TEMP TRIGGER forget_interrupt AFTER %s ON main.%s BEGIN
						SELECT sum(n) FROM (
							WITH RECURSIVE steps(n) AS (
								VALUES(1) UNION ALL SELECT n+1 FROM steps WHERE n < 32
							) SELECT n FROM steps
						);
					END`, operation, target.table))
				if err != nil {
					t.Fatal(err)
				}
				entered, proceed := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(proceed) })
				defer release()
				var paused sync.Once
				var rollbacks atomic.Int32
				setForgetHooks(t, f.store.write, func(change sqliteDriver.SQLitePreUpdateData) {
					if change.TableName == target.table && change.Op == target.op {
						paused.Do(func() {
							close(entered)
							<-proceed
						})
					}
				}, func() { rollbacks.Add(1) })
				request, cancel := context.WithCancel(t.Context())
				defer cancel()
				finished := make(chan error, 1)
				go func() { finished <- f.store.Forget(request, garbage) }()
				<-entered
				cancel()
				// SQLite is inside its write opcode. Waiting here lets any driver
				// cancellation interrupt that statement before it can leave the hook.
				synctest.Wait()
				release()
				err = <-finished
				setForgetHooks(t, f.store.write, nil, nil)
				if _, err := f.store.write.ExecContext(t.Context(), `DROP TRIGGER forget_interrupt`); err != nil {
					t.Fatal(err)
				}
				if err != nil {
					t.Errorf("admitted Forget at %s returned %v; native rollbacks=%d", target.table, err, rollbacks.Load())
				}
				if rollbacks.Load() != 0 {
					t.Errorf("caller cancellation caused %d native rollbacks", rollbacks.Load())
				}
				if fault := f.store.coordinator.healthy(); fault != nil {
					f.store.coordinator.health.RLock()
					*f.authorityFence = f.store.coordinator.poison
					f.store.coordinator.health.RUnlock()
					t.Errorf("caller cancellation fenced the store: %v", fault)
				}
				var retained, generation int64
				if err := f.store.read.QueryRowContext(t.Context(),
					`SELECT count(*) FROM objects WHERE key = ?`, string(garbage[0])).Scan(&retained); err != nil || retained != 0 {
					t.Errorf("admitted cleanup retained %d records: %v", retained, err)
				}
				if err := f.store.read.QueryRowContext(t.Context(),
					`SELECT generation FROM database_state WHERE singleton = 1`).Scan(&generation); err != nil || generation != before.Generation+1 {
					t.Errorf("generation advanced from %d to %d: %v", before.Generation, generation, err)
				}
				if _, err := f.store.Stat(t.Context(), "file"); err != nil {
					t.Errorf("reading the current node after cleanup: %v", err)
				}
				if f.store.write.Stats().InUse != 0 {
					t.Error("admitted cleanup retained its SQL transaction")
				}
				if err := f.store.Close(); err != nil {
					t.Errorf("closing after admitted cleanup: %v", err)
				}
				if f.store.write.Stats().OpenConnections != 0 || f.store.read.Stats().OpenConnections != 0 {
					t.Error("Close retained SQL pool connections")
				}
			})
		})
	}
}

func setForgetHooks(t *testing.T, writer *sql.DB, update sqliteDriver.PreUpdateHookFn, rollback sqliteDriver.RollbackHookFn) {
	t.Helper()
	connection, err := writer.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.Raw(func(raw any) error {
		hooks := raw.(sqliteDriver.HookRegisterer)
		hooks.RegisterPreUpdateHook(update)
		hooks.RegisterRollbackHook(rollback)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
