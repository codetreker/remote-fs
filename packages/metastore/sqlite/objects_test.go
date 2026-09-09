package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestAdmittedForgetCompletesAfterCallerCancellation(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	f.put(t, t.Context(), "file", 7)
	garbage, err := f.store.Garbage(t.Context(), 1)
	if err != nil || len(garbage) != 1 {
		t.Fatalf("expected one retired object, got %v: %v", garbage, err)
	}
	before, err := f.store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.store.coordinator.health.RLock()
	release := sync.OnceFunc(f.store.coordinator.health.RUnlock)
	finished := make(chan error, 1)
	consumed := false
	defer func() {
		cancel()
		release()
		if !consumed {
			<-finished
		}
	}()
	go func() { finished <- f.store.Forget(request, garbage) }()

	// A waiting writer makes TryRLock fail while our original read lock prevents
	// admission. Forget has completed its statements and is awaiting final commit.
	deadline := time.Now().Add(5 * time.Second)
	for f.store.coordinator.health.TryRLock() {
		f.store.coordinator.health.RUnlock()
		select {
		case err := <-finished:
			consumed = true
			t.Fatalf("Forget ended before commit admission: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Forget did not wait at final commit admission")
		}
		runtime.Gosched()
	}
	cancel()
	release()
	err = <-finished
	consumed = true
	if err != nil {
		t.Errorf("Forget after successful staging returned %v, want completed commit", err)
	}

	// A failed health assertion must not strand the real SQL pools. The operation
	// and Close assertions still require an unfenced Store.
	if fault := f.store.coordinator.healthy(); fault != nil {
		f.store.coordinator.health.RLock()
		*f.authorityFence = f.store.coordinator.poison
		f.store.coordinator.health.RUnlock()
		t.Errorf("caller cancellation fenced the admitted cleanup: %v", fault)
	}
	status, statusErr := f.store.locks.Status(t.Context())
	if statusErr != nil || status.Unavailable {
		t.Errorf("cancellation before COMMIT fenced the authority: %+v, %v", status, statusErr)
	}
	var retained, generation int64
	if err := f.store.read.QueryRowContext(t.Context(),
		`SELECT count(*) FROM objects WHERE key = ?`, string(garbage[0])).Scan(&retained); err != nil || retained != 0 {
		t.Errorf("admitted cleanup left %d retired records: %v", retained, err)
	}
	if err := f.store.read.QueryRowContext(t.Context(),
		`SELECT generation FROM database_state WHERE singleton = 1`).Scan(&generation); err != nil || generation != before.Generation+1 {
		t.Errorf("admitted cleanup moved generation %d to %d: %v", before.Generation, generation, err)
	}
	if _, err := f.store.Stat(t.Context(), "file"); err != nil {
		t.Errorf("read after canceled maintenance failed: %v", err)
	}
	if err := f.store.Forget(t.Context(), garbage); err != nil {
		t.Errorf("retired record could not be forgotten after cancellation: %v", err)
	}
	if err := f.store.Close(); err != nil {
		t.Errorf("closing after canceled maintenance failed: %v", err)
	}
}

type observedForgetContext struct {
	context.Context
	seen chan struct{}
	once sync.Once
}

func (c *observedForgetContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.seen) })
	return c.Context.Done()
}

func TestForgetCancellationBeforeAdmissionHasNoEffects(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued %t", queued), func(t *testing.T) {
			f := newPublicationFixture(t)
			f.put(t, t.Context(), "file", 3)
			f.put(t, t.Context(), "file", 7)
			keys, err := f.store.Garbage(t.Context(), 1)
			if err != nil || len(keys) != 1 {
				t.Fatalf("garbage=%v error=%v", keys, err)
			}
			request, cancel := context.WithCancel(t.Context())
			defer cancel()
			var returned error
			if queued {
				if err := f.store.coordinator.commit.acquire(t.Context()); err != nil {
					t.Fatal(err)
				}
				release := sync.OnceFunc(f.store.coordinator.commit.release)
				defer release()
				observed := &observedForgetContext{Context: request, seen: make(chan struct{})}
				done := make(chan error, 1)
				go func() { done <- f.store.Forget(observed, keys) }()
				<-observed.seen
				cancel()
				select {
				case returned = <-done:
				case <-time.After(5 * time.Second):
					release()
					returned = <-done
					t.Error("queued Forget ignored cancellation")
				}
				if f.store.write.Stats().InUse != 0 {
					t.Error("queued cancellation entered a SQL transaction")
				}
				release()
			} else {
				cancel()
				returned = f.store.Forget(request, keys)
			}
			if storage.ErrnoOf(returned) != syscall.EINTR || !errors.Is(returned, context.Canceled) {
				t.Errorf("canceled admission=%v", returned)
			}
			remaining, err := f.store.Garbage(t.Context(), 1)
			if err != nil || len(remaining) != 1 || remaining[0] != keys[0] {
				t.Errorf("canceled admission changed garbage: %v, %v", remaining, err)
			}
			if err := f.store.Forget(t.Context(), keys); err != nil {
				t.Errorf("admission gate remained unusable: %v", err)
			}
		})
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

type closeAdmissionContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *closeAdmissionContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestClosePreservesAFailureFromAdmittedForgetAfterAuthorityRetirement(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	s := opened.Store
	// This case deliberately fences a native owner. After all assertions, release only
	// test-owned handles whose pools have been explicitly drained.
	t.Cleanup(func() {
		if err := errors.Join(s.read.Close(), s.snapshotRead.Close(), s.write.Close()); err != nil {
			t.Error(err)
			return
		}
		if !s.closed {
			if err := s.locks.Close(); err != nil {
				t.Logf("retiring the deliberately fenced test authority: %v", err)
			}
			releaseCoordinator(s.coordinator)
		}
		if err := opened.closeOwnership(); err != nil {
			t.Error(err)
		}
	})
	key, err := s.Reserve(t.Context(), "file", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	// A deferred constraint fails in the real driver COMMIT, after the DELETE and
	// generation update have succeeded. No injected context error chooses the outcome.
	if _, err := s.write.ExecContext(t.Context(), `
		CREATE TABLE forget_commit_failure (
			node INTEGER REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TRIGGER fail_forget_commit AFTER DELETE ON objects BEGIN
			INSERT INTO forget_commit_failure(node) VALUES (-1);
		END;
	`); err != nil {
		t.Fatal(err)
	}
	s.coordinator.health.RLock()
	release := sync.OnceFunc(s.coordinator.health.RUnlock)
	forgotten := make(chan error, 1)
	closed := make(chan error, 1)
	forgetConsumed, closeStarted, closeConsumed := false, false, false
	defer func() {
		release()
		if !forgetConsumed {
			<-forgotten
		}
		if closeStarted && !closeConsumed {
			<-closed
		}
	}()
	go func() { forgotten <- s.Forget(t.Context(), []metastore.Key{key}) }()
	until := time.Now().Add(5 * time.Second)
	for s.coordinator.health.TryRLock() {
		s.coordinator.health.RUnlock()
		select {
		case err := <-forgotten:
			forgetConsumed = true
			t.Fatalf("Forget ended before finalization: %v", err)
		default:
		}
		if time.Now().After(until) {
			t.Fatal("Forget did not reach finalization")
		}
		runtime.Gosched()
	}
	closeContext := &closeAdmissionContext{Context: t.Context(), entered: make(chan struct{})}
	closeStarted = true
	go func() { closed <- opened.CloseContext(closeContext) }()
	select {
	case <-closeContext.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not retire authority and wait for the admitted transaction")
	}
	select {
	case err := <-closed:
		closeConsumed = true
		t.Fatalf("Close did not wait for the admitted transaction: %v", err)
	default:
	}
	release()
	forgetErr := <-forgotten
	forgetConsumed = true
	closeErr := <-closed
	closeConsumed = true
	if storage.ErrnoOf(forgetErr) != syscall.EIO || !sqlerr.IsUncertainCommit(forgetErr) {
		t.Fatalf("real driver COMMIT failure returned %v", forgetErr)
	}
	var commitFailure *sqlerr.UncertainCommitError
	if !errors.As(forgetErr, &commitFailure) {
		t.Fatalf("Forget did not retain its COMMIT failure: %v", forgetErr)
	}
	if storage.ErrnoOf(closeErr) != syscall.EIO || !errors.Is(closeErr, commitFailure) {
		t.Errorf("Close after authority retirement = %v, want the admitted failure %v", closeErr, forgetErr)
	}
	if again := opened.Close(); again != closeErr {
		t.Errorf("repeated Close = %v, want the same result %v", again, closeErr)
	}
	contender, err := nativelease.AcquireDatabase(config.Database, true, false)
	if contender != nil {
		contender.Close()
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Errorf("uncertain cleanup released native database ownership: %v", err)
	}
}

type observedMaintenanceForget struct {
	metastore.Store
	entered  chan context.Context
	finished chan error
}

func (m *observedMaintenanceForget) Forget(ctx context.Context, keys []metastore.Key) error {
	m.entered <- ctx
	err := m.Store.Forget(ctx, keys)
	m.finished <- err
	return err
}

func TestObjectstoreCloseDrainsAdmittedForgetAndCancelsQueuedForget(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(fmt.Sprintf("finalization admitted %t", admitted), func(t *testing.T) {
			config := lockingTestConfig(t)
			opened, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			s := opened.Store
			defer func() {
				if err := opened.Close(); err != nil {
					t.Error(err)
				}
			}()
			key, err := s.Reserve(t.Context(), "file", 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Abandon(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			objects := memory.New()
			if _, err := objects.Put(t.Context(), string(key), []byte("x")); err != nil {
				t.Fatal(err)
			}
			var release func()
			if admitted {
				s.coordinator.health.RLock()
				release = sync.OnceFunc(s.coordinator.health.RUnlock)
			} else {
				if err := s.coordinator.commit.acquire(t.Context()); err != nil {
					t.Fatal(err)
				}
				release = sync.OnceFunc(s.coordinator.commit.release)
			}
			defer release()
			meta := &observedMaintenanceForget{Store: opened, entered: make(chan context.Context, 1), finished: make(chan error, 1)}
			backing := objectstore.New(objects, meta)
			defer func() {
				release()
				if err := backing.Close(); err != nil {
					t.Error(err)
				}
			}()
			var maintenance context.Context
			select {
			case maintenance = <-meta.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("maintenance did not reach native Forget")
			}
			if admitted {
				until := time.Now().Add(5 * time.Second)
				for s.coordinator.health.TryRLock() {
					s.coordinator.health.RUnlock()
					if time.Now().After(until) {
						t.Fatal("maintenance did not reach native finalization")
					}
					runtime.Gosched()
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- backing.Close() }()
			select {
			case <-maintenance.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the maintenance lifetime")
			}
			select {
			case err := <-closed:
				t.Fatalf("Close completed before native drain: %v", err)
			default:
			}
			var forgetErr error
			if !admitted {
				select {
				case forgetErr = <-meta.finished:
				case <-time.After(5 * time.Second):
					t.Fatal("queued Forget did not honor Close cancellation")
				}
				if storage.ErrnoOf(forgetErr) != syscall.EINTR || !errors.Is(forgetErr, context.Canceled) {
					t.Errorf("queued Forget returned %v", forgetErr)
				}
			}
			release()
			if admitted {
				select {
				case forgetErr = <-meta.finished:
				case <-time.After(5 * time.Second):
					t.Fatal("admitted Forget did not finish")
				}
				if forgetErr != nil {
					t.Errorf("admitted Forget returned %v", forgetErr)
				}
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close after native drain: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after native drain")
			}
			config.Initialize = false
			reopened, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			remaining, err := reopened.Garbage(t.Context(), 1)
			want := 1
			if admitted {
				want = 0
			}
			if err != nil || len(remaining) != want || (!admitted && remaining[0] != key) {
				t.Errorf("reopened garbage=%v, error=%v, want %d retained records", remaining, err, want)
			}
		})
	}
}
