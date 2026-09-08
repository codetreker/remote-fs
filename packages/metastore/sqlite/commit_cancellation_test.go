package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
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

type commitCancellationWitness struct {
	cancel context.CancelFunc
	fault  error
	calls  int
}

func (w *commitCancellationWitness) Accept(DurableState) error {
	w.calls++
	w.cancel()
	return w.fault
}

func (*commitCancellationWitness) Checkpoint(DurableState) error { return nil }

func TestCancellationAfterCommitPreservesTheDurabilityOutcome(t *testing.T) {
	for _, failAcceptance := range []bool{false, true} {
		t.Run(fmt.Sprintf("acceptance failed %t", failAcceptance), func(t *testing.T) {
			f := newPublicationFixture(t)
			f.put(t, t.Context(), "file", 3)
			before := f.node(t, "file")
			object := f.stage(t, t.Context(), "file", 7)
			owner := f.owner(t)
			grant := f.grant(t, owner, "file", locking.Exclusive)
			request, cancel := context.WithCancel(t.Context())
			defer cancel()
			witness := &commitCancellationWitness{cancel: cancel}
			if failAcceptance {
				witness.fault = errors.New("accepted state could not be persisted")
				*f.authorityFence = witness.fault
			}
			originalWitness := f.store.witness
			f.store.witness = witness
			defer func() { f.store.witness = originalWitness }()
			err := f.store.Commit(publicationScope(request, owner, grant), "file", object)
			if witness.calls != 1 || request.Err() != context.Canceled {
				t.Fatalf("commit reached acceptance %d times with request error %v", witness.calls, request.Err())
			}
			if failAcceptance {
				if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witness.fault) {
					t.Errorf("failed acceptance after request cancellation = %v, want original durability failure", err)
				}
			} else if err != nil {
				t.Errorf("acknowledged commit became cancellation: %v", err)
			}
			status, statusErr := f.store.locks.Status(t.Context())
			if statusErr != nil || status.Unavailable != failAcceptance {
				t.Errorf("acceptance left authority status %+v, error %v", status, statusErr)
			}
			var size int64
			var content string
			if err := f.store.read.QueryRowContext(t.Context(),
				`SELECT size, content FROM nodes WHERE id = ?`, before.ID).Scan(&size, &content); err != nil ||
				size != object.Size || content != string(object.Key) {
				t.Errorf("physical COMMIT did not precede acceptance: size=%d content=%q error=%v", size, content, err)
			}
			_, readErr := f.store.Stat(t.Context(), "file")
			if failAcceptance {
				if storage.ErrnoOf(readErr) != syscall.EIO || !errors.Is(readErr, witness.fault) {
					t.Errorf("unconfirmed committed state was not fenced: %v", readErr)
				}
			} else if readErr != nil {
				t.Errorf("confirmed state became unavailable after cancellation: %v", readErr)
			}
		})
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

type cancelForgetStatementContext struct {
	context.Context
	cancel context.CancelFunc
	writer *sql.DB
	once   sync.Once
}

func (c *cancelForgetStatementContext) Done() <-chan struct{} {
	if c.writer.Stats().InUse != 0 {
		c.once.Do(c.cancel)
	}
	return c.Context.Done()
}

func TestForgetStagingCancellationRollsBackExplicitly(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	f.put(t, t.Context(), "file", 7)
	keys, err := f.store.Garbage(t.Context(), 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("garbage=%v error=%v", keys, err)
	}
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := &cancelForgetStatementContext{Context: request, cancel: cancel, writer: f.store.write}
	err = f.store.Forget(observed, keys)
	if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || request.Err() != context.Canceled {
		t.Errorf("staging cancellation=%v, request=%v", err, request.Err())
	}
	remaining, err := f.store.Garbage(t.Context(), 1)
	if err != nil || len(remaining) != 1 || remaining[0] != keys[0] {
		t.Errorf("staging cancellation changed garbage: %v, %v", remaining, err)
	}
	if f.store.write.Stats().InUse != 0 {
		t.Error("staging cancellation retained the SQL transaction")
	}
	if err := f.store.Forget(t.Context(), keys); err != nil {
		t.Errorf("explicit rollback left the store unusable: %v", err)
	}
}
