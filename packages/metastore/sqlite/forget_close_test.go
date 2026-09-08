package sqlite

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

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
	if storage.ErrnoOf(forgetErr) != syscall.EIO || !isUncertainCommit(forgetErr) {
		t.Fatalf("real driver COMMIT failure returned %v", forgetErr)
	}
	var commitFailure *uncertainCommitError
	if !errors.As(forgetErr, &commitFailure) {
		t.Fatalf("Forget did not retain its COMMIT failure: %v", forgetErr)
	}
	if storage.ErrnoOf(closeErr) != syscall.EIO || !errors.Is(closeErr, commitFailure) {
		t.Errorf("Close after authority retirement = %v, want the admitted failure %v", closeErr, forgetErr)
	}
	if again := opened.Close(); again != closeErr {
		t.Errorf("repeated Close = %v, want the same result %v", again, closeErr)
	}
	contender, err := acquireLeaseDatabase(config.Database, true, false)
	if contender != nil {
		contender.Close()
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Errorf("uncertain cleanup released native database ownership: %v", err)
	}
}
