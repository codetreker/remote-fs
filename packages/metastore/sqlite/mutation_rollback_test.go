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

	"github.com/codetreker/remote-fs/packages/storage"
)

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
