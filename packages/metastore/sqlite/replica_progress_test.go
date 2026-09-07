package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestReplicaWriterProgressUnderContinuousListings(t *testing.T) {
	replica, err := OpenReplica(t.Context(), filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := replica.Close(); err != nil {
			t.Errorf("closing replica: %v", err)
		}
	}()
	seeding, err := replica.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer seeding.Close()
	root := metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755}
	rows := []metastore.Row{{Node: root}}
	for i := range 4096 {
		rows = append(rows, metastore.Row{
			Parent: 1, Name: []byte(fmt.Sprintf("file-%04d", i)),
			Node: metastore.Node{ID: int64(i + 2), Mode: 0o644},
		})
	}
	if err := seeding.Add(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := seeding.Complete(t.Context(), 1); err != nil {
		t.Fatal(err)
	}

	var stopped atomic.Bool
	stopReaders := func() { stopped.Store(true) }
	var group sync.WaitGroup
	var completed atomic.Int64
	failures := make(chan error, 128)
	var held []*sql.Conn
	var releaseOnce sync.Once
	releaseReaders := func() {
		releaseOnce.Do(func() {
			for _, connection := range held {
				if err := connection.Close(); err != nil {
					t.Errorf("releasing held reader connection: %v", err)
				}
			}
		})
	}
	defer releaseReaders()
	capacity := replica.store.read.Stats().MaxOpenConnections
	for range capacity {
		connection, err := replica.store.read.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, connection)
	}
	waiters := make([]*replicaWaitContext, 128)
	for i := range waiters {
		waiters[i] = observeReplicaWait(t.Context())
		group.Go(func() {
			for !stopped.Load() {
				children, err := replica.List(waiters[i], "")
				if err != nil {
					failures <- err
					return
				}
				if len(children) != 4096 {
					failures <- fmt.Errorf("listing returned %d entries, want 4096", len(children))
					return
				}
				completed.Add(1)
			}
		})
	}
	defer func() {
		releaseReaders()
		stopReaders()
		group.Wait()
	}()
	for _, waiter := range waiters {
		receiveReplica(t, waiter.entered)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		replica.admission.mu.Lock()
		active := replica.admission.activeReaders
		waiting := replica.admission.waitingReaders
		replica.admission.mu.Unlock()
		if active == capacity {
			if waiting != 0 || len(replica.readSlots) != capacity {
				t.Fatalf("pool waiters reached the phase gate: waiting=%d permits=%d", waiting, len(replica.readSlots))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d reader calls reached replica admission, want %d", active, capacity)
		}
		runtime.Gosched()
	}
	root.Mode = fs.ModeDir | 0o700
	change := metastore.Change{Position: 2, Kind: metastore.Modified, Node: &root}
	// Already admitted scans must finish before Apply can enter. The bound includes
	// one full reader pool under the race detector; replication latency has a separate test.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	writer := observeReplicaWait(ctx)
	type appliedChange struct {
		applied bool
		err     error
	}
	written := make(chan appliedChange, 1)
	go func() {
		applied, err := replica.Apply(writer, change)
		written <- appliedChange{applied, err}
	}()
	receiveReplica(t, writer.entered)
	started := time.Now()
	releaseReaders()
	outcome := <-written
	elapsed := time.Since(started)
	applied, applyErr := outcome.applied, outcome.err
	cancel()
	stopReaders()
	group.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent listing failed: %v", err)
	}
	if !applied || applyErr != nil {
		t.Fatalf("Apply under continuous listings: applied=%v err=%v after %v", applied, applyErr, elapsed)
	}
	got, err := replica.Stat(t.Context(), "")
	if err != nil || got.Mode != root.Mode || replica.Position() != 2 {
		t.Fatalf("applied change was not visible: root=%+v err=%v position=%d", got, err, replica.Position())
	}
	t.Logf("Apply completed in %v while 128 readers completed %d listings of 4096 files", elapsed, completed.Load())
}
