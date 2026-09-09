package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

func seededAdmissionReplica(t *testing.T) *Replica {
	t.Helper()
	return seedAdmissionReplica(t, func(err error) {
		if err != nil {
			t.Errorf("closing replica: %v", err)
		}
	})
}

func seedAdmissionReplica(t *testing.T, checkClose func(error)) *Replica {
	t.Helper()
	replica, err := OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		checkClose(replica.Close())
	})
	seeding, err := replica.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer seeding.Close()
	rows := []metastore.Row{
		{Node: metastore.Node{ID: 10, Mode: fs.ModeDir | 0o755}},
		{Parent: 10, Name: []byte("file"), Node: metastore.Node{ID: 11, Mode: 0o644, Size: 7}},
	}
	if err := seeding.Add(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := seeding.Complete(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	return replica
}

var replicaWriters = []struct {
	name string
	run  func(context.Context, *Replica) error
}{
	{"Apply", func(ctx context.Context, replica *Replica) error {
		_, err := replica.Apply(ctx, metastore.Change{Position: 1})
		return err
	}},
	{"Reseed", func(ctx context.Context, replica *Replica) error {
		seeding, err := replica.Reseed(ctx)
		if err != nil {
			return err
		}
		return seeding.Close()
	}},
}

func TestReplicaWaitingWriterCancellationReopensReaderAdmission(t *testing.T) {
	for _, writer := range replicaWriters {
		t.Run(writer.name, func(t *testing.T) {
			replica := seededAdmissionReplica(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			result, err := storage.NewListResult(1024, 0,
				func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
					close(entered)
					<-release
					return nameBytes + 64, nil
				})
			if err != nil {
				t.Fatal(err)
			}
			listed := make(chan error, 1)
			go func() { listed <- replica.ListBounded(t.Context(), "", result) }()
			defer func() {
				close(release)
				if err := receiveReplica(t, listed); err != nil {
					t.Errorf("completing the held listing: %v", err)
				}
			}()
			receiveReplica(t, entered)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waiting := observeReplicaWait(ctx)
			written := make(chan error, 1)
			go func() { written <- writer.run(waiting, replica) }()
			receiveReplica(t, waiting.entered)
			readerContext := observeReplicaWait(t.Context())
			read := make(chan error, 1)
			go func() { _, err := replica.Stat(readerContext, "file"); read <- err }()
			receiveReplica(t, readerContext.entered)
			waitForReplicaReaders(t, &replica.admission, 1)
			replica.admission.mu.Lock()
			pending := replica.admission.pendingWriters
			replica.admission.mu.Unlock()
			if pending != 1 {
				t.Fatalf("writer was not registered: %d", pending)
			}
			cancel()
			if err := receiveReplica(t, written); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled writer returned %v", err)
			}
			if err := receiveReplica(t, read); err != nil {
				t.Fatalf("queued reader was not admitted after writer cancellation: %v", err)
			}
			if _, err := replica.Stat(t.Context(), "file"); err != nil {
				t.Fatalf("new reader could not join the still-active reader: %v", err)
			}
			if err := replica.store.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			replica.store.coordinator.commit.release()
		})
	}
}

func TestReplicaWriterCancellationAtCommitGateReleasesAdmission(t *testing.T) {
	for _, writer := range replicaWriters {
		t.Run(writer.name, func(t *testing.T) {
			replica := seededAdmissionReplica(t)
			commit := replica.store.coordinator.commit
			if err := commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waiting := observeReplicaWait(ctx)
			written := make(chan error, 1)
			go func() { written <- writer.run(waiting, replica) }()
			receiveReplica(t, waiting.entered)
			replica.admission.mu.Lock()
			owned := replica.admission.writerActive
			replica.admission.mu.Unlock()
			if !owned {
				t.Fatal("writer reached commit wait without owning replica admission")
			}
			cancel()
			if err := receiveReplica(t, written); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled writer returned %v", err)
			}
			requireReplicaGateIdle(t, &replica.admission)
			if len(commit) != 0 {
				t.Fatal("canceled waiter released a commit grant it did not own")
			}
			commit.release()
			if err := writer.run(t.Context(), replica); err != nil {
				t.Fatalf("writer could not acquire both gates after cancellation: %v", err)
			}
			requireReplicaGateIdle(t, &replica.admission)
		})
	}
}

func TestReplicaReadersCancelBehindSeeding(t *testing.T) {
	for _, method := range []string{"Stat", "List", "ListBounded"} {
		t.Run(method, func(t *testing.T) {
			replica := seededAdmissionReplica(t)
			seeding, err := replica.Reseed(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer seeding.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waiting := observeReplicaWait(ctx)
			result, err := storage.NewListResult(1024, 0,
				func(_ int, nameBytes int64, _ storage.Attr) (int64, error) { return nameBytes + 64, nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Add(storage.Entry{Name: "prefix"}); err != nil {
				t.Fatal(err)
			}
			read := make(chan error, 1)
			go func() {
				var err error
				switch method {
				case "Stat":
					_, err = replica.Stat(waiting, "file")
				case "List":
					_, err = replica.List(waiting, "")
				case "ListBounded":
					err = replica.ListBounded(waiting, "", result)
				}
				read <- err
			}()
			receiveReplica(t, waiting.entered)
			waitForReplicaReaders(t, &replica.admission, 1)
			cancel()
			err = receiveReplica(t, read)
			if len(replica.readSlots) != 0 {
				t.Fatal("reader canceled at the phase gate retained its SQL permit")
			}
			var pathErr *fs.PathError
			if !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR ||
				errors.Is(err, syscall.EIO) || !errors.As(err, &pathErr) {
				t.Fatalf("canceled read lost its path/error chain: %v", err)
			}
			wantOp, wantPath := "list", ""
			if method == "Stat" {
				wantOp, wantPath = "stat", "file"
			}
			if pathErr.Op != wantOp || pathErr.Path != wantPath {
				t.Fatalf("unexpected path error: %+v", pathErr)
			}
			if method == "ListBounded" {
				if entries, resultErr := result.Entries(); entries != nil || !errors.Is(resultErr, context.Canceled) {
					t.Fatalf("canceled listing exposed prefix entries=%v err=%v", entries, resultErr)
				}
			}
			if err := seeding.Close(); err != nil {
				t.Fatal(err)
			}
			requireReplicaGateIdle(t, &replica.admission)
			if _, err := replica.Stat(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicaSeedingPublishesTreeAndPositionTogether(t *testing.T) {
	for _, outcome := range []string{"complete", "rollback", "invalid row", "invalid completion"} {
		t.Run(outcome, func(t *testing.T) {
			var commitFailure *sqlerr.UncertainCommitError
			var replica *Replica
			replica = seedAdmissionReplica(t, func(err error) {
				if commitFailure == nil {
					if err != nil {
						t.Errorf("closing replica: %v", err)
					}
					return
				}
				if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, commitFailure) {
					t.Errorf("closing failed seeding returned %v, want its original COMMIT failure", err)
				}
				if again := replica.Close(); again != err {
					t.Errorf("repeated replica Close returned %v, want the cached result %v", again, err)
				}
				if !replica.store.Terminal() || replica.store.write.Stats().OpenConnections != 0 ||
					replica.store.read.Stats().OpenConnections != 0 || replica.store.snapshotRead.Stats().OpenConnections != 0 {
					t.Error("replica Close retained SQL pools after reporting the COMMIT failure")
				}
			})
			seeding, err := replica.Reseed(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer seeding.Close()
			root := metastore.Row{Node: metastore.Node{ID: 20, Mode: fs.ModeDir | 0o700}}
			if err := seeding.Add(t.Context(), []metastore.Row{root}); err != nil {
				t.Fatal(err)
			}
			readContext := observeReplicaWait(t.Context())
			type observation struct {
				children []metastore.Child
				position metastore.Position
				err      error
			}
			observed := make(chan observation, 1)
			go func() {
				children, err := replica.List(readContext, "")
				observed <- observation{children, replica.Position(), err}
			}()
			receiveReplica(t, readContext.entered)
			position := make(chan metastore.Position, 1)
			go func() { position <- replica.Position() }()
			waitForReplicaReaders(t, &replica.admission, 2)
			select {
			case at := <-position:
				t.Fatalf("Position returned %d during an incomplete picture", at)
			default:
			}

			switch outcome {
			case "complete":
				rows := []metastore.Row{{Parent: 20, Name: []byte("replacement"), Node: metastore.Node{ID: 21, Mode: 0o600, Size: 19}}}
				if err := seeding.Add(t.Context(), rows); err != nil {
					t.Fatal(err)
				}
				if err := seeding.Complete(t.Context(), 2); err != nil {
					t.Fatal(err)
				}
			case "invalid row":
				if err := seeding.Add(t.Context(), []metastore.Row{root}); !errors.Is(err, syscall.EIO) {
					t.Fatalf("duplicate row returned %v", err)
				}
			case "invalid completion":
				rows := []metastore.Row{{Parent: 999, Name: []byte("orphan"), Node: metastore.Node{ID: 21, Mode: 0o600}}}
				if err := seeding.Add(t.Context(), rows); err != nil {
					t.Fatal(err)
				}
				if err := seeding.Complete(t.Context(), 2); !errors.Is(err, syscall.EIO) || !errors.As(err, &commitFailure) {
					t.Fatalf("unresolved parent returned %v", err)
				}
			}
			if err := seeding.Close(); err != nil {
				t.Fatal(err)
			}
			if err := seeding.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
			got := receiveReplica(t, observed)
			at := receiveReplica(t, position)
			requireReplicaGateIdle(t, &replica.admission)
			if outcome == "invalid completion" {
				if !errors.Is(got.err, syscall.EIO) || !errors.Is(got.err, commitFailure) || got.children != nil || at != 1 || got.position != 1 {
					t.Fatalf("uncertain commit exposed a tree or advanced position: %+v, Position=%d", got, at)
				}
				return
			}
			wantName, wantSize, wantPosition := "file", int64(7), metastore.Position(1)
			if outcome == "complete" {
				wantName, wantSize, wantPosition = "replacement", 19, 2
			}
			if got.err != nil || len(got.children) != 1 || string(got.children[0].Name) != wantName || got.children[0].Node.Size != wantSize || got.position != wantPosition || at != wantPosition {
				t.Fatalf("incoherent tree and position: %+v, Position=%d; want %s size=%d at=%d", got, at, wantName, wantSize, wantPosition)
			}
		})
	}
}

func waitForReplicaReaders(t *testing.T, gate *replicaGate, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		gate.mu.Lock()
		waiting := gate.waitingReaders
		gate.mu.Unlock()
		if waiting == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica has %d waiting readers, want %d", waiting, count)
		}
		runtime.Gosched()
	}
}

func TestReplicaReadBatchProgressesThroughApplyBacklog(t *testing.T) {
	replica := seededAdmissionReplica(t)
	if err := replica.admission.acquireRead(t.Context()); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	written := make(chan error, writers)
	node := metastore.Node{ID: 11, Mode: 0o644, Size: 19}
	for range writers {
		waiting := observeReplicaWait(t.Context())
		go func() {
			_, err := replica.Apply(waiting, metastore.Change{Position: 2, Kind: metastore.Modified, Node: &node})
			written <- err
		}()
		receiveReplica(t, waiting.entered)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := storage.NewListResult(1024, 0,
		func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
			close(entered)
			<-release
			return nameBytes + 64, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	waiting := observeReplicaWait(t.Context())
	listed := make(chan error, 1)
	go func() { listed <- replica.ListBounded(waiting, "", result) }()
	receiveReplica(t, waiting.entered)
	waitForReplicaReaders(t, &replica.admission, 1)
	replica.admission.releaseRead()
	receiveReplica(t, entered)
	replica.admission.mu.Lock()
	pending := replica.admission.pendingWriters
	activeReaders := replica.admission.activeReaders
	replica.admission.mu.Unlock()
	close(release)
	if err := receiveReplica(t, listed); err != nil {
		t.Fatal(err)
	}
	for range writers {
		if err := receiveReplica(t, written); err != nil {
			t.Fatal(err)
		}
	}
	if pending != writers-1 || activeReaders != 1 {
		t.Fatalf("reader did not run between exclusive grants: pending=%d readers=%d", pending, activeReaders)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Attr.Size != 19 {
		t.Fatalf("reader cohort saw entries=%+v err=%v", entries, err)
	}
	requireReplicaGateIdle(t, &replica.admission)
}

func TestReplicaReadPermitsMatchTheReaderPool(t *testing.T) {
	replica := seededAdmissionReplica(t)
	capacity := replica.store.read.Stats().MaxOpenConnections
	if capacity <= 0 || cap(replica.readSlots) != capacity {
		t.Fatalf("read permits=%d, reader pool=%d", cap(replica.readSlots), capacity)
	}
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Stat(t.Context(), "file"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading a closed replica: %v", err)
	}
	if len(replica.readSlots) != 0 {
		t.Fatal("failed read retained its permit after Close")
	}
	requireReplicaGateIdle(t, &replica.admission)
}

func TestReplicaReadersCancelBeforeThePhaseWhenPermitsAreFull(t *testing.T) {
	for _, method := range []string{"Stat", "List", "ListBounded"} {
		t.Run(method, func(t *testing.T) {
			replica := seededAdmissionReplica(t)
			for range cap(replica.readSlots) {
				if err := replica.acquireRead(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				for range cap(replica.readSlots) {
					replica.releaseRead()
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waiting := observeReplicaWait(ctx)
			result, err := storage.NewListResult(1024, 0,
				func(_ int, nameBytes int64, _ storage.Attr) (int64, error) { return nameBytes + 64, nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Add(storage.Entry{Name: "prefix"}); err != nil {
				t.Fatal(err)
			}
			read := make(chan error, 1)
			go func() {
				var err error
				switch method {
				case "Stat":
					_, err = replica.Stat(waiting, "file")
				case "List":
					_, err = replica.List(waiting, "")
				case "ListBounded":
					err = replica.ListBounded(waiting, "", result)
				}
				read <- err
			}()
			receiveReplica(t, waiting.entered)
			cancel()
			err = receiveReplica(t, read)
			var pathErr *fs.PathError
			if !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR ||
				errors.Is(err, syscall.EIO) || !errors.As(err, &pathErr) {
				t.Fatalf("canceled permit wait lost its path/error chain: %v", err)
			}
			replica.admission.mu.Lock()
			active, queued := replica.admission.activeReaders, replica.admission.waitingReaders
			replica.admission.mu.Unlock()
			if active != cap(replica.readSlots) || queued != 0 || len(replica.readSlots) != cap(replica.readSlots) {
				t.Fatalf("canceled permit waiter entered the phase: active=%d queued=%d permits=%d", active, queued, len(replica.readSlots))
			}
			if method == "ListBounded" {
				if entries, resultErr := result.Entries(); entries != nil || !errors.Is(resultErr, context.Canceled) {
					t.Fatalf("canceled listing exposed entries=%v err=%v", entries, resultErr)
				}
			}
		})
	}
}

func TestReplicaCancellationAfterPermitHandoffReturnsThePermit(t *testing.T) {
	replica := &Replica{admission: newReplicaGate(), readSlots: make(chan struct{}, 1)}
	replica.readSlots <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := observeReplicaWait(ctx)
	acquired := make(chan error, 1)
	go func() { acquired <- replica.acquireRead(waiting) }()
	receiveReplica(t, waiting.entered)
	replica.admission.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(replica.admission.mu.Unlock) }
	defer unlock()
	<-replica.readSlots
	deadline := time.Now().Add(5 * time.Second)
	for len(replica.readSlots) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("waiting reader did not receive the released permit")
		}
		runtime.Gosched()
	}
	cancel()
	unlock()
	if err := receiveReplica(t, acquired); !errors.Is(err, context.Canceled) {
		t.Fatalf("reader canceled after permit handoff returned %v", err)
	}
	if len(replica.readSlots) != 0 {
		t.Fatal("canceled reader retained its transferred permit")
	}
	requireReplicaGateIdle(t, &replica.admission)
	if err := replica.acquireRead(t.Context()); err != nil {
		t.Fatal(err)
	}
	replica.releaseRead()
	requireReplicaGateIdle(t, &replica.admission)
}

func TestReplicaPositionAndWritersDoNotUseSQLReadPermits(t *testing.T) {
	replica := seededAdmissionReplica(t)
	for range cap(replica.readSlots) {
		replica.readSlots <- struct{}{}
	}
	defer func() {
		for range cap(replica.readSlots) {
			<-replica.readSlots
		}
	}()
	position := make(chan uint64, 1)
	go func() { position <- uint64(replica.Position()) }()
	if got := receiveReplica(t, position); got != 1 {
		t.Fatalf("Position=%d", got)
	}
	for _, writer := range replicaWriters {
		written := make(chan error, 1)
		go func() { written <- writer.run(t.Context(), replica) }()
		if err := receiveReplica(t, written); err != nil {
			t.Fatalf("%s: %v", writer.name, err)
		}
	}
	if len(replica.readSlots) != cap(replica.readSlots) {
		t.Fatal("Position or a writer changed SQL read permit ownership")
	}
	requireReplicaGateIdle(t, &replica.admission)
}

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
