package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
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
		{Node: metastore.Node{ID: 10, Kind: storage.NodeDirectory, DirectoryRevision: []byte{0, 0, 0, 0, 0, 0, 0, 1}}},
		{Parent: 10, Name: []byte("file"), Node: metastore.Node{ID: 11, Kind: storage.NodeRegular, Size: 7}},
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
				func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
					close(entered)
					<-release
					return nameBytes + metadataBytes + 64, nil
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
				func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
					return nameBytes + metadataBytes + 64, nil
				})
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
			root := metastore.Row{Node: metastore.Node{ID: 20, Kind: storage.NodeDirectory, DirectoryRevision: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}
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
				rows := []metastore.Row{{Parent: 20, Name: []byte("replacement"), Node: metastore.Node{ID: 21, Kind: storage.NodeRegular, Size: 19}}}
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
				rows := []metastore.Row{{Parent: 999, Name: []byte("orphan"), Node: metastore.Node{ID: 21, Kind: storage.NodeRegular}}}
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
	node := metastore.Node{ID: 11, Kind: storage.NodeRegular, Size: 19}
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
		func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			close(entered)
			<-release
			return nameBytes + metadataBytes + 64, nil
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
				func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
					return nameBytes + metadataBytes + 64, nil
				})
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
			if method != "Stat" {
				deadline := time.Now().Add(5 * time.Second)
				for len(replica.listingSlots) != 1 {
					if time.Now().After(deadline) {
						t.Fatal("listing did not hold its quota while waiting for a shared permit")
					}
					runtime.Gosched()
				}
			}
			cancel()
			err = receiveReplica(t, read)
			if len(replica.listingSlots) != 0 {
				t.Fatal("canceled listing retained its scan quota")
			}
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
	seedStarted := time.Now()
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
	root := metastore.Node{ID: 1, Kind: storage.NodeDirectory, DirectoryRevision: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	rows := []metastore.Row{{Node: root}}
	for i := range 4096 {
		rows = append(rows, metastore.Row{
			Parent: 1, Name: []byte(fmt.Sprintf("file-%04d", i)),
			Node: metastore.Node{ID: int64(i + 2), Kind: storage.NodeRegular},
		})
	}
	if err := seeding.Add(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := seeding.Complete(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	seedElapsed := time.Since(seedStarted)

	readerContext, cancelReaders := context.WithCancelCause(t.Context())
	readerStop := errors.New("reader workload stopped")
	var stopped atomic.Bool
	stopReaders := func() {
		stopped.Store(true)
		cancelReaders(readerStop)
	}
	defer stopReaders()
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
		waiters[i] = observeReplicaWait(readerContext)
		group.Go(func() {
			for !stopped.Load() {
				children, err := replica.List(waiters[i], "")
				if err != nil {
					if stopped.Load() && context.Cause(readerContext) == readerStop &&
						errors.Is(err, context.Canceled) && storage.ErrnoOf(err) == syscall.EINTR {
						return
					}
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
		stopReaders()
		releaseReaders()
		group.Wait()
	}()
	for _, waiter := range waiters {
		receiveReplica(t, waiter.entered)
	}
	scanCapacity := cap(replica.listingSlots)
	deadline := time.Now().Add(5 * time.Second)
	for {
		replica.admission.mu.Lock()
		active := replica.admission.activeReaders
		waiting := replica.admission.waitingReaders
		replica.admission.mu.Unlock()
		if active == scanCapacity {
			if waiting != 0 || len(replica.readSlots) != scanCapacity {
				t.Fatalf("pool waiters reached the phase gate: waiting=%d permits=%d", waiting, len(replica.readSlots))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d reader calls reached replica admission, want %d", active, scanCapacity)
		}
		runtime.Gosched()
	}
	root.Metadata = replicaTestMetadata(7)
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
	shutdownStarted := time.Now()
	applied, applyErr := outcome.applied, outcome.err
	cancel()
	stopReaders()
	group.Wait()
	shutdownElapsed := time.Since(shutdownStarted)
	requireReplicaGateIdle(t, &replica.admission)
	if permits := len(replica.readSlots) + len(replica.listingSlots); permits != 0 {
		t.Fatalf("reader shutdown retained %d permits", permits)
	}
	close(failures)
	for err := range failures {
		t.Errorf("concurrent listing failed: %v", err)
	}
	if !applied || applyErr != nil {
		t.Fatalf("Apply under continuous listings: applied=%v err=%v after %v", applied, applyErr, elapsed)
	}
	got, err := replica.Stat(t.Context(), "")
	if err != nil || !reflect.DeepEqual(got.Metadata, root.Metadata) || replica.Position() != 2 {
		t.Fatalf("applied change was not visible: root=%+v err=%v position=%d", got, err, replica.Position())
	}
	t.Logf("Seeding completed in %v; reader shutdown after Apply took %v", seedElapsed, shutdownElapsed)
	t.Logf("Apply completed in %v while 128 readers completed %d listings of 4096 files", elapsed, completed.Load())
}

func TestReplicaAppliesEveryChangeWithoutLosingIdentityOrRollback(t *testing.T) {
	r := seededAdmissionReplica(t)
	ctx := t.Context()
	node := metastore.Node{ID: 12, Kind: storage.NodeRegular, Size: 3, AccessTime: time.Unix(100, 0), ModTime: time.Unix(200, 0)}
	apply := func(change metastore.Change) {
		t.Helper()
		changed, err := r.Apply(ctx, change)
		if err != nil || !changed || r.Position() != change.Position {
			t.Fatalf("apply %+v = %v, %v; position %d", change, changed, err, r.Position())
		}
	}
	apply(metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("new"), Node: &node})
	if got, err := r.Stat(ctx, "new"); err != nil || !replicaNodesEqual(got, node) {
		t.Fatalf("created node = %+v, %v", got, err)
	}
	node.Size, node.Metadata = 9, replicaTestMetadata(64)
	apply(metastore.Change{Position: 3, Kind: metastore.Modified, Parent: 10, Name: []byte("new"), Node: &node})
	if got, err := r.Stat(ctx, "new"); err != nil || !replicaNodesEqual(got, node) {
		t.Fatalf("modified node = %+v, %v", got, err)
	}
	node.Metadata, node.ModTime = replicaTestMetadata(66), time.Unix(300, 0)
	apply(metastore.Change{Position: 4, Kind: metastore.Renamed, Parent: 10, Name: []byte("renamed"), From: &metastore.Location{Parent: 10, Name: []byte("new")}, Node: &node})
	if got, err := r.Stat(ctx, "renamed"); err != nil || !replicaNodesEqual(got, node) {
		t.Fatalf("renamed node = %+v, %v", got, err)
	}
	if _, err := r.Stat(ctx, "new"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old name = %v", err)
	}
	for _, change := range []metastore.Change{
		{Kind: metastore.Created, Parent: 10, Name: []byte("missing-node")},
		{Kind: metastore.Renamed, Parent: 10, Name: []byte("missing-from"), Node: &node},
		{Kind: metastore.Removed, Parent: 10, Name: []byte("absent")},
		{Kind: metastore.Renamed, Parent: 10, Name: []byte("other"), From: &metastore.Location{Parent: 10, Name: []byte("absent")}, Node: &node},
		{Kind: metastore.Renamed, Parent: 10, Name: []byte("file"), From: &metastore.Location{Parent: 10, Name: []byte("renamed")}, Node: &node},
		{Kind: metastore.Created, Parent: 10, Name: []byte("reused-id"), Node: &node},
		{Kind: 99, Node: &node},
	} {
		change.Position = 5
		if changed, err := r.Apply(ctx, change); changed || !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid change %+v = %v, %v", change, changed, err)
		}
		if r.Position() != 4 {
			t.Fatalf("failed change advanced position to %d", r.Position())
		}
		if got, err := r.Stat(ctx, "renamed"); err != nil || !replicaNodesEqual(got, node) {
			t.Fatalf("failed change altered node: %+v, %v", got, err)
		}
		if children, err := r.List(ctx, ""); err != nil || len(children) != 2 {
			t.Fatalf("failed change altered directory: %+v, %v", children, err)
		}
	}
	apply(metastore.Change{Position: 5, Kind: metastore.Removed, Parent: 10, Name: []byte("renamed")})
	if _, err := r.Stat(ctx, "renamed"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("removed node = %v", err)
	}
	if changed, err := r.Apply(ctx, metastore.Change{Position: 5}); changed || err != nil {
		t.Fatalf("replayed position = %v, %v", changed, err)
	}
	if children, err := r.List(ctx, ""); err != nil || len(children) != 1 || string(children[0].Name) != "file" {
		t.Fatalf("unrelated node = %+v, %v", children, err)
	}
}

func replicaTestMetadata(marker byte) map[string]storage.OpaquePayload {
	return map[string]storage.OpaquePayload{"test.v1": {Version: []byte{marker}, Data: []byte{marker, 0, 255}}}
}

func replicaNodesEqual(got, want metastore.Node) bool {
	sameTime := func(a, b *time.Time) bool {
		if a == nil || b == nil {
			return a == nil && b == nil
		}
		return a.Equal(*b)
	}
	return got.ID == want.ID && got.Kind == want.Kind && got.Size == want.Size && got.Content == want.Content &&
		got.AccessTime.Equal(want.AccessTime) && got.ModTime.Equal(want.ModTime) && sameTime(got.BirthTime, want.BirthTime) && sameTime(got.ChangeTime, want.ChangeTime) &&
		reflect.DeepEqual(got.Metadata, want.Metadata) && bytes.Equal(got.LinkTarget, want.LinkTarget) && bytes.Equal(got.DirectoryRevision, want.DirectoryRevision)
}

func TestReplicaPreservesGenericSourceFactsAndUnknownDirectoryRevision(t *testing.T) {
	r := seededAdmissionReplica(t)
	birth := time.Date(10000, 1, 2, 3, 4, 5, 123, time.UTC)
	changed := time.Time{}
	link := metastore.Node{ID: 12, Kind: storage.NodeSymlink, Size: 9, LinkTarget: []byte("../target"), BirthTime: &birth, ChangeTime: &changed, Metadata: replicaTestMetadata(9)}
	if applied, err := r.Apply(t.Context(), metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("link"), Node: &link}); err != nil || !applied {
		t.Fatalf("create link=%v %v", applied, err)
	}
	got, err := r.Stat(t.Context(), "link")
	if err != nil || !replicaNodesEqual(got, link) {
		t.Fatalf("replicated link=%+v error=%v", got, err)
	}
	link.Metadata["test.v1"].Data[0] = 99
	link.LinkTarget[0] = 'x'
	if got.Metadata["test.v1"].Data[0] != 9 || string(got.LinkTarget) != "../target" {
		t.Fatal("replica aliases input source payload")
	}
	root, err := r.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	root.DirectoryRevision = nil
	root.BirthTime = nil
	root.ChangeTime = nil
	root.Metadata = replicaTestMetadata(2)
	if applied, err := r.Apply(t.Context(), metastore.Change{Position: 3, Kind: metastore.Modified, Node: &root}); err != nil || !applied {
		t.Fatalf("historical directory modification=%v %v", applied, err)
	}
	got, err = r.Stat(t.Context(), "")
	if err != nil || !replicaNodesEqual(got, root) || len(got.DirectoryRevision) != 0 {
		t.Fatalf("unknown source directory token was invented: %+v %v", got, err)
	}
	file := metastore.Node{ID: 13, Kind: storage.NodeRegular, Size: 3, Content: "source-object-key", Metadata: replicaTestMetadata(4)}
	if applied, err := r.Apply(t.Context(), metastore.Change{Position: 4, Kind: metastore.Created, Parent: 10, Name: []byte("remote-bytes"), Node: &file}); err != nil || !applied {
		t.Fatalf("source content metadata=%v %v", applied, err)
	}
	got, err = r.Stat(t.Context(), "remote-bytes")
	file.Content = ""
	if err != nil || !replicaNodesEqual(got, file) {
		t.Fatalf("replica retained a content key or lost source attributes: %+v %v", got, err)
	}
	missing := root
	missing.ID = 9999
	if applied, err := r.Apply(t.Context(), metastore.Change{Position: 5, Kind: metastore.Modified, Node: &missing}); applied || !errors.Is(err, syscall.EIO) || r.Position() != 4 {
		t.Fatalf("missing source identity=%v %v position=%d", applied, err, r.Position())
	}
}

func TestReplicaMetadataBudgetPrecedesSeedAndApplyPublication(t *testing.T) {
	r := seededAdmissionReplica(t)
	link := metastore.Node{ID: 12, Kind: storage.NodeSymlink, Size: 6, LinkTarget: []byte("target"), Metadata: replicaTestMetadata(3)}
	encoded, err := storage.EncodeMetadata(link.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	const existingMetadata = 12
	charge := int64(len(encoded) + len(link.LinkTarget))
	r.store.maxMetadataBytes = existingMetadata + charge - 1
	change := metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("link"), Node: &link}
	if applied, err := r.Apply(t.Context(), change); applied || !errors.Is(err, syscall.EFBIG) || r.Position() != 1 {
		t.Fatalf("over-budget apply=%v %v position=%d", applied, err, r.Position())
	}
	if _, err := r.Stat(t.Context(), "link"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed apply created link: %v", err)
	}
	r.store.maxMetadataBytes++
	if applied, err := r.Apply(t.Context(), change); err != nil || !applied {
		t.Fatalf("exact-budget apply=%v %v", applied, err)
	}
	var used int64
	if err := r.store.read.QueryRow(`SELECT metadata_used FROM volumes WHERE id=?`, r.store.volume).Scan(&used); err != nil || used != existingMetadata+charge {
		t.Fatalf("metadata used=%d error=%v", used, err)
	}
	seed, err := r.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	r.store.maxMetadataBytes = 6 + charge
	rows := []metastore.Row{{Node: metastore.Node{ID: 20, Kind: storage.NodeDirectory, DirectoryRevision: []byte{1}}}, {Parent: 20, Name: []byte("link"), Node: link}}
	if err := seed.Add(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := seed.Add(t.Context(), []metastore.Row{{Parent: 20, Name: []byte("overflow"), Node: metastore.Node{ID: 21, Kind: storage.NodeRegular}}}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("seed exceeded staged metadata budget: %v", err)
	}
	var count int
	if err := seed.tx.QueryRow(`SELECT count(*) FROM nodes WHERE volume=?`, r.store.volume).Scan(&count); err != nil || count != 2 {
		t.Fatalf("refused seed row was inserted: count=%d error=%v", count, err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	r.store.maxMetadataBytes = existingMetadata + charge
	if r.Position() != 2 {
		t.Fatalf("aborted seed advanced position=%d", r.Position())
	}
	got, err := r.Stat(t.Context(), "link")
	if err != nil || !replicaNodesEqual(got, link) {
		t.Fatalf("failed seed lost original metadata: %+v %v", got, err)
	}
}

func TestReplicaRejectsMalformedGenericFactsWithoutAdvancingIdentity(t *testing.T) {
	r := seededAdmissionReplica(t)
	for _, test := range []struct {
		name  string
		alter func(*metastore.Node)
	}{
		{"unknown kind", func(n *metastore.Node) { n.Kind = 99 }},
		{"negative size", func(n *metastore.Node) { n.Size = -1 }},
		{"regular target", func(n *metastore.Node) { n.LinkTarget = []byte("target") }},
		{"regular directory token", func(n *metastore.Node) { n.DirectoryRevision = []byte{1} }},
		{"directory bytes", func(n *metastore.Node) { n.Kind = storage.NodeDirectory }},
		{"empty symbolic link", func(n *metastore.Node) { n.Kind = storage.NodeSymlink; n.Size = 0 }},
		{"symbolic link size", func(n *metastore.Node) { n.Kind = storage.NodeSymlink; n.LinkTarget = []byte("target") }},
		{"large directory token", func(n *metastore.Node) { n.DirectoryRevision = make([]byte, storage.MaxObservationTokenBytes+1) }},
		{"large link target", func(n *metastore.Node) { n.LinkTarget = make([]byte, storage.MaxLinkTargetBytes+1) }},
		{"metadata without version", func(n *metastore.Node) {
			n.Metadata = map[string]storage.OpaquePayload{"test.v1": {Data: []byte("unversioned")}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := metastore.Node{ID: 12, Kind: storage.NodeRegular, Size: 3}
			test.alter(&node)
			applied, err := r.Apply(t.Context(), metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("candidate"), Node: &node})
			if applied || err == nil || r.Position() != 1 {
				t.Fatalf("bad source facts published: applied=%v error=%v position=%d", applied, err, r.Position())
			}
			if _, err := r.Stat(t.Context(), "candidate"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("bad source left a name: %v", err)
			}
		})
	}
	node := metastore.Node{ID: 12, Kind: storage.NodeRegular, Size: 3}
	if applied, err := r.Apply(t.Context(), metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("candidate"), Node: &node}); err != nil || !applied {
		t.Fatalf("failed source validation consumed identity: %v %v", applied, err)
	}
}

func TestReplicaPointReadPassesQueuedScansWithoutOvertakingAWriter(t *testing.T) {
	r := seededAdmissionReplica(t)
	capacity := r.store.read.Stats().MaxOpenConnections
	scans := cap(r.listingSlots)
	if cap(r.readSlots) != capacity || scans != capacity-1 || scans < 1 {
		t.Fatalf("reader allocation: total=%d scans=%d SQL=%d", cap(r.readSlots), scans, capacity)
	}
	entered := make(chan struct{}, scans)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var readers sync.WaitGroup
	finished := make(chan error, scans)
	defer func() { unblock(); readers.Wait() }()
	for range scans {
		result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			entered <- struct{}{}
			<-release
			return nameBytes + metadataBytes + 64, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		readers.Go(func() {
			err := r.ListBounded(t.Context(), "", result)
			if err == nil {
				entries, readErr := result.Entries()
				err = readErr
				if err == nil && (len(entries) != 1 || entries[0].Attr.ID != 11 || entries[0].Attr.Size != 7) {
					err = fmt.Errorf("held scan changed its captured view: %+v", entries)
				}
			}
			finished <- err
		})
	}
	for range scans {
		receiveReplica(t, entered)
	}
	var pending []<-chan error
	var cancellations []context.CancelFunc
	defer func() {
		for _, cancel := range cancellations {
			cancel()
		}
	}()
	for _, bounded := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		cancellations = append(cancellations, cancel)
		waiting := observeReplicaWait(ctx)
		result := make(chan error, 1)
		pending = append(pending, result)
		go func() {
			if bounded {
				page, err := storage.NewListResult(1024, 0, func(int, int64, int64, storage.Attr) (int64, error) { return 64, nil })
				if err != nil {
					result <- err
					return
				}
				result <- r.ListBounded(waiting, "", page)
			} else {
				_, err := r.List(waiting, "")
				result <- err
			}
		}()
		receiveReplica(t, waiting.entered)
	}
	r.admission.mu.Lock()
	active, waiting := r.admission.activeReaders, r.admission.waitingReaders
	r.admission.mu.Unlock()
	if active != scans || waiting != 0 || len(r.readSlots) != scans || len(r.listingSlots) != scans {
		t.Fatalf("queued scans reached SQL/phase admission: active=%d waiting=%d shared=%d scan=%d", active, waiting, len(r.readSlots), len(r.listingSlots))
	}
	point := make(chan error, 1)
	go func() {
		attr, err := r.Stat(t.Context(), "file")
		if err == nil && (attr.ID != 11 || attr.Size != 7) {
			err = fmt.Errorf("point read=%+v", attr)
		}
		point <- err
	}()
	if err := receiveReplica(t, point); err != nil {
		t.Fatalf("point read could not pass queued scans: %v", err)
	}
	for i, result := range pending {
		select {
		case err := <-result:
			t.Fatalf("queued scan bypassed scan quota: %v", err)
		default:
		}
		cancellations[i]()
		if err := receiveReplica(t, result); !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR {
			t.Fatalf("cancel queued scan=%v", err)
		}
	}
	writerCtx, cancelWriter := context.WithCancel(t.Context())
	defer cancelWriter()
	writer := observeReplicaWait(writerCtx)
	written := make(chan error, 1)
	go func() {
		node := metastore.Node{ID: 11, Kind: storage.NodeRegular, Size: 9}
		applied, err := r.Apply(writer, metastore.Change{Position: 2, Kind: metastore.Modified, Node: &node})
		if err == nil && !applied {
			err = errors.New("writer did not apply its change")
		}
		written <- err
	}()
	receiveReplica(t, writer.entered)
	r.admission.mu.Lock()
	writers, active := r.admission.pendingWriters, r.admission.activeReaders
	r.admission.mu.Unlock()
	if writers != 1 || active != scans {
		t.Fatalf("writer did not wait for admitted scans: writers=%d active=%d", writers, active)
	}
	select {
	case err := <-written:
		t.Fatalf("writer overtook live scan snapshots: %v", err)
	default:
	}
	unblock()
	readers.Wait()
	for range scans {
		if err := receiveReplica(t, finished); err != nil {
			t.Fatal(err)
		}
	}
	if err := receiveReplica(t, written); err != nil {
		t.Fatal(err)
	}
	if attr, err := r.Stat(t.Context(), "file"); err != nil || attr.Size != 9 {
		t.Fatalf("writer publication=%+v %v", attr, err)
	}
	if len(r.readSlots) != 0 || len(r.listingSlots) != 0 {
		t.Fatal("finished work retained reader quota")
	}
	requireReplicaGateIdle(t, &r.admission)
}
