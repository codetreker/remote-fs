package sqlite

import (
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
		{Node: metastore.Node{ID: 10, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1, Metadata: replicaTestMetadata("initial")}},
		{EntryID: 12, Parent: 10, Name: []byte("file"), Node: metastore.Node{ID: 11, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial"), Size: 7}},
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
				func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) { return nameBytes + 64, nil })
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
	for _, outcome := range []string{"complete", "rollback", "invalid row", "commit failure"} {
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
			root := metastore.Row{Node: metastore.Node{ID: 20, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1, Metadata: replicaTestMetadata("initial")}}
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
				rows := []metastore.Row{{EntryID: 22, Parent: 20, Name: []byte("replacement"), Node: metastore.Node{ID: 21, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial"), Size: 19}}}
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
			case "commit failure":
				rows := []metastore.Row{{EntryID: 22, Parent: 20, Name: []byte("child"), Node: metastore.Node{ID: 21, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial")}}}
				if err := seeding.Add(t.Context(), rows); err != nil {
					t.Fatal(err)
				}
				// A deferred foreign-key failure exercises uncertain COMMIT handling after a valid tree has been staged.
				if _, err := seeding.tx.ExecContext(t.Context(), `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec) VALUES('replica-commit-failure',999,0,0,0,0)`); err != nil {
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
			if outcome == "commit failure" {
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
	node := metastore.Node{ID: 11, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial"), Size: 19}
	change := replicaTestChange(t, replica, metastore.Change{Position: 2, Kind: metastore.Modified, Node: &node})
	for range writers {
		waiting := observeReplicaWait(t.Context())
		go func() {
			_, err := replica.Apply(waiting, change)
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
				func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) { return nameBytes + 64, nil })
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
	root := metastore.Node{ID: 1, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1, Metadata: replicaTestMetadata("initial")}
	rows := []metastore.Row{{Node: root}}
	for i := range 4096 {
		rows = append(rows, metastore.Row{
			EntryID: storage.EntryID(i + 5000), Parent: 1, Name: []byte(fmt.Sprintf("file-%04d", i)),
			Node: metastore.Node{ID: int64(i + 2), Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial")},
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
	beforeRoot := root.Clone()
	root.Metadata = replicaTestMetadata("updated")
	root.MetadataRevision++
	change := metastore.Change{Position: 2, Kind: metastore.Modified, Node: &root, Notification: &metastore.Notification{SubjectID: root.ID, SubjectKind: root.Kind, ChangeMask: metastore.NotificationMask(beforeRoot, root), Before: &metastore.EventImage{Attr: beforeRoot.Attr(), Location: storage.EntryLocation{State: storage.LocationRoot, RootNodeID: uint64(root.ID), NodeID: uint64(root.ID)}}, After: &metastore.EventImage{Attr: root.Attr(), Location: storage.EntryLocation{State: storage.LocationRoot, RootNodeID: uint64(root.ID), NodeID: uint64(root.ID)}}}}
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
	if permits := len(replica.readSlots); permits != 0 {
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
	if err != nil || !sameReplicaNode(got, root) || replica.Position() != 2 {
		t.Fatalf("applied change was not visible: root=%+v err=%v position=%d", got, err, replica.Position())
	}
	t.Logf("Seeding completed in %v; reader shutdown after Apply took %v", seedElapsed, shutdownElapsed)
	t.Logf("Apply completed in %v while 128 readers completed %d listings of 4096 files", elapsed, completed.Load())
}

func TestReplicaAppliesEveryChangeWithoutLosingIdentityOrRollback(t *testing.T) {
	r := seededAdmissionReplica(t)
	ctx := t.Context()
	node := metastore.Node{ID: 13, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: replicaTestMetadata("initial"), Size: 3, AccessTime: time.Unix(100, 0), ModTime: time.Unix(200, 0)}
	apply := func(change metastore.Change) {
		t.Helper()
		changed, err := r.Apply(ctx, replicaTestChange(t, r, change))
		if err != nil || !changed || r.Position() != change.Position {
			t.Fatalf("apply %+v = %v, %v; position %d", change, changed, err, r.Position())
		}
	}
	apply(metastore.Change{Position: 2, Kind: metastore.Created, Parent: 10, Name: []byte("new"), Node: &node})
	if got, err := r.Stat(ctx, "new"); err != nil || !sameReplicaNode(got, node) {
		t.Fatalf("created node = %+v, %v", got, err)
	}
	node.Size, node.Metadata = 9, replicaTestMetadata("updated")
	node.MetadataRevision++
	apply(metastore.Change{Position: 3, Kind: metastore.Modified, Parent: 10, Name: []byte("new"), Node: &node})
	if got, err := r.Stat(ctx, "new"); err != nil || !sameReplicaNode(got, node) {
		t.Fatalf("modified node = %+v, %v", got, err)
	}
	changed := time.Unix(300, 0).UTC()
	node.ChangeTime = &changed
	node.MetadataRevision++
	apply(metastore.Change{Position: 4, Kind: metastore.Renamed, Parent: 10, Name: []byte("renamed"), From: &metastore.Location{Parent: 10, Name: []byte("new")}, Node: &node})
	if got, err := r.Stat(ctx, "renamed"); err != nil || !sameReplicaNode(got, node) {
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
		if changed, err := r.Apply(ctx, replicaTestChange(t, r, change)); changed || !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid change %+v = %v, %v", change, changed, err)
		}
		if r.Position() != 4 {
			t.Fatalf("failed change advanced position to %d", r.Position())
		}
		if got, err := r.Stat(ctx, "renamed"); err != nil || !sameReplicaNode(got, node) {
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

func replicaTestMetadata(value string) storage.Metadata {
	return storage.Metadata{{Key: "test.tag", Version: 1, Data: []byte(value)}}
}
func sameReplicaNode(a, b metastore.Node) bool {
	return reflect.DeepEqual(a.Attr().Clone(), b.Attr().Clone()) && string(a.LinkTarget) == string(b.LinkTarget) && a.Content == b.Content
}

func replicaTestChange(t *testing.T, r *Replica, change metastore.Change) metastore.Change {
	t.Helper()
	if change.Kind != metastore.Removed && change.Node == nil || change.Kind == metastore.Renamed && change.From == nil || change.Kind < metastore.Created || change.Kind > metastore.Renamed {
		return change
	}
	tx, err := r.store.read.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var before metastore.Node
	var beforeLocation storage.EntryLocation
	id := int64(13)
	if change.Node != nil {
		id = change.Node.ID
	}
	if change.Kind != metastore.Created {
		before, err = nodeByID(t.Context(), tx, id)
		if err != nil {
			t.Fatal(err)
		}
		beforeLocation, err = r.store.captureLocation(t.Context(), tx, r.store.root, id)
		if err != nil {
			t.Fatal(err)
		}
		if change.Kind == metastore.Modified && change.Parent == 0 && id != r.store.root {
			at := beforeLocation.Ancestors[len(beforeLocation.Ancestors)-1]
			change.Parent, change.Name = int64(at.ParentID), append([]byte{}, at.Name...)
		}
		if beforeLocation.State == storage.LocationLinked {
			at := &beforeLocation.Ancestors[len(beforeLocation.Ancestors)-1]
			if change.Kind == metastore.Removed {
				at.ParentID, at.Name = uint64(change.Parent), append([]byte{}, change.Name...)
			}
			if change.Kind == metastore.Renamed {
				at.ParentID, at.Name = uint64(change.From.Parent), append([]byte{}, change.From.Name...)
			}
		}
	}
	notification := &metastore.Notification{SubjectID: id}
	if change.Kind != metastore.Created {
		notification.Before = &metastore.EventImage{Attr: before.Attr(), Location: beforeLocation, LinkTarget: append([]byte{}, before.LinkTarget...)}
		notification.SubjectKind = before.Kind
	}
	if change.Node != nil {
		notification.SubjectKind = change.Node.Kind
		afterLocation := beforeLocation.Clone()
		if change.Kind == metastore.Created || change.Kind == metastore.Renamed {
			parent, err := nodeByID(t.Context(), tx, change.Parent)
			if err != nil {
				t.Fatal(err)
			}
			parentLocation, err := r.store.captureLocation(t.Context(), tx, r.store.root, change.Parent)
			if err != nil {
				t.Fatal(err)
			}
			entryID := storage.EntryID(14)
			if change.Kind == metastore.Renamed {
				entryID = beforeLocation.Ancestors[len(beforeLocation.Ancestors)-1].EntryID
			}
			afterLocation = storage.EntryLocation{State: storage.LocationLinked, RootNodeID: uint64(r.store.root), NodeID: uint64(id), Ancestors: append(parentLocation.Ancestors, storage.EntryCondition{ParentID: uint64(change.Parent), NodeID: uint64(id), EntryID: entryID, Name: append([]byte{}, change.Name...), DirectoryRevision: parent.DirectoryRevision})}
		}
		notification.After = &metastore.EventImage{Attr: change.Node.Attr(), Location: afterLocation, LinkTarget: append([]byte{}, change.Node.LinkTarget...)}
	}
	switch change.Kind {
	case metastore.Created, metastore.Removed:
		notification.ChangeMask = metastore.ChangeName
	case metastore.Renamed:
		notification.ChangeMask = metastore.ChangeName | metastore.NotificationMask(before, *change.Node)
	case metastore.Modified:
		notification.ChangeMask = metastore.NotificationMask(before, *change.Node)
	}
	change.Notification = notification
	return change
}

func TestReplicaImportsUnorderedEntryIdentitiesAndOpaqueNodeFacts(t *testing.T) {
	r := seededAdmissionReplica(t)
	initial, err := r.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer initial.Close()
	wide := time.Date(12000, 2, 3, 4, 5, 6, 789, time.UTC)
	child := metastore.Node{ID: 20, Kind: storage.NodeSymlink, Size: 3, MetadataRevision: 77,
		AccessTime: time.Unix(-90000000000, 123).UTC(), ModTime: wide, ChangeTime: &wide,
		Metadata: storage.Metadata{{Key: "foreign.owner", Version: 99, Data: []byte{0, 255, 1}}}, LinkTarget: []byte("./x")}
	root := metastore.Node{ID: 10, Kind: storage.NodeDirectory, MetadataRevision: 51, DirectoryRevision: 901, Metadata: replicaTestMetadata("root")}
	name := []byte{255, '.', 'l'}
	if err := initial.Add(t.Context(), []metastore.Row{{EntryID: 500, Parent: 10, Name: name, Node: child}, {EntryID: 499, Parent: 10, Name: []byte("regular"), Node: metastore.Node{ID: 19, Kind: storage.NodeRegular, MetadataRevision: 1, Size: 7, Content: "source-only-key"}}}); err != nil {
		t.Fatal(err)
	}
	if err := initial.Add(t.Context(), []metastore.Row{{Node: root}}); err != nil {
		t.Fatal(err)
	}
	if err := initial.Complete(t.Context(), 7); err != nil {
		t.Fatal(err)
	}
	got, err := r.Stat(t.Context(), string(name))
	if err != nil {
		t.Fatal(err)
	}
	if regular, err := r.Stat(t.Context(), "regular"); err != nil || regular.Content != "" || regular.Size != 7 {
		t.Fatalf("replica retained remote object key: %+v,%v", regular, err)
	}
	expected := child.Clone()
	expected.Content = ""
	if !sameReplicaNode(got, expected) || got.CreationTime != nil {
		t.Fatalf("replicated facts=%+v; expected=%+v", got, expected)
	}
	var entry, high, sequence int64
	if err := r.store.read.QueryRowContext(t.Context(), `SELECT id FROM entries WHERE volume=? AND node=?`, r.store.volume, child.ID).Scan(&entry); err != nil {
		t.Fatal(err)
	}
	if err := r.store.read.QueryRowContext(t.Context(), `SELECT node_high_water FROM database_state`).Scan(&high); err != nil {
		t.Fatal(err)
	}
	if err := r.store.read.QueryRowContext(t.Context(), `SELECT seq FROM sqlite_sequence WHERE name='nodes'`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if entry != 500 || high != 500 || sequence != 500 {
		t.Fatalf("imported entry/high/sequence=%d/%d/%d", entry, high, sequence)
	}
	if got, err := r.Stat(t.Context(), ""); err != nil || !sameReplicaNode(got, root) {
		t.Fatalf("root facts=%+v,%v", got, err)
	}
}

func TestReplicaRejectsInvalidEntryIdentityAndDisconnectedSnapshots(t *testing.T) {
	for _, scenario := range []string{"missing entry identity", "non-directory parent", "disconnected cycle"} {
		t.Run(scenario, func(t *testing.T) {
			r := seededAdmissionReplica(t)
			seed, err := r.Reseed(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			root := metastore.Node{ID: 20, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1}
			file := metastore.Node{ID: 21, Kind: storage.NodeRegular, MetadataRevision: 1}
			rows := []metastore.Row{{Node: root}, {EntryID: 23, Parent: 20, Name: []byte("file"), Node: file}}
			switch scenario {
			case "missing entry identity":
				rows[1].EntryID = 0
			case "non-directory parent":
				rows = append(rows, metastore.Row{EntryID: 25, Parent: 21, Name: []byte("child"), Node: metastore.Node{ID: 24, Kind: storage.NodeRegular, MetadataRevision: 1}})
			case "disconnected cycle":
				rows = append(rows, metastore.Row{EntryID: 26, Parent: 25, Name: []byte("a"), Node: metastore.Node{ID: 24, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1}}, metastore.Row{EntryID: 27, Parent: 24, Name: []byte("b"), Node: metastore.Node{ID: 25, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1}})
			}
			err = seed.Add(t.Context(), rows)
			if err == nil {
				err = seed.Complete(t.Context(), 2)
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid snapshot=%v", err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := r.Stat(t.Context(), "file"); err != nil || got.ID != 11 || r.Position() != 1 {
				t.Fatalf("rejected snapshot replaced prior view: %+v,%v position=%d", got, err, r.Position())
			}
		})
	}
}
