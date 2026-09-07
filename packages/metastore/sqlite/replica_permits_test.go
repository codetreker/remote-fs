package sqlite

import (
	"context"
	"errors"
	"io/fs"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

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
			if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EIO) || !errors.As(err, &pathErr) {
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
