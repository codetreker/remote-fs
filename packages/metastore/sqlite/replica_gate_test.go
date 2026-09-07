package sqlite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type replicaWaitContext struct {
	context.Context
	entered chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func observeReplicaWait(ctx context.Context) *replicaWaitContext {
	return &replicaWaitContext{Context: ctx, entered: make(chan struct{})}
}

func (c *replicaWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	if c.resume != nil {
		<-c.resume
	}
	return c.Context.Done()
}

func receiveReplica[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("replica operation did not reach its expected synchronization point")
		var zero T
		return zero
	}
}

func requireReplicaGateIdle(t *testing.T, gate *replicaGate) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.activeReaders != 0 || gate.writerActive || gate.pendingWriters != 0 || gate.waitingReaders != 0 {
		t.Fatalf("gate retained readers=%d, writer=%v, pending writers=%d, waiting readers=%d",
			gate.activeReaders, gate.writerActive, gate.pendingWriters, gate.waitingReaders)
	}
}

func TestReplicaGateQueuesWriterBeforeNewReaders(t *testing.T) {
	gate := newReplicaGate()
	if err := gate.acquireRead(t.Context()); err != nil {
		t.Fatal(err)
	}
	writerContext := observeReplicaWait(t.Context())
	written := make(chan error, 1)
	go func() { written <- gate.acquireWrite(writerContext) }()
	receiveReplica(t, writerContext.entered)

	readerContext := observeReplicaWait(t.Context())
	read := make(chan error, 1)
	go func() { read <- gate.acquireRead(readerContext) }()
	receiveReplica(t, readerContext.entered)
	gate.releaseRead()
	if err := receiveReplica(t, written); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-read:
		t.Fatalf("reader overtook the writer: %v", err)
	default:
	}
	gate.releaseWrite()
	if err := receiveReplica(t, read); err != nil {
		t.Fatal(err)
	}
	gate.releaseRead()
	requireReplicaGateIdle(t, &gate)
}

func TestReplicaGateReservesReaderBatchBeforeWakingIt(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "canceled"}[canceled], func(t *testing.T) {
			gate := newReplicaGate()
			if err := gate.acquireWrite(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			resume := make(chan struct{})
			var resumeOnce sync.Once
			t.Cleanup(func() { resumeOnce.Do(func() { close(resume) }) })
			readerContext := observeReplicaWait(ctx)
			readerContext.resume = resume
			read := make(chan error, 1)
			go func() { read <- gate.acquireRead(readerContext) }()
			receiveReplica(t, readerContext.entered)

			writerContext := observeReplicaWait(t.Context())
			written := make(chan error, 1)
			go func() { written <- gate.acquireWrite(writerContext) }()
			receiveReplica(t, writerContext.entered)
			gate.releaseWrite()
			gate.mu.Lock()
			reserved, activeWriter, waiting := gate.activeReaders, gate.writerActive, gate.waitingReaders
			gate.mu.Unlock()
			if reserved != 1 || activeWriter || waiting != 0 {
				t.Fatalf("reader batch was not reserved before wakeup: reserved=%d writer=%v waiting=%d", reserved, activeWriter, waiting)
			}
			laterContext := observeReplicaWait(t.Context())
			laterRead := make(chan error, 1)
			go func() { laterRead <- gate.acquireRead(laterContext) }()
			receiveReplica(t, laterContext.entered)
			if canceled {
				cancel()
			}
			resumeOnce.Do(func() { close(resume) })
			err := receiveReplica(t, read)
			if canceled {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled reserved reader returned %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-written:
					t.Fatalf("writer overtook its reserved reader batch: %v", err)
				default:
				}
				gate.releaseRead()
			}
			if err := receiveReplica(t, written); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-laterRead:
				t.Fatalf("later reader extended the preceding batch: %v", err)
			default:
			}
			gate.releaseWrite()
			if err := receiveReplica(t, laterRead); err != nil {
				t.Fatal(err)
			}
			gate.releaseRead()
			requireReplicaGateIdle(t, &gate)
		})
	}
}

func TestReplicaGateCancellationWithdrawsOnlyItsOwnWriter(t *testing.T) {
	gate := newReplicaGate()
	if err := gate.acquireRead(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceledContext := observeReplicaWait(ctx)
	canceledWriter := make(chan error, 1)
	go func() { canceledWriter <- gate.acquireWrite(canceledContext) }()
	receiveReplica(t, canceledContext.entered)
	writerContext := observeReplicaWait(t.Context())
	written := make(chan error, 1)
	go func() { written <- gate.acquireWrite(writerContext) }()
	receiveReplica(t, writerContext.entered)
	readerContext := observeReplicaWait(t.Context())
	read := make(chan error, 1)
	go func() { read <- gate.acquireRead(readerContext) }()
	receiveReplica(t, readerContext.entered)
	cancel()
	if err := receiveReplica(t, canceledWriter); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled writer returned %v", err)
	}
	gate.mu.Lock()
	readers, writers, waiting := gate.activeReaders, gate.pendingWriters, gate.waitingReaders
	gate.mu.Unlock()
	if readers != 1 || writers != 1 || waiting != 1 {
		t.Fatalf("cancellation changed another waiter's admission: readers=%d writers=%d waiting=%d", readers, writers, waiting)
	}
	gate.releaseRead()
	if err := receiveReplica(t, written); err != nil {
		t.Fatal(err)
	}
	gate.releaseWrite()
	if err := receiveReplica(t, read); err != nil {
		t.Fatal(err)
	}
	gate.releaseRead()
	requireReplicaGateIdle(t, &gate)
}
