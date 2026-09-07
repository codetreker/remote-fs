package sqlite

import (
	"context"
	"sync"
)

// replicaGate alternates exclusive writers with finite batches of waiting readers.
// Reserved reader grants count as active before their goroutines resume, so a queued
// writer cannot overtake the batch. Writers do not have a FIFO ordering guarantee.
type replicaGate struct {
	mu             sync.Mutex
	activeReaders  int
	writerActive   bool
	pendingWriters int
	waitingReaders int
	readerBatch    chan struct{}
	changed        chan struct{}
}

func newReplicaGate() replicaGate {
	return replicaGate{readerBatch: make(chan struct{}), changed: make(chan struct{})}
}

func (g *replicaGate) acquireRead(ctx context.Context) error {
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	if !g.writerActive && g.pendingWriters == 0 {
		g.activeReaders++
		g.mu.Unlock()
		return nil
	}
	g.waitingReaders++
	batch := g.readerBatch
	g.mu.Unlock()

	select {
	case <-batch:
	case <-ctx.Done():
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		if batch == g.readerBatch {
			g.waitingReaders--
		} else {
			g.releaseReadLocked()
		}
		return err
	}
	return nil
}

func (g *replicaGate) releaseRead() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.releaseReadLocked()
}

func (g *replicaGate) releaseReadLocked() {
	g.activeReaders--
	if g.activeReaders == 0 {
		g.notifyLocked()
	}
}

func (g *replicaGate) acquireWrite(ctx context.Context) error {
	g.mu.Lock()
	g.pendingWriters++
	for {
		if err := ctx.Err(); err != nil {
			g.pendingWriters--
			if !g.writerActive && g.pendingWriters == 0 {
				g.grantReadersLocked()
			}
			g.notifyLocked()
			g.mu.Unlock()
			return err
		}
		if !g.writerActive && g.activeReaders == 0 {
			g.pendingWriters--
			g.writerActive = true
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		g.mu.Lock()
	}
}

func (g *replicaGate) releaseWrite() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writerActive = false
	g.grantReadersLocked()
	g.notifyLocked()
}

func (g *replicaGate) grantReadersLocked() {
	if g.waitingReaders == 0 {
		return
	}
	g.activeReaders += g.waitingReaders
	g.waitingReaders = 0
	batch := g.readerBatch
	g.readerBatch = make(chan struct{})
	close(batch)
}

func (g *replicaGate) notifyLocked() {
	changed := g.changed
	g.changed = make(chan struct{})
	close(changed)
}
