package localstore

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	checkpointRetryInterval = time.Second
	closeCheckpointTimeout  = 5 * time.Second
)

type durableMetastore struct {
	*sqlite.Store
	witness *metastoreWitness

	stop context.CancelFunc
	done chan struct{}

	mu                 sync.Mutex
	checkpointError    error
	checkpointAttempts int64
	retryInterval      time.Duration
	retryWaiting       bool
	closeRunning       *closeAttempt
	closed             bool
	lastCloseError     error
}

type closeAttempt struct {
	done chan struct{}
	err  error
}

type durabilityHeldObjects struct {
	*localdisk.Objects
	durable *durableMetastore
}

var _ objectstore.BoundedObjects = (*durabilityHeldObjects)(nil)

func (o *durabilityHeldObjects) Close() error {
	if !o.durable.closedSuccessfully() {
		return fmt.Errorf("the metastore is not durably closed: %w", syscall.EIO)
	}
	return o.Objects.Close()
}

func newDurableMetastore(store *sqlite.Store, witness *metastoreWitness) *durableMetastore {
	lifetime, stop := context.WithCancel(context.Background())
	durable := &durableMetastore{
		Store: store, witness: witness, stop: stop, done: make(chan struct{}),
		retryInterval: checkpointRetryInterval,
	}
	go durable.maintainCheckpoints(lifetime, witness.checkpoint)
	select {
	case witness.checkpoint <- struct{}{}:
	default:
	}
	return durable
}

func (d *durableMetastore) status() CheckpointStatus {
	d.mu.Lock()
	lastError := d.checkpointError
	d.mu.Unlock()
	accepted, checkpointed := d.witness.generations()
	return CheckpointStatus{
		AcceptedGeneration:     accepted,
		CheckpointedGeneration: checkpointed,
		Pending:                checkpointed < accepted,
		LastError:              lastError,
	}
}

func (d *durableMetastore) maintainCheckpoints(ctx context.Context, requests <-chan struct{}) {
	defer close(d.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
		}
		for {
			d.mu.Lock()
			d.checkpointAttempts++
			d.mu.Unlock()
			result, err := d.Store.Checkpoint(ctx, sqlite.PassiveCheckpoint)
			if ctx.Err() != nil {
				return
			}
			if err == nil && result.Complete {
				d.clearCheckpointError()
				break
			}
			if err != nil {
				d.mu.Lock()
				if d.checkpointError == nil {
					d.checkpointError = err
				}
				d.mu.Unlock()
			}
			d.mu.Lock()
			retryInterval := d.retryInterval
			d.retryWaiting = true
			d.mu.Unlock()
			retry := time.NewTimer(retryInterval)
		waitForRetry:
			for {
				select {
				case <-ctx.Done():
					d.mu.Lock()
					d.retryWaiting = false
					d.mu.Unlock()
					if !retry.Stop() {
						select {
						case <-retry.C:
						default:
						}
					}
					return
				case <-requests:
					// Signals only record that work remains. They do not reset or bypass
					// the retry interval while a snapshot pins the WAL or an error persists.
					select {
					case <-retry.C:
						break waitForRetry
					default:
					}
				case <-retry.C:
					break waitForRetry
				}
			}
			d.mu.Lock()
			d.retryWaiting = false
			d.mu.Unlock()
		}
	}
}

func (d *durableMetastore) clearCheckpointError() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpointError = nil
}

func (d *durableMetastore) Close() error {
	d.mu.Lock()
	if d.closed {
		err := d.lastCloseError
		d.mu.Unlock()
		return err
	}
	if d.closeRunning != nil {
		attempt := d.closeRunning
		d.mu.Unlock()
		<-attempt.done
		return attempt.err
	}
	attempt := &closeAttempt{done: make(chan struct{})}
	d.closeRunning = attempt
	d.mu.Unlock()

	d.stop()
	<-d.done
	closeContext, cancel := context.WithTimeout(context.Background(), closeCheckpointTimeout)
	err := d.Store.CloseContext(closeContext)
	cancel()

	d.mu.Lock()
	if err == nil || d.Store.Terminal() {
		d.closed = true
		if err == nil {
			d.checkpointError = nil
		} else {
			d.checkpointError = err
		}
	} else {
		d.checkpointError = err
	}
	d.lastCloseError = err
	attempt.err = err
	d.closeRunning = nil
	close(attempt.done)
	d.mu.Unlock()
	return err
}

func (d *durableMetastore) terminallyClosed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed
}

func (d *durableMetastore) closedSuccessfully() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed && d.lastCloseError == nil
}

func (d *durableMetastore) abortUnexposed() error {
	d.stop()
	<-d.done
	err := d.Store.Abort()
	d.mu.Lock()
	if d.Store.Terminal() {
		d.closed = true
	}
	if err != nil {
		d.checkpointError = err
	}
	d.lastCloseError = err
	d.mu.Unlock()
	return err
}
