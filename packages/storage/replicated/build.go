package replicated

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// build owns one subscription from setup through snapshot replay. Only the
// subscription survives handoff; caller values and temporary cancellation links
// belong to setup. The returned cancellation releases the subscription's parent
// context when its sole reader closes it.
func (s *Storage) build(ctx context.Context) (_ *httprest.Subscription, _ context.CancelFunc, returned error) {
	setup, cancelSetup := context.WithCancelCause(ctx)
	detachLifetime := linkBuildCancellation(s.lifetime, cancelSetup)
	subscribing, cancelSubscription := context.WithCancelCause(s.lifetime)
	detachSetup := linkBuildCancellation(setup, cancelSubscription)
	var sub *httprest.Subscription
	handedOff := false
	defer func() {
		detachSetup()
		detachLifetime()
		if !handedOff {
			if ended := errors.Join(setup.Err(), context.Cause(setup), s.lifetime.Err(), context.Cause(s.lifetime)); ended != nil {
				returned = errors.Join(returned, ended, syscall.EIO)
			}
			cancelSubscription(context.Canceled)
			if sub != nil {
				returned = errors.Join(returned, sub.Close())
			}
		}
		cancelSetup(context.Canceled)
	}()

	var err error
	sub, err = s.remote.Subscribe(subscribing)
	if err != nil {
		return nil, nil, fmt.Errorf("watching the volume for changes: %w", err)
	}
	if err := s.fill(setup, sub, cancelSubscription); err != nil {
		return nil, nil, err
	}

	detachSetup()
	detachLifetime()
	if err := errors.Join(setup.Err(), context.Cause(setup), s.lifetime.Err(), context.Cause(s.lifetime)); err != nil {
		return nil, nil, fmt.Errorf("finishing replica setup: %w", errors.Join(err, syscall.EIO))
	}
	incarnation, at := s.watching()
	s.seeded(incarnation, at)
	handedOff = true
	return sub, func() { cancelSubscription(context.Canceled) }, nil
}

// linkBuildCancellation stops and joins its callback before returning from the
// detach function. Cancellation alone never calls Subscription.Close: the reader
// retains that ownership until its blocked Next has returned.
func linkBuildCancellation(ctx context.Context, cancel context.CancelCauseFunc) func() {
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(finished)
		cancel(context.Cause(ctx))
	})
	return sync.OnceFunc(func() {
		if !stop() {
			<-finished
		}
	})
}

// fill installs one captured tree while keeping it unavailable, then drains the
// original subscription through one checkpoint obtained after snapshot EOF.
// The stream's bounded HTTP buffers retain intervening changes; no second queue
// or background reader participates in this gate.
func (s *Storage) fill(ctx context.Context, sub *httprest.Subscription, cancelSubscription context.CancelCauseFunc) (returned error) {
	snap, err := s.remote.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("taking a picture of the volume's tree: %w", err)
	}
	snapshotHeld := true
	defer func() {
		if snapshotHeld {
			returned = errors.Join(returned, snap.Close())
		}
	}()

	seeding, err := s.local.Reseed(ctx)
	if err != nil {
		return fmt.Errorf("emptying the local copy: %w", err)
	}
	defer func() { returned = errors.Join(returned, seeding.Close()) }()
	for {
		rows, err := snap.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading the picture of the volume's tree: %w", err)
		}
		if err := seeding.Add(ctx, rows); err != nil {
			return fmt.Errorf("putting the picture into the local copy: %w", err)
		}
	}

	gate, cancelGate := context.WithTimeout(ctx, s.options.ReplayTimeout)
	defer cancelGate()
	detachGate := linkBuildCancellation(gate, cancelSubscription)
	defer detachGate()
	// Semantic EOF completes the picture but does not release its HTTP body.
	// Release that connection before the checkpoint needs another request slot.
	closeErr := snap.Close()
	snapshotHeld = false
	if closeErr != nil {
		return replayGateError(gate, fmt.Errorf("closing the delivered snapshot: %w", closeErr))
	}
	target, err := s.remote.Checkpoint(gate)
	if err != nil {
		return replayGateError(gate, fmt.Errorf("capturing the post-snapshot checkpoint: %w", err))
	}
	position := snap.Position()
	if position < sub.Tail() {
		return replayGateError(gate, fmt.Errorf("the snapshot is at position %d before its subscription's tail %d", position, sub.Tail()))
	}
	if metastore.Incarnation(target.Incarnation) != sub.Incarnation() {
		return replayGateError(gate, errors.New("the post-snapshot checkpoint belongs to a different log incarnation"))
	}
	if target.Position < int64(position) {
		return replayGateError(gate, fmt.Errorf("the post-snapshot checkpoint at %d is before the snapshot at %d", target.Position, position))
	}
	if err := seeding.Complete(gate, position); err != nil {
		return replayGateError(gate, fmt.Errorf("installing the captured tree: %w", err))
	}
	s.installed(sub.Incarnation(), position, metastore.Position(target.Position))
	if err := s.replay(gate, sub, position, metastore.Position(target.Position)); err != nil {
		return replayGateError(gate, err)
	}
	detachGate()
	if err := gate.Err(); err != nil {
		return replayGateError(gate, err)
	}
	return nil
}

func (s *Storage) replay(ctx context.Context, sub *httprest.Subscription, snapshot, target metastore.Position) error {
	at := snapshot
	for at < target {
		if err := ctx.Err(); err != nil {
			return err
		}
		change, err := sub.Next()
		if err != nil {
			return fmt.Errorf("replaying changes after the snapshot: %w", err)
		}
		if change.Position <= snapshot {
			continue
		}
		applied, err := s.local.Apply(ctx, change)
		if err != nil {
			return fmt.Errorf("applying snapshot replay at %d: %w", change.Position, err)
		}
		if applied {
			at = change.Position
			s.replayed(change)
		}
	}
	return nil
}

func replayGateError(ctx context.Context, cause error) error {
	return fmt.Errorf("the snapshot replay gate failed: %w", errors.Join(cause, ctx.Err(), context.Cause(ctx), syscall.EIO))
}
