package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

type LifecycleStorage interface {
	storage.VolumeSummary
	storage.QuotaControl
	storage.QuotaAcceptance
	storage.ActionNamespaces
}

type LifecycleLease struct {
	Store   LifecycleStorage
	Release func() error
}

type LifecycleRetirement struct {
	Close  func(context.Context) ([]byte, error)
	Cancel func() error
}

// Hooks use the runtime registry's admission gate. Freeze must reserve retirement
// before stopping a handler, and Close must confirm the entire native settlement.
type LifecycleHooks struct {
	Initialize func(context.Context, management.Volume, int64) ([]byte, error)
	Acquire    func(context.Context, management.Volume) (LifecycleLease, error)
	Freeze     func(context.Context, management.Volume) (LifecycleRetirement, error)
}

type LifecycleConfig struct {
	Management              *management.Store
	VolumesDir              string
	QuarantineDir           string
	Hooks                   LifecycleHooks
	MaxReclaimEntries       int
	MaxConcurrentOperations int
}

type Lifecycle struct {
	config    LifecycleConfig
	mu        sync.Mutex
	locks     map[management.ID]*lifecycleLock
	admission chan struct{}
}

type lifecycleLock struct {
	gate  chan struct{}
	users int
}

func NewLifecycle(config LifecycleConfig) (*Lifecycle, error) {
	if config.Management == nil || config.Hooks.Initialize == nil || config.Hooks.Acquire == nil || config.Hooks.Freeze == nil || config.MaxReclaimEntries < 1 || config.MaxConcurrentOperations < 1 {
		return nil, syscall.EINVAL
	}
	if err := validateLifecycleDirectories(config.VolumesDir, config.QuarantineDir); err != nil {
		return nil, err
	}
	return &Lifecycle{config: config, locks: make(map[management.ID]*lifecycleLock), admission: make(chan struct{}, config.MaxConcurrentOperations)}, nil
}

// Run advances the original durable operation. A failed call preserves its phase;
// it never turns an uncertain native effect or cleanup into a successful receipt.
func (l *Lifecycle) Run(ctx context.Context, supplied management.Operation) (management.Operation, error) {
	op, err := l.config.Management.Operation(ctx, supplied.ActorUserID, supplied.ID)
	if err != nil {
		return management.Operation{}, err
	}
	release, err := l.lockOperation(ctx, op.VolumeID)
	if err != nil {
		return op, err
	}
	defer release()
	op, err = l.config.Management.Operation(ctx, supplied.ActorUserID, supplied.ID)
	if err != nil {
		return management.Operation{}, err
	}
	if op.State == management.OperationSucceeded || op.State == management.OperationFailed {
		return op, nil
	}
	volume, err := l.config.Management.GetVolume(ctx, op.ActorUserID, op.VolumeID)
	if err != nil {
		return op, err
	}
	switch op.Kind {
	case management.OperationCreate:
		return l.create(ctx, op, volume)
	case management.OperationQuota:
		return l.quota(ctx, op, volume)
	case management.OperationDelete:
		return l.delete(ctx, op, volume)
	default:
		return op, fmt.Errorf("unsupported lifecycle operation %q: %w", op.Kind, syscall.EINVAL)
	}
}

func (l *Lifecycle) Recover(ctx context.Context) error {
	var after management.ID
	var failures []error
	for {
		operations, err := l.config.Management.PendingOperations(ctx, after, 50)
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if len(operations) == 0 {
			return errors.Join(failures...)
		}
		for _, op := range operations {
			if _, err := l.Run(ctx, op); err != nil {
				failures = append(failures, fmt.Errorf("operation %s: %w", op.ID, err))
			}
			after = op.ID
		}
	}
}

func lifecycleMutation(op management.Operation) (management.VolumeMutation, error) {
	var payload struct {
		Actor   management.ID
		Kind    management.OperationKind
		Request management.VolumeMutation
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return management.VolumeMutation{}, errors.Join(management.ErrCorrupt, err)
	}
	if payload.Actor != op.ActorUserID || payload.Kind != op.Kind || payload.Request.Action != op.Action {
		return management.VolumeMutation{}, management.ErrCorrupt
	}
	return payload.Request, nil
}

func (l *Lifecycle) transition(ctx context.Context, op management.Operation, phase management.OperationPhase, change management.OperationTransition) (management.Operation, error) {
	change.State = management.OperationRunning
	change.Phase = phase
	changed, err := l.config.Management.TransitionOperation(ctx, op.ID, op.Revision, change)
	if err != nil {
		return op, err
	}
	return changed, nil
}

func (l *Lifecycle) complete(ctx context.Context, op management.Operation, result any) (management.Operation, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return op, err
	}
	changed, err := l.config.Management.TransitionOperation(ctx, op.ID, op.Revision, management.OperationTransition{State: management.OperationSucceeded, Phase: management.PhaseCompleted, Settled: true, Result: data})
	if err != nil {
		return op, err
	}
	return changed, nil
}

func (l *Lifecycle) lockOperation(ctx context.Context, volume management.ID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case l.admission <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	l.mu.Lock()
	lock := l.locks[volume]
	if lock == nil {
		lock = &lifecycleLock{gate: make(chan struct{}, 1)}
		l.locks[volume] = lock
	}
	lock.users++
	l.mu.Unlock()
	forget := func() {
		l.mu.Lock()
		lock.users--
		if lock.users == 0 {
			delete(l.locks, volume)
		}
		l.mu.Unlock()
		<-l.admission
	}
	select {
	case lock.gate <- struct{}{}:
		return func() { <-lock.gate; forget() }, nil
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	}
}
