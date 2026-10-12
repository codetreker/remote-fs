package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
)

func (l *Lifecycle) create(ctx context.Context, op management.Operation, volume management.Volume) (management.Operation, error) {
	initializedNow := false
	mutation, err := lifecycleMutation(op)
	if err != nil {
		return op, err
	}
	if op.Phase == management.PhaseAccepted {
		op, err = l.transition(ctx, op, management.PhaseInitializing, management.OperationTransition{})
		if err != nil {
			return op, err
		}
	}
	if op.Phase == management.PhaseInitializing {
		if err := l.prepareCreatedRoot(op, volume); err != nil {
			return op, err
		}
		native, err := l.config.Hooks.Initialize(ctx, volume, mutation.QuotaBytes)
		if err != nil {
			return op, err
		}
		if len(native) == 0 {
			return op, syscall.EIO
		}
		// The initialized root can survive a crash while its parent entry does not.
		if err := syncLifecycleDirectory(l.config.VolumesDir); err != nil {
			return op, err
		}
		marker, err := readLifecycleBinding(l.volumePath(volume))
		if err != nil {
			return op, err
		}
		proof, err := json.Marshal(lifecycleRootProof{Volume: volume.ID, Operation: op.ID, StorageName: volume.StorageName, Native: native, BindingDigest: sha256.Sum256(marker)})
		if err != nil {
			return op, err
		}
		op, err = l.transition(ctx, op, management.PhaseBound, management.OperationTransition{VerifiedRootProof: proof})
		if err != nil {
			return op, err
		}
		initializedNow = true
	}
	if op.Phase != management.PhaseBound {
		return op, management.ErrCorrupt
	}
	if err := validateLifecycleCreateMarker(l.volumePath(volume), op); err != nil {
		return op, err
	}
	if err := l.verifyRootProof(ctx, op, volume, l.volumePath(volume)); err != nil {
		return op, err
	}
	if !initializedNow {
		native, err := l.config.Hooks.Initialize(ctx, volume, mutation.QuotaBytes)
		if err != nil {
			return op, err
		}
		var proof lifecycleRootProof
		if err := json.Unmarshal(op.VerifiedRootProof, &proof); err != nil {
			return op, err
		}
		if !bytes.Equal(native, proof.Native) {
			return op, syscall.EIO
		}
	}
	if err := syncLifecycleDirectory(l.config.VolumesDir); err != nil {
		return op, err
	}
	result := struct {
		VolumeID management.ID `json:"volumeId"`
	}{volume.ID}
	completed, err := l.complete(ctx, op, result)
	if err != nil {
		return op, err
	}
	return completed, nil
}

func (l *Lifecycle) failDefinitive(ctx context.Context, op management.Operation, code string) (management.Operation, error) {
	changed, err := l.config.Management.TransitionOperation(ctx, op.ID, op.Revision, management.OperationTransition{State: management.OperationFailed, Phase: op.Phase, ErrorCode: code, Settled: true})
	if err != nil {
		return op, err
	}
	return changed, nil
}
