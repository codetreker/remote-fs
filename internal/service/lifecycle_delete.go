package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
)

type lifecycleRootProof struct {
	Volume        management.ID           `json:"volume"`
	Operation     management.ID           `json:"operation"`
	StorageName   string                  `json:"storageName"`
	Native        []byte                  `json:"native"`
	BindingDigest [sha256.Size]byte       `json:"bindingDigest"`
	ClosedBinding *lifecycleClosedBinding `json:"closedBinding,omitempty"`
}

type lifecycleClosedBinding struct {
	WitnessDigest  [sha256.Size]byte `json:"witnessDigest"`
	DatabaseDigest [sha256.Size]byte `json:"databaseDigest"`
	DatabaseBytes  int64             `json:"databaseBytes"`
}

type lifecycleReclaimMarker struct {
	Volume    management.ID `json:"volume"`
	Operation management.ID `json:"operation"`
	Token     string        `json:"token"`
}

type lifecycleReclaimProof struct {
	Path   string `json:"path"`
	Marker []byte `json:"marker"`
	Root   []byte `json:"root"`
}

func (l *Lifecycle) delete(ctx context.Context, op management.Operation, volume management.Volume) (management.Operation, error) {
	var err error
	if op.Phase == management.PhaseAccepted || op.Phase == management.PhaseFrozen {
		retirement, err := l.config.Hooks.Freeze(ctx, volume)
		if err != nil {
			if op.Phase == management.PhaseAccepted && errors.Is(err, syscall.EBUSY) {
				failed, transitionErr := l.failDefinitive(ctx, op, "volume-busy")
				return failed, errors.Join(err, transitionErr)
			}
			return op, err
		}
		if retirement.Close == nil || retirement.Cancel == nil {
			return op, syscall.EIO
		}
		if op.Phase == management.PhaseAccepted {
			changed, err := l.transition(ctx, op, management.PhaseFrozen, management.OperationTransition{})
			if err != nil {
				return op, errors.Join(err, retirement.Cancel())
			}
			op = changed
		}
		native, err := retirement.Close(ctx)
		if err != nil {
			return op, err
		}
		if len(native) == 0 {
			return op, syscall.EIO
		}
		marker, err := readLifecycleBinding(l.volumePath(volume))
		if err != nil {
			return op, err
		}
		closed, err := l.closedBinding(ctx, l.volumePath(volume))
		if err != nil {
			return op, err
		}
		root, err := json.Marshal(lifecycleRootProof{Volume: volume.ID, Operation: op.ID, StorageName: volume.StorageName, Native: native, BindingDigest: sha256.Sum256(marker), ClosedBinding: &closed})
		if err != nil {
			return op, err
		}
		var token [32]byte
		if _, err := rand.Read(token[:]); err != nil {
			return op, err
		}
		planned, err := json.Marshal(lifecycleReclaimMarker{Volume: volume.ID, Operation: op.ID, Token: hex.EncodeToString(token[:])})
		if err != nil {
			return op, err
		}
		op, err = l.transition(ctx, op, management.PhaseVerifiedClose, management.OperationTransition{VerifiedRootProof: root, PlannedReclaimProof: planned, QuarantineLeaf: string(volume.ID) + "-" + string(op.ID)})
		if err != nil {
			return op, err
		}
	}
	if op.Phase == management.PhaseVerifiedClose {
		if err := l.quarantineRoot(ctx, op, volume); err != nil {
			return op, err
		}
		op, err = l.transition(ctx, op, management.PhaseQuarantined, management.OperationTransition{})
		if err != nil {
			return op, err
		}
	}
	if op.Phase == management.PhaseQuarantined {
		path := l.quarantinePath(op)
		if err := l.verifyRootProof(ctx, op, volume, path); err != nil {
			return op, err
		}
		if err := installLifecycleXattr(path, reclaimProofXattr, op.PlannedReclaimProof); err != nil {
			return op, err
		}
		if err := syncLifecycleDirectory(path); err != nil {
			return op, err
		}
		if err := syncLifecycleDirectory(l.config.QuarantineDir); err != nil {
			return op, err
		}
		proof, err := json.Marshal(lifecycleReclaimProof{Path: path, Marker: op.PlannedReclaimProof, Root: op.VerifiedRootProof})
		if err != nil {
			return op, err
		}
		op, err = l.transition(ctx, op, management.PhaseReclaiming, management.OperationTransition{ReclaimingProof: proof})
		if err != nil {
			return op, err
		}
	}
	if op.Phase != management.PhaseReclaiming {
		return op, management.ErrCorrupt
	}
	done, err := l.reclaim(ctx, op)
	if err != nil {
		return op, err
	}
	if !done {
		return op, nil
	}
	return l.complete(ctx, op, struct {
		VolumeID management.ID `json:"volumeId"`
	}{volume.ID})
}

func (l *Lifecycle) verifyRootProof(ctx context.Context, op management.Operation, volume management.Volume, path string) error {
	var proof lifecycleRootProof
	if err := json.Unmarshal(op.VerifiedRootProof, &proof); err != nil {
		return errors.Join(management.ErrCorrupt, err)
	}
	if proof.Volume != volume.ID || proof.Operation != op.ID || proof.StorageName != volume.StorageName || len(proof.Native) == 0 {
		return management.ErrCorrupt
	}
	marker, err := readLifecycleBinding(path)
	if err != nil {
		return err
	}
	if sha256.Sum256(marker) != proof.BindingDigest {
		return syscall.EIO
	}
	if op.Kind == management.OperationDelete {
		if proof.ClosedBinding == nil {
			return management.ErrCorrupt
		}
		closed, err := l.closedBinding(ctx, path)
		if err != nil {
			return err
		}
		if closed != *proof.ClosedBinding {
			return syscall.EIO
		}
	}
	return nil
}
