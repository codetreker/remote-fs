package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
)

// Offline inspection consumes durable evidence without opening or repairing a
// partially initialized or reclaimed authority.
func validateMaintenanceLifecycle(ctx context.Context, m *Maintenance, volumes []management.Volume) error {
	if m == nil || m.store == nil || m.root == nil {
		return syscall.EINVAL
	}
	lifecycle := &Lifecycle{config: LifecycleConfig{Management: m.store, VolumesDir: m.root.VolumesPath, QuarantineDir: m.root.QuarantinePath}}
	knownRoots := make(map[string]bool, len(volumes))
	knownQuarantines := make(map[string]bool)
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := lifecycle.volumePath(volume)
		if volume.State == management.VolumeActive {
			knownRoots[volume.StorageName] = true
			continue
		}
		if volume.State == management.VolumeDeleted {
			// The tombstone persists after the original terminal receipt expires.
			if err := lifecyclePathAbsent(path); err != nil {
				return err
			}
			leaf := string(volume.ID) + "-" + string(volume.LastOperationID)
			if err := lifecyclePathAbsent(filepath.Join(m.root.QuarantinePath, leaf)); err != nil {
				return err
			}
			continue
		}
		op, err := m.store.Operation(ctx, volume.OwnerUserID, volume.LastOperationID)
		if err != nil {
			return err
		}
		if volume.State == management.VolumeCreating || volume.State == management.VolumeFailed {
			if op.Kind != management.OperationCreate {
				return management.ErrCorrupt
			}
			if op.Phase == management.PhaseAccepted {
				if err := lifecyclePathAbsent(path); err != nil {
					return err
				}
				continue
			}
			if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) && op.Phase == management.PhaseInitializing {
				continue
			} else if err != nil {
				return err
			}
			if err := validateLifecycleCreateMarker(path, op); err != nil {
				return err
			}
			if op.Phase == management.PhaseBound || op.Phase == management.PhaseCompleted {
				if err := lifecycle.verifyRootProof(ctx, op, volume, path); err != nil {
					return err
				}
			}
			knownRoots[volume.StorageName] = true
			continue
		}
		if volume.State != management.VolumeDeleting || op.Kind != management.OperationDelete {
			return management.ErrCorrupt
		}
		switch op.Phase {
		case management.PhaseFrozen:
			if _, err := readLifecycleBinding(path); err != nil {
				return err
			}
			knownRoots[volume.StorageName] = true
		case management.PhaseVerifiedClose:
			target := lifecycle.quarantinePath(op)
			sourceErr := lifecyclePathAbsent(path)
			targetErr := lifecyclePathAbsent(target)
			if sourceErr == nil && targetErr == nil {
				return syscall.EIO
			}
			if sourceErr != nil && targetErr != nil {
				return errors.Join(sourceErr, targetErr)
			}
			if sourceErr != nil {
				if !errors.Is(sourceErr, syscall.EEXIST) {
					return sourceErr
				}
				if err := lifecycle.verifyRootProof(ctx, op, volume, path); err != nil {
					return err
				}
				knownRoots[volume.StorageName] = true
			} else {
				if !errors.Is(targetErr, syscall.EEXIST) {
					return targetErr
				}
				if err := lifecycle.verifyRootProof(ctx, op, volume, target); err != nil {
					return err
				}
				if err := validateLifecycleReclaimMarker(target, op.PlannedReclaimProof, true); err != nil {
					return err
				}
				knownQuarantines[op.QuarantineLeaf] = true
			}
		case management.PhaseQuarantined:
			if err := lifecyclePathAbsent(path); err != nil {
				return err
			}
			target := lifecycle.quarantinePath(op)
			if err := lifecycle.verifyRootProof(ctx, op, volume, target); err != nil {
				return err
			}
			if err := validateLifecycleReclaimMarker(target, op.PlannedReclaimProof, true); err != nil {
				return err
			}
			knownQuarantines[op.QuarantineLeaf] = true
		case management.PhaseReclaiming:
			if err := lifecyclePathAbsent(path); err != nil {
				return err
			}
			target := lifecycle.quarantinePath(op)
			var proof lifecycleReclaimProof
			if err := json.Unmarshal(op.ReclaimingProof, &proof); err != nil {
				return errors.Join(management.ErrCorrupt, err)
			}
			if proof.Path != target || !bytes.Equal(proof.Marker, op.PlannedReclaimProof) || !bytes.Equal(proof.Root, op.VerifiedRootProof) {
				return management.ErrCorrupt
			}
			if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				return err
			}
			if err := validateLifecycleReclaimMarker(target, proof.Marker, false); err != nil {
				return err
			}
			knownQuarantines[op.QuarantineLeaf] = true
		default:
			return management.ErrCorrupt
		}
	}
	if err := validateLifecycleKnownEntries(ctx, m.root.VolumesPath, knownRoots); err != nil {
		return err
	}
	return validateLifecycleKnownEntries(ctx, m.root.QuarantinePath, knownQuarantines)
}

func lifecyclePathAbsent(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected lifecycle root %s: %w", path, syscall.EEXIST)
}

func validateLifecycleKnownEntries(ctx context.Context, path string, known map[string]bool) error {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return err
	}
	defer root.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := root.ReadDir(50)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for _, entry := range entries {
			if !known[entry.Name()] {
				return fmt.Errorf("unregistered lifecycle root %s: %w", filepath.Join(path, entry.Name()), syscall.EIO)
			}
			directory, err := lifecycleDirectory(filepath.Join(path, entry.Name()))
			if err != nil {
				return err
			}
			if err := directory.Close(); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}
