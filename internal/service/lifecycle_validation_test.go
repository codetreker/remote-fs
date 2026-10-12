package service

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
)

func TestMaintenanceDeletedTombstoneRemainsValidAfterReceiptGC(t *testing.T) {
	f := newLifecycleFixture(t, func(o *management.Options) { o.NamespaceLifetime = time.Second; o.ReceiptRetention = time.Millisecond })
	created := f.create()
	op := f.deletion(created)
	var err error
	for i := 0; op.State != management.OperationSucceeded && i < 10; i++ {
		op, err = f.controller.Run(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
	}
	if op.State != management.OperationSucceeded {
		t.Fatal("delete not settled")
	}
	time.Sleep(1050 * time.Millisecond)
	if _, err := f.controller.RetireNamespaces(t.Context(), f.actor.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Operation(t.Context(), f.actor.ID, op.ID); !errors.Is(err, management.ErrNotFound) {
		t.Fatalf("terminal receipt was not GC'd: %v", err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, op.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	maintenance := &Maintenance{store: f.store, root: &stateRoot{VolumesPath: f.config.VolumesDir, QuarantinePath: f.config.QuarantineDir}}
	if err := validateMaintenanceLifecycle(t.Context(), maintenance, []management.Volume{volume}); err != nil {
		t.Fatalf("valid archived tombstone rejected: %v", err)
	}
	if err := os.Mkdir(filepath.Join(f.config.QuarantineDir, string(volume.ID)+"-"+string(volume.LastOperationID)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateMaintenanceLifecycle(t.Context(), maintenance, []management.Volume{volume}); err == nil {
		t.Fatal("archived tombstone accepted undeleted quarantine")
	}
}

func TestMaintenancePendingLifecycleEvidenceSurvivesEachDurableBoundary(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	maintenance := &Maintenance{store: f.store, root: &stateRoot{VolumesPath: f.config.VolumesDir, QuarantinePath: f.config.QuarantineDir}}
	op, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("create"), "volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, op.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	assertValid := func() {
		t.Helper()
		var err error
		volume, err = f.store.GetVolume(t.Context(), f.actor.ID, op.VolumeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateMaintenanceLifecycle(t.Context(), maintenance, []management.Volume{volume}); err != nil {
			t.Fatalf("phase %s evidence invalid: %v", op.Phase, err)
		}
	}
	assertValid()
	op, err = f.controller.transition(t.Context(), op, management.PhaseInitializing, management.OperationTransition{})
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	if err := f.controller.prepareCreatedRoot(op, volume); err != nil {
		t.Fatal(err)
	}
	assertValid()
	native, err := f.config.Hooks.Initialize(t.Context(), volume, 4096)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := readLifecycleBinding(f.controller.volumePath(volume))
	if err != nil {
		t.Fatal(err)
	}
	rootProof, err := json.Marshal(lifecycleRootProof{Volume: volume.ID, Operation: op.ID, StorageName: volume.StorageName, Native: native, BindingDigest: sha256.Sum256(marker)})
	if err != nil {
		t.Fatal(err)
	}
	op, err = f.controller.transition(t.Context(), op, management.PhaseBound, management.OperationTransition{VerifiedRootProof: rootProof})
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	op, err = f.controller.Run(t.Context(), op)
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	op = f.deletion(op)
	op, err = f.controller.transition(t.Context(), op, management.PhaseFrozen, management.OperationTransition{})
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	closed, err := f.controller.closedBinding(t.Context(), f.controller.volumePath(volume))
	if err != nil {
		t.Fatal(err)
	}
	rootProof, err = json.Marshal(lifecycleRootProof{Volume: volume.ID, Operation: op.ID, StorageName: volume.StorageName, Native: native, BindingDigest: sha256.Sum256(marker), ClosedBinding: &closed})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := json.Marshal(lifecycleReclaimMarker{Volume: volume.ID, Operation: op.ID, Token: "durable-operation-proof"})
	if err != nil {
		t.Fatal(err)
	}
	op, err = f.controller.transition(t.Context(), op, management.PhaseVerifiedClose, management.OperationTransition{VerifiedRootProof: rootProof, PlannedReclaimProof: planned, QuarantineLeaf: string(volume.ID) + "-" + string(op.ID)})
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	if err := f.controller.quarantineRoot(t.Context(), op, volume); err != nil {
		t.Fatal(err)
	}
	assertValid()
	op, err = f.controller.transition(t.Context(), op, management.PhaseQuarantined, management.OperationTransition{})
	if err != nil {
		t.Fatal(err)
	}
	assertValid()
	target := f.controller.quarantinePath(op)
	if err := installLifecycleXattr(target, reclaimProofXattr, planned); err != nil {
		t.Fatal(err)
	}
	if err := installLifecycleXattr(target, reclaimProofXattr, planned); err != nil {
		t.Fatalf("matching interrupted proof did not replay: %v", err)
	}
	if err := installLifecycleXattr(target, reclaimProofXattr, []byte("changed")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("proof overwrite accepted: %v", err)
	}
	assertValid()
	if err := unix.Setxattr(target, reclaimProofXattr, []byte("wrong"), 0); err != nil {
		t.Fatal(err)
	}
	if err := validateMaintenanceLifecycle(t.Context(), maintenance, []management.Volume{volume}); err == nil {
		t.Fatal("offline inspector accepted mismatched proof")
	}
	if err := unix.Setxattr(target, reclaimProofXattr, planned, 0); err != nil {
		t.Fatal(err)
	}
	reclaimProof, err := json.Marshal(lifecycleReclaimProof{Path: target, Marker: planned, Root: rootProof})
	if err != nil {
		t.Fatal(err)
	}
	op, err = f.controller.transition(t.Context(), op, management.PhaseReclaiming, management.OperationTransition{ReclaimingProof: reclaimProof})
	if err != nil {
		t.Fatal(err)
	}
	for _, leaf := range []string{"LOCALSTORE", "METASTORE", "metastore.sqlite"} {
		if err := os.Remove(filepath.Join(target, leaf)); err != nil {
			t.Fatal(err)
		}
	}
	assertValid()
	op, err = f.controller.Run(t.Context(), op)
	if err != nil || op.State != management.OperationSucceeded {
		t.Fatalf("partially reclaimed authority could not complete: %+v %v", op, err)
	}
	assertValid()
	orphan := filepath.Join(f.config.VolumesDir, "unregistered")
	if err := os.Mkdir(orphan, 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateMaintenanceLifecycle(t.Context(), maintenance, []management.Volume{volume}); err == nil {
		t.Fatal("offline inspector imported orphan")
	}
}
