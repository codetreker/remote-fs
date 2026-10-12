package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func newNativeLifecycleFixture(t *testing.T, adjust func(*management.Options)) *lifecycleFixture {
	f := newLifecycleFixture(t, adjust)
	open := func(ctx context.Context, v management.Volume, initialize bool, quota int64) (*localstore.Store, error) {
		return localstore.Open(ctx, localstore.Config{Root: filepath.Join(f.config.VolumesDir, v.StorageName), Volume: v.StorageName, Quota: quota, QuotaPolicy: sqlite.DurableQuotaPolicy, InitializeQuota: initialize, Window: sqlite.DefaultWindow(), Maintenance: objectstore.DefaultOptions()})
	}
	proof := func(ctx context.Context, store *localstore.Store) ([]byte, error) {
		id, err := store.BackendIdentity(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(struct {
			Volume     storage.VolumeID `json:"volume"`
			RootNodeID uint64           `json:"rootNodeId"`
		}{id.Volume, id.RootNodeID})
	}
	f.config.Hooks.Initialize = func(ctx context.Context, v management.Volume, q int64) (result []byte, resultErr error) {
		store, err := open(ctx, v, true, q)
		if err != nil {
			return nil, err
		}
		defer func() { resultErr = errors.Join(resultErr, store.Close()) }()
		return proof(ctx, store)
	}
	f.config.Hooks.Acquire = func(ctx context.Context, v management.Volume) (LifecycleLease, error) {
		store, err := open(ctx, v, false, 0)
		if err != nil {
			return LifecycleLease{}, err
		}
		return LifecycleLease{Store: store, Release: store.Close}, nil
	}
	f.config.Hooks.Freeze = func(ctx context.Context, v management.Volume) (LifecycleRetirement, error) {
		store, err := open(ctx, v, false, 0)
		if err != nil {
			return LifecycleRetirement{}, err
		}
		return LifecycleRetirement{Close: func(ctx context.Context) ([]byte, error) {
			identity, err := proof(ctx, store)
			if err != nil {
				return nil, err
			}
			return identity, store.Close()
		}, Cancel: store.Close}, nil
	}
	var err error
	f.controller, err = NewLifecycle(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestLifecycleNativeBoundCreateRejectsMissingWitness(t *testing.T) {
	f := newNativeLifecycleFixture(t, nil)
	op, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("create"), "volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, op.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	op, err = f.controller.transition(t.Context(), op, management.PhaseInitializing, management.OperationTransition{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.controller.prepareCreatedRoot(op, volume); err != nil {
		t.Fatal(err)
	}
	native, err := f.config.Hooks.Initialize(t.Context(), volume, 4096)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := readLifecycleBinding(f.controller.volumePath(volume))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := json.Marshal(lifecycleRootProof{Volume: volume.ID, Operation: op.ID, StorageName: volume.StorageName, Native: native, BindingDigest: sha256.Sum256(marker)})
	if err != nil {
		t.Fatal(err)
	}
	op, err = f.controller.transition(t.Context(), op, management.PhaseBound, management.OperationTransition{VerifiedRootProof: proof})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.controller.volumePath(volume), "METASTORE")
	witness, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	recovered, err := f.controller.Run(t.Context(), op)
	if err == nil || recovered.State != management.OperationRunning {
		t.Fatalf("incomplete native root confirmed: %+v %v", recovered, err)
	}
	if err := os.WriteFile(path, witness, 0600); err != nil {
		t.Fatal(err)
	}
	recovered, err = f.controller.Run(t.Context(), op)
	if err != nil || recovered.State != management.OperationSucceeded {
		t.Fatalf("valid native root cannot recover: %+v %v", recovered, err)
	}
}

func TestLifecycleNativeAcceptedQuotaSurvivesExpiredNamespace(t *testing.T) {
	f := newNativeLifecycleFixture(t, func(o *management.Options) { o.NamespaceLifetime = time.Second })
	created := f.create()
	op, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("quota"), created.VolumeID, 1, 8192)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1050 * time.Millisecond)
	recovered, err := f.controller.Run(t.Context(), op)
	if err != nil || recovered.State != management.OperationSucceeded {
		t.Fatalf("durably accepted quota expired: %+v %v", recovered, err)
	}
}

func TestLifecycleNativeCreateOnlyNamespaceRetirement(t *testing.T) {
	f := newNativeLifecycleFixture(t, func(o *management.Options) { o.NamespaceLifetime = time.Second; o.ReceiptRetention = time.Millisecond })
	created := f.create()
	time.Sleep(1050 * time.Millisecond)
	floor, err := f.controller.RetireNamespaces(t.Context(), f.actor.ID)
	if err != nil || floor != created.Action.Namespace {
		t.Fatalf("create-only volume cannot retire native floor: %d %v", floor, err)
	}
}

func TestLifecycleNativeAcceptedQuotaSurvivesNewerRegisteredNamespace(t *testing.T) {
	f := newNativeLifecycleFixture(t, nil)
	created := f.create()
	op, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("quota"), created.VolumeID, 1, 8192)
	if err != nil {
		t.Fatal(err)
	}
	later, err := f.store.IssueNamespace(t.Context(), f.actor.ID)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, created.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.config.Hooks.Acquire(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	registeredErr := lease.Store.RegisterActionNamespace(t.Context(), storage.ActionNamespace{SubjectID: string(f.actor.ID), Sequence: uint64(later.Sequence), AcceptUntil: later.AcceptUntil})
	if err := errors.Join(registeredErr, lease.Release()); err != nil {
		t.Fatal(err)
	}
	recovered, err := f.controller.Run(t.Context(), op)
	if err != nil || recovered.State != management.OperationSucceeded {
		t.Fatalf("newer namespace prevented accepted older effect: %+v %v", recovered, err)
	}
}

func TestLifecycleNativeQuotaRefusalWithSemanticErrorSettlesManagement(t *testing.T) {
	f := newNativeLifecycleFixture(t, nil)
	created := f.create()
	first, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("quota"), created.VolumeID, 1, 8192)
	if err != nil {
		t.Fatal(err)
	}
	first, err = f.controller.Run(t.Context(), first)
	if err != nil || first.State != management.OperationSucceeded {
		t.Fatalf("first quota: %+v %v", first, err)
	}
	stale, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("stale"), created.VolumeID, 1, 16384)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := f.controller.Run(t.Context(), stale)
	if err != nil || failed.State != management.OperationFailed || !failed.Settled {
		t.Fatalf("valid native refusal stayed applying: %+v %v", failed, err)
	}
	replay, err := f.controller.Run(t.Context(), stale)
	if err != nil || replay.State != management.OperationFailed || replay.ErrorCode != failed.ErrorCode || replay.Revision != failed.Revision {
		t.Fatalf("refusal replay changed: %+v %v", replay, err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, created.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.config.Hooks.Acquire(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	summary, summaryErr := lease.Store.Summary(t.Context())
	if err := errors.Join(summaryErr, lease.Release()); err != nil {
		t.Fatal(err)
	}
	if summary.QuotaBytes != 8192 || summary.QuotaRevision != 2 {
		t.Fatalf("refused quota changed state: %+v", summary)
	}
	if _, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("later"), created.VolumeID, 2, 12288); err != nil {
		t.Fatalf("refused operation still blocked later mutation: %v", err)
	}
}

func TestLifecycleNativeDeletionCapturesCompleteClosedBinding(t *testing.T) {
	f := newNativeLifecycleFixture(t, nil)
	created := f.create()
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, created.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.config.Hooks.Acquire(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := lease.Store.(*localstore.Store).Write(t.Context(), "file", []byte("durable payload"))
	if err := errors.Join(writeErr, lease.Release()); err != nil {
		t.Fatal(err)
	}
	op := f.deletion(created)
	for i := 0; op.State != management.OperationSucceeded && i < 100; i++ {
		op, err = f.controller.Run(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
	}
	if op.State != management.OperationSucceeded {
		t.Fatal("native deletion remained pending")
	}
	var proof lifecycleRootProof
	if err := json.Unmarshal(op.VerifiedRootProof, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.ClosedBinding == nil || proof.ClosedBinding.DatabaseBytes < 4096 {
		t.Fatalf("missing durable closed native files: %+v", proof)
	}
	if _, err := os.Stat(filepath.Join(f.config.VolumesDir, string(volume.ID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native root remains: %v", err)
	}
	if _, err := os.Stat(f.controller.quarantinePath(op)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native quarantine remains: %v", err)
	}
}
