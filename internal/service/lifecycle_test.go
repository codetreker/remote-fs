package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

type lifecycleNativeFixture struct {
	mu       sync.Mutex
	actions  map[storage.ActionKey]storage.QuotaReceipt
	quota    int64
	revision uint64
	effects  int
	retired  uint64
	refusal  syscall.Errno
	failure  error
}

func (s *lifecycleNativeFixture) CheckVolumeSummary() error { return nil }
func (s *lifecycleNativeFixture) Summary(context.Context) (storage.VolumeSummaryResult, error) {
	return storage.VolumeSummaryResult{QuotaBytes: s.quota, QuotaRevision: s.revision}, nil
}
func (s *lifecycleNativeFixture) CheckQuotaAcceptance() error { return nil }
func (s *lifecycleNativeFixture) AcceptQuotaAction(_ context.Context, a storage.AcceptedQuotaAction) (storage.QuotaReceipt, error) {
	if err := a.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.actions[a.Command.Action]; ok {
		return r, nil
	}
	return storage.QuotaReceipt{Action: a.Command.Action, Fingerprint: a.Command.Fingerprint, Outcome: storage.DurableActionPending}, nil
}
func (s *lifecycleNativeFixture) CheckQuotaControl() error     { return nil }
func (s *lifecycleNativeFixture) CheckActionNamespaces() error { return nil }
func (s *lifecycleNativeFixture) RegisterActionNamespace(context.Context, storage.ActionNamespace) error {
	return nil
}
func (s *lifecycleNativeFixture) RetireActionNamespaces(_ context.Context, r storage.ActionNamespaceRetirement) error {
	s.retired = r.Before
	return s.failure
}
func (s *lifecycleNativeFixture) QueryQuotaAction(_ context.Context, k storage.ActionKey) (storage.QuotaReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.actions[k]
	if !ok {
		return storage.QuotaReceipt{}, syscall.ENOENT
	}
	return r, nil
}
func (s *lifecycleNativeFixture) SetQuota(_ context.Context, c storage.QuotaCommand) (storage.QuotaReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.actions[c.Action]; ok {
		return r, nil
	}
	r := storage.QuotaReceipt{Action: c.Action, Fingerprint: c.Fingerprint, Outcome: storage.DurableActionCompleted, QuotaBytes: c.QuotaBytes, QuotaRevision: s.revision + 1}
	if s.refusal != 0 {
		r.Outcome = storage.DurableActionNotExecuted
		r.Failure = s.refusal
		r.QuotaBytes = 0
		r.QuotaRevision = 0
	} else {
		s.effects++
		s.quota = c.QuotaBytes
		s.revision++
	}
	s.actions[c.Action] = r
	if r.Failure != 0 {
		return r, r.Failure
	}
	return r, s.failure
}

type lifecycleFixture struct {
	t           *testing.T
	store       *management.Store
	actor       management.User
	controller  *Lifecycle
	config      LifecycleConfig
	native      *lifecycleNativeFixture
	freezeError error
	closeError  error
	initialized int
	canceled    int
}

func newLifecycleFixture(t *testing.T, adjust func(*management.Options)) *lifecycleFixture {
	t.Helper()
	base, err := filepath.Abs("../../.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(base, "lifecycle-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	options := management.DefaultOptions()
	options.Create = true
	if adjust != nil {
		adjust(&options)
	}
	store, err := management.Open(t.Context(), filepath.Join(dir, "management.sqlite"), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	actor, err := store.BootstrapAdmin(t.Context(), "root", "Root", "hash")
	if err != nil {
		t.Fatal(err)
	}
	f := &lifecycleFixture{t: t, store: store, actor: actor, native: &lifecycleNativeFixture{actions: make(map[storage.ActionKey]storage.QuotaReceipt), quota: 4096, revision: 1}}
	volumes := filepath.Join(dir, "volumes")
	quarantine := filepath.Join(dir, "quarantine")
	for _, path := range []string{volumes, quarantine} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	f.config = LifecycleConfig{Management: store, VolumesDir: volumes, QuarantineDir: quarantine, MaxReclaimEntries: 2, MaxConcurrentOperations: 4, Hooks: LifecycleHooks{
		Initialize: func(_ context.Context, v management.Volume, q int64) ([]byte, error) {
			f.initialized++
			f.native.quota = q
			marker := make([]byte, 72)
			copy(marker, "durable test binding")
			if err := os.WriteFile(filepath.Join(volumes, v.StorageName, "LOCALSTORE"), marker, 0600); err != nil {
				return nil, err
			}
			for _, leaf := range []string{"METASTORE", "metastore.sqlite"} {
				if err := os.WriteFile(filepath.Join(volumes, v.StorageName, leaf), []byte("closed test authority"), 0600); err != nil {
					return nil, err
				}
			}
			return []byte("verified-native-identity"), nil
		},
		Acquire: func(context.Context, management.Volume) (LifecycleLease, error) {
			return LifecycleLease{Store: f.native, Release: func() error { return nil }}, nil
		},
		Freeze: func(context.Context, management.Volume) (LifecycleRetirement, error) {
			if f.freezeError != nil {
				return LifecycleRetirement{}, f.freezeError
			}
			return LifecycleRetirement{Close: func(context.Context) ([]byte, error) { return []byte("verified-native-identity"), f.closeError }, Cancel: func() error { f.canceled++; return nil }}, nil
		},
	}}
	f.controller, err = NewLifecycle(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *lifecycleFixture) key(nonce string) management.ActionKey {
	f.t.Helper()
	ns, err := f.store.IssueNamespace(f.t.Context(), f.actor.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	return management.ActionKey{Namespace: ns.Sequence, Nonce: nonce}
}
func (f *lifecycleFixture) create() management.Operation {
	f.t.Helper()
	op, err := f.store.CreateVolume(f.t.Context(), f.actor.ID, f.key("create"), "volume", 4096)
	if err != nil {
		f.t.Fatal(err)
	}
	op, err = f.controller.Run(f.t.Context(), op)
	if err != nil {
		f.t.Fatal(err)
	}
	if op.State != management.OperationSucceeded {
		f.t.Fatalf("create not completed: %+v", op)
	}
	return op
}
func (f *lifecycleFixture) deletion(create management.Operation) management.Operation {
	f.t.Helper()
	volume, err := f.store.GetVolume(f.t.Context(), f.actor.ID, create.VolumeID)
	if err != nil {
		f.t.Fatal(err)
	}
	op, err := f.store.DeleteVolume(f.t.Context(), f.actor.ID, f.key("delete"), volume.ID, volume.ManagementRevision)
	if err != nil {
		f.t.Fatal(err)
	}
	return op
}

func TestLifecycleCreateRejectsUnknownRootAndRecoversInitializedIntent(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	op, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("create"), "volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.config.VolumesDir, string(op.VolumeID))
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	running, err := f.controller.Run(t.Context(), op)
	if err == nil || running.Phase != management.PhaseInitializing || f.initialized != 0 {
		t.Fatalf("unknown root initialized: op=%+v err=%v calls=%d", running, err, f.initialized)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	original := f.config.Hooks.Initialize
	f.controller.config.Hooks.Initialize = func(ctx context.Context, v management.Volume, q int64) ([]byte, error) {
		proof, err := original(ctx, v, q)
		if err != nil {
			return nil, err
		}
		if err := os.Chmod(f.config.VolumesDir, 0755); err != nil {
			return nil, err
		}
		return proof, nil
	}
	running, err = f.controller.Run(t.Context(), running)
	if err == nil || running.Phase != management.PhaseInitializing {
		t.Fatalf("parent sync failure confirmed create: %+v %v", running, err)
	}
	if err := os.Chmod(f.config.VolumesDir, 0700); err != nil {
		t.Fatal(err)
	}
	f.controller, err = NewLifecycle(f.config)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := f.controller.Run(t.Context(), op)
	if err != nil || completed.State != management.OperationSucceeded {
		t.Fatalf("create intent recovery: %+v %v", completed, err)
	}
	again, err := f.controller.Run(t.Context(), op)
	if err != nil || again.ID != completed.ID || f.initialized != 2 {
		t.Fatalf("create re-executed: %+v %v %d", again, err, f.initialized)
	}
}

func TestLifecycleBusyDeleteLeavesActiveAndCloseFailureRetainsFrozen(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	created := f.create()
	op := f.deletion(created)
	f.freezeError = syscall.EBUSY
	running, err := f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EBUSY) || running.Phase != management.PhaseAccepted || running.State != management.OperationFailed {
		t.Fatalf("busy deletion froze: %+v %v", running, err)
	}
	volume, err := f.store.GetVolume(t.Context(), f.actor.ID, created.VolumeID)
	if err != nil || volume.State != management.VolumeActive {
		t.Fatalf("busy deletion changed active state: %+v %v", volume, err)
	}
	f.freezeError = nil
	op = f.deletion(created)
	f.closeError = syscall.EIO
	running, err = f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EIO) || running.Phase != management.PhaseFrozen {
		t.Fatalf("unknown close released: %+v %v", running, err)
	}
	if _, err := os.Stat(filepath.Join(f.config.VolumesDir, string(created.VolumeID))); err != nil {
		t.Fatal(err)
	}
	f.closeError = nil
	for i := 0; i < 10 && running.State != management.OperationSucceeded; i++ {
		running, err = f.controller.Run(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
	}
	if running.State != management.OperationSucceeded {
		t.Fatalf("delete not settled: %+v", running)
	}
	volume, err = f.store.GetVolume(t.Context(), f.actor.ID, created.VolumeID)
	if err != nil || volume.State != management.VolumeDeleted {
		t.Fatalf("delete lacks tombstone: %+v %v", volume, err)
	}
}

func TestLifecycleQuarantineCollisionCannotDeleteUnknownData(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	created := f.create()
	op := f.deletion(created)
	target := filepath.Join(f.config.QuarantineDir, string(op.VolumeID)+"-"+string(op.ID))
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(target, "sentinel")
	if err := os.WriteFile(sentinel, []byte("unknown"), 0600); err != nil {
		t.Fatal(err)
	}
	running, err := f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EEXIST) || running.Phase != management.PhaseVerifiedClose {
		t.Fatalf("collision accepted: %+v %v", running, err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "unknown" {
		t.Fatalf("unknown target changed: %q %v", data, err)
	}
}

func TestLifecycleReclaimRequiresCopyPreservedProofAndKeepsBounds(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	created := f.create()
	root := filepath.Join(f.config.VolumesDir, string(created.VolumeID))
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	op := f.deletion(created)
	running, err := f.controller.Run(t.Context(), op)
	if err != nil || running.Phase != management.PhaseReclaiming || running.State != management.OperationRunning {
		t.Fatalf("bounded delete: %+v %v", running, err)
	}
	target := f.controller.quarantinePath(running)
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("reclaimed unbounded entries: %d", len(entries))
	}
	if err := unix.Removexattr(target, reclaimProofXattr); err != nil {
		t.Fatal(err)
	}
	resumed, err := f.controller.Run(t.Context(), op)
	if err == nil || resumed.Phase != management.PhaseReclaiming {
		t.Fatalf("missing proof reclaimed: %+v %v", resumed, err)
	}
	if err := unix.Setxattr(target, reclaimProofXattr, []byte("wrong"), unix.XATTR_CREATE); err != nil {
		t.Fatal(err)
	}
	if _, err := f.controller.Run(t.Context(), op); err == nil {
		t.Fatal("wrong proof reclaimed")
	}
	if err := unix.Setxattr(target, reclaimProofXattr, running.PlannedReclaimProof, 0); err != nil {
		t.Fatal(err)
	}
	copyRoot := target + "-copy"
	if err := os.Mkdir(copyRoot, 0700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(target, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copyRoot, entry.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Setxattr(copyRoot, reclaimProofXattr, running.PlannedReclaimProof, unix.XATTR_CREATE); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(target, target+"-original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(copyRoot, target); err != nil {
		t.Fatal(err)
	}
	f.controller, err = NewLifecycle(f.config)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && resumed.State != management.OperationSucceeded; i++ {
		resumed, err = f.controller.Run(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
	}
	if resumed.State != management.OperationSucceeded {
		t.Fatal("copied proof did not resume")
	}
	if _, err := os.Stat(target + "-original"); err != nil {
		t.Fatalf("unregistered copy deleted: %v", err)
	}
}

func TestLifecycleQuotaRecoversExactNativeEffectAndDefinitiveRefusal(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	created := f.create()
	op, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("quota"), created.VolumeID, 1, 8192)
	if err != nil {
		t.Fatal(err)
	}
	f.native.failure = syscall.EIO
	running, err := f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EIO) || running.Phase != management.PhaseApplying {
		t.Fatalf("unknown native result forgotten: %+v %v", running, err)
	}
	f.native.failure = nil
	completed, err := f.controller.Run(t.Context(), op)
	if err != nil || completed.State != management.OperationSucceeded || f.native.effects != 1 {
		t.Fatalf("quota replay changed effect: %+v %v count=%d", completed, err, f.native.effects)
	}
	f.native.refusal = syscall.EDQUOT
	op, err = f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("refusal"), created.VolumeID, 2, 4096)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := f.controller.Run(t.Context(), op)
	if err != nil || refused.State != management.OperationFailed || !refused.Settled {
		t.Fatalf("native refusal not queryable: %+v %v", refused, err)
	}
}

func TestLifecycleRetirementWaitsForNativeAndPreventsReplay(t *testing.T) {
	f := newLifecycleFixture(t, func(o *management.Options) { o.NamespaceLifetime = time.Second; o.ReceiptRetention = time.Millisecond })
	created := f.create()
	// Wait only for the explicit admission window; the durable effect is already settled.
	time.Sleep(1050 * time.Millisecond)
	f.native.failure = syscall.EIO
	if _, err := f.controller.RetireNamespaces(t.Context(), f.actor.ID); !errors.Is(err, syscall.EIO) {
		t.Fatalf("native error lost: %v", err)
	}
	if _, err := f.store.FindOperation(t.Context(), f.actor.ID, created.Action); err != nil {
		t.Fatalf("management GC preceded native settlement: %v", err)
	}
	f.native.failure = nil
	floor, err := f.controller.RetireNamespaces(t.Context(), f.actor.ID)
	if err != nil || floor != created.Action.Namespace || f.native.retired != uint64(floor)+1 {
		t.Fatalf("retirement floor mismatch: %d %v native=%d", floor, err, f.native.retired)
	}
	if _, err := f.store.CreateVolume(t.Context(), f.actor.ID, created.Action, "volume", 4096); !errors.Is(err, management.ErrHistoryExpired) {
		t.Fatalf("retired action reexecuted: %v", err)
	}
}

func TestLifecycleSeparateVolumesProgressAndWaitersStayBounded(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	first, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("first"), "first", 4096)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("second"), "second", 4096)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	unblock := make(chan struct{})
	finished := make(chan error, 1)
	f.controller.config.Hooks.Initialize = func(_ context.Context, v management.Volume, _ int64) ([]byte, error) {
		if v.ID == first.VolumeID {
			close(entered)
			<-unblock
		}
		marker := make([]byte, 72)
		if err := os.WriteFile(filepath.Join(f.config.VolumesDir, v.StorageName, "LOCALSTORE"), marker, 0600); err != nil {
			return nil, err
		}
		return []byte("verified-native-identity"), nil
	}
	go func() { _, err := f.controller.Run(t.Context(), first); finished <- err }()
	<-entered
	completed, err := f.controller.Run(t.Context(), second)
	if err != nil || completed.State != management.OperationSucceeded {
		t.Fatalf("separate volume blocked: %+v %v", completed, err)
	}
	close(unblock)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	held, err := f.controller.lockOperation(t.Context(), first.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.controller.lockOperation(canceled, first.VolumeID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter admitted: %v", err)
	}
	held()
	f.controller.mu.Lock()
	remaining := len(f.controller.locks)
	f.controller.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("retained volume locks: %d", remaining)
	}
}

func TestLifecycleBoundCreateRevalidatesCompleteNativeAuthority(t *testing.T) {
	f := newLifecycleFixture(t, nil)
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
	f.controller.config.Hooks.Initialize = func(context.Context, management.Volume, int64) ([]byte, error) { return nil, syscall.EIO }
	recovered, err := f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EIO) || recovered.State != management.OperationRunning || recovered.Phase != management.PhaseBound {
		t.Fatalf("incomplete native authority reported created: %+v %v", recovered, err)
	}
	f.controller.config.Hooks.Initialize = func(context.Context, management.Volume, int64) ([]byte, error) {
		return []byte("other-native-identity"), nil
	}
	if _, err := f.controller.Run(t.Context(), op); !errors.Is(err, syscall.EIO) {
		t.Fatalf("changed native identity accepted: %v", err)
	}
	f.controller.config.Hooks.Initialize = f.config.Hooks.Initialize
	completed, err := f.controller.Run(t.Context(), op)
	if err != nil || completed.State != management.OperationSucceeded {
		t.Fatalf("valid native bound recovery failed: %+v %v", completed, err)
	}
}

func TestLifecycleVerifiedCloseRejectsMissingOrReplacedDatabaseBinding(t *testing.T) {
	f := newLifecycleFixture(t, nil)
	created := f.create()
	op := f.deletion(created)
	target := filepath.Join(f.config.QuarantineDir, string(op.VolumeID)+"-"+string(op.ID))
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	running, err := f.controller.Run(t.Context(), op)
	if !errors.Is(err, syscall.EEXIST) || running.Phase != management.PhaseVerifiedClose {
		t.Fatalf("failed to establish closed responsibility: %+v %v", running, err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(f.config.VolumesDir, string(op.VolumeID))
	for _, leaf := range []string{"METASTORE", "metastore.sqlite"} {
		path := filepath.Join(root, leaf)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		checked, err := f.controller.Run(t.Context(), op)
		if err == nil || checked.Phase != management.PhaseVerifiedClose {
			t.Fatalf("missing %s accepted: %+v %v", leaf, checked, err)
		}
		if err := os.WriteFile(path, []byte("replacement from another authority"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := f.controller.Run(t.Context(), op); !errors.Is(err, syscall.EIO) {
			t.Fatalf("changed %s accepted: %v", leaf, err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; running.State != management.OperationSucceeded && i < 10; i++ {
		running, err = f.controller.Run(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
	}
	if running.State != management.OperationSucceeded {
		t.Fatal("intact closed root could not finish")
	}
}

func TestLifecycleRecoverySweepPreservesUnknownEffectsAndRetiresSettledActors(t *testing.T) {
	f := newLifecycleFixture(t, func(o *management.Options) { o.NamespaceLifetime = time.Second; o.ReceiptRetention = time.Millisecond })
	create, err := f.store.CreateVolume(t.Context(), f.actor.ID, f.key("create"), "volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.controller.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	create, err = f.store.Operation(t.Context(), f.actor.ID, create.ID)
	if err != nil || create.State != management.OperationSucceeded {
		t.Fatalf("sweep did not recover create: %+v %v", create, err)
	}
	quota, err := f.store.ChangeQuota(t.Context(), f.actor.ID, f.key("quota"), create.VolumeID, 1, 8192)
	if err != nil {
		t.Fatal(err)
	}
	f.native.failure = syscall.EIO
	if err := f.controller.Recover(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sweep hid native error: %v", err)
	}
	quota, err = f.store.Operation(t.Context(), f.actor.ID, quota.ID)
	if err != nil || quota.Phase != management.PhaseApplying || quota.State != management.OperationRunning {
		t.Fatalf("sweep forgot responsibility: %+v %v", quota, err)
	}
	f.native.failure = nil
	if err := f.controller.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.native.effects != 1 {
		t.Fatalf("recovery repeated native effect: %d", f.native.effects)
	}
	time.Sleep(1050 * time.Millisecond)
	if err := f.controller.RetireAllNamespaces(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.FindOperation(t.Context(), f.actor.ID, quota.Action); !errors.Is(err, management.ErrHistoryExpired) {
		t.Fatalf("actor sweep failed to preserve retirement floor: %v", err)
	}
	if err := f.controller.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
}
