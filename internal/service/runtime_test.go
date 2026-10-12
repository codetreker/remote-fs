package service

import (
	"bytes"
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

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

func runtimeFixture(t *testing.T) (*runtimeManager, Config) { return runtimeFixtureAdjusted(t, nil) }

func runtimeFixtureAdjusted(t *testing.T, adjust func(*Config)) (*runtimeManager, Config) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	volumes := filepath.Join(root, "volumes")
	spools := filepath.Join(root, "transfers")
	for _, path := range []string{volumes, spools} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	c := DefaultConfig(root, "https://service.example")
	c.IdleRuntimeTimeout = time.Hour
	c.ShutdownTimeout = 2 * time.Second
	if adjust != nil {
		adjust(&c)
	}
	auth := newAuthFixture(t, nil)
	m := newRuntimeManager(c, &stateRoot{VolumesPath: volumes, TransfersPath: spools}, auth.auth, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return m, c
}

func runtimeVolume(t *testing.T, m *runtimeManager, name string) management.Volume {
	t.Helper()
	v := management.Volume{ID: management.ID(name), OwnerUserID: "owner", StorageName: name, LastOperationID: "create", State: management.VolumeCreating}
	path := filepath.Join(m.root.VolumesPath, name)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	marker, err := json.Marshal(struct {
		Volume    management.ID `json:"volume"`
		Operation management.ID `json:"operation"`
	}{v.ID, v.LastOperationID})
	if err != nil {
		t.Fatal(err)
	}
	if err := installLifecycleXattr(path, createProofXattr, marker); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Initialize(context.Background(), v, 1<<20); err != nil {
		t.Fatal(err)
	}
	v.State = management.VolumeActive
	return v
}

func TestRuntimeColdSummariesDoNotCreateFileHandlers(t *testing.T) {
	m, _ := runtimeFixture(t)
	for _, name := range []string{"a", "b", "c", "d"} {
		v := runtimeVolume(t, m, name)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		summary, err := m.Summary(ctx, v)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if summary.FileCount != 0 || summary.DirectoryCount != 0 || summary.QuotaBytes != 1<<20 {
			t.Fatalf("unexpected summary: %+v", summary)
		}
		m.mu.Lock()
		entry := m.entries[v.ID]
		if entry.handler != nil {
			t.Error("summary activated a data handler")
		}
		m.mu.Unlock()
	}
}

func TestRuntimeFilePromotionAndBusyRetirement(t *testing.T) {
	m, _ := runtimeFixture(t)
	v := runtimeVolume(t, m, "promotion")
	lease, err := m.acquireFile(context.Background(), v, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Handler == nil {
		t.Fatal("file acquisition returned no handler")
	}
	if _, err := m.Freeze(context.Background(), v); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("active request retirement = %v", err)
	}
	if err := lease.Store.Create(context.Background(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	retirement, err := m.Freeze(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.acquireFile(context.Background(), v, false); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("frozen admission = %v", err)
	}
	if err := retirement.Cancel(); err != nil {
		t.Fatal(err)
	}
	summary, err := m.Summary(context.Background(), v)
	if err != nil || summary.FileCount != 1 {
		t.Fatalf("cancel did not preserve runtime: %+v %v", summary, err)
	}
	retirement, err = m.Freeze(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := retirement.Close(context.Background())
	if err != nil || len(proof) == 0 {
		t.Fatalf("retirement: %s %v", proof, err)
	}
	retry, err := retirement.Close(context.Background())
	if err != nil || string(retry) != string(proof) {
		t.Fatalf("retirement retry %s %v", retry, err)
	}
	if err := retirement.Cancel(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("committed cancellation = %v", err)
	}
}

func TestRuntimeSharedActivationAndStableProof(t *testing.T) {
	m, _ := runtimeFixture(t)
	v := runtimeVolume(t, m, "shared")
	original, err := m.Initialize(context.Background(), v, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := m.acquireFile(context.Background(), v, false)
			if err != nil {
				t.Error(err)
				return
			}
			if lease.Handler == nil {
				t.Error("missing data handler")
			}
			if err := lease.Release(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	m.mu.Lock()
	if len(m.entries) != 1 || m.referencesLocked() != 0 {
		t.Error("shared activation miscounted")
	}
	m.mu.Unlock()
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := newRuntimeManager(m.config, m.root, m.auth, nil)
	t.Cleanup(func() {
		if err := restarted.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	reopened, err := restarted.Initialize(context.Background(), v, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(original) != string(reopened) {
		t.Fatalf("durable proof changed after restart: %s vs %s", original, reopened)
	}
}

func TestRuntimeRejectedInitializationDoesNotInventSuccess(t *testing.T) {
	m, _ := runtimeFixture(t)
	v := management.Volume{ID: "invalid", OwnerUserID: "owner", StorageName: "invalid", LastOperationID: "create", State: management.VolumeCreating}
	path := filepath.Join(m.root.VolumesPath, v.StorageName)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := installLifecycleXattr(path, createProofXattr, []byte(`{"volume":"other","operation":"create"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Initialize(context.Background(), v, 0); !errors.Is(err, syscall.EIO) {
		t.Fatalf("wrong marker = %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) != 0 {
		t.Error("rejected initialization retained proven-unopened runtime")
	}
}

func TestRuntimeIdleEvictionReleasesNativeOwnership(t *testing.T) {
	m, _ := runtimeFixtureAdjusted(t, func(c *Config) { c.IdleRuntimeTimeout = 20 * time.Millisecond })
	v := runtimeVolume(t, m, "idle")
	deadline := time.After(2 * time.Second)
	for {
		m.mu.Lock()
		remaining := m.entries[v.ID] != nil
		m.mu.Unlock()
		if !remaining {
			break
		}
		select {
		case <-deadline:
			t.Fatal("idle runtime was not retired")
		case <-time.After(time.Millisecond):
		}
	}
	// Reopening proves the old process-native root and database locks settled.
	summary, err := m.Summary(context.Background(), v)
	if err != nil || summary.QuotaBytes != 1<<20 {
		t.Fatalf("idle reopen = %+v %v", summary, err)
	}
}

func TestRuntimeDetachedDurableImportPinsDeletionAcrossRuntimeClose(t *testing.T) {
	m, _ := runtimeFixture(t)
	v := runtimeVolume(t, m, "sealed")
	ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: string(v.OwnerUserID), AccessID: "original-access"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := m.acquire(ctx, v, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if err := lease.Store.RegisterActionNamespace(ctx, storage.ActionNamespace{SubjectID: string(v.OwnerUserID), Sequence: 1, AcceptUntil: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	root, err := lease.Store.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("durable unpublished data")
	command := storage.ImportCommand{Action: storage.ActionKey{SubjectID: string(v.OwnerUserID), Namespace: 1, Nonce: "sealed"}, Target: storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate, DeclaredBytes: int64(len(payload)), Digest: sha256.Sum256(payload)}
	command.Fingerprint = command.EffectFingerprint()
	upload, err := lease.Store.BeginImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	didDetach := false
	t.Cleanup(func() {
		if !didDetach {
			result, err := upload.Cancel(context.WithoutCancel(ctx))
			if err != nil || !result.Released || !result.Determined {
				t.Errorf("import cleanup = %+v %v", result, err)
			}
		}
	})
	if _, err := upload.Receive(ctx, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	detached, err := upload.Detach(ctx)
	if err != nil || !detached.Determined || !detached.Released {
		t.Fatalf("detach = %+v %v", detached, err)
	}
	didDetach = true
	if lease.Store.LiveTransferCount() != 0 {
		t.Fatal("detached import retained volatile handle")
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Freeze(ctx, v); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("pending durable work allowed deletion: %v", err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted := newRuntimeManager(m.config, m.root, m.auth, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := restarted.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	renewed, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: string(v.OwnerUserID), AccessID: "renewed-access"})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := restarted.acquire(renewed, v, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Release()
	receipt, err := reopened.Store.QueryImport(renewed, command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionPending {
		t.Fatalf("durable receipt lost on close: %+v %v", receipt, err)
	}
	resumed, err := reopened.Store.ResumeImport(renewed, command.Action)
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := resumed.Cancel(renewed)
	if err != nil || !canceled.Determined || !canceled.Released {
		t.Fatalf("cancel = %+v %v", canceled, err)
	}
	if err := reopened.Release(); err != nil {
		t.Fatal(err)
	}
	retirement, err := restarted.Freeze(renewed, v)
	if err != nil {
		t.Fatalf("settled work still pinned deletion: %v", err)
	}
	if _, err := retirement.Close(renewed); err != nil {
		t.Fatal(err)
	}
}
