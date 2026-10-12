package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestServiceFreshImportChecksActorClockAcrossAuthorities(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	s := f.current.Load()
	second := f.createVolume(f.ordinary, "Second authority")
	namespace, err := s.maintenance.IssueNamespace(t.Context(), f.ordinary.ID)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(t.Context(), "POST", "https://service.invalid/", nil)
	request.Header.Set("Authorization", "Bearer "+string(f.userCredential.ID)+"."+f.userSecret)
	principal, err := s.auth.Authenticate(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithPrincipal(t.Context(), principal)
	binding, err := s.auth.AccessBinding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = storage.WithAccessBinding(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.runtimes.acquire(ctx, f.userVolume, true)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	other, err := s.runtimes.acquire(ctx, second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	key := storage.ActionKey{SubjectID: string(f.ordinary.ID), Namespace: uint64(namespace.Sequence), Nonce: "known-before-clock-frontier"}
	if err := s.runtimes.registerNamespace(ctx, f.userVolume.ID, first.Store, key); err != nil {
		t.Fatal(err)
	}
	root, err := first.Store.BackendIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.ImportCommand{Action: key, Target: storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.RootNodeID}, RawLeaf: []byte("pending-import")}}, Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate, Digest: sha256.Sum256(nil)}
	command.Fingerprint = command.EffectFingerprint()
	file, err := first.Store.BeginImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		result, err := file.Cancel(context.WithoutCancel(ctx))
		if err != nil || !result.Released {
			t.Errorf("cancel retained import: %+v %v", result, err)
		}
	}()
	// Both native authorities still observe a live immutable window. Only the
	// management actor has already observed that window ending.
	if err := other.Store.RegisterActionNamespace(ctx, storage.ActionNamespace{SubjectID: key.SubjectID, Sequence: key.Namespace, AcceptUntil: namespace.AcceptUntil}); err != nil {
		t.Fatal(err)
	}
	if err := s.maintenance.store.RegisterNamespaceParticipation(ctx, f.ordinary.ID, namespace.Sequence, second.ID); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(f.config.StateRoot, "data", "management.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, updateErr := database.ExecContext(ctx, "UPDATE users SET logical_time_high_water=? WHERE id=?", namespace.AcceptUntil.UnixMilli()+1, f.ordinary.ID)
	if err := errors.Join(updateErr, database.Close()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		volume management.Volume
		lease  *runtimeLease
	}{{f.userVolume, first}, {second, other}} {
		fresh := key
		fresh.Nonce = "fresh-after-clock-frontier"
		if err := s.runtimes.registerNamespace(ctx, test.volume.ID, test.lease.Store, fresh); !errors.Is(err, management.ErrExpired) {
			t.Fatalf("fresh action in %s accepted past actor frontier: %v", test.volume.ID, err)
		}
		receipt, queryErr := test.lease.Store.QueryImport(ctx, fresh)
		if receipt.Outcome != storage.DurableActionUnknown || queryErr == nil {
			t.Fatalf("refused fresh action left native work: %+v %v", receipt, queryErr)
		}
	}
	if err := s.runtimes.registerNamespace(ctx, f.userVolume.ID, first.Store, key); err != nil {
		t.Fatalf("known durable action lost replay after clock frontier: %v", err)
	}
}

func TestServiceNativeRegistrationFailureKeepsParticipant(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	s := f.current.Load()
	namespace, err := s.maintenance.IssueNamespace(t.Context(), f.ordinary.ID)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(t.Context(), "POST", "https://service.invalid/", nil)
	request.Header.Set("Authorization", "Bearer "+string(f.userCredential.ID)+"."+f.userSecret)
	principal, err := s.auth.Authenticate(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithPrincipal(t.Context(), principal)
	binding, err := s.auth.AccessBinding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = storage.WithAccessBinding(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.runtimes.acquire(ctx, f.userVolume, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	key := storage.ActionKey{SubjectID: string(f.ordinary.ID), Namespace: uint64(namespace.Sequence), Nonce: "fixed-window-conflict"}
	if err := lease.Store.RegisterActionNamespace(ctx, storage.ActionNamespace{SubjectID: key.SubjectID, Sequence: key.Namespace, AcceptUntil: namespace.AcceptUntil.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err := s.runtimes.registerNamespace(ctx, f.userVolume.ID, lease.Store, key); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed native window accepted: %v", err)
	}
	database, err := sql.Open("sqlite", filepath.Join(f.config.StateRoot, "data", "management.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var count int
	if err := database.QueryRowContext(ctx, "SELECT count(*) FROM namespace_volume_participants WHERE actor_user_id=? AND sequence=? AND volume_id=?", f.ordinary.ID, namespace.Sequence, f.userVolume.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("native registration failure erased its catalog participant")
	}
	receipt, queryErr := lease.Store.QueryImport(ctx, key)
	if receipt.Outcome != storage.DurableActionUnknown || queryErr == nil {
		t.Fatalf("failed registration created native work: %+v %v", receipt, queryErr)
	}
}
