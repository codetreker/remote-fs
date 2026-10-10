package objectstore_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestFileSessionIdentitySharesBackendAndSeparatesSessionEpochs(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 8192, nil)
	backend, err := volume.BackendIdentity(t.Context())
	if err != nil || backend.Check() != nil {
		t.Fatalf("backend identity=%+v, %v", backend, err)
	}
	first := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	second := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	var identities []storage.FileSessionIdentityResult
	for _, session := range []storage.FileSession{first, second} {
		if err := session.(storage.FileSessionIdentity).CheckFileSessionIdentity(); err != nil {
			t.Fatal(err)
		}
		if err := session.(storage.StableReferenceIdentity).CheckStableReferenceIdentity(); err != nil {
			t.Fatal(err)
		}
		identity, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
		if err != nil || identity.Backend != backend {
			t.Fatalf("session bound wrong backend: %+v, %v", identity, err)
		}
		status, err := session.Status(t.Context())
		if err != nil || identity.SessionEpoch != status.Epoch {
			t.Fatalf("session epoch differs: %+v, %+v, %v", identity, status, err)
		}
		identities = append(identities, identity)
	}
	if identities[0].SessionEpoch == identities[1].SessionEpoch {
		t.Fatal("independent sessions share epoch")
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := first.(storage.FileSessionIdentity).FileSessionIdentity(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired session returned identity: %v", err)
	}
	if after, err := second.(storage.FileSessionIdentity).FileSessionIdentity(t.Context()); err != nil || after != identities[1] {
		t.Fatalf("unrelated session identity changed: %+v, %v", after, err)
	}
}

type changingBackendIdentity struct {
	*sqlite.LockingStore
	reported *storage.BackendIdentityResult
}

func (s *changingBackendIdentity) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	if s.reported != nil {
		return *s.reported, nil
	}
	return s.LockingStore.BackendIdentity(ctx)
}

func TestFileSessionIdentityRejectsBackendChangesWithinSession(t *testing.T) {
	_, native := fileVolume(t, memory.New(), 8192, nil)
	authority := &changingBackendIdentity{LockingStore: native}
	volume := objectstore.New(memory.New(), authority)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, change := range []func(*storage.BackendIdentityResult){
		func(r *storage.BackendIdentityResult) { r.Volume = "other-volume" },
		func(r *storage.BackendIdentityResult) { r.Authority = "other-authority" },
		func(r *storage.BackendIdentityResult) { r.RootNodeID++ },
	} {
		session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		identity, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		changed := identity.Backend
		change(&changed)
		authority.reported = &changed
		result, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
		if !errors.Is(err, syscall.ESTALE) || result != (storage.FileSessionIdentityResult{}) {
			t.Fatalf("session rebound to changed backend: %+v, %v", result, err)
		}
		authority.reported = nil
		if err := session.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}
