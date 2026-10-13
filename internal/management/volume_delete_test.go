package management

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestVolumeDeletionRemovesOwnerAndAdminListingAndReusesCapacity(t *testing.T) {
	store, _ := authTestStore(t, func(options *Options) { options.MaxVolumes = 1 })
	owner := authTestOrdinaryUser(t, store, "owner")
	other := authTestOrdinaryUser(t, store, "other")
	first := activeTestVolume(t, store, owner.ID, "files")
	deleting, err := store.DeleteVolume(t.Context(), owner.ID, first.ID, first.ManagementRevision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateVolume(t.Context(), other.ID, "files", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("pending deletion released capacity: %v", err)
	}
	failed, err := store.SetVolumeState(t.Context(), owner.ID, deleting.ID, deleting.ManagementRevision, VolumeFailed, "storage_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateVolume(t.Context(), other.ID, "files", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("failed deletion released capacity: %v", err)
	}
	deleting, err = store.DeleteVolume(t.Context(), owner.ID, failed.ID, failed.ManagementRevision)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.SetVolumeState(t.Context(), owner.ID, deleting.ID, deleting.ManagementRevision, VolumeDeleted, "")
	if err != nil || deleted.State != VolumeDeleted || deleted.ID != first.ID {
		t.Fatalf("completion result lost: %+v %v", deleted, err)
	}
	if _, err := store.GetVolume(t.Context(), owner.ID, first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed deletion remains in catalog: %v", err)
	}
	// Owner and administrator summary routes use the same owner-filtered query.
	for _, user := range []ID{owner.ID, other.ID} {
		rows, err := store.ListVolumes(t.Context(), user, "", store.options.MaxPageSize)
		if err != nil || len(rows) != 0 {
			t.Fatalf("current volume listing contains deleted rows: %+v %v", rows, err)
		}
	}
	rows, err := store.ListCatalogVolumes(t.Context(), "", store.options.MaxPageSize)
	if err != nil || len(rows) != 0 {
		t.Fatalf("deleted row remains in catalog listing: %+v %v", rows, err)
	}
	second := activeTestVolume(t, store, other.ID, "files")
	if second.ID == first.ID || second.StorageName == first.StorageName {
		t.Fatal("recreated volume reused deleted identity or root")
	}
	if _, err := store.CreateVolume(t.Context(), owner.ID, "files", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("global capacity no longer enforced across owners: %v", err)
	}
}

func TestVolumeDeletionRemovesCredentialSelectionsAtomically(t *testing.T) {
	store, path := authTestStore(t, nil)
	owner := authTestAdmin(t, store, "owner")
	first := activeTestVolume(t, store, owner.ID, "first")
	second := activeTestVolume(t, store, owner.ID, "second")
	digest := bytes.Repeat([]byte{42}, 32)
	credential, err := store.CreateCredential(t.Context(), CredentialInput{UserID: owner.ID, AuthEpoch: owner.AuthEpoch, Label: "selected", SecretDigest: digest, Scopes: []string{"volumes:read", "volumes:write"}, VolumeIDs: []ID{first.ID, second.ID}, ExpiresAt: store.now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	deleting, err := store.DeleteVolume(t.Context(), owner.ID, first.ID, first.ManagementRevision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), "CREATE TRIGGER reject_volume_delete BEFORE DELETE ON volumes BEGIN SELECT RAISE(ABORT,'catalog deletion unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetVolumeState(t.Context(), owner.ID, first.ID, deleting.ManagementRevision, VolumeDeleted, ""); err == nil {
		t.Fatal("catalog deletion failure reported success")
	}
	retained, err := store.GetVolume(t.Context(), owner.ID, first.ID)
	if err != nil || retained.State != VolumeDeleting || retained.ManagementRevision != deleting.ManagementRevision {
		t.Fatalf("failed transaction removed catalog responsibility: %+v %v", retained, err)
	}
	access, err := store.ValidateCredential(t.Context(), credential.ID, digest)
	if err != nil || len(access.VolumeIDs) != 2 {
		t.Fatalf("failed catalog deletion changed credential selections: %+v %v", access, err)
	}
	if _, err := store.write.ExecContext(t.Context(), "DROP TRIGGER reject_volume_delete"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetVolumeState(t.Context(), owner.ID, first.ID, deleting.ManagementRevision, VolumeDeleted, ""); err != nil {
		t.Fatal(err)
	}
	access, err = store.ValidateCredential(t.Context(), credential.ID, digest)
	if err != nil || access.AllOwnedVolumes || len(access.VolumeIDs) != 1 || access.VolumeIDs[0] != second.ID {
		t.Fatalf("completed deletion widened or lost retained authority: %+v %v", access, err)
	}
	deleting, err = store.DeleteVolume(t.Context(), owner.ID, second.ID, second.ManagementRevision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetVolumeState(t.Context(), owner.ID, second.ID, deleting.ManagementRevision, VolumeDeleted, ""); err != nil {
		t.Fatal(err)
	}
	access, err = store.ValidateCredential(t.Context(), credential.ID, digest)
	if err != nil || access.AllOwnedVolumes || len(access.VolumeIDs) != 0 {
		t.Fatalf("last deletion invalidated authentication or widened authority: %+v %v", access, err)
	}
	now := store.now()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path, store.options)
	if err != nil {
		t.Fatalf("completed deletion left inconsistent relationships: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	reopened.now = func() time.Time { return now }
	access, err = reopened.ValidateCredential(t.Context(), credential.ID, digest)
	if err != nil || access.AllOwnedVolumes || len(access.VolumeIDs) != 0 {
		t.Fatalf("zero-volume credential changed after reopen: %+v %v", access, err)
	}
	if _, err := reopened.CreateCredential(t.Context(), CredentialInput{UserID: owner.ID, AuthEpoch: owner.AuthEpoch, Label: "empty", SecretDigest: bytes.Repeat([]byte{43}, 32), Scopes: []string{"volumes:read"}, ExpiresAt: now.Add(time.Hour)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("issuance accepted an empty volume selection: %v", err)
	}
	fresh := activeTestVolume(t, reopened, owner.ID, "first")
	if fresh.ID == first.ID || fresh.ID == second.ID {
		t.Fatal("new volume reused a deleted identity")
	}
	access, err = reopened.ValidateCredential(t.Context(), credential.ID, digest)
	if err != nil || access.AllOwnedVolumes || len(access.VolumeIDs) != 0 {
		t.Fatalf("old selection acquired the recreated volume: %+v %v", access, err)
	}
}
