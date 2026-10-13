package management

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
)

func activeTestVolume(t *testing.T, store *Store, owner ID, name string) Volume {
	t.Helper()
	volume, err := store.CreateVolume(t.Context(), owner, name, 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err = store.SetVolumeState(t.Context(), owner, volume.ID, volume.ManagementRevision, VolumeActive, "")
	if err != nil {
		t.Fatal(err)
	}
	return volume
}

func TestVolumeRegistryOwnershipAndRenameCAS(t *testing.T) {
	store, _ := managementFixture(t)
	owner := authTestAdmin(t, store, "owner")
	other := authTestAdmin(t, store, "other")
	volume := activeTestVolume(t, store, owner.ID, "files")
	if volume.StorageName != string(volume.ID) || volume.InitialQuotaBytes != 4096 {
		t.Fatalf("unstable storage configuration: %+v", volume)
	}
	if _, err := store.GetVolume(t.Context(), other.ID, volume.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("administrator read another owner's volume: %v", err)
	}
	if _, err := store.RenameVolume(t.Context(), other.ID, volume.ID, volume.ManagementRevision, "other"); !errors.Is(err, ErrDenied) {
		t.Fatalf("administrator changed another owner's volume: %v", err)
	}
	if _, err := store.DeleteVolume(t.Context(), other.ID, volume.ID, volume.ManagementRevision); !errors.Is(err, ErrDenied) {
		t.Fatalf("administrator deleted another owner's volume: %v", err)
	}
	if _, err := store.RenameVolume(t.Context(), owner.ID, volume.ID, volume.ManagementRevision-1, "stale"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale rename succeeded: %v", err)
	}
	renamed, err := store.RenameVolume(t.Context(), owner.ID, volume.ID, volume.ManagementRevision, "renamed")
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID != volume.ID || renamed.StorageName != volume.StorageName || renamed.InitialQuotaBytes != volume.InitialQuotaBytes || renamed.DisplayName != "renamed" {
		t.Fatalf("rename changed root configuration: %+v", renamed)
	}
	rows, err := store.ListVolumes(t.Context(), other.ID, "", 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("owner listing leaked records: %+v %v", rows, err)
	}
}

func TestVolumeRegistryLifecycleAndPersistentFailure(t *testing.T) {
	store, path := managementFixture(t)
	owner := authTestAdmin(t, store, "owner")
	volume, err := store.CreateVolume(t.Context(), owner.ID, "files", 8192)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenameVolume(t.Context(), owner.ID, volume.ID, volume.ManagementRevision, "invalid"); !errors.Is(err, ErrConflict) {
		t.Fatalf("creating volume renamed: %v", err)
	}
	failed, err := store.SetVolumeState(t.Context(), owner.ID, volume.ID, volume.ManagementRevision, VolumeFailed, "storage_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	known, err := store.GetVolume(t.Context(), owner.ID, volume.ID)
	if err != nil || known.State != VolumeFailed || known.ErrorCode != "storage_unavailable" || known.InitialQuotaBytes != 8192 {
		t.Fatalf("failed registry outcome lost: %+v %v", known, err)
	}
	deleting, err := store.DeleteVolume(t.Context(), owner.ID, failed.ID, failed.ManagementRevision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetVolumeState(t.Context(), owner.ID, deleting.ID, deleting.ManagementRevision, VolumeActive, ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("deletion reactivated volume: %v", err)
	}
	deleted, err := store.SetVolumeState(t.Context(), owner.ID, deleting.ID, deleting.ManagementRevision, VolumeDeleted, "")
	if err != nil || deleted.State != VolumeDeleted {
		t.Fatalf("delete result not recorded: %+v %v", deleted, err)
	}
	if _, err := store.CreateVolume(t.Context(), owner.ID, "files", 4096); err != nil {
		t.Fatalf("deleted display name remains occupied: %v", err)
	}
}

func TestVolumeRegistryBoundsNamesAndListingErrors(t *testing.T) {
	store, _ := authTestStore(t, func(options *Options) { options.MaxVolumes = 2 })
	owner := authTestAdmin(t, store, "owner")
	for _, invalid := range []string{"", "line\nfeed", strings.Repeat("x", 129), string([]byte{0xff})} {
		if _, err := store.CreateVolume(t.Context(), owner.ID, invalid, 4096); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid volume name accepted: %q %v", invalid, err)
		}
	}
	if _, err := store.CreateVolume(t.Context(), owner.ID, "small", 4095); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unsupported initial quota accepted: %v", err)
	}
	first := activeTestVolume(t, store, owner.ID, "first")
	if _, err := store.CreateVolume(t.Context(), owner.ID, "first", 4096); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate display name accepted: %v", err)
	}
	second := activeTestVolume(t, store, owner.ID, "second")
	if _, err := store.RenameVolume(t.Context(), owner.ID, second.ID, second.ManagementRevision, "first"); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate rename accepted: %v", err)
	}
	if _, err := store.CreateVolume(t.Context(), owner.ID, "third", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("registry bound exceeded: %v", err)
	}
	for _, limit := range []int{0, store.options.MaxPageSize + 1} {
		if _, err := store.ListVolumes(t.Context(), owner.ID, "", limit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid owner page accepted: %v", err)
		}
		if _, err := store.ListCatalogVolumes(t.Context(), "", limit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid catalog page accepted: %v", err)
		}
	}
	page, err := store.ListCatalogVolumes(t.Context(), "", 1)
	if err != nil || len(page) != 1 {
		t.Fatalf("catalog page failed: %+v %v", page, err)
	}
	next, err := store.ListCatalogVolumes(t.Context(), page[0].ID, 1)
	if err != nil || len(next) != 1 || next[0].ID <= page[0].ID {
		t.Fatalf("catalog pagination failed: %+v %v", next, err)
	}
	if _, err := store.SetVolumeState(t.Context(), owner.ID, first.ID, first.ManagementRevision, VolumeFailed, ""); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("failed outcome without error accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.GetVolume(ctx, owner.ID, first.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read became absent data: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetVolume(t.Context(), owner.ID, first.ID); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("closed registry became absent data: %v", err)
	}
}

func TestVolumeRegistryRejectsCorruptIdentity(t *testing.T) {
	store, _ := managementFixture(t)
	owner := authTestAdmin(t, store, "owner")
	volume := activeTestVolume(t, store, owner.ID, "files")
	if _, err := store.write.ExecContext(t.Context(), "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), "UPDATE volumes SET management_revision=0 WHERE id=?", volume.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetVolume(t.Context(), owner.ID, volume.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt registry record returned: %v", err)
	}
}
