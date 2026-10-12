package management

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestVolumeOwnerIsolationAndAtomicRename(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "owner")
	other := authTestAdmin(t, s, "other-admin")
	namespace := actionTestNamespace(t, s, actor.ID)
	op, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "create"}, "original", 4096)
	if err != nil {
		t.Fatal(err)
	}
	op = actionTestFinishCreate(t, s, op)
	volume, err := s.GetVolume(context.Background(), actor.ID, op.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetVolume(context.Background(), other.ID, volume.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("admin gained another owner's volume: %v", err)
	}
	if _, err := s.Operation(context.Background(), other.ID, op.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("admin gained another actor receipt: %v", err)
	}
	otherNamespace := actionTestNamespace(t, s, other.ID)
	if _, err := s.RenameVolume(context.Background(), other.ID, ActionKey{otherNamespace.Sequence, "denied"}, volume.ID, 1, "stolen"); !errors.Is(err, ErrDenied) {
		t.Fatalf("admin renamed another owner's volume: %v", err)
	}
	var group sync.WaitGroup
	errorsOut := make(chan error, 2)
	for _, name := range []string{"left", "right"} {
		group.Go(func() {
			_, err := s.RenameVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, name}, volume.ID, 1, name)
			errorsOut <- err
		})
	}
	group.Wait()
	close(errorsOut)
	var successes, conflicts int
	for err := range errorsOut {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("CAS winners: %d successes %d conflicts", successes, conflicts)
	}
	changed, err := s.GetVolume(context.Background(), actor.ID, volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	if changed.StorageName != volume.StorageName || changed.OwnerUserID != volume.OwnerUserID || changed.ManagementRevision != 2 {
		t.Fatalf("rename changed durable binding: %#v", changed)
	}
	receipt, err := s.Operation(context.Background(), actor.ID, changed.LastOperationID)
	if err != nil || receipt.State != OperationSucceeded || !receipt.Settled {
		t.Fatalf("atomic rename receipt: %#v %v", receipt, err)
	}
}

func TestVolumeNamesValidationUniquenessAndBounds(t *testing.T) {
	s, _ := authTestStore(t, func(o *Options) { o.MaxVolumes = 2 })
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	for i, name := range []string{"", "\xff", "line\nfeed", "zero\x00", strings.Repeat("字", 129)} {
		if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, string(rune('a' + i))}, name, 4096); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid name %q: %v", name, err)
		}
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "quota"}, "a", -1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative quota: %v", err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "first"}, "Name", 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "duplicate"}, "Name", 4096); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate exact name: %v", err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "case"}, "name", 4096); err != nil {
		t.Fatalf("names unexpectedly case folded: %v", err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "capacity"}, "another", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("volume limit: %v", err)
	}
	if _, err := s.ListVolumes(context.Background(), actor.ID, "", s.options.MaxPageSize+1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("page bound: %v", err)
	}
	first, err := s.ListVolumes(context.Background(), actor.ID, "", 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first page: %#v %v", first, err)
	}
	second, err := s.ListVolumes(context.Background(), actor.ID, first[0].ID, 1)
	if err != nil || len(second) != 1 || second[0].ID <= first[0].ID {
		t.Fatalf("stable second page: %#v %v", second, err)
	}
}

func TestDeleteStateProgressionKeepsTombstone(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	created, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "create"}, "data", 4096)
	if err != nil {
		t.Fatal(err)
	}
	created = actionTestFinishCreate(t, s, created)
	op, err := s.DeleteVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "delete"}, created.VolumeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := s.GetVolume(context.Background(), actor.ID, created.VolumeID)
	if err != nil || queued.State != VolumeActive {
		t.Fatalf("queued delete froze before zero-pin gate: %#v %v", queued, err)
	}
	changes := []OperationTransition{
		{State: OperationRunning, Phase: PhaseFrozen},
		{State: OperationRunning, Phase: PhaseVerifiedClose, VerifiedRootProof: []byte("close-settled-and-binding"), PlannedReclaimProof: []byte("random-proof-id"), QuarantineLeaf: string(op.VolumeID) + "-" + string(op.ID)},
		{State: OperationRunning, Phase: PhaseQuarantined},
		{State: OperationRunning, Phase: PhaseReclaiming, ReclaimingProof: []byte("xattr-and-parent-sync-confirmed")},
		{State: OperationSucceeded, Phase: PhaseCompleted, Settled: true, Result: []byte(`{"deleted":true}`)},
	}
	for _, change := range changes {
		op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, change)
		if err != nil {
			t.Fatalf("%s: %v", change.Phase, err)
		}
	}
	tombstone, err := s.GetVolume(context.Background(), actor.ID, created.VolumeID)
	if err != nil || tombstone.State != VolumeDeleted || tombstone.ID != created.VolumeID || tombstone.OwnerUserID != actor.ID || tombstone.ManagementRevision != 2 {
		t.Fatalf("deleted binding not retained: %#v %v", tombstone, err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "replace-name"}, "data", 4096); err != nil {
		t.Fatalf("deleted display name not released: %v", err)
	}
	if _, err := s.RenameVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "old-id"}, tombstone.ID, 2, "reused"); !errors.Is(err, ErrConflict) {
		t.Fatalf("tombstone mutated: %v", err)
	}
}

func TestQuotaIntentKeepsNativeRevisionAndCatalogIndependent(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	created, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "create"}, "data", 4096)
	if err != nil {
		t.Fatal(err)
	}
	created = actionTestFinishCreate(t, s, created)
	key := ActionKey{namespace.Sequence, "quota"}
	op, err := s.ChangeQuota(context.Background(), actor.ID, key, created.VolumeID, 7, 8192)
	if err != nil || op.Kind != OperationQuota || op.ExpectedRevision != 7 || op.State != OperationPending {
		t.Fatalf("native quota intent: %#v %v", op, err)
	}
	var payload struct {
		Actor   ID
		Kind    OperationKind
		Request VolumeMutation
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil || payload.Request.QuotaBytes != 8192 || payload.Request.ExpectedRevision != 7 || payload.Actor != actor.ID {
		t.Fatalf("quota payload: %#v %v", payload, err)
	}
	volume, err := s.GetVolume(context.Background(), actor.ID, op.VolumeID)
	if err != nil || volume.ManagementRevision != 1 || volume.State != VolumeActive {
		t.Fatalf("quota intent changed catalog policy: %#v %v", volume, err)
	}
	op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationRunning, Phase: PhaseApplying})
	if err != nil {
		t.Fatal(err)
	}
	op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationSucceeded, Phase: PhaseCompleted, Settled: true, Result: []byte(`{"quotaBytes":"8192","quotaRevision":"8"}`)})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.ChangeQuota(context.Background(), actor.ID, key, created.VolumeID, 7, 8192)
	if err != nil || replayed.ID != op.ID || replayed.State != OperationSucceeded {
		t.Fatalf("quota replay lost original native revision: %#v %v", replayed, err)
	}
	if _, err := s.ChangeQuota(context.Background(), actor.ID, key, created.VolumeID, 8, 8192); !errors.Is(err, ErrConflict) {
		t.Fatalf("quota key changed native condition: %v", err)
	}
}

func TestOfflineCatalogEnumerationIncludesOwnersAndLifecycleStates(t *testing.T) {
	s, _ := authTestStore(t, nil)
	owners := []User{authTestAdmin(t, s, "first"), authTestAdmin(t, s, "second")}
	wantStates := []VolumeState{VolumeCreating, VolumeActive, VolumeFailed, VolumeDeleting, VolumeDeleted}
	want := make(map[ID]VolumeState)
	for index, state := range wantStates {
		owner := owners[index%len(owners)]
		namespace := actionTestNamespace(t, s, owner.ID)
		created, err := s.CreateVolume(context.Background(), owner.ID, ActionKey{namespace.Sequence, "create"}, string(state), 4096)
		if err != nil {
			t.Fatal(err)
		}
		want[created.VolumeID] = state
		if state == VolumeCreating {
			continue
		}
		if state == VolumeFailed {
			if _, err := s.TransitionOperation(context.Background(), created.ID, created.Revision, OperationTransition{State: OperationFailed, Phase: PhaseAccepted, ErrorCode: "initialize_rejected", Settled: true}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		created = actionTestFinishCreate(t, s, created)
		if state == VolumeActive {
			continue
		}
		deletion, err := s.DeleteVolume(context.Background(), owner.ID, ActionKey{namespace.Sequence, "delete"}, created.VolumeID, 1)
		if err != nil {
			t.Fatal(err)
		}
		changes := []OperationTransition{
			{State: OperationRunning, Phase: PhaseFrozen},
			{State: OperationRunning, Phase: PhaseVerifiedClose, VerifiedRootProof: []byte("closed-binding"), PlannedReclaimProof: []byte("planned-marker"), QuarantineLeaf: string(deletion.VolumeID) + "-" + string(deletion.ID)},
			{State: OperationRunning, Phase: PhaseQuarantined},
			{State: OperationRunning, Phase: PhaseReclaiming, ReclaimingProof: []byte("marker-and-parents-synced")},
			{State: OperationSucceeded, Phase: PhaseCompleted, Settled: true, Result: []byte("confirmed-reclaimed")},
		}
		if state == VolumeDeleting {
			changes = changes[:1]
		}
		for _, change := range changes {
			deletion, err = s.TransitionOperation(context.Background(), deletion.ID, deletion.Revision, change)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	seenOwners := make(map[ID]int)
	var after ID
	for {
		page, err := s.ListCatalogVolumes(context.Background(), after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, volume := range page {
			if volume.ID <= after || want[volume.ID] != volume.State {
				t.Fatalf("catalog row missing, repeated, or changed: %#v after %s", volume, after)
			}
			delete(want, volume.ID)
			seenOwners[volume.OwnerUserID]++
			after = volume.ID
		}
	}
	if len(want) != 0 || seenOwners[owners[0].ID] != 3 || seenOwners[owners[1].ID] != 2 {
		t.Fatalf("catalog projection omitted lifecycle/owner rows: remaining=%v owners=%v", want, seenOwners)
	}
	for _, limit := range []int{0, s.options.MaxPageSize + 1} {
		if _, err := s.ListCatalogVolumes(context.Background(), "", limit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid catalog limit %d: %v", limit, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListCatalogVolumes(context.Background(), "", 2); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("closed catalog reported absent data: %v", err)
	}
}

func TestOfflineCatalogEnumerationRejectsMalformedRows(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	created, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "create"}, "valid", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.write.ExecContext(context.Background(), "UPDATE volumes SET display_name=? WHERE id=?", "invalid\nname", created.VolumeID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListCatalogVolumes(context.Background(), "", 2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed catalog row published: %v", err)
	}
}

func TestZeroQuotaCreationAndQuotaIntent(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	created, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "zero-create"}, "empty", 0)
	if err != nil {
		t.Fatalf("zero quota rejected at creation: %v", err)
	}
	var payload struct{ Request VolumeMutation }
	if err := json.Unmarshal(created.Payload, &payload); err != nil || payload.Request.QuotaBytes != 0 {
		t.Fatalf("zero creation intent changed: %#v %v", payload, err)
	}
	created = actionTestFinishCreate(t, s, created)
	if _, err := s.ChangeQuota(context.Background(), actor.ID, ActionKey{namespace.Sequence, "negative"}, created.VolumeID, 1, -1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative quota accepted: %v", err)
	}
	key := ActionKey{namespace.Sequence, "zero-quota"}
	op, err := s.ChangeQuota(context.Background(), actor.ID, key, created.VolumeID, 1, 0)
	if err != nil {
		t.Fatalf("zero quota intent rejected: %v", err)
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil || payload.Request.QuotaBytes != 0 || op.ExpectedRevision != 1 {
		t.Fatalf("zero quota intent changed: %#v %v", payload, err)
	}
	replayed, err := s.ChangeQuota(context.Background(), actor.ID, key, created.VolumeID, 1, 0)
	if err != nil || replayed.ID != op.ID {
		t.Fatalf("zero quota intent replay: %#v %v", replayed, err)
	}
}
