package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReferenceInformationDoesNotGrantMetadataAndTracksDetachedObject(t *testing.T) {
	store, root, node, _ := guardedFileFixture(t)
	opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	ref := opened.Reference.(*retainedNodeReference)
	if err := ref.CheckReferenceInformation(); err != nil {
		t.Fatal(err)
	}
	want := storage.ReferenceInformation{NodeID: uint64(node.ID), Kind: storage.NodeRegular, Size: 4, AllocationSize: 4096, AllocationKnown: true}
	if got, err := ref.Information(t.Context()); err != nil || got != want {
		t.Fatalf("scalar information = %+v, %v", got, err)
	}
	if _, err := ref.Node(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("scalar capability granted public metadata: %v", err)
	}
	if _, err := ref.State(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("scalar capability granted full state: %v", err)
	}
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}
	if _, err := store.MutateName(t.Context(), storage.NameCommand{Action: fileAction(t), Kind: storage.NameRemove, Name: name,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}}); err != nil {
		t.Fatal(err)
	}
	want.Detached = true
	if got, err := ref.Information(t.Context()); err != nil || got != want {
		t.Fatalf("detached identity = %+v, %v", got, err)
	}
}

func TestReferenceInformationDirectoryAndSymlinkKinds(t *testing.T) {
	store, root, _, _ := guardedFileFixture(t)
	for _, test := range []struct {
		kind storage.NodeKind
		name string
		link []byte
	}{
		{storage.NodeDirectory, "directory", nil},
		{storage.NodeSymlink, "link", []byte("target")},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte(test.name)}),
				storage.NodeRefOptions{Kind: test.kind, Create: true, Exclusive: true, Action: fileAction(t),
					Target:       storage.ChildCondition{State: storage.Absent},
					InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: test.link}}})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Reference.Close(context.Background())
			got, err := opened.Reference.(*retainedNodeReference).Information(t.Context())
			if err != nil || got.NodeID != uint64(opened.State.ID) || got.Kind != test.kind || got.Size != int64(len(test.link)) ||
				!got.AllocationKnown || got.AllocationSize != 0 || got.Detached || got.PendingUnlink {
				t.Fatalf("node information = %+v, %v", got, err)
			}
		})
	}
}

func TestReferenceInformationZeroesLifetimeAndAuthorityFailures(t *testing.T) {
	_, file := openContentEffectFile(t, nil)
	denied := errors.New("publication denied")
	for _, test := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"other session", metastore.WithReferenceSession(t.Context(), new(advisory.Session)), storage.ErrInvalidScope},
		{"publication", metastore.WithFilePublicationGuard(t.Context(), func() error { return denied }), denied},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := file.Information(test.ctx); !errors.Is(err, test.want) || got != (storage.ReferenceInformation{}) {
				t.Fatalf("failed information = %+v, %v", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := file.Information(ctx); !errors.Is(err, context.Canceled) || got != (storage.ReferenceInformation{}) {
		t.Fatalf("cancelled information = %+v, %v", got, err)
	}
	calls := 0
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error {
		calls++
		if calls > 1 {
			return denied
		}
		return nil
	})
	if got, err := file.Information(guarded); !errors.Is(err, denied) || got != (storage.ReferenceInformation{}) {
		t.Fatalf("post-capture authority failure = %+v, %v", got, err)
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := file.Information(t.Context()); !errors.Is(err, syscall.ESTALE) || got != (storage.ReferenceInformation{}) {
		t.Fatalf("retired information = %+v, %v", got, err)
	}
}

func TestReferenceInformationRejectsMalformedStoredScalars(t *testing.T) {
	for _, update := range []string{
		`kind=0`, `kind=X'31'`, `size=-1`, `size=X'31'`, `allocation_size=NULL`, `allocation_size=8192`,
		`detached=1`, `detached=2`, `detached=X'30'`, `pending_unlink=2`, `pending_unlink=X'30'`,
		`pending_generation=-1`, `pending_generation=X'30'`, `pending_unlink=1,pending_generation=0`,
	} {
		t.Run(update, func(t *testing.T) {
			store, file := openContentEffectFile(t, nil)
			before, err := file.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := store.write.ExecContext(context.Background(), `UPDATE nodes SET kind=?,size=?,allocation_size=?,detached=0,pending_unlink=0,pending_generation=0 WHERE volume=? AND id=?`,
					before.Kind, before.Size, before.AllocationSize, store.volume, file.id); err != nil {
					t.Error(err)
				}
			})
			connection, err := store.write.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			defer connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`)
			if _, err := connection.ExecContext(t.Context(), `UPDATE nodes SET `+update+` WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
				t.Fatal(err)
			}
			if got, err := file.Information(t.Context()); !errors.Is(err, syscall.EIO) || got != (storage.ReferenceInformation{}) {
				t.Fatalf("malformed information = %+v, %v", got, err)
			}
		})
	}
}

func TestReferenceInformationProjectsPendingGenerationWithoutPayloads(t *testing.T) {
	store, file := openContentEffectFile(t, nil)
	want, err := file.Information(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = store.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE nodes SET pending_generation=7 WHERE volume=? AND id=?`, store.volume, file.id)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := file.Information(t.Context()); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("historical generation = %+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_unlink=1 WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
		t.Fatal(err)
	}
	want.PendingUnlink = true
	if got, err := file.Information(t.Context()); err != nil || got != want {
		t.Fatalf("explicit pending = %+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_unlink=0 WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeGeometryUsesAuthoritativeIdentity(t *testing.T) {
	store, _, _, _ := guardedFileFixture(t)
	if err := store.CheckVolumeGeometry(); err != nil {
		t.Fatal(err)
	}
	identity, err := store.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.VolumeGeometry(t.Context())
	if err != nil || got.Volume != identity.Volume || got.BytesPerSector != 4096 || got.SectorsPerAllocationUnit != 1 {
		t.Fatalf("native geometry = %+v, %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := store.VolumeGeometry(ctx); !errors.Is(err, context.Canceled) || got != (storage.VolumeGeometry{}) {
		t.Fatalf("cancelled geometry = %+v, %v", got, err)
	}
	denied := errors.New("volume publication denied")
	calls := 0
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error {
		calls++
		if calls > 1 {
			return denied
		}
		return nil
	})
	if got, err := store.VolumeGeometry(guarded); !errors.Is(err, denied) || got != (storage.VolumeGeometry{}) {
		t.Fatalf("post-capture geometry failure = %+v, %v", got, err)
	}
}

func TestReferenceInformationProjectsAcceptedIntentAndRejectsMalformedOutcome(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	file, _, intent := openCloseUnlinkReference(t, store, false, storage.NodeRegular,
		storage.UseClaim{Uses: storage.ReadData | storage.DeleteName})
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for _, outcome := range []storage.DeleteIntentOutcome{storage.DeleteIntentArmed, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed} {
		var failure any
		if outcome == storage.DeleteIntentCleanupFailed {
			failure = int64(syscall.EIO)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE delete_intents SET outcome=?,failure=? WHERE volume=? AND intent=?`,
			outcome, failure, store.volume, intent); err != nil {
			t.Fatal(err)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_generation=1 WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
		got, err := file.Information(t.Context())
		if err != nil || got.PendingUnlink != (outcome != storage.DeleteIntentArmed) {
			t.Fatalf("outcome %d information = %+v, %v", outcome, got, err)
		}
		state, err := file.State(t.Context())
		if err != nil || state.PendingUnlink != got.PendingUnlink || state.PendingUnlink && len(state.PendingGeneration) != 8 {
			t.Fatalf("outcome %d full state = %+v, %v", outcome, state, err)
		}
	}
	connection, err := store.write.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	defer connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`)
	for _, outcome := range []string{`X'32'`, `0`, `6`, `7`, `2.5`} {
		if _, err := connection.ExecContext(t.Context(), `UPDATE delete_intents SET outcome=`+outcome+` WHERE volume=? AND intent=?`, store.volume, intent); err != nil {
			t.Fatal(err)
		}
		if got, err := file.Information(t.Context()); !errors.Is(err, syscall.EIO) || got != (storage.ReferenceInformation{}) {
			t.Fatalf("malformed outcome %s scalar projection = %+v, %v", outcome, got, err)
		}
		if got, err := file.State(t.Context()); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
			t.Fatalf("malformed outcome %s full projection = %+v, %v", outcome, got, err)
		}
	}
	if _, err := connection.ExecContext(t.Context(), `UPDATE delete_intents SET outcome=?,failure=NULL WHERE volume=? AND intent=?`,
		storage.DeleteIntentArmed, store.volume, intent); err != nil {
		t.Fatal(err)
	}
}
