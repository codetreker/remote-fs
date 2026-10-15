package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReferenceScalarStateDoesNotGrantMetadataAndTracksDetachedObject(t *testing.T) {
	store, root, node, _ := guardedFileFixture(t)
	opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	ref := opened.Reference.(*retainedNodeReference)
	if err := ref.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	want := metastore.ReferenceState{State: metastore.FileState{Node: metastore.Node{ID: node.ID, Kind: storage.NodeRegular, Size: 4, AllocationSize: 4096, AllocationKnown: true}}}
	if got, err := ref.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("scalar state = %+v, %v", got, err)
	}
	if _, err := ref.Node(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("scalar capability granted public metadata: %v", err)
	}
	if _, err := ref.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("scalar capability granted full state: %v", err)
	}
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}
	if _, err := store.MutateName(t.Context(), storage.NameCommand{Action: fileAction(t), Kind: storage.NameRemove, Name: name,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}}); err != nil {
		t.Fatal(err)
	}
	want.State.Detached = true
	if got, err := ref.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("detached identity = %+v, %v", got, err)
	}
}

func TestReferenceStateRejectsUnknownProjection(t *testing.T) {
	_, file := openContentEffectFile(t, nil)
	for _, projection := range []storage.StateProjection{0, 3, 255} {
		if got, err := file.State(t.Context(), projection); !errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
			t.Fatalf("invalid projection %d = %+v, %v", projection, got, err)
		}
	}
}

func TestReferenceScalarStateDirectoryAndSymlinkKinds(t *testing.T) {
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
			got, err := opened.Reference.(*retainedNodeReference).State(t.Context(), storage.StateScalars)
			want := metastore.ReferenceState{State: metastore.FileState{Node: metastore.Node{
				ID: opened.State.ID, Kind: test.kind, Size: int64(len(test.link)), AllocationKnown: true,
			}}}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("node scalar state = %+v, %v", got, err)
			}
		})
	}
}

func TestReferenceScalarStateDoesNotLoadOversizedSymlinkTarget(t *testing.T) {
	store, root, _, _ := guardedFileFixture(t)
	target := []byte("target")
	opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("observed-link")}),
		storage.NodeRefOptions{Kind: storage.NodeSymlink, Create: true, Exclusive: true, Action: fileAction(t),
			MetadataAccess: storage.ReadMetadata, Target: storage.ChildCondition{State: storage.Absent},
			InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: target}}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	ref := opened.Reference.(*retainedNodeReference)
	want, err := ref.State(t.Context(), storage.StateScalars)
	if err != nil {
		t.Fatal(err)
	}
	if full, err := ref.State(t.Context(), storage.StateFull); err != nil || !bytes.Equal(full.LinkTarget, target) {
		t.Fatalf("baseline full symlink state = %+v, %v", full, err)
	}
	connection, err := store.write.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), `UPDATE nodes SET link_target=? WHERE volume=? AND id=?`, target, store.volume, opened.State.ID); err != nil {
			t.Error(err)
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`); err != nil {
			t.Error(err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(), `UPDATE nodes SET link_target=? WHERE volume=? AND id=?`,
		bytes.Repeat([]byte{'x'}, storage.MaxLinkTargetBytes+1), store.volume, opened.State.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := ref.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("scalar state depended on unrequested symlink target = %+v, %v", got, err)
	}
	if got, err := ref.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
		t.Fatalf("full state accepted oversized symlink target = %+v, %v", got, err)
	}
}

func TestReferenceScalarStateZeroesLifetimeAndAuthorityFailures(t *testing.T) {
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
			if got, err := file.State(test.ctx, storage.StateScalars); !errors.Is(err, test.want) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
				t.Fatalf("failed scalar state = %+v, %v", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := file.State(ctx, storage.StateScalars); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
		t.Fatalf("cancelled scalar state = %+v, %v", got, err)
	}
	calls := 0
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error {
		calls++
		if calls > 1 {
			return denied
		}
		return nil
	})
	if got, err := file.State(guarded, storage.StateScalars); !errors.Is(err, denied) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
		t.Fatalf("post-capture authority failure = %+v, %v", got, err)
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := file.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.ESTALE) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
		t.Fatalf("retired scalar state = %+v, %v", got, err)
	}
}

func TestReferenceScalarStateRejectsMalformedStoredScalars(t *testing.T) {
	store, file := openContentEffectFile(t, nil)
	ctx := t.Context()
	before, err := file.Node(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scalars, err := file.State(ctx, storage.StateScalars)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := store.DurableState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var quota [2]int64
	if err := store.read.QueryRowContext(ctx, `SELECT used,allocated_used FROM volumes WHERE id=?`, store.volume).
		Scan(&quota[0], &quota[1]); err != nil {
		t.Fatal(err)
	}
	assertFixture := func(t *testing.T) {
		t.Helper()
		if err := errors.Join(store.coordinator.healthy(), store.checkFileOwnership()); err != nil {
			t.Fatalf("shared scalar fixture lost its authority: %v", err)
		}
		if got, err := file.Node(ctx); err != nil || !reflect.DeepEqual(got, before) {
			t.Fatalf("shared scalar fixture node = %+v, %v; want %+v", got, err, before)
		}
		if got, err := file.State(ctx, storage.StateScalars); err != nil || !reflect.DeepEqual(got, scalars) {
			t.Fatalf("shared scalar fixture projection = %+v, %v; want %+v", got, err, scalars)
		}
		if got, err := store.DurableState(ctx); err != nil || got != durable {
			t.Fatalf("shared scalar fixture durable state = %+v, %v; want %+v", got, err, durable)
		}
		var gotQuota [2]int64
		if err := store.read.QueryRowContext(ctx, `SELECT used,allocated_used FROM volumes WHERE id=?`, store.volume).
			Scan(&gotQuota[0], &gotQuota[1]); err != nil || gotQuota != quota {
			t.Fatalf("shared scalar fixture quota = %v, %v; want %v", gotQuota, err, quota)
		}
		if count := store.read.Stats().InUse; count != 0 {
			t.Fatalf("shared scalar fixture retains %d reader connections", count)
		}
	}
	for _, update := range []string{
		`kind=0`, `kind=X'31'`, `size=-1`, `size=X'31'`, `allocation_size=NULL`, `allocation_size=8192`,
		`detached=1`, `detached=2`, `detached=X'30'`, `pending_unlink=2`, `pending_unlink=X'30'`,
		`pending_generation=-1`, `pending_generation=X'30'`, `pending_unlink=1,pending_generation=0`,
	} {
		if !t.Run(update, func(t *testing.T) {
			assertFixture(t)
			connection, err := store.write.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_, restoreErr := connection.ExecContext(context.Background(), `UPDATE nodes SET kind=?,size=?,allocation_size=?,detached=0,pending_unlink=0,pending_generation=0 WHERE volume=? AND id=?`,
					before.Kind, before.Size, before.AllocationSize, store.volume, file.id)
				_, pragmaErr := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`)
				if err := errors.Join(restoreErr, pragmaErr, connection.Close()); err != nil {
					t.Errorf("restoring the shared scalar fixture: %v", err)
					return
				}
				assertFixture(t)
			})
			if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := connection.ExecContext(t.Context(), `UPDATE nodes SET `+update+` WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
				t.Fatal(err)
			}
			if got, err := file.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
				t.Fatalf("malformed scalar state = %+v, %v", got, err)
			}
			if count := store.read.Stats().InUse; count != 0 {
				t.Fatalf("rejected scalar read retains %d reader connections", count)
			}
		}) {
			return
		}
	}
}

func TestReferenceScalarStateProjectsPendingGenerationWithoutPayloads(t *testing.T) {
	store, file := openContentEffectFile(t, nil)
	want, err := file.State(t.Context(), storage.StateScalars)
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
	if got, err := file.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("historical generation = %+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_unlink=1 WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
		t.Fatal(err)
	}
	want.PendingUnlink = true
	want.PendingGeneration = binary.BigEndian.AppendUint64(nil, 7)
	if got, err := file.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit pending = %+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_unlink=0 WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
		t.Fatal(err)
	}
}

func TestReferenceScalarStateProjectsAcceptedIntentAndRejectsMalformedOutcome(t *testing.T) {
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
		got, err := file.State(t.Context(), storage.StateScalars)
		if err != nil || got.PendingUnlink != (outcome != storage.DeleteIntentArmed) {
			t.Fatalf("outcome %d information = %+v, %v", outcome, got, err)
		}
		state, err := file.State(t.Context(), storage.StateFull)
		if err != nil || state.PendingUnlink != got.PendingUnlink || !bytes.Equal(state.PendingGeneration, got.PendingGeneration) || state.PendingUnlink && len(state.PendingGeneration) != 8 {
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
		if got, err := file.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
			t.Fatalf("malformed outcome %s scalar projection = %+v, %v", outcome, got, err)
		}
		if got, err := file.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
			t.Fatalf("malformed outcome %s full projection = %+v, %v", outcome, got, err)
		}
	}
	if _, err := connection.ExecContext(t.Context(), `UPDATE delete_intents SET outcome=?,failure=NULL WHERE volume=? AND intent=?`,
		storage.DeleteIntentArmed, store.volume, intent); err != nil {
		t.Fatal(err)
	}
}
