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

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReferenceProjectionsRejectCorruptPendingDeletion(t *testing.T) {
	for _, update := range []string{
		`pending_unlink=2`, `pending_unlink=X'30'`, `pending_unlink=0.5`,
		`pending_generation=-1`, `pending_generation=X'30'`, `pending_generation=0.5`,
		`pending_unlink=1,pending_generation=0`,
	} {
		t.Run(update, func(t *testing.T) {
			store, file := openContentEffectFile(t, nil)
			reader, err := store.OpenNode(t.Context(), uint64(file.id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close(context.Background())
			connection, err := store.write.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			defer connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`)
			defer connection.ExecContext(context.Background(), `UPDATE nodes SET pending_unlink=0,pending_generation=0 WHERE volume=? AND id=?`, store.volume, file.id)
			if _, err := connection.ExecContext(t.Context(), `UPDATE nodes SET `+update+` WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
				t.Fatal(err)
			}
			if got, err := file.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
				t.Fatalf("corrupt pending scalar projection = %+v, %v", got, err)
			}
			if got, err := reader.(*retainedFile).State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, metastore.ReferenceState{}) {
				t.Fatalf("corrupt pending full projection = %+v, %v", got, err)
			}
		})
	}
}

func TestNamespaceCapturePreservesObserverKindRules(t *testing.T) {
	for _, update := range []string{`kind=0`, `kind=X'31'`, `kind=1.5`, `metadata='invalid'`} {
		t.Run(update, func(t *testing.T) {
			store, _, node, selection := guardedFileFixture(t)
			effect := testContentEffect()
			opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{Write: true, Existing: storage.Keep,
				MetadataAccess: storage.WriteMetadata, Action: fileAction(t), Use: storage.UseClaim{Uses: storage.WriteData},
				Target:                 storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
				ContentMetadataEffects: []storage.ContentMetadataEffect{effect}})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.File.Close(context.Background())
			file := opened.File.(*retainedFile)
			valid, err := storage.EncodeMetadata(nil)
			if err != nil {
				t.Fatal(err)
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
			defer connection.ExecContext(context.Background(), `UPDATE nodes SET kind=?,metadata=? WHERE volume=? AND id=?`, storage.NodeRegular, valid, store.volume, file.id)
			if _, err := connection.ExecContext(t.Context(), `UPDATE nodes SET `+update+` WHERE volume=? AND id=?`, store.volume, file.id); err != nil {
				t.Fatal(err)
			}
			for _, request := range []storage.MetadataObservationRequest{
				{Purpose: storage.MetadataObservationContentEffect},
				namespaceObservation(effect.Namespace, effect.PayloadBytes),
			} {
				if got, err := file.ObserveMetadata(t.Context(), request); !errors.Is(err, syscall.EIO) || got.NodeID != 0 || got.Value != nil {
					t.Fatalf("corrupt namespace observation = %+v, %v", got, err)
				}
			}
		})
	}
}

func TestReferenceCapturesShareThePassedSnapshot(t *testing.T) {
	store, file := openContentEffectFile(t, nil)
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := storage.EncodeMetadata(before.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{
		"test.snapshot": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte("later")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.write.ExecContext(context.Background(), `UPDATE nodes SET size=?,pending_unlink=0,pending_generation=0,metadata=? WHERE volume=? AND id=?`,
		before.Size, baseline, store.volume, file.id)
	err = store.inspect(t.Context(), func(tx *sql.Tx) error {
		previous, err := store.referenceScalarState(t.Context(), tx, file.id)
		if err != nil {
			return err
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET size=8,pending_unlink=1,pending_generation=7,metadata=? WHERE volume=? AND id=?`,
			changed, store.volume, file.id); err != nil {
			return err
		}
		information, err := store.referenceScalarState(t.Context(), tx, file.id)
		if err != nil {
			return err
		}
		state, err := store.referenceState(t.Context(), tx, file.id)
		if err != nil {
			return err
		}
		_, observed, err := store.captureNamespace(t.Context(), tx, file.id, "test.snapshot")
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(information, previous) || state.State.Size != before.Size || state.PendingUnlink || observed.Value != nil {
			t.Fatalf("capture escaped original snapshot: information=%+v, state=%+v, namespace=%+v", information, state, observed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if current, err := file.State(t.Context(), storage.StateScalars); err != nil || current.State.Size != 8 || !current.PendingUnlink {
		t.Fatalf("fresh scalar capture = %+v, %v", current, err)
	}
}

func TestReferenceScalarStateDoesNotLoadOversizedMetadata(t *testing.T) {
	store, file := openContentEffectFile(t, nil)
	want, err := file.State(t.Context(), storage.StateScalars)
	if err != nil {
		t.Fatal(err)
	}
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storage.EncodeMetadata(before.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.OpenNode(t.Context(), uint64(file.id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(context.Background())
	defer store.write.ExecContext(context.Background(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, valid, store.volume, file.id)
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`,
		bytes.Repeat([]byte{0}, storage.MaxMetadataBytes+1), store.volume, file.id); err != nil {
		t.Fatal(err)
	}
	if got, err := file.State(t.Context(), storage.StateScalars); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("scalar observation depended on unrequested metadata = %+v, %v", got, err)
	}
	if _, err := reader.(*retainedFile).State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EIO) {
		t.Fatalf("full observation failed to check metadata bound: %v", err)
	}
}

func TestMetadataCaptureSupportsWriteOnlyDirectoryReference(t *testing.T) {
	store, root, _, _ := guardedFileFixture(t)
	opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("observed-dir")}),
		storage.NodeRefOptions{Kind: storage.NodeDirectory, Create: true, Exclusive: true, Action: fileAction(t),
			MetadataAccess: storage.WriteMetadata, Target: storage.ChildCondition{State: storage.Absent}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	ref := opened.Reference.(*retainedNodeReference)
	if got, err := ref.ObserveMetadata(t.Context(), namespaceObservation("test.directory", 0)); err != nil || got.NodeID != uint64(opened.State.ID) || got.Value != nil {
		t.Fatalf("write-only directory absence = %+v, %v", got, err)
	}
	value, err := store.SetMetadata(t.Context(), uint64(opened.State.ID), "test.directory", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ref.ObserveMetadata(t.Context(), namespaceObservation("test.directory", 0)); err != nil || got.Value == nil || len(got.Value.Data) != 0 || !bytes.Equal(got.Value.Version, value.Version) {
		t.Fatalf("write-only directory present empty = %+v, %v", got, err)
	}
	if _, err := ref.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("directory observer granted full metadata read: %v", err)
	}
}
