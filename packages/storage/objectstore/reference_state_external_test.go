package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestObjectScalarStateTracksExactObjectWithoutMetadataGrant(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file := openEffectFile(t, session, root.ID, "file", sealedEffect())
	stateAccess := file.(storage.ReferenceStateAccess)
	before, err := stateAccess.State(t.Context(), storage.StateScalars)
	if err != nil || before.Attr.ID == 0 || before.Attr.Size != 0 || before.Attr.Kind != storage.NodeRegular {
		t.Fatalf("scalar state=%+v,%v", before, err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write-only Stat=%v", err)
	}
	if got, err := stateAccess.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
		t.Fatalf("write-only full state=%+v,%v", got, err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := volume.Rename(t.Context(), "file", "old"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Remove(t.Context(), "old"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	after, err := stateAccess.State(t.Context(), storage.StateScalars)
	if err != nil || after.Attr.ID != before.Attr.ID || after.Attr.Size != 3 || !after.Detached || !after.Attr.AllocationKnown || after.Attr.AllocationSize != 4096 {
		t.Fatalf("detached scalar state=%+v,%v", after, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := stateAccess.State(t.Context(), storage.StateScalars); err == nil || !reflect.DeepEqual(got, storage.ReferenceState{}) {
		t.Fatalf("closed scalar state=%+v,%v", got, err)
	}
	unit, err := session.(storage.AllocationReporting).AllocationUnitBytes(t.Context())
	if err != nil || unit != 4096 {
		t.Fatalf("allocation unit=%d,%v", unit, err)
	}

}

func TestObjectMetadataObservationAllNodePaths(t *testing.T) {
	for _, path := range []string{"file", "node", "child"} {
		t.Run(path, func(t *testing.T) {
			volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			root, err := volume.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			err = volume.Write(t.Context(), "file", []byte("body"))
			if err != nil {
				t.Fatal(err)
			}
			fileAttr, err := volume.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: 2}
			var reference interface {
				Close(context.Context) error
				Stat(context.Context) (storage.Attr, error)
			}
			var observer storage.ReferenceMetadataAccess
			if path == "file" {
				opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, storage.OpenAtOptions{Write: true, Use: storage.UseClaim{Uses: storage.WriteData}, MetadataAccess: storage.WriteMetadata, Target: storage.ChildCondition{State: storage.SameNode, NodeID: fileAttr.ID}, Existing: storage.Keep, Action: fileActionFor(t, session)})
				if err != nil {
					t.Fatal(err)
				}
				reference = opened.File
				observer = opened.File.(storage.ReferenceMetadataAccess)
			} else {
				options := storage.NodeRefOptions{Kind: storage.NodeRegular, MetadataAccess: storage.WriteMetadata, Action: fileActionFor(t, session), Target: storage.ChildCondition{State: storage.SameNode, NodeID: fileAttr.ID}}
				var opened storage.NodeOpenResult
				if path == "node" {
					opened, err = session.(storage.NodeReferences).OpenNodeRef(t.Context(), fileAttr.ID, options)
				} else {
					options.Target = storage.ChildCondition{State: storage.SameNode, NodeID: fileAttr.ID}
					opened, err = session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, options)
				}
				if err != nil {
					t.Fatal(err)
				}
				reference = opened.Reference
				observer = opened.Reference.(storage.ReferenceMetadataAccess)
			}
			t.Cleanup(func() {
				if err := reference.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if _, err := reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("metadata-only Stat=%v", err)
			}
			absent, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || absent.NodeID != fileAttr.ID || absent.Value != nil {
				t.Fatalf("absent=%+v,%v", absent, err)
			}
			payload, err := session.(storage.MetadataAccess).SetMetadata(t.Context(), fileAttr.ID, "test.flags", nil, []byte{1, 2})
			if err != nil {
				t.Fatal(err)
			}
			present, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || present.Value == nil || !bytes.Equal(present.Value.Data, []byte{1, 2}) || !bytes.Equal(present.Value.Version, payload.Version) {
				t.Fatalf("present=%+v,%v", present, err)
			}
			present.Value.Data[0] = 9
			fresh, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || fresh.Value.Data[0] != 1 {
				t.Fatalf("alias=%+v,%v", fresh, err)
			}
			if _, err := session.(storage.MetadataAccess).SetMetadata(t.Context(), fileAttr.ID, "test.flags", payload.Version, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), request); !errors.Is(err, syscall.EFBIG) || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
				t.Fatalf("oversize=%+v,%v", got, err)
			}
			if err := reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), request); err == nil || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
				t.Fatalf("closed=%+v,%v", got, err)
			}
		})
	}
}

func TestObjectSealedTruncatePublishesContentAndMetadataAtomically(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file := openEffectFile(t, session, root.ID, "file", sealedEffect())
	if _, err := file.WriteAt(t.Context(), 0, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	observer := file.(storage.ReferenceMetadataAccess)
	mutator := file.(storage.ConditionalFileMutation)
	var version []byte
	for _, size := range []int64{3, 7, 0, 4, 4} {
		mutation := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateTruncate, Size: size, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": bytes.Clone(version)}}
		changed, err := mutator.MutateFile(t.Context(), mutation)
		if err != nil || changed.Size != size || !bytes.Equal(changed.Metadata["test.flags"].Data, []byte{0x31, 0x20}) {
			t.Fatalf("truncate %d=%+v,%v", size, changed, err)
		}
		body, err := volume.Read(t.Context(), "file")
		if err != nil || int64(len(body)) != size {
			t.Fatalf("body=%q,%v", body, err)
		}
		if size == 3 && string(body) != "abc" {
			t.Fatalf("shrink=%q", body)
		}
		if size == 7 && !bytes.Equal(body, []byte{'a', 'b', 'c', 0, 0, 0, 0}) {
			t.Fatalf("growth=%q", body)
		}
		if size == 4 && !bytes.Equal(body, make([]byte, 4)) {
			t.Fatalf("zero growth=%q", body)
		}
		replayed, err := mutator.MutateFile(t.Context(), mutation)
		if err != nil || !bytes.Equal(replayed.Metadata["test.flags"].Version, changed.Metadata["test.flags"].Version) {
			t.Fatalf("replay=%+v,%v", replayed, err)
		}
		version = bytes.Clone(changed.Metadata["test.flags"].Version)
	}
	current, err := observer.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.(storage.MetadataAccess).SetMetadata(t.Context(), current.NodeID, "test.flags", current.Value.Version, []byte{0x31, 0x10}); err != nil {
		t.Fatal(err)
	}
	stale := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateTruncate, Size: 1, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": current.Value.Version}}
	if _, err := mutator.MutateFile(t.Context(), stale); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale truncate=%v", err)
	}
	body, err := volume.Read(t.Context(), "file")
	if err != nil || len(body) != 4 {
		t.Fatalf("stale changed size=%q,%v", body, err)
	}
	actual, err := observer.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0})
	if err != nil || !bytes.Equal(actual.Value.Data, []byte{0x31, 0x10}) {
		t.Fatalf("stale changed metadata=%+v,%v", actual, err)
	}
}
