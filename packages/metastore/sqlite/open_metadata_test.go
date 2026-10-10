package sqlite

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNativeAtomicOpenEnforcesIndependentMetadataPermissions(t *testing.T) {
	store, legacy := openPublicationFile(t)
	if err := legacy.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, permissions := range []storage.MetadataPermissions{0, storage.ReadMetadata, storage.WriteMetadata, storage.ReadMetadata | storage.WriteMetadata} {
		t.Run(fmt.Sprintf("metadata=%d", permissions), func(t *testing.T) {
			opened, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{
				Parent: storage.DirectoryTarget{NodeID: uint64(store.root)}, RawLeaf: []byte("file"),
			}), storage.OpenAtOptions{Write: true, MetadataAccess: permissions, Target: storage.ChildCondition{State: storage.Any},
				Action: fileAction(t), Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}})
			if err != nil || opened.File == nil || opened.State.ID == 0 {
				t.Fatalf("open=%+v, %v", opened, err)
			}
			defer opened.File.Close(t.Context())
			assertAccess := func(label string, err error, allowed bool) {
				t.Helper()
				if allowed && err != nil || !allowed && !errors.Is(err, syscall.EBADF) {
					t.Fatalf("%s allowed=%t: %v", label, allowed, err)
				}
			}
			at := time.Unix(123, 456)
			_, err = opened.File.SetAttr(t.Context(), storage.AttrChange{ModTime: &at})
			assertAccess("SetAttr", err, permissions&storage.WriteMetadata != 0)
			_, err = opened.File.SetMetadata(t.Context(), fmt.Sprintf("test.rights%d", permissions), nil, []byte("value"))
			assertAccess("SetMetadata", err, permissions&storage.WriteMetadata != 0)
			_, err = opened.File.(metastore.ReferenceStateAccess).State(t.Context())
			assertAccess("State", err, permissions&storage.ReadMetadata != 0)
			_, err = opened.File.Node(metastore.WithFileAccess(t.Context(), metastore.FileAccess{Uses: storage.ReadData}))
			assertAccess("byte read with execute Use", err, false)
			_, err = opened.File.Node(metastore.WithFileAccess(t.Context(), metastore.FileAccess{Uses: storage.WriteData}))
			assertAccess("byte write capture", err, true)
			id, err := storage.ReferenceNodeID(opened.File)
			if err != nil || id != uint64(opened.State.ID) {
				t.Fatalf("immutable reference identity=%d, %v", id, err)
			}
			if err := opened.File.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if closedID, err := storage.ReferenceNodeID(opened.File); err != nil || closedID != id {
				t.Fatalf("released identity changed: %d, %v", closedID, err)
			}
		})
	}
}

func TestInvalidAtomicOpenPermissionsHaveNoNativeEffects(t *testing.T) {
	store, legacy := openPublicationFile(t)
	if err := legacy.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	files, pins := store.fileDomain.files, len(store.coordinator.pins)
	options := storage.OpenAtOptions{Write: true, Create: true, Exclusive: true, MetadataAccess: storage.ReadMetadata,
		Action: fileAction(t), Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep,
		Use: storage.UseClaim{Uses: storage.WriteData}, Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"test.initial": []byte("value")}}}}
	for _, change := range []func(*storage.OpenAtOptions){
		func(o *storage.OpenAtOptions) { o.MetadataAccess = 4 },
		func(o *storage.OpenAtOptions) { o.Use.Uses = storage.ReadData },
		func(o *storage.OpenAtOptions) { o.Read = true; o.Use.Uses = storage.WriteData },
	} {
		invalid := options
		change(&invalid)
		result, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{
			Parent: storage.DirectoryTarget{NodeID: uint64(store.root)}, RawLeaf: []byte("invalid"),
		}), invalid)
		if !errors.Is(err, syscall.EINVAL) || result.File != nil || result.State.ID != 0 {
			t.Fatalf("invalid options returned ownership or result: %+v, %v", result, err)
		}
		if _, err := store.Stat(t.Context(), "invalid"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("invalid open created a name: %v", err)
		}
		after, err := store.DurableState(t.Context())
		if err != nil || after != before || store.fileDomain.files != files || len(store.coordinator.pins) != pins {
			t.Fatalf("invalid open changed durable state or retention: %+v, %+v, %v", before, after, err)
		}
	}
}
