package objectstore_test

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestAtomicOpenIndependentMetadataAndBytePermissions(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.(storage.OpenMetadataAccess).CheckOpenMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	for _, access := range []storage.OpenAccess{{Read: true}, {Write: true}, {Read: true, Write: true}} {
		for _, metadata := range []storage.MetadataPermissions{0, storage.ReadMetadata, storage.WriteMetadata, storage.ReadMetadata | storage.WriteMetadata} {
			t.Run(fmt.Sprintf("read=%t/write=%t/metadata=%d", access.Read, access.Write, metadata), func(t *testing.T) {
				name := fmt.Sprintf("file-%t-%t-%d", access.Read, access.Write, metadata)
				// The superset read Use models execute sharing without byte-read permission.
				uses := storage.ReadData
				if access.Write {
					uses |= storage.WriteData
				}
				opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{
					Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)},
				}, storage.OpenAtOptions{Read: access.Read, Write: access.Write, MetadataAccess: metadata,
					Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
					Existing: storage.Keep, Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: uses}})
				if err != nil || opened.File == nil || opened.Attr.ID == 0 || !opened.Attr.AllocationKnown || opened.Outcome != storage.Created {
					t.Fatalf("atomic result=%+v, %v", opened, err)
				}
				defer opened.File.Close(t.Context())
				assertAccess := func(label string, err error, allowed bool) {
					t.Helper()
					if allowed && err != nil || !allowed && !errors.Is(err, syscall.EBADF) {
						t.Fatalf("%s allowed=%t: %v", label, allowed, err)
					}
				}
				_, err = opened.File.Stat(t.Context())
				assertAccess("Stat", err, metadata&storage.ReadMetadata != 0)
				_, err = opened.File.(storage.ReferenceStateAccess).State(t.Context())
				assertAccess("State", err, metadata&storage.ReadMetadata != 0)
				at := time.Unix(123, 456)
				_, err = opened.File.SetAttr(t.Context(), storage.AttrChange{ModTime: &at})
				assertAccess("SetAttr", err, metadata&storage.WriteMetadata != 0)
				_, err = opened.File.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.rights", nil, []byte("value"))
				assertAccess("SetMetadata", err, metadata&storage.WriteMetadata != 0)
				_, err = opened.File.ReadAt(t.Context(), 0, 1)
				assertAccess("ReadAt", err, access.Read)
				_, err = opened.File.WriteAt(t.Context(), 0, []byte("body"))
				assertAccess("WriteAt", err, access.Write)
				_, err = opened.File.Truncate(t.Context(), 2)
				assertAccess("Truncate", err, access.Write)
				if metadata&storage.WriteMetadata == 0 && access.Write {
					_, err = opened.File.(storage.ConditionalFileMutation).MutateFile(t.Context(), storage.FileMutation{
						Action: fileActionFor(t, session), Kind: storage.MutateTruncate, Size: 0,
						Metadata: map[string]storage.OpaquePayload{"test.extra": {Data: []byte("value")}},
					})
					assertAccess("truncate with metadata", err, false)
					current, err := volume.Stat(t.Context(), name)
					if err != nil || current.Size != 2 {
						t.Fatalf("rejected mixed mutation changed size: %+v, %v", current, err)
					}
				}
				if err := opened.File.Sync(t.Context()); err != nil {
					t.Fatalf("Sync depends on metadata access: %v", err)
				}
			})
		}
	}
}
