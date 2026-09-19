package httprest

import (
	"context"
	"errors"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestNameRemovalRetainsOpenIdentityAndReplaysItsEmptyResult(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "file"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "remove-result", 1<<20, locking.DefaultOptions())
			handler, err := NewHandler(backend, meta)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := opened.(*remoteFileSession)
			var retained storage.NodeReference
			var original storage.Attr
			if directory {
				if err := backend.Mkdir(t.Context(), name); err != nil {
					t.Fatal(err)
				}
				original, err = backend.Stat(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				result, e := session.OpenNodeRef(t.Context(), original.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, MetadataAccess: storage.ReadMetadata})
				if e != nil {
					t.Fatal(e)
				}
				retained = result.Reference
			} else {
				file, e := session.OpenFile(t.Context(), name, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
				if e != nil {
					t.Fatal(e)
				}
				original, err = file.WriteAt(t.Context(), 0, []byte("retained"))
				if err != nil {
					t.Fatal(err)
				}
				retained = file
			}
			t.Cleanup(func() {
				if err := session.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			kind := storage.NameRemove
			if directory {
				kind = storage.NameRemoveDir
			}
			command := storage.NameCommand{Kind: kind, Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)}, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}}
			lost := &loseCapabilityReply{next: client.http.Transport, op: storage.OpFileMutateName}
			client.http.Transport = lost
			result, barrier, err := session.MutateNameWithBarrier(t.Context(), command)
			if err != nil || result.Attr != nil || barrier == nil || !lost.lost.Load() {
				t.Fatalf("removal result=%+v,barrier=%+v,err=%v,lost=%v", result, barrier, err, lost.lost.Load())
			}
			if _, err := backend.Stat(t.Context(), name); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("removed name still resolves: %v", err)
			}
			captured, err := retained.Stat(t.Context())
			if err != nil || captured.ID != original.ID {
				t.Fatalf("retained identity changed: %+v,%v", captured, err)
			}
			if !directory {
				read, err := retained.(storage.File).ReadAt(t.Context(), 0, 32)
				if err != nil || string(read.Data) != "retained" || read.Attr.ID != original.ID {
					t.Fatalf("retained bytes changed: %+v,%v", read, err)
				}
			}
			current, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
			if err != nil || barrier.Incarnation != string(current.Incarnation) || barrier.Position != int64(current.Position) {
				t.Fatalf("replayed barrier=%+v,current=%+v,err=%v", barrier, current, err)
			}
			if err := retained.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNameMutationReplyRequiresAttributesExceptForDeletion(t *testing.T) {
	for _, kind := range []storage.NameOperation{storage.NameCreate, storage.NameMkdir, storage.NameSymlink, storage.NameRename, storage.NameRemove, storage.NameRemoveDir} {
		req := fileRequest{Op: storage.OpFileMutateName, Name: &nameCommand{Kind: kind}}
		empty := fileResponse{Epoch: 1, Data: []byte{}}
		err := validateFileResponse(req, empty)
		deletion := kind == storage.NameRemove || kind == storage.NameRemoveDir
		if (err == nil) != deletion {
			t.Fatalf("kind %d empty result: %v", kind, err)
		}
		empty.Attr = AttrOf(storage.Attr{ID: 1, Kind: storage.NodeRegular})
		if err := validateFileResponse(req, empty); err != nil {
			t.Fatalf("kind %d captured result: %v", kind, err)
		}
	}
}
