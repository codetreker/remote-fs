package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestObjectGuardedRenameJournalReplaysOriginalObject(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	for _, directory := range []string{"a", "b"} {
		if err := volume.Mkdir(t.Context(), directory); err != nil {
			t.Fatal(err)
		}
	}
	if err := volume.Write(t.Context(), "a/file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	a, err := volume.Stat(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := volume.Stat(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	file, err := volume.Stat(t.Context(), "a/file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	command := storage.NameCommand{
		Action: fileActionFor(t, session), Kind: storage.NameRename,
		Name:        storage.ChildName{Parent: storage.DirectoryTarget{NodeID: a.ID}, RawLeaf: []byte("file")},
		Target:      storage.ChildCondition{State: storage.SameNode, NodeID: file.ID},
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: b.ID}, ObservedLeaf: []byte("new"), OutputLeaf: []byte("new"), Expected: storage.ChildCondition{State: storage.Absent}},
		Guards: &storage.NamespaceGuards{RootID: root.ID, Edges: []storage.ObservedEdge{
			{ParentID: root.ID, RawLeaf: []byte("a"), ChildID: a.ID},
			{ParentID: root.ID, RawLeaf: []byte("b"), ChildID: b.ID},
			{ParentID: a.ID, RawLeaf: []byte("file"), ChildID: file.ID},
		}}, RequireNoOpenDescendants: true,
	}
	for _, id := range []uint64{root.ID, a.ID, b.ID} {
		observed, err := session.(storage.DirectoryReader).ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: id})
		if err != nil {
			t.Fatal(err)
		}
		command.Guards.Directories = append(command.Guards.Directories, observed.Observation)
	}
	first, err := session.(storage.NamespaceAccess).MutateName(t.Context(), command)
	if err != nil || first.Attr == nil || first.Attr.ID != file.ID {
		t.Fatalf("rename=%+v,%v", first, err)
	}
	if err := volume.Write(t.Context(), "a/file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	reordered := command
	reordered.Guards = command.Guards.Clone()
	reordered.Guards.Directories[0], reordered.Guards.Directories[2] = reordered.Guards.Directories[2], reordered.Guards.Directories[0]
	reordered.Guards.Edges[0], reordered.Guards.Edges[2] = reordered.Guards.Edges[2], reordered.Guards.Edges[0]
	replayed, err := session.(storage.NamespaceAccess).MutateName(t.Context(), reordered)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replay=%+v,%v", replayed, err)
	}
	changed := command
	changed.RequireNoOpenDescendants = false
	if _, err := session.(storage.NamespaceAccess).MutateName(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed admission replay=%v", err)
	}
	body, err := volume.Read(t.Context(), "a/file")
	if err != nil || string(body) != "replacement" {
		t.Fatalf("replacement=%q,%v", body, err)
	}
	receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), command.Action)
	if err != nil || receipt.Operation != storage.OpFileMutateName || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("receipt=%+v,%v", receipt, err)
	}
}

func TestObjectDeleteResetReturnsNoDataGrantAndScalarPendingToken(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	if err := volume.Write(t.Context(), "file", []byte("old content")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	original, err := volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}
	target := storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}
	opener := session.(storage.AtomicFileOpener)
	held, err := opener.OpenAt(t.Context(), selection, storage.OpenAtOptions{Read: true, Use: storage.UseClaim{Uses: storage.ReadData, Deny: storage.WriteData}, Target: target, Existing: storage.Keep, Action: fileActionFor(t, session)})
	if err != nil {
		t.Fatal(err)
	}
	options := storage.OpenAtOptions{Use: storage.UseClaim{Uses: storage.DeleteName}, Target: target, Existing: storage.ResetContent, ResetUse: storage.DeleteName, Action: fileActionFor(t, session)}
	reset, err := opener.OpenAt(t.Context(), selection, options)
	if err != nil || reset.File == nil || reset.Attr.ID != original.ID || reset.Attr.Size != 0 || reset.Outcome != storage.Reset {
		t.Fatalf("delete reset=%+v,%v", reset, err)
	}
	replayed, err := opener.OpenAt(t.Context(), selection, options)
	if err != nil || replayed.File != reset.File || !reflect.DeepEqual(replayed.Attr, reset.Attr) {
		t.Fatalf("reset replay=%+v,%v", replayed, err)
	}
	changed := options
	changed.ResetUse, changed.Write, changed.Use.Uses = 0, true, storage.WriteData
	if _, err := opener.OpenAt(t.Context(), selection, changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed reset use replay=%v", err)
	}
	if _, err := reset.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete reset granted read=%v", err)
	}
	if _, err := reset.File.WriteAt(t.Context(), 0, []byte{1}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete reset granted write=%v", err)
	}
	if _, err := reset.File.Truncate(t.Context(), 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete reset granted truncate=%v", err)
	}
	if _, err := reset.File.(storage.ReferenceStateAccess).State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete reset granted metadata=%v", err)
	}
	if _, err := session.(storage.MetadataAccess).SetMetadata(t.Context(), original.ID, "app.private", nil, bytes.Repeat([]byte{7}, storage.MaxMetadataValueBytes)); err != nil {
		t.Fatal(err)
	}
	pending, err := reset.File.(storage.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: fileActionFor(t, session), Condition: storage.UnlinkFile})
	if err != nil || !pending.PendingUnlink || len(pending.PendingGeneration) == 0 {
		t.Fatalf("set pending=%+v,%v", pending, err)
	}
	scalar, err := reset.File.(storage.ReferenceStateAccess).State(t.Context(), storage.StateScalars)
	if err != nil || scalar.Attr.ID != original.ID || !scalar.PendingUnlink || !bytes.Equal(scalar.PendingGeneration, pending.PendingGeneration) || scalar.Attr.Metadata != nil || scalar.Attr.BirthTime != nil || scalar.Attr.ChangeTime != nil || !scalar.Attr.ModTime.IsZero() || scalar.LinkTarget != nil {
		t.Fatalf("scalar pending=%+v,%v", scalar, err)
	}
	cleared, err := reset.File.(storage.DeleteIntent).ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileActionFor(t, session), Generation: scalar.PendingGeneration})
	if err != nil || cleared.PendingUnlink {
		t.Fatalf("clear pending=%+v,%v", cleared, err)
	}
	for _, file := range []storage.File{held.File, reset.File} {
		if err := file.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	current, err := volume.Stat(t.Context(), "file")
	if err != nil || current.ID != original.ID || current.Size != 0 {
		t.Fatalf("final state=%+v,%v", current, err)
	}
}

func TestObjectDeleteResetSyncRequiresDataGrantAndPreservesClose(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "http"}[remote], func(t *testing.T) {
			objectDeleteResetSync(t, remote)
		})
	}
}

func objectDeleteResetSync(t *testing.T, remote bool) {
	t.Helper()
	volume, meta := fileVolume(t, memory.New(), 1<<20, nil)
	if err := volume.Write(t.Context(), "file", []byte("old")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	original, err := volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	var fileStorage storage.FileStorage = volume
	if remote {
		handler, err := httprest.NewHandler(volume, meta)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(func() {
			server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := handler.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		client, err := httprest.Dial(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		fileStorage = client
	}
	session := fileSessionFor(t, fileStorage, storage.DefaultFileSessionOptions())
	selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}
	target := storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}
	opener := session.(storage.AtomicFileOpener)
	reset, err := opener.OpenAt(t.Context(), selection, storage.OpenAtOptions{Use: storage.UseClaim{Uses: storage.DeleteName}, Target: target, Existing: storage.ResetContent, ResetUse: storage.DeleteName, MetadataAccess: storage.ReadMetadata, Action: fileActionFor(t, session)})
	if err != nil || reset.File == nil || reset.Attr.ID != original.ID || reset.Outcome != storage.Reset {
		t.Fatalf("reset=%+v,%v", reset, err)
	}
	if attr, err := reset.File.Stat(t.Context()); err != nil || attr.ID != original.ID {
		t.Fatalf("metadata grant=%+v,%v", attr, err)
	}
	writer, err := opener.OpenAt(t.Context(), selection, storage.OpenAtOptions{Write: true, Use: storage.UseClaim{Uses: storage.WriteData}, Target: target, Existing: storage.Keep, Action: fileActionFor(t, session)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.File.WriteAt(t.Context(), 0, []byte("new content")); err != nil {
		t.Fatal(err)
	}
	if err := reset.File.Sync(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only reference synchronized content=%v", err)
	}
	if result, err := reset.File.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("no-data close=%+v,%v", result, err)
	}
	if err := writer.File.Sync(t.Context()); err != nil {
		t.Fatalf("write-only sync=%v", err)
	}
	if err := writer.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reader, err := opener.OpenAt(t.Context(), selection, storage.OpenAtOptions{Read: true, Use: storage.UseClaim{Uses: storage.ReadData}, Target: target, Existing: storage.Keep, Action: fileActionFor(t, session)})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.File.Sync(t.Context()); err != nil {
		t.Fatalf("read-only sync=%v", err)
	}
	if err := reader.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
