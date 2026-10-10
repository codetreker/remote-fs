package replicated_test

import (
	"bytes"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestContentMetadataMutationConfirmsAuthorityAndReplicaThroughHTTP(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	if err := s.storage.Write(t.Context(), "file", []byte("old")); err != nil {
		t.Fatal(err)
	}
	mounted, _ := mount(t, s)
	session := retainedSession(t, mounted)
	if err := session.(storage.OpenContentMetadata).CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	root, err := mounted.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	original, err := mounted.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	effects := []storage.ContentMetadataEffect{{Namespace: "app.flags", PayloadBytes: 3, PresentPrefix: []byte{'M'}, AbsentPayload: []byte{'M', 0, 7}, ClearMask: []byte{0, 1, 0}, SetMask: []byte{0, 2, 0}}}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{
		Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")},
	}, storage.OpenAtOptions{Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, Action: replicatedFileActionFor(t, session), Use: storage.UseClaim{Uses: storage.WriteData}, Existing: storage.Keep, ContentMetadataEffects: effects})
	if err != nil || opened.File == nil {
		t.Fatalf("byte-only enrollment = %+v, %v", opened, err)
	}
	file := opened.File
	observer := file.(storage.ReferenceContentMetadata)
	if err := observer.CheckContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("content enrollment granted public Stat: %v", err)
	}
	if _, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "app.flags", nil, []byte("bad")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("content enrollment granted public metadata mutation: %v", err)
	}
	observed, err := observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observed.NodeID != original.ID || observed.Value != nil {
		t.Fatalf("initial absence = %+v, %v", observed, err)
	}
	effects[0].Namespace = "app.changed"
	effects[0].AbsentPayload[2] = 9
	command := storage.FileMutation{Action: replicatedFileActionFor(t, session), Kind: storage.MutateWriteAt, Data: []byte("new"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"app.flags": nil}}
	changed, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), command)
	if err != nil || changed.ID != original.ID || !bytes.Equal(changed.Metadata["app.flags"].Data, []byte{'M', 2, 7}) {
		t.Fatalf("confirmed derived mutation = %+v, %v", changed, err)
	}
	local, err := mounted.Stat(t.Context(), "file")
	if err != nil || local.Size != 3 || !bytes.Equal(local.Metadata["app.flags"].Data, []byte{'M', 2, 7}) || !bytes.Equal(local.Metadata["app.flags"].Version, changed.Metadata["app.flags"].Version) {
		t.Fatalf("mutation returned before metadata barrier: %+v, %v", local, err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "new" {
		t.Fatalf("authority bytes = %q, %v", content, err)
	}
	observed, err = observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observed.Value == nil {
		t.Fatalf("derived observation = %+v, %v", observed, err)
	}
	if err := s.elsewhere.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.elsewhere.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if err := s.elsewhere.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	command.Action = replicatedFileActionFor(t, session)
	command.Kind = storage.MutateAppend
	command.Data = []byte("!")
	command.ExpectedMetadata["app.flags"] = observed.Value.Version
	changed, err = file.(storage.ConditionalFileMutation).MutateFile(t.Context(), command)
	if err != nil || changed.ID != original.ID || changed.Size != 4 {
		t.Fatalf("detached derived append = %+v, %v", changed, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "replacement" {
		t.Fatalf("detached mutation affected replacement: %q, %v", content, err)
	}
}
