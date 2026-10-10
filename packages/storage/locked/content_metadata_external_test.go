package locked_test

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestContentMetadataSurvivesLockedAllowanceChainOnByteOnlyUnlinkedFile(t *testing.T) {
	backend := pairedBackend(t)
	if err := backend.Write(t.Context(), "file", []byte("old")); err != nil {
		t.Fatal(err)
	}
	inner, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	allowance, err := limited.New(t.Context(), inner, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := locked.New(allowance)
	if err != nil {
		t.Fatal(err)
	}
	session, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := session.(storage.OpenContentMetadata).CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	original, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	nextAction := func() storage.FileActionID {
		t.Helper()
		action, err := storage.NewFileActionID(status.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		return action
	}
	effects := []storage.ContentMetadataEffect{{Namespace: "app.flags", PayloadBytes: 3, PresentPrefix: []byte{'M'}, AbsentPayload: []byte{'M', 0, 7}, ClearMask: []byte{0, 1, 0}, SetMask: []byte{0, 2, 0}}}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{
		Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")},
	}, storage.OpenAtOptions{Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, Action: nextAction(), Use: storage.UseClaim{Uses: storage.WriteData}, Existing: storage.Keep, ContentMetadataEffects: effects})
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
	effects[0].SetMask[1] = 0xff
	mutator := file.(storage.ConditionalFileMutation)
	command := storage.FileMutation{Action: nextAction(), Kind: storage.MutateWriteAt, Data: []byte("new"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"app.flags": nil}}
	changed, err := mutator.MutateFile(t.Context(), command)
	if err != nil || changed.ID != original.ID || !bytes.Equal(changed.Metadata["app.flags"].Data, []byte{'M', 2, 7}) || changed.AllocationKnown {
		t.Fatalf("confirmed derived mutation = %+v, %v", changed, err)
	}
	if content, err := backend.Read(t.Context(), "file"); err != nil || string(content) != "new" {
		t.Fatalf("confirmed bytes = %q, %v", content, err)
	}
	observed, err = observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observed.Value == nil {
		t.Fatalf("derived observation = %+v, %v", observed, err)
	}
	command.Action = nextAction()
	command.Data = bytes.Repeat([]byte{'X'}, limited.MinLimit+1)
	command.ExpectedMetadata["app.flags"] = observed.Value.Version
	if _, err := mutator.MutateFile(t.Context(), command); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("allowance did not reject oversize effect write: %v", err)
	}
	if after, err := observer.ObserveContentMetadata(t.Context(), 0); err != nil || after.Value == nil || !bytes.Equal(after.Value.Version, observed.Value.Version) {
		t.Fatalf("refused write changed metadata = %+v, %v", after, err)
	}
	if err := volume.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if err := volume.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	command.Action = nextAction()
	command.Kind = storage.MutateAppend
	command.Data = []byte("!")
	changed, err = mutator.MutateFile(t.Context(), command)
	if err != nil || changed.ID != original.ID || changed.Size != 4 {
		t.Fatalf("detached effect append = %+v, %v", changed, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if content, err := backend.Read(t.Context(), "file"); err != nil || string(content) != "replacement" {
		t.Fatalf("detached mutation affected replacement: %q, %v", content, err)
	}
}
