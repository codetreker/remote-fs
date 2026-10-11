package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func sealedEffect() storage.ContentMetadataEffect {
	return storage.ContentMetadataEffect{Namespace: "test.flags", PayloadBytes: 2, PresentPrefix: []byte{0x31}, AbsentPayload: []byte{0x31, 0}, ClearMask: []byte{0, 0x10}, SetMask: []byte{0, 0x20}}
}

func openEffectFile(t *testing.T, session storage.FileSession, nodeID uint64, name string, effect storage.ContentMetadataEffect) storage.File {
	t.Helper()
	result, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: nodeID}, RawLeaf: []byte(name)}}, storage.OpenAtOptions{
		Write: true, Create: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep, Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: storage.WriteData}, ContentMetadataEffects: []storage.ContentMetadataEffect{effect},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := result.File.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return result.File
}

func TestSealedContentEffectsWriteOnlyCASReplayAndOrdinaryWrites(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 1<<20, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	if err := session.(storage.OpenContentMetadata).CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	effect := sealedEffect()
	file := openEffectFile(t, session, root.ID, "file", effect)
	observer := file.(storage.ReferenceContentMetadata)
	if err := observer.CheckContentMetadata(); err != nil {
		t.Fatal(err)
	}
	observation, err := observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observation.Value != nil || observation.NodeID == 0 {
		t.Fatalf("absent observation=%+v,%v", observation, err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write-only Stat=%v", err)
	}
	if _, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), effect.Namespace, nil, []byte{0x31, 0}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write-only metadata=%v", err)
	}
	// Caller mutation after enrollment must not change the transform or format.
	effect.ClearMask[1] = 0xff
	effect.SetMask[1] = 0xff
	effect.PresentPrefix[0] = 0
	mutation := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateWriteAt, Data: []byte("body"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": nil}}
	changed, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), mutation)
	if err != nil || changed.Size != 4 || !bytes.Equal(changed.Metadata["test.flags"].Data, []byte{0x31, 0x20}) {
		t.Fatalf("mutation=%+v,%v", changed, err)
	}
	replayed, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), mutation)
	if err != nil || replayed.ID != changed.ID || !bytes.Equal(replayed.Metadata["test.flags"].Version, changed.Metadata["test.flags"].Version) {
		t.Fatalf("replay=%+v,%v", replayed, err)
	}
	mutation.Data = []byte("other")
	if _, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), mutation); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed action=%v", err)
	}
	observation, err = observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	observedVersion := bytes.Clone(observation.Value.Version)
	metadata := session.(storage.MetadataAccess)
	current, err := metadata.SetMetadata(t.Context(), changed.ID, "test.flags", observedVersion, []byte{0x31, 0xd5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.SetMetadata(t.Context(), changed.ID, "test.other", nil, []byte("other")); err != nil {
		t.Fatal(err)
	}
	stale := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateWriteAt, Data: []byte("stale"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": observedVersion}}
	if _, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), stale); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale mutation=%v", err)
	}
	body, err := volume.Read(t.Context(), "file")
	if err != nil || string(body) != "body" {
		t.Fatalf("CAS changed body=%q,%v", body, err)
	}
	appendCommand := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateAppend, Data: []byte("!"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": current.Version}}
	appended, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), appendCommand)
	if err != nil || appended.Size != 5 || !bytes.Equal(appended.Metadata["test.flags"].Data, []byte{0x31, 0xe5}) || string(appended.Metadata["test.other"].Data) != "other" {
		t.Fatalf("append=%+v,%v", appended, err)
	}
	after, err := file.WriteAt(t.Context(), 0, []byte("B"))
	if err != nil || !bytes.Equal(after.Metadata["test.flags"].Version, appended.Metadata["test.flags"].Version) {
		t.Fatalf("ordinary write changed effect=%+v,%v", after, err)
	}
	observation.Value.Data[0] = 0
	fresh, err := observer.ObserveContentMetadata(t.Context(), 0)
	if err != nil || fresh.Value.Data[0] != 0x31 {
		t.Fatalf("observation alias=%+v,%v", fresh, err)
	}
	if _, err := observer.ObserveContentMetadata(t.Context(), 1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid index=%v", err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := observer.ObserveContentMetadata(t.Context(), 0); err == nil {
		t.Fatal("closed reference observed metadata")
	}
}
