package replicated_test

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	"syscall"
	"testing"
)

func TestReferenceStateAndMetadataObservationConfirmAuthorityThroughHTTP(t *testing.T) {
	served := serve(t, httprest.DefaultLimits())
	backend := served.storage
	if err := backend.Write(t.Context(), "file", []byte("old")); err != nil {
		t.Fatal(err)
	}
	mounted, _ := mount(t, served)
	session := retainedSession(t, mounted)
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
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
	action := func() storage.FileActionID {
		a, e := storage.NewFileActionID(status.ActionEpoch)
		if e != nil {
			t.Fatal(e)
		}
		return a
	}
	observationRequest := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "app.flags", MaxPayloadBytes: 1}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, storage.OpenAtOptions{Read: true, Use: storage.UseClaim{Uses: storage.ReadData}, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, Action: action(), Existing: storage.Keep, MetadataAccess: storage.WriteMetadata})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.File
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only Stat = %v", err)
	}
	observed, err := file.(storage.ReferenceMetadataAccess).ObserveMetadata(t.Context(), observationRequest)
	if err != nil || observed.NodeID != original.ID || observed.Value != nil {
		t.Fatalf("absence=%+v %v", observed, err)
	}
	mutated, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), storage.FileMutation{Action: action(), Kind: storage.MutateAttributes, Metadata: map[string]storage.OpaquePayload{"app.flags": {Data: []byte{9}}}, ExpectedMetadata: map[string][]byte{"app.flags": nil}})
	if err != nil || mutated.ID != original.ID {
		t.Fatalf("mutation=%+v %v", mutated, err)
	}
	observed, err = file.(storage.ReferenceMetadataAccess).ObserveMetadata(t.Context(), observationRequest)
	if err != nil || observed.Value == nil || len(observed.Value.Data) != 1 || observed.Value.Data[0] != 9 {
		t.Fatalf("bounded value=%+v %v", observed, err)
	}

	effects := []storage.ContentMetadataEffect{{Namespace: "app.content", PayloadBytes: 1, AbsentPayload: []byte{0}, ClearMask: []byte{0}, SetMask: []byte{1}}}
	writer, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, storage.OpenAtOptions{Write: true, Use: storage.UseClaim{Uses: storage.WriteData}, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, Action: action(), Existing: storage.Keep, ContentMetadataEffects: effects})
	if err != nil {
		t.Fatal(err)
	}
	truncated, err := writer.File.(storage.ConditionalFileMutation).MutateFile(t.Context(), storage.FileMutation{Action: action(), Kind: storage.MutateTruncate, Size: 5, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"app.content": nil}})
	if err != nil || truncated.ID != original.ID || truncated.Size != 5 || truncated.Metadata["app.content"].Data[0] != 1 {
		t.Fatalf("sealed truncate=%+v %v", truncated, err)
	}
	if _, err := writer.File.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("sealed truncate granted Stat: %v", err)
	}
	content, err := backend.Read(t.Context(), "file")
	if err != nil || string(content) != "old\x00\x00" {
		t.Fatalf("extended content=%q %v", content, err)
	}
	if err := served.elsewhere.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := served.elsewhere.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	info, err := file.(storage.ReferenceStateAccess).State(t.Context(), storage.StateScalars)
	if err != nil || info.Attr.ID != original.ID || info.Attr.Size != 5 || !info.Detached {
		t.Fatalf("detached scalar=%+v %v", info, err)
	}
	observed, err = file.(storage.ReferenceMetadataAccess).ObserveMetadata(t.Context(), observationRequest)
	if err != nil || observed.NodeID != original.ID || observed.Value == nil || observed.Value.Data[0] != 9 {
		t.Fatalf("detached metadata=%+v %v", observed, err)
	}
	for _, child := range []bool{false, true} {
		options := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: root.ID}, Action: action(), MetadataAccess: storage.WriteMetadata}
		var result storage.NodeOpenResult
		if child {
			options.Kind = storage.NodeRegular
			replacement, e := backend.Stat(t.Context(), "file")
			if e != nil {
				t.Fatal(e)
			}
			options.Target.NodeID = replacement.ID
			result, err = session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, options)
		} else {
			result, err = session.(storage.NodeReferences).OpenNodeRef(t.Context(), root.ID, options)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatal(err)
		}
		if err := result.Reference.(storage.ReferenceStateAccess).CheckReferenceState(); err != nil {
			t.Fatal(err)
		}
		if err := result.Reference.(storage.ReferenceMetadataAccess).CheckMetadataAccess(); err != nil {
			t.Fatal(err)
		}
		if _, err := result.Reference.(storage.ReferenceStateAccess).State(t.Context(), storage.StateScalars); err != nil {
			t.Fatal(err)
		}
		if _, err := result.Reference.(storage.ReferenceMetadataAccess).ObserveMetadata(t.Context(), observationRequest); err != nil {
			t.Fatal(err)
		}
	}
}
