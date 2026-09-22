package objectstore_test

import (
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestIdentityNamespaceAndReferenceCapabilities(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	if err := session.(storage.FileActions).CheckFileActions(); err != nil {
		t.Fatal(err)
	}
	namespace := session.(storage.NamespaceAccess)
	if err := namespace.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	fileName := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}
	created, err := namespace.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameCreate, Action: fileActionFor(t, session), Name: fileName,
		Target: storage.ChildCondition{State: storage.Absent},
	})
	if err != nil || created.Attr == nil || created.Attr.Kind != storage.NodeRegular {
		t.Fatalf("create=%+v error=%v", created, err)
	}
	lookedUp, err := namespace.LookupAt(t.Context(), fileName)
	if err != nil || lookedUp.ID != created.Attr.ID {
		t.Fatalf("lookup=%+v error=%v", lookedUp, err)
	}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: fileName}, storage.OpenAtOptions{
		Read: true, Write: true, Existing: storage.Keep, Action: fileActionFor(t, session),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: lookedUp.ID},
		Use:    storage.UseClaim{Uses: storage.ReadData | storage.WriteData},
	})
	if err != nil || opened.File == nil {
		t.Fatalf("open=%+v error=%v", opened, err)
	}
	fileState, err := opened.File.(storage.ReferenceStateAccess).State(t.Context())
	if err != nil || fileState.Attr.ID != lookedUp.ID || fileState.Detached {
		t.Fatalf("file state=%+v error=%v", fileState, err)
	}

	directoryName := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("directory")}
	directory, err := namespace.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameMkdir, Action: fileActionFor(t, session), Name: directoryName,
		Target: storage.ChildCondition{State: storage.Absent},
	})
	if err != nil || directory.Attr == nil || directory.Attr.Kind != storage.NodeDirectory {
		t.Fatalf("mkdir=%+v error=%v", directory, err)
	}
	referenceResult, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: directoryName}, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Action: fileActionFor(t, session),
		Target:         storage.ChildCondition{State: storage.SameNode, NodeID: directory.Attr.ID},
		MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
	})
	if err != nil || referenceResult.Reference == nil {
		t.Fatalf("open directory reference=%+v error=%v", referenceResult, err)
	}
	reference := referenceResult.Reference
	modified := time.Unix(7_654, 321).UTC()
	changed, err := reference.SetAttr(t.Context(), storage.AttrChange{ModTime: &modified})
	if err != nil || changed.ID != directory.Attr.ID || !changed.ModTime.Equal(modified) {
		t.Fatalf("setattr=%+v error=%v", changed, err)
	}
	metadata := reference.(storage.ReferenceMetadataAccess)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	payload, err := metadata.SetMetadata(t.Context(), "test.coverage", nil, []byte("value"))
	if err != nil || string(payload.Data) != "value" || len(payload.Version) == 0 {
		t.Fatalf("metadata=%+v error=%v", payload, err)
	}
	stateAccess := reference.(storage.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	referenceState, err := stateAccess.State(t.Context())
	if err != nil || referenceState.Attr.ID != directory.Attr.ID || string(referenceState.Attr.Metadata["test.coverage"].Data) != "value" {
		t.Fatalf("reference state=%+v error=%v", referenceState, err)
	}
	scope, err := reference.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	owner, err := session.(storage.UseOwners).NewUseOwner(t.Context(), directory.Attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil || owner == 0 {
		t.Fatalf("reference owner=%d error=%v", owner, err)
	}
}

func TestMaintenanceAccountingBindsCurrentUsage(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "charged", []byte("body")); err != nil {
		t.Fatal(err)
	}
	accounting := any(volume).(storage.MaintenanceAccounting)
	if err := accounting.CheckMaintenanceAccounting(); err != nil {
		t.Fatal(err)
	}
	chain := (storage.PublicationAccountingChain{}).With(func(int64, int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error { return nil }, nil
	})
	initialized := int64(-1)
	if err := accounting.BindMaintenanceAccounting(t.Context(), chain, func(used int64) { initialized = used }); err != nil {
		t.Fatal(err)
	}
	if initialized != int64(len("body")) {
		t.Fatalf("initialized usage=%d, want %d", initialized, len("body"))
	}
}
