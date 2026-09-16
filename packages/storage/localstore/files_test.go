package localstore_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
)

func TestStoreCloseRetiresRetainedFilesBeforeClosingDurableStorage(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	quota, err := limited.New(t.Context(), store, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	session, err := quota.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := quota.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if used, err := store.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("live retained usage = %d, %v", used, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing store with retained files: %v", err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed store reference returned %v", err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatalf("repeated session close: %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if used, err := reopened.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("reopened retained usage = %d, %v", used, err)
	}
	if _, err := reopened.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("closing a retained file recreated its name: %v", err)
	}
}

func TestStoreCloseKeepsOwnershipUntilRetainedCleanupSucceeds(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	failure := errors.New("injected final reference cleanup refusal")
	var reject atomic.Bool
	reject.Store(true)
	t.Cleanup(func() {
		reject.Store(false)
		closeStore(t, store)
	})
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if reject.Load() && previous > 0 && next == 0 {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	session, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, failure) {
		t.Fatalf("close lost retained cleanup failure: %v", err)
	}
	contender, err := localstore.Open(t.Context(), config)
	if err == nil {
		contender.Close()
		t.Fatal("failed retained cleanup released the local store ownership")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retained ownership returned %v, want EBUSY", err)
	}
	reject.Store(false)
	if err := store.Close(); err != nil {
		t.Fatalf("retrying completed retained cleanup: %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if used, err := reopened.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("reopened usage = %d, %v", used, err)
	}
}

func TestStorePreservesOptionalReferencesAndMaintenanceAccounting(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var liveDelta atomic.Int64
	var liveSettlements atomic.Int64
	liveContext := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if result == storage.PublicationApplied {
				liveDelta.Add(next - previous)
				liveSettlements.Add(1)
			} else if result != storage.PublicationNotApplied {
				return errors.New("live cleanup outcome unknown")
			}
			return nil
		}, nil
	})
	session, err := store.NewFileSession(liveContext, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	references, ok := session.(storage.NodeReferences)
	if !ok {
		t.Fatal("store hid node references")
	}
	if err := references.CheckNodeReferences(); err != nil {
		t.Fatal(err)
	}
	directory, err := references.OpenNodeRef(t.Context(), root.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: root.ID}, Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata})
	if err != nil || directory.Reference == nil || directory.Attr.ID != root.ID {
		t.Fatalf("directory reference = %+v, %v", directory, err)
	}
	if _, ok := directory.Reference.(storage.File); ok {
		t.Fatal("directory reference exposed file bytes")
	}
	scopeAccess := directory.Reference.(storage.ScopedReference)
	if err := scopeAccess.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	scope, err := scopeAccess.Scope(t.Context())
	if err != nil || scope.Token == "" {
		t.Fatalf("directory scope = %+v, %v", scope, err)
	}
	opener := session.(storage.AtomicFileOpener)
	if err := opener.CheckAtomicFileOpen(); err != nil {
		t.Fatal(err)
	}
	opened, err := opener.OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID, Scope: &scope}, RawLeaf: []byte("file")}, storage.OpenAtOptions{Read: true, Write: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}, Existing: storage.Keep, Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"test.tag": {0xff, 0}}}}})
	if err != nil || opened.File == nil || opened.Outcome != storage.Created || !bytes.Equal(opened.Attr.Metadata["test.tag"].Data, []byte{0xff, 0}) {
		t.Fatalf("atomic file = %+v, %v", opened, err)
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	namespace := session.(storage.NamespaceAccess)
	if err := namespace.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	listing, err := namespace.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root.ID, Scope: &scope})
	if err != nil || len(listing.Entries) != 1 || string(listing.Entries[0].RawLeaf) != "file" || listing.Entries[0].Attr.ID != opened.Attr.ID {
		t.Fatalf("scoped directory = %+v, %v", listing, err)
	}
	metadata := opened.File.(storage.ReferenceMetadataAccess)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	updated, err := metadata.SetMetadata(t.Context(), "test.tag", opened.Attr.Metadata["test.tag"].Version, []byte("updated"))
	if err != nil || len(updated.Version) == 0 || string(updated.Data) != "updated" {
		t.Fatalf("reference metadata = %+v, %v", updated, err)
	}
	stateAccess := opened.File.(storage.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	state, err := stateAccess.State(t.Context())
	if err != nil || state.Detached || state.PendingUnlink || state.Attr.Size != 3 {
		t.Fatalf("reference state = %+v, %v", state, err)
	}
	var used atomic.Int64
	hook := storage.PublicationAccounting(func(previous, next int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			switch result {
			case storage.PublicationApplied:
				used.Add(next - previous)
			case storage.PublicationNotApplied:
			default:
				return errors.New("maintenance outcome unknown")
			}
			return nil
		}, nil
	})
	chain := storage.PublicationAccountingFrom(storage.WithPublicationAccounting(context.Background(), hook))
	if err := store.CheckMaintenanceAccounting(); err != nil {
		t.Fatal(err)
	}
	if err := store.BindMaintenanceAccounting(t.Context(), chain, func(value int64) { used.Store(value) }); err != nil {
		t.Fatal(err)
	}
	if used.Load() != 3 {
		t.Fatalf("initial maintenance usage = %d", used.Load())
	}
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if state, err := stateAccess.State(t.Context()); err != nil || !state.Detached || state.Attr.ID != opened.Attr.ID {
		t.Fatalf("detached state = %+v, %v", state, err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if actual, err := store.Usage(t.Context()); err != nil || actual != 0 {
		t.Fatalf("final detached logical usage = %d, %v", actual, err)
	}
	if liveDelta.Load() != -3 || liveSettlements.Load() != 1 {
		t.Fatalf("captured live cleanup delta = %d across %d settlements", liveDelta.Load(), liveSettlements.Load())
	}
	if used.Load() != 3 {
		t.Fatalf("later maintenance binding took ownership of live cleanup: %d", used.Load())
	}
	if err := directory.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
