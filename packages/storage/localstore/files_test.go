package localstore_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	session, _, err := quota.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file := createRetained(t, store, session, "file", storage.NodeRegular, nil)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("retained")}, storeAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := quota.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if used, err := store.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("live retained usage = %d, %v", used, err)
	}
	closeAction := storeAction(t, session)
	if err := store.Close(); err != nil {
		t.Fatalf("closing store with retained files: %v", err)
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed store reference returned %v", err)
	}
	if _, err := session.Close(t.Context(), closeAction); err != nil {
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
	session, _, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file := createRetained(t, store, session, "file", storage.NodeRegular, nil)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("retained")}, storeAction(t, session)); err != nil {
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

func storeAction(t *testing.T, session storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}
func createRetained(t *testing.T, store storage.FileStorage, session storage.FileSession, name string, kind storage.NodeKind, prepared *storage.RemovalCondition) storage.File {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, storeAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: storage.EntryTarget{Parent: retained.Reference, ParentID: root.ID, Name: []byte(name), DirectoryRevision: root.DirectoryRevision, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: root.ID, NodeID: root.ID}}, Initial: storage.NodeInitial{Kind: kind}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}, Prepared: prepared}, storeAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestStoreRetainsDirectoryIdentityAndVolumeIdentityAcrossReopen(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	state, err := store.FileState(t.Context())
	if err != nil || state.VolumeIdentity == "" {
		t.Fatalf("volume state = %+v, %v", state, err)
	}
	session, _, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	directory := createRetained(t, store, session, "dir", storage.NodeDirectory, nil)
	observed, err := directory.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "application", Version: 1, Data: []byte("retained child")}}
	created, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: storage.EntryTarget{Parent: directory.Reference(), ParentID: observed.Attr.ID, Name: []byte("file"), DirectoryRevision: observed.Attr.DirectoryRevision, Witness: observed.Location}, Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: metadata}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, storeAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	child, err := session.Reference(t.Context(), created.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "dir", "moved"); err != nil {
		t.Fatal(err)
	}
	renamed, err := directory.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || renamed.Attr.ID != observed.Attr.ID || renamed.Attr.Kind != storage.NodeDirectory || string(renamed.Location.Ancestors[0].Name) != "moved" {
		t.Fatalf("retained renamed directory = %+v, %v", renamed, err)
	}
	childState, err := child.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || childState.Attr.ID != created.Observation.Attr.ID || len(childState.Location.Ancestors) != 2 || string(childState.Location.Ancestors[0].Name) != "moved" {
		t.Fatalf("retained child = %+v, %v", childState, err)
	}
	page, err := directory.ListAt(t.Context(), storage.DirectoryPageRequest{Revision: renamed.Attr.DirectoryRevision, MaxEntries: 2, MaxBytes: 4096})
	if err != nil || !page.Done || len(page.Entries) != 1 || string(page.Entries[0].Name) != "file" || !reflect.DeepEqual(page.Entries[0].Attr.Metadata, metadata) || page.Entries[0].Attr.ID != childState.Attr.ID {
		t.Fatalf("retained directory page = %+v, %v", page, err)
	}
	if err := store.Remove(t.Context(), "moved/file"); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveDir(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	detached, err := directory.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || detached.Attr.ID != observed.Attr.ID || detached.Location.State != storage.LocationDetached || len(detached.Location.Ancestors) != 0 {
		t.Fatalf("detached directory = %+v, %v", detached, err)
	}
	empty, err := directory.ListAt(t.Context(), storage.DirectoryPageRequest{Revision: detached.Attr.DirectoryRevision, MaxEntries: 1, MaxBytes: 4096})
	if err != nil || !empty.Done || len(empty.Entries) != 0 {
		t.Fatalf("detached directory page = %+v, %v", empty, err)
	}
	if err := store.Mkdir(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.Stat(t.Context(), "moved")
	if err != nil || replacement.ID == observed.Attr.ID {
		t.Fatalf("replacement identity = %+v, %v", replacement, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed store retained directory = %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	restored, err := reopened.FileState(t.Context())
	if err != nil || restored.VolumeIdentity != state.VolumeIdentity || restored.RootID != state.RootID {
		t.Fatalf("reopened volume state = %+v, %v", restored, err)
	}
	got, err := reopened.Stat(t.Context(), "moved")
	if err != nil || got.ID != replacement.ID {
		t.Fatalf("reopened replacement = %+v, %v", got, err)
	}
}

func TestStorePreparedRemovalRetainsOwnershipAcrossCancelledCreator(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	failure := errors.New("injected prepared retirement cleanup refusal")
	var reject atomic.Bool
	reject.Store(true)
	t.Cleanup(func() { reject.Store(false); closeStore(t, store) })
	creation, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := storage.WithPublicationAccounting(creation, func(previous, next int64) (storage.PublicationSettlement, error) {
		if reject.Load() && previous > 0 && next == 0 {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	options := storage.DefaultFileSessionOptions()
	options.Lease = 5 * time.Second
	session, _, err := store.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	file := createRetained(t, store, session, "file", storage.NodeRegular, nil)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("retained")}, storeAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	leaf := observed.Location.Ancestors[len(observed.Location.Ancestors)-1]
	if _, err := file.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: leaf, Witness: *observed.Location, Condition: storage.RemovalFile}, storeAction(t, session)); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := store.Close(); !errors.Is(err, failure) {
		t.Fatalf("prepared cleanup failure = %v", err)
	}
	contender, err := localstore.Open(t.Context(), config)
	if err == nil {
		contender.Close()
		t.Fatal("failed prepared cleanup released store ownership")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retained ownership = %v", err)
	}
	reject.Store(false)
	if err := store.Close(); err != nil {
		t.Fatalf("retry prepared cleanup: %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if used, err := reopened.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("post-retirement usage = %d, %v", used, err)
	}
	if _, err := reopened.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("prepared entry survived: %v", err)
	}
}
