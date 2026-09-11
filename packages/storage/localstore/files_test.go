package localstore_test

import (
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
