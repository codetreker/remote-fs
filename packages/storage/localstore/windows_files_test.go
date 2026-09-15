package localstore_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
)

func TestWindowsStoreRetainsDirectoriesAndStableVolumeIdentity(t *testing.T) {
	config := windowsStoreConfig(t)
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	state := enableWindowsStore(t, store)
	session, next := windowsStoreSession(t, store, t.Context())
	root, err := session.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{
		Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll,
		Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory,
	}}, next())
	if err != nil {
		t.Fatal(err)
	}
	directory, err := session.Open(t.Context(), storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsDirectory},
		Lookup:            storage.WindowsLookup{ParentID: root.Attr.ID, ParentReference: root.File.Reference(), Name: "dir"}, Mode: 0o700,
	}, next())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Open(t.Context(), storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsRegularFile},
		Lookup:            storage.WindowsLookup{ParentID: directory.Attr.ID, ParentReference: directory.File.Reference(), Name: "file"}, Mode: 0o600, DOSAttributes: storage.WindowsDOSHidden,
	}, next())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "dir", "moved"); err != nil {
		t.Fatal(err)
	}
	dirAttr, err := directory.File.Stat(t.Context())
	if err != nil || dirAttr.ID != directory.Attr.ID || !dirAttr.Mode.IsDir() || dirAttr.NameInfo.Path != "moved" {
		t.Fatalf("retained renamed directory = %+v, %v", dirAttr, err)
	}
	fileAttr, err := file.File.Stat(t.Context())
	if err != nil || fileAttr.ID != file.Attr.ID || fileAttr.NameInfo.Path != "moved/file" {
		t.Fatalf("retained child name = %+v, %v", fileAttr, err)
	}
	listing, err := storage.NewWindowsListResult(4096, 0, func(_ int, nameBytes int64, _ storage.WindowsBasicAttr) (int64, error) { return nameBytes + 128, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.File.ListBounded(t.Context(), listing); err != nil {
		t.Fatal(err)
	}
	entries, err := listing.Entries()
	want := fileAttr.WindowsBasicAttr
	want.AccessTime, want.ModTime = want.AccessTime.UTC(), want.ModTime.UTC()
	want.CreationTime, want.ChangeTime = want.CreationTime.UTC(), want.ChangeTime.UTC()
	if err != nil || len(entries) != 1 || entries[0].Name != "file" || !reflect.DeepEqual(entries[0].Attr, want) {
		t.Fatalf("retained directory listing = %+v, %v; want child metadata %+v", entries, err, want)
	}
	if err := store.Remove(t.Context(), "moved/file"); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveDir(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	detached, err := directory.File.Stat(t.Context())
	if err != nil || detached.ID != directory.Attr.ID || detached.NameInfo.State != storage.WindowsNameDetached || detached.NameInfo.Path != "" {
		t.Fatalf("retained detached directory = %+v, %v", detached, err)
	}
	empty, err := storage.NewWindowsListResult(0, 0, func(_ int, nameBytes int64, _ storage.WindowsBasicAttr) (int64, error) { return nameBytes + 128, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.File.ListBounded(t.Context(), empty); err != nil {
		t.Fatalf("listing a detached directory: %v", err)
	}
	if entries, err := empty.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("detached directory listing = %+v, %v", entries, err)
	}
	if err := store.Mkdir(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.Stat(t.Context(), "moved")
	if err != nil || replacement.ID == directory.Attr.ID {
		t.Fatalf("replacement reused directory identity: %+v, %v", replacement, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.File.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed store retained directory = %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	restored, err := windowsStoreRecoveredState(t, reopened)
	if err != nil || !restored.Enabled || restored.VolumeIdentity != state.VolumeIdentity || restored.VolumeSerial != state.VolumeSerial {
		t.Fatalf("reopened Windows state = %+v, %v; original %+v", restored, err, state)
	}
	got, err := reopened.Stat(t.Context(), "moved")
	if err != nil || got.ID != replacement.ID {
		t.Fatalf("reopened replacement = %+v, %v", got, err)
	}
}

func enableWindowsStore(t *testing.T, store *localstore.Store) storage.WindowsState {
	t.Helper()
	if err := store.CheckWindowsStorage(); err != nil {
		t.Fatal(err)
	}
	state, err := store.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action := windowsStoreAction(t, state.ActionEpoch)
	activation, err := store.EnableWindows(t.Context(), action)
	if err != nil || !activation.Enabled {
		t.Fatalf("Windows activation = %+v, %v", activation, err)
	}
	queried, err := store.QueryWindowsActivation(t.Context(), action)
	if err != nil || queried.Action != action || queried.State != activation.State || !queried.Enabled {
		t.Fatalf("Windows activation query = %+v, %v", queried, err)
	}
	state, err = store.WindowsState(t.Context())
	if err != nil || !state.Enabled || state.VolumeIdentity == "" {
		t.Fatalf("Windows state = %+v, %v", state, err)
	}
	return state
}

func windowsStoreAction(t *testing.T, epoch uint64) storage.WindowsActionID {
	t.Helper()
	action, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func windowsStoreSession(t *testing.T, store *localstore.Store, ctx context.Context) (storage.WindowsSession, func() storage.WindowsActionID) {
	t.Helper()
	options := storage.DefaultFileSessionOptions()
	options.Lease = 5 * time.Second
	session, err := store.NewWindowsSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return session, func() storage.WindowsActionID { return windowsStoreAction(t, status.ActionEpoch) }
}

func TestWindowsStoreCloseKeepsOwnershipUntilCleanupSucceeds(t *testing.T) {
	config := windowsStoreConfig(t)
	store := open(t, config)
	enableWindowsStore(t, store)
	failure := errors.New("injected Windows cleanup refusal")
	var reject atomic.Bool
	reject.Store(true)
	t.Cleanup(func() {
		reject.Store(false)
		closeStore(t, store)
	})
	creation, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := storage.WithPublicationAccounting(creation, func(previous, next int64) (storage.PublicationSettlement, error) {
		if reject.Load() && previous > 0 && next == 0 {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	session, next := windowsStoreSession(t, store, ctx)
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsRegularFile, DeleteOnClose: true},
		Lookup:            storage.WindowsLookup{ParentID: root.ID, Name: "file"}, Mode: 0o600,
	}, next())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("retained"), next()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := store.Close(); !errors.Is(err, failure) {
		t.Fatalf("Windows cleanup failure = %v", err)
	}
	contender, err := localstore.Open(t.Context(), config)
	if err == nil {
		contender.Close()
		t.Fatal("failed Windows cleanup released store ownership")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retained ownership = %v, want EBUSY", err)
	}
	reject.Store(false)
	if err := store.Close(); err != nil {
		t.Fatalf("retry Windows cleanup: %v", err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if used, err := reopened.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("usage after Windows cleanup = %d, %v", used, err)
	}
	if _, err := reopened.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("delete-on-close name survived: %v", err)
	}
}

func windowsStoreConfig(t *testing.T) localstore.Config {
	t.Helper()
	config := testConfig(privateRoot(t))
	options := locking.DefaultOptions()
	options.MaxLease = time.Millisecond
	config.Locks, config.InitializeLocks = &options, true
	return config
}

func windowsStoreRecoveredState(t *testing.T, store *localstore.Store) (storage.WindowsState, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := store.WindowsState(ctx)
		if !errors.Is(err, syscall.EAGAIN) {
			return state, err
		}
		select {
		case <-ctx.Done():
			return storage.WindowsState{}, ctx.Err()
		case <-ticker.C:
		}
	}
}
