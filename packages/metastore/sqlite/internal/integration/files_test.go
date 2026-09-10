package integration_test

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

func ownedRetainedStore(t *testing.T, allowance int64, options sqlite.Options) (*sqlite.Store, string) {
	t.Helper()
	path := database(t)
	owned, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: path, Volume: "workspace", Allowance: allowance,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owned.Close(); err != nil {
			t.Error(err)
		}
	})
	return owned.Store, path
}

func retainedOpen(t *testing.T, store *sqlite.Store, path string, options storage.FileOpenOptions) metastore.File {
	t.Helper()
	file, err := store.OpenFile(t.Context(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return file
}

func retainedState(t *testing.T, file metastore.File) metastore.FileState {
	t.Helper()
	state, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func retainedPublish(t *testing.T, file metastore.File, size int64) metastore.FileState {
	t.Helper()
	before := retainedState(t, file)
	object := metastore.Object{Size: size, ModTime: time.Now()}
	if size != 0 {
		var err error
		object.Key, err = file.Reserve(t.Context(), size)
		if err != nil {
			t.Fatal(err)
		}
	}
	state, err := file.Commit(t.Context(), before.Revision, object)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func assertRetainedUsage(t *testing.T, store *sqlite.Store, want int64) {
	t.Helper()
	used, err := store.Usage(t.Context())
	if err != nil || used != want {
		t.Fatalf("usage = %d, %v; want %d", used, err, want)
	}
	space, err := store.Space(t.Context())
	if err != nil || space.Used != want {
		t.Fatalf("space = %+v, %v; want %d used bytes", space, err, want)
	}
}

func TestNativeRetainedIdentitySurvivesRenameUnlinkAndNameReuse(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	commit(t, store, "original", 5)
	file := retainedOpen(t, store, "original", storage.FileOpenOptions{Read: true, Write: true})
	initial := retainedState(t, file)
	if err := store.Rename(t.Context(), "original", "moved"); err != nil {
		t.Fatal(err)
	}
	if got := retainedState(t, file); got.ID != initial.ID || got.Revision != initial.Revision || got.Detached {
		t.Fatalf("renamed reference = %+v; initial %+v", got, initial)
	}
	retainedPublish(t, file, 7)
	if named, err := store.Stat(t.Context(), "moved"); err != nil || named.ID != initial.ID || named.Size != 7 {
		t.Fatalf("renamed publication = %+v, %v", named, err)
	}
	if err := store.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	commit(t, store, "moved", 3)
	retainedPublish(t, file, 9)
	mode := fs.FileMode(0o600)
	state, err := file.SetAttr(t.Context(), storage.AttrChange{Mode: &mode})
	if err != nil || state.ID != initial.ID || !state.Detached || state.Size != 9 || state.Mode.Perm() != mode {
		t.Fatalf("unnamed retained attributes = %+v, %v", state, err)
	}
	if named, err := store.Stat(t.Context(), "moved"); err != nil || named.ID == initial.ID || named.Size != 3 || named.Mode.Perm() != 0o644 {
		t.Fatalf("replacement changed with detached reference: %+v, %v", named, err)
	}
	assertRetainedUsage(t, store, 12)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 3)
	if _, err := store.StatNode(t.Context(), uint64(initial.ID)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("last-closed old identity: %v; want ESTALE", err)
	}
}

func TestNativeRetainedRenameReplacementKeepsBothIdentities(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	commit(t, store, "target", 5)
	commit(t, store, "source", 7)
	displaced := retainedOpen(t, store, "target", storage.FileOpenOptions{Read: true, Write: true})
	moved := retainedOpen(t, store, "source", storage.FileOpenOptions{Read: true})
	displacedID, movedID := retainedState(t, displaced).ID, retainedState(t, moved).ID
	if err := store.Rename(t.Context(), "source", "target"); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 12)
	if state := retainedState(t, displaced); state.ID != displacedID || !state.Detached || state.Size != 5 {
		t.Fatalf("displaced reference = %+v", state)
	}
	if state := retainedState(t, moved); state.ID != movedID || state.Detached || state.Size != 7 {
		t.Fatalf("moved reference = %+v", state)
	}
	retainedPublish(t, displaced, 11)
	if named, err := store.Stat(t.Context(), "target"); err != nil || named.ID != movedID || named.Size != 7 {
		t.Fatalf("named target = %+v, %v", named, err)
	}
	assertRetainedUsage(t, store, 18)
	if err := displaced.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 7)
}

func TestNativeRetainedLastPinUsesCurrentDetachedSize(t *testing.T) {
	store, _ := ownedRetainedStore(t, 20, sqlite.DefaultOptions())
	commit(t, store, "file", 5)
	first := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true})
	second := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true})
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	published := retainedPublish(t, first, 13)
	if observed := retainedState(t, second); observed.Size != 13 || observed.Content != published.Content || observed.Revision != published.Revision {
		t.Fatalf("second reference did not observe current publication: %+v; want %+v", observed, published)
	}
	assertRetainedUsage(t, store, 13)
	if _, err := store.Reserve(t.Context(), "new-file", 8); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("new file ignored retained quota: %v; want EDQUOT", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 13)
	retainedPublish(t, second, 4)
	assertRetainedUsage(t, store, 4)
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 0)
	if err := second.Close(t.Context()); err != nil {
		t.Fatalf("idempotent last close: %v", err)
	}
	assertRetainedUsage(t, store, 0)
}

func TestNativeRetainedRevisionIsPerNodeAndRejectsEmptyABA(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	file := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600})
	initial := retainedState(t, file)
	empty := retainedPublish(t, file, 0)
	if empty.Revision != initial.Revision+1 || empty.Content != "" || empty.Size != 0 {
		t.Fatalf("empty publication = %+v; initial %+v", empty, initial)
	}
	if _, err := file.Commit(t.Context(), initial.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("empty ABA commit = %v; want EAGAIN", err)
	}
	commit(t, store, "other", 3)
	mode := fs.FileMode(0o640)
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "file", "renamed"); err != nil {
		t.Fatal(err)
	}
	state, err := file.Commit(t.Context(), empty.Revision, metastore.Object{ModTime: time.Now()})
	if err != nil || state.Revision != empty.Revision+1 {
		t.Fatalf("unrelated and metadata changes invalidated content revision: %+v, %v", state, err)
	}
}

func TestNativeRetainedRevisionExhaustionPreservesState(t *testing.T) {
	store, path := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	file := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600})
	before := retainedPublish(t, file, 5)
	damageDatabase(t, path, `UPDATE nodes SET content_revision = ? WHERE id = ?`, int64(math.MaxInt64), before.ID)
	exhausted := retainedState(t, file)
	if _, err := file.Commit(t.Context(), exhausted.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("exhausted revision commit = %v; want EOVERFLOW", err)
	}
	if after := retainedState(t, file); after.Revision != exhausted.Revision || after.Size != before.Size || after.Content != before.Content {
		t.Fatalf("exhausted commit changed state: %+v; before %+v", after, before)
	}
	assertRetainedUsage(t, store, 5)
}

func TestNativeRetainedCreateOpenChecksIdentityModeAccessAndTruncate(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	file := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true, Create: true, Exclusive: true, Mode: 0o600})
	created := retainedState(t, file)
	if created.Mode.Perm() != 0o600 || created.Size != 0 || created.Revision != 1 {
		t.Fatalf("create/open initial state = %+v", created)
	}
	before := retainedPublish(t, file, 5)
	readOnly := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Create: true, Mode: 0o777})
	if opened := retainedState(t, readOnly); opened.ID != before.ID || opened.Mode.Perm() != 0o600 || opened.Revision != before.Revision {
		t.Fatalf("nonexclusive existing open = %+v; before %+v", opened, before)
	}
	if _, err := readOnly.Reserve(t.Context(), 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only reserve = %v; want EBADF", err)
	}
	if _, err := readOnly.Commit(t.Context(), before.Revision, metastore.Object{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only commit = %v; want EBADF", err)
	}
	for _, test := range []struct {
		name    string
		options storage.FileOpenOptions
		want    error
	}{
		{"exclusive exists", storage.FileOpenOptions{Read: true, Create: true, Exclusive: true}, syscall.EEXIST},
		{"wrong identity", storage.FileOpenOptions{Read: true, ExpectedID: uint64(before.ID + 1)}, syscall.ESTALE},
		{"no access", storage.FileOpenOptions{}, syscall.EINVAL},
		{"read-only truncate", storage.FileOpenOptions{Read: true, Truncate: true}, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened, err := store.OpenFile(t.Context(), "file", test.options)
			if opened != nil {
				if closeErr := opened.Close(t.Context()); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("open = %v; want %v", err, test.want)
			}
		})
	}
	truncated := retainedOpen(t, store, "file", storage.FileOpenOptions{
		Read: true, Write: true, Create: true, Truncate: true, ExpectedID: uint64(before.ID), Mode: 0o777,
	})
	if after := retainedState(t, truncated); after.ID != before.ID || after.Mode.Perm() != 0o600 || after.Size != 0 || after.Revision != before.Revision+1 {
		t.Fatalf("atomic open/truncate state = %+v; before %+v", after, before)
	}
	assertRetainedUsage(t, store, 0)
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if opened, err := store.OpenFile(t.Context(), "directory", storage.FileOpenOptions{Read: true}); !errors.Is(err, syscall.EISDIR) {
		if opened != nil {
			if closeErr := opened.Close(t.Context()); closeErr != nil {
				t.Error(closeErr)
			}
		}
		t.Fatalf("directory file open = %v; want EISDIR", err)
	}
}

func TestNativeRetainedFinalCloseIgnoresPendingAdmission(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.ObjectLimits.MaxPendingObjects = 1
	store, _ := ownedRetainedStore(t, 100, options)
	file := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600})
	retainedPublish(t, file, 5)
	if _, err := store.Reserve(t.Context(), "pending", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 5)
	if err := file.Close(t.Context()); err != nil {
		t.Fatalf("last close at full pending admission: %v", err)
	}
	assertRetainedUsage(t, store, 0)
	status, err := store.ObjectStatus(t.Context())
	if err != nil || !status.OverLimit || status.ReservedCount != 1 || status.GarbageCount != 1 || status.GarbageBytes != 5 {
		t.Fatalf("last-close pending state = %+v, %v", status, err)
	}
}

func TestNativeRetainedAdmissionRequiresPhysicalRelease(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.MaxRetainedFiles = 1
	store, _ := ownedRetainedStore(t, 100, options)
	file := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, Create: true, Mode: 0o600})
	id := retainedState(t, file).ID
	for _, retired := range []bool{false, true} {
		if retired {
			if err := file.Retire(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		opened, err := store.OpenFile(t.Context(), "overflow", storage.FileOpenOptions{Read: true, Create: true, Mode: 0o600})
		if opened != nil {
			if closeErr := opened.Close(t.Context()); closeErr != nil {
				t.Error(closeErr)
			}
		}
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("retired=%v capacity admission = %v; want EAGAIN", retired, err)
		}
		if _, err := store.Stat(t.Context(), "overflow"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("failed capacity admission created a name: %v", err)
		}
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened := retainedOpen(t, store, "file", storage.FileOpenOptions{Read: true, ExpectedID: uint64(id)})
	if got := retainedState(t, reopened); got.ID != id {
		t.Fatalf("admission after release returned %+v; want identity %d", got, id)
	}
}

func detachRecoveryFiles(t *testing.T, f objectIntegrityFixture) {
	t.Helper()
	detachIntegrityFile(t, f)
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE content = ?`, f.foreign)
	damageDatabase(t, f.path, `DELETE FROM entries WHERE node = (SELECT id FROM nodes WHERE content = ?)`, f.foreign)
}

func TestExclusiveOwnerReapsEveryVolumeAtFullPendingAdmission(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	options := sqlite.DefaultOptions()
	options.ObjectLimits.MaxPendingObjects = 1
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatalf("exclusive retained-file recovery: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	status, err := store.ObjectStatus(t.Context())
	if err != nil || !status.OverLimit || status.GarbageCount != 2 || status.GarbageBytes != 40 {
		t.Fatalf("recovered pending objects = %+v, %v", status, err)
	}
	db := raw(t, f.path)
	defer db.Close()
	var detached, used, retired int64
	if err := db.QueryRow(`SELECT
		(SELECT count(*) FROM nodes WHERE detached = 1),
		(SELECT sum(used) FROM volumes),
		(SELECT count(*) FROM objects WHERE key IN (?, ?) AND state = 2)`, f.live, f.foreign).Scan(
		&detached, &used, &retired); err != nil {
		t.Fatal(err)
	}
	if detached != 0 || used != 0 || retired != 2 {
		t.Fatalf("recovery retained=%d used=%d retired=%d; want 0, 0, 2", detached, used, retired)
	}
}

func TestExclusiveOwnerRefusesCorruptRetainedGraphBeforeReaping(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `UPDATE volumes SET used = 0 WHERE name = 'neighbour'`)
	before := historicalLeaseRows(t, f.path)
	beforeSchema := schemaOf(t, f.path)
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("exclusive recovery with corrupt foreign retained quota: %v; want EIO", err)
	}
	assertHistoricalLeaseRows(t, f.path, before)
	if after := schemaOf(t, f.path); after != beforeSchema {
		t.Fatal("refused recovery changed schema")
	}
}

func TestExclusiveOwnerReapRollsBackObjectAndQuotaChanges(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `CREATE TRIGGER refuse_retained_delete BEFORE DELETE ON nodes
		WHEN OLD.detached = 1 BEGIN SELECT RAISE(ABORT, 'retained cleanup blocked'); END`)
	before := historicalLeaseRows(t, f.path)
	state, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("recovery with failed node cleanup: %v; want EIO", err)
	}
	assertHistoricalLeaseRows(t, f.path, before)
	after, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil || after != state {
		t.Fatalf("failed reap changed durable generation: %+v, %v; want %+v", after, err, state)
	}
}
