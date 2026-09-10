package integration_test

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	_ "modernc.org/sqlite"
)

func TestABackingStoreBindingSurvivesReopen(t *testing.T) {
	path := database(t)
	first, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("binding a new database: %v", err)
	}
	if err := first.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("reopening with the same backing store: %v", err)
	}
	defer second.Close()
	if _, err := second.Stat(t.Context(), "held"); err != nil {
		t.Fatalf("the volume did not survive its bound reopen: %v", err)
	}

	db := raw(t, path)
	defer db.Close()
	var storeID string
	if err := db.QueryRow(`SELECT store_id FROM backing_store WHERE singleton = 1`).Scan(&storeID); err != nil {
		t.Fatal(err)
	}
	if storeID != "store-a" {
		t.Fatalf("the database is bound to %q, want store-a", storeID)
	}
}

func TestOpenBoundWithObjectLimitsUsesTheRequestedPendingLimit(t *testing.T) {
	store, err := sqlite.OpenBoundWithObjectLimits(
		t.Context(), database(t), "workspace", "store-a", 0, sqlite.DefaultWindow(),
		sqlite.ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Reserve(t.Context(), "first", 1); err != nil {
		t.Fatalf("reserving within the configured object limit: %v", err)
	}
	if _, err := store.Reserve(t.Context(), "second", 1); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("reserving beyond the configured object limit: %v, want EAGAIN", err)
	}
}

func TestABoundDatabaseRefusesADifferentBackingStoreWithoutCreatingAVolume(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	other, err := sqlite.OpenBound(t.Context(), path, "wrong", "store-b", 0, sqlite.DefaultWindow())
	if err == nil {
		other.Close()
		t.Fatal("opening a bound database with another backing store succeeded, want a refusal")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a bound database with another backing store: %v, want EINVAL", err)
	}

	db := raw(t, path)
	defer db.Close()
	var volumes int
	if err := db.QueryRow(`SELECT count(*) FROM volumes WHERE name = 'wrong'`).Scan(&volumes); err != nil {
		t.Fatal(err)
	}
	if volumes != 0 {
		t.Fatal("the refused opener created its volume")
	}
}

func TestOpenCannotBypassABackingStoreBinding(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	bypass, err := sqlite.Open(t.Context(), path, "bypass", 0, sqlite.DefaultWindow())
	if err == nil {
		bypass.Close()
		t.Fatal("Open served a bound database without its backing store identity")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a bound database without its backing store identity: %v, want EINVAL", err)
	}

	db := raw(t, path)
	defer db.Close()
	var volumes int
	if err := db.QueryRow(`SELECT count(*) FROM volumes WHERE name = 'bypass'`).Scan(&volumes); err != nil {
		t.Fatal(err)
	}
	if volumes != 0 {
		t.Fatal("the refused bypass created its volume")
	}
}

func TestOpenCannotBypassACorruptBackingStoreBinding(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE backing_store SET singleton = 2`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	bypass, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		bypass.Close()
		t.Fatal("Open served a database whose backing-store binding was hidden under another singleton")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening a database with a corrupt backing-store binding: %v, want EIO", err)
	}
	db = raw(t, path)
	defer db.Close()
	var singleton int
	if err := db.QueryRow(`SELECT singleton FROM backing_store`).Scan(&singleton); err != nil {
		t.Fatal(err)
	}
	if singleton != 2 {
		t.Fatalf("a refused bypass rewrote the corrupt singleton to %d", singleton)
	}
}

func TestOpenRefusesMultipleBackingStoreBindings(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO backing_store (singleton, store_id) VALUES (2, 'store-b')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("OpenBound accepted multiple backing-store bindings")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening multiple backing-store bindings: %v, want EIO", err)
	}
	db = raw(t, path)
	defer db.Close()
	var bindings int
	if err := db.QueryRow(`SELECT count(*) FROM backing_store`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != 2 {
		t.Fatalf("a refused open rewrote %d backing-store bindings", bindings)
	}
}

func TestANonemptyUnboundDatabaseCannotBeBound(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)

	store, err := sqlite.OpenBound(t.Context(), path, "new", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("binding a database that already holds an unbound volume succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("binding a database that already holds an unbound volume: %v, want EINVAL", err)
	}

	// The migration and binding shared the refused transaction, so neither was committed.
	db := raw(t, path)
	var version int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("the refused binding moved schema version 1 to %d", version)
	}
	var bindingTable int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'backing_store'`).Scan(&bindingTable); err != nil {
		t.Fatal(err)
	}
	if bindingTable != 0 {
		t.Fatal("the refused binding committed its schema migration")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The original API remains the way an existing unbound database is reopened.
	unbound, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("reopening the unbound database: %v", err)
	}
	defer unbound.Close()
	if _, err := unbound.Stat(t.Context(), "d/f"); err != nil {
		t.Fatalf("the refused binding disturbed the existing volume: %v", err)
	}
}

func TestABindingRollsBackWhenVolumeCreationFails(t *testing.T) {
	path := database(t)
	store := open(t, path, "temporary", 0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	for _, statement := range []string{
		`DELETE FROM logs`,
		`DELETE FROM entries`,
		`DELETE FROM nodes`,
		`DELETE FROM objects`,
		`DELETE FROM volumes`,
		`CREATE TRIGGER reject_volume BEFORE INSERT ON volumes
		 BEGIN SELECT RAISE(ABORT, 'volume creation rejected'); END`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("preparing an empty database: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	bound, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		bound.Close()
		t.Fatal("opening through a volume creation failure succeeded")
	}

	db = raw(t, path)
	defer db.Close()
	var bindings int
	if err := db.QueryRow(`SELECT count(*) FROM backing_store`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != 0 {
		t.Fatal("the backing store binding survived a failed volume creation")
	}
}

func TestOpenBoundRefusesAnEmptyBackingStoreIdentity(t *testing.T) {
	store, err := sqlite.OpenBound(t.Context(), database(t), "workspace", "", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening with an empty backing store identity succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening with an empty backing store identity: %v, want EINVAL", err)
	}
}

func TestBoundOpenComparesALargeStoredIdentityWithoutReturningIt(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE backing_store SET store_id = CAST(zeroblob(4 * 1024 * 1024) AS TEXT)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenBound(t.Context(), path, "workspace", "store-b", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening a database with another large stored identity succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a database with another large stored identity returned %v, want EINVAL", err)
	}
}

func database(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "metastore.db")
}

func open(t *testing.T, path, volume string, allowance int64) *sqlite.Store {
	t.Helper()
	return openUnder(t, path, volume, allowance, sqlite.DefaultWindow())
}

// openUnder opens a store whose log is held to a window of the case's choosing, which is what
// the retention cases need: the shipped window keeps ten thousand entries for ten minutes, and
// a case that filled it would be measuring how fast a test machine writes.
func openUnder(t *testing.T, path, volume string, allowance int64, window sqlite.Window) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(t.Context(), path, volume, allowance, window)
	if err != nil {
		t.Fatalf("opening %q in %s: %v", volume, path, err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return store
}

// One database holds many volumes, and one Store is bound to one of them. Nothing above
// this interface names a volume, so the separation has to be complete: a name made in one
// is not a name in the other, and neither is the other's allowance.
func TestVolumesInOneDatabaseAreSeparate(t *testing.T) {
	path := database(t)
	first := open(t, path, "first", 1000)
	second := open(t, path, "second", 2000)

	if err := first.Create(t.Context(), "mine"); err != nil {
		t.Fatalf("creating a file in the first volume: %v", err)
	}
	if _, err := second.Stat(t.Context(), "mine"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the second volume sees the first's file: %v, want ENOENT", err)
	}
	// The same name in both is two files, not a collision.
	if err := second.Create(t.Context(), "mine"); err != nil {
		t.Fatalf("creating the same name in the second volume: %v", err)
	}

	commit(t, first, "big", 900)
	space, err := second.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if space.Total != 2000 || space.Used != 0 {
		t.Fatalf("the second volume reports %+v, want its own 2000 byte allowance with nothing used", space)
	}

	// An object reserved in one volume is not one the other may commit or collect.
	key, err := second.Reserve(t.Context(), "borrowed", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = first.Commit(t.Context(), "borrowed", metastore.Object{Key: key, Size: 1, ModTime: time.Now()})
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("committing the other volume's reservation: %v, want EINVAL", err)
	}
}

// The tree, the attributes, the object records and the byte counter all live in the
// database rather than in the Store, so a volume is exactly what the last Store left
// when the next one opens it.
func TestAVolumeOutlivesTheStoreThatMadeIt(t *testing.T) {
	path := database(t)
	changed := time.Date(2400, 6, 1, 12, 0, 0, 500000000, time.UTC)

	first, err := sqlite.Open(t.Context(), path, "workspace", 4096, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := first.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	key := commit(t, first, "d/f", 700)
	if err := first.SetAttr(t.Context(), "d/f", storage.AttrChange{Mode: mode(0o640), ModTime: &changed}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	second := open(t, path, "workspace", 4096)
	node, err := second.Stat(t.Context(), "d/f")
	if err != nil {
		t.Fatalf("the file did not survive the reopen: %v", err)
	}
	if node.Content != key || node.Size != 700 {
		t.Fatalf("the file references %q and holds %d bytes, want %q and 700", node.Content, node.Size, key)
	}
	if node.Mode.Perm() != 0o640 || !node.ModTime.Equal(changed) {
		t.Fatalf("the file has mode %v and modification time %v, want 0640 at %v",
			node.Mode, node.ModTime.UTC(), changed)
	}
	space, err := second.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if space.Used != 700 {
		t.Fatalf("the reopened volume reports %d bytes used, want 700", space.Used)
	}
}

func mode(m fs.FileMode) *fs.FileMode { return &m }

// commit writes an object of the given length at path.
func commit(t *testing.T, s metastore.Store, path string, size int64) metastore.Key {
	t.Helper()
	key, err := s.Reserve(t.Context(), path, size)
	if err != nil {
		t.Fatalf("reserving a key: %v", err)
	}
	if err := s.Commit(t.Context(), path, metastore.Object{Key: key, Size: size, ModTime: time.Now()}); err != nil {
		t.Fatalf("committing %d bytes at %q: %v", size, path, err)
	}
	return key
}

func TestOpenRefusesArgumentsThatNameNothing(t *testing.T) {
	if store, err := sqlite.Open(t.Context(), database(t), "", 0, sqlite.DefaultWindow()); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening a volume with no name: %v, want EINVAL", err)
	}
	if store, err := sqlite.Open(t.Context(), database(t), "workspace", -1, sqlite.DefaultWindow()); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening under an allowance of -1 bytes: %v, want EINVAL", err)
	}
}

// A database that cannot be created is a failure to report, not a volume to serve.
func TestOpenReportsADatabaseItCannotCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-directory", "metastore.db")
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening a database under a directory that does not exist succeeded, want a failure")
	}
}

// Every mutation reads before it writes — the quota check reads the counter it is about to
// move — so concurrent writers are where a lost update would show. The allowance is exactly
// what the writers together ask for, which makes an over-count refuse a write that should
// have fitted and an under-count accept one that should not have.
func TestConcurrentCommitsEachTakeTheirOwnBytes(t *testing.T) {
	const (
		writers = 8
		each    = 16
		size    = 64
	)
	store := open(t, database(t), "workspace", writers*each*size)

	var wg sync.WaitGroup
	failures := make(chan error, writers*each)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				path := fmt.Sprintf("w%d-%d", w, i)
				key, err := store.Reserve(t.Context(), path, size)
				if err != nil {
					failures <- err
					return
				}
				if err := store.Commit(t.Context(), path, metastore.Object{
					Key: key, Size: size, ModTime: time.Now(),
				}); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("a concurrent commit failed: %v", err)
	}

	space, err := store.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(writers * each * size); space.Used != want {
		t.Fatalf("the volume reports %d bytes used, want %d", space.Used, want)
	}
	if space.Avail != 0 {
		t.Fatalf("the volume reports %d bytes available, want none", space.Avail)
	}
	// One more byte does not fit, which is the count being exact rather than approximately
	// right in the safe direction. The reservation is where it is refused, so the byte is
	// never uploaded.
	if _, err := store.Reserve(t.Context(), "overflow", 1); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("reserving one byte past a full volume: %v, want EDQUOT", err)
	}
}
