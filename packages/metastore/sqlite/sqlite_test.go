package sqlite_test

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/metastoretest"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

// TestTheContract is the whole of metastore.Store's obligations, run against a database in
// a file rather than in memory: an in-memory database is a different engine configuration —
// no WAL, no second connection reaching the same data — and passing there would say nothing
// about the one that ships.
func TestTheContract(t *testing.T) {
	metastoretest.Run(t, func(t *testing.T, allowance int64) metastore.Store {
		return open(t, database(t), "workspace", allowance)
	})
}

func database(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "metastore.db")
}

func open(t *testing.T, path, namespace string, allowance int64) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(t.Context(), path, namespace, allowance)
	if err != nil {
		t.Fatalf("opening %q in %s: %v", namespace, path, err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return store
}

// One database holds many namespaces, and one Store is bound to one of them. Nothing above
// this interface names a namespace, so the separation has to be complete: a name made in one
// is not a name in the other, and neither is the other's allowance.
func TestNamespacesInOneDatabaseAreSeparate(t *testing.T) {
	path := database(t)
	first := open(t, path, "first", 1000)
	second := open(t, path, "second", 2000)

	if err := first.Create(t.Context(), "mine"); err != nil {
		t.Fatalf("creating a file in the first namespace: %v", err)
	}
	if _, err := second.Stat(t.Context(), "mine"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the second namespace sees the first's file: %v, want ENOENT", err)
	}
	// The same name in both is two files, not a collision.
	if err := second.Create(t.Context(), "mine"); err != nil {
		t.Fatalf("creating the same name in the second namespace: %v", err)
	}

	commit(t, first, "big", 900)
	space, err := second.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if space.Total != 2000 || space.Used != 0 {
		t.Fatalf("the second namespace reports %+v, want its own 2000 byte allowance with nothing used", space)
	}

	// An object reserved in one namespace is not one the other may commit or collect.
	key, err := second.Reserve(t.Context(), "borrowed", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = first.Commit(t.Context(), "borrowed", metastore.Object{Key: key, Size: 1, ModTime: time.Now()})
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("committing the other namespace's reservation: %v, want EINVAL", err)
	}
}

// The tree, the attributes, the object records and the byte counter all live in the
// database rather than in the Store, so a namespace is exactly what the last Store left
// when the next one opens it.
func TestANamespaceOutlivesTheStoreThatMadeIt(t *testing.T) {
	path := database(t)
	changed := time.Date(2400, 6, 1, 12, 0, 0, 500000000, time.UTC)

	first, err := sqlite.Open(t.Context(), path, "workspace", 4096)
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
		t.Fatalf("the reopened namespace reports %d bytes used, want 700", space.Used)
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

// A database written by a version we do not understand is refused rather than adapted.
// Every statement here addresses columns by the meaning this version gives them, so running
// them against another layout would not fail loudly — it would update the wrong things.
func TestADatabaseFromAnotherSchemaVersionIsRefused(t *testing.T) {
	path := database(t)
	store, err := sqlite.Open(t.Context(), path, "workspace", 0)
	if err != nil {
		t.Fatalf("opening a fresh database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE schema_version SET version = 999`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(t.Context(), path, "workspace", 0)
	if err == nil {
		reopened.Close()
		t.Fatal("a database of an unknown schema version opened, want a refusal")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a database of an unknown schema version: %v, want EINVAL", err)
	}
}

func TestOpenRefusesArgumentsThatNameNothing(t *testing.T) {
	if store, err := sqlite.Open(t.Context(), database(t), "", 0); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening a namespace with no name: %v, want EINVAL", err)
	}
	if store, err := sqlite.Open(t.Context(), database(t), "workspace", -1); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening under an allowance of -1 bytes: %v, want EINVAL", err)
	}
}

// A database that cannot be created is a failure to report, not a namespace to serve.
func TestOpenReportsADatabaseItCannotCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-directory", "metastore.db")
	store, err := sqlite.Open(t.Context(), path, "workspace", 0)
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
		t.Fatalf("the namespace reports %d bytes used, want %d", space.Used, want)
	}
	if space.Avail != 0 {
		t.Fatalf("the namespace reports %d bytes available, want none", space.Avail)
	}
	// One more byte does not fit, which is the count being exact rather than approximately
	// right in the safe direction. The reservation is where it is refused, so the byte is
	// never uploaded.
	if _, err := store.Reserve(t.Context(), "overflow", 1); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("reserving one byte past a full namespace: %v, want EDQUOT", err)
	}
}
