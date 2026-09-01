package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
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
//
// The store it hands over is not alone in its database, and that is the second half of the
// same argument. Positions come from one sequence shared by every namespace in a file, so a
// store that is alone in one gets consecutive positions and quietly satisfies any assumption
// about adjacency — which is how two separate readings of "has this caller fallen behind"
// came to be written against the distance to the oldest surviving entry, and why neither was
// caught here. A change to a neighbour is recorded before each change to this store, so the
// positions this suite sees have gaps in them wherever real ones would.
func TestTheContract(t *testing.T) {
	metastoretest.Run(t, func(t *testing.T, allowance int64) metastore.Store {
		path := database(t)
		return withNeighbour{
			Store:     open(t, path, "workspace", allowance),
			neighbour: open(t, path, "neighbour", 0),
			gaps:      new(atomic.Int64),
			t:         t,
		}
	})
}

// withNeighbour records a change to another namespace in the same database before each change
// to this one, so that this one's positions are never consecutive.
//
// Every operation that changes the tree is wrapped, because a position is allocated by each
// of them and one left unwrapped would hand this suite a pair of adjacent positions to be
// accidentally right about.
type withNeighbour struct {
	*sqlite.Store
	neighbour *sqlite.Store
	gaps      *atomic.Int64
	t         *testing.T
}

// gap consumes a position in the neighbouring namespace, which is what leaves a hole in this
// one's. A neighbour that will not take it is reported rather than passed over: the gap would
// silently not be there, and every case after it would be measuring dense positions again.
func (n withNeighbour) gap(ctx context.Context) {
	if err := n.neighbour.Create(ctx, fmt.Sprintf("gap-%d", n.gaps.Add(1))); err != nil {
		n.t.Errorf("consuming a position in the neighbouring namespace: %v", err)
	}
}

func (n withNeighbour) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	n.gap(ctx)
	return n.Store.SetAttr(ctx, path, change)
}

func (n withNeighbour) Create(ctx context.Context, path string) error {
	n.gap(ctx)
	return n.Store.Create(ctx, path)
}

func (n withNeighbour) Mkdir(ctx context.Context, path string) error {
	n.gap(ctx)
	return n.Store.Mkdir(ctx, path)
}

func (n withNeighbour) Remove(ctx context.Context, path string) error {
	n.gap(ctx)
	return n.Store.Remove(ctx, path)
}

func (n withNeighbour) RemoveDir(ctx context.Context, path string) error {
	n.gap(ctx)
	return n.Store.RemoveDir(ctx, path)
}

func (n withNeighbour) Rename(ctx context.Context, from, to string) error {
	n.gap(ctx)
	return n.Store.Rename(ctx, from, to)
}

func (n withNeighbour) Commit(ctx context.Context, path string, object metastore.Object) error {
	n.gap(ctx)
	return n.Store.Commit(ctx, path, object)
}

func database(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "metastore.db")
}

func open(t *testing.T, path, namespace string, allowance int64) *sqlite.Store {
	t.Helper()
	return openUnder(t, path, namespace, allowance, sqlite.DefaultWindow())
}

// openUnder opens a store whose log is held to a window of the case's choosing, which is what
// the retention cases need: the shipped window keeps ten thousand entries for ten minutes, and
// a case that filled it would be measuring how fast a test machine writes.
func openUnder(t *testing.T, path, namespace string, allowance int64, window sqlite.Window) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(t.Context(), path, namespace, allowance, window)
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

func TestOpenRefusesArgumentsThatNameNothing(t *testing.T) {
	if store, err := sqlite.Open(t.Context(), database(t), "", 0, sqlite.DefaultWindow()); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening a namespace with no name: %v, want EINVAL", err)
	}
	if store, err := sqlite.Open(t.Context(), database(t), "workspace", -1, sqlite.DefaultWindow()); !errors.Is(err, syscall.EINVAL) {
		if err == nil {
			store.Close()
		}
		t.Fatalf("opening under an allowance of -1 bytes: %v, want EINVAL", err)
	}
}

// A database that cannot be created is a failure to report, not a namespace to serve.
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
