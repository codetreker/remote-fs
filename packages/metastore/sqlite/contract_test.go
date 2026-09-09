package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

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
	store, err := sqlite.Open(t.Context(), path, namespace, allowance, sqlite.DefaultWindow())
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
