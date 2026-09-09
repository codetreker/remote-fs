package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	metastoretest.Run(t, contractStoreFactory(database(t)))
}

// Fresh namespace pairs isolate cases while retaining one physical schema. Each
// child still owns the complete lifetime of its database connections.
func contractStoreFactory(path string) metastoretest.NewStore {
	var sequence atomic.Uint64
	return func(t *testing.T, allowance int64) metastore.Store {
		suffix := strconv.FormatUint(sequence.Add(1), 10)
		return withNeighbour{
			Store:     open(t, path, "workspace-"+suffix, allowance),
			neighbour: open(t, path, "neighbour-"+suffix, 0),
			gaps:      new(atomic.Int64),
			t:         t,
		}
	}
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
	modified := time.Unix(n.gaps.Add(1), 0)
	if err := n.neighbour.SetAttr(ctx, "", storage.AttrChange{ModTime: &modified}); err != nil {
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

func TestNeighbourGapsKeepSparsePositionsWithoutGrowingTheTree(t *testing.T) {
	path := database(t)
	n := withNeighbour{
		Store:     open(t, path, "workspace", 0),
		neighbour: open(t, path, "neighbour", 0),
		gaps:      new(atomic.Int64), t: t,
	}
	ctx := t.Context()
	root, err := n.neighbour.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	ownRoot, err := n.Store.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		modified := time.Unix(100+int64(i), 0)
		if err := n.SetAttr(ctx, "", storage.AttrChange{ModTime: &modified}); err != nil {
			t.Fatal(err)
		}
	}
	var callers sync.WaitGroup
	for range 4 {
		callers.Go(func() { n.gap(ctx) })
	}
	callers.Wait()
	readChanges := func(store *sqlite.Store) []metastore.Change {
		t.Helper()
		result, err := metastore.NewChangeResult(64*1024, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
			return 128 + lengths.Name + lengths.FromName + lengths.Content, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Since(ctx, 0, 16, result); err != nil {
			t.Fatal(err)
		}
		changes, err := result.Changes()
		if err != nil {
			t.Fatal(err)
		}
		return changes
	}
	own, neighbour := readChanges(n.Store), readChanges(n.neighbour)
	if len(own) != 4 || len(neighbour) != 8 {
		t.Fatalf("own changes=%d, neighbour changes=%d", len(own), len(neighbour))
	}
	for i, change := range own {
		if change.Kind != metastore.Modified || change.Node == nil || change.Node.ID != ownRoot.ID || !change.Node.ModTime.Equal(time.Unix(100+int64(i), 0)) || change.Position != neighbour[i].Position+1 {
			t.Fatalf("own change %d = %+v, preceding neighbour=%+v", i, change, neighbour[i])
		}
		if i > 0 && change.Position <= own[i-1].Position+1 {
			t.Fatalf("adjacent own positions: %d, %d", own[i-1].Position, change.Position)
		}
	}
	seen := make(map[int64]bool)
	for i, change := range neighbour {
		if change.Kind != metastore.Modified || change.Node == nil || change.Node.ID != root.ID || len(change.Name) != 0 {
			t.Fatalf("neighbour change %d did not modify the same root: %+v", i, change)
		}
		second := change.Node.ModTime.Unix()
		if second < 1 || second > 8 || seen[second] || !change.Node.ModTime.Equal(time.Unix(second, 0)) {
			t.Fatalf("neighbour change %d has nonunique counter timestamp %v", i, change.Node.ModTime)
		}
		seen[second] = true
		if i > 0 && change.Position <= neighbour[i-1].Position {
			t.Fatalf("neighbour positions did not increase: %+v", neighbour)
		}
	}
	if children, err := n.neighbour.List(ctx, ""); err != nil || len(children) != 0 {
		t.Fatalf("neighbour tree grew: %+v, %v", children, err)
	}
	if after, err := n.neighbour.Stat(ctx, ""); err != nil || after.ID != root.ID || after.Mode != root.Mode {
		t.Fatalf("neighbour root changed identity or mode: %+v, %v", after, err)
	}
}

func TestContractFactoryKeepsSequentialNamespacesIsolated(t *testing.T) {
	path := database(t)
	factory := contractStoreFactory(path)
	var first, second withNeighbour
	var savedNode metastore.Node
	var savedSpace storage.Space
	var savedStatus sqlite.ObjectStatus
	var savedBarrier, savedNeighbourBarrier metastore.LogBarrier
	t.Run("populated namespace", func(t *testing.T) {
		first = factory(t, 128).(withNeighbour)
		key, err := first.Reserve(t.Context(), "same", 7)
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Commit(t.Context(), "same", metastore.Object{Key: key, Size: 7, ModTime: time.Unix(100, 0)}); err != nil {
			t.Fatal(err)
		}
		if _, err := first.Reserve(t.Context(), "reserved", 3); err != nil {
			t.Fatal(err)
		}
		unresolved, err := first.Reserve(t.Context(), "unresolved", 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Quarantine(t.Context(), unresolved); err != nil {
			t.Fatal(err)
		}
		garbage, err := first.Reserve(t.Context(), "garbage", 5)
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Abandon(t.Context(), garbage); err != nil {
			t.Fatal(err)
		}
		savedNode, err = first.Stat(t.Context(), "same")
		if err != nil || savedNode.Content != key || savedNode.Size != 7 {
			t.Fatalf("first node=%+v, error=%v", savedNode, err)
		}
		savedSpace, err = first.Space(t.Context())
		if err != nil || savedSpace != (storage.Space{Total: 128, Used: 7, Avail: 121}) {
			t.Fatalf("first quota=%+v, error=%v", savedSpace, err)
		}
		savedStatus, err = first.ObjectStatus(t.Context())
		want := sqlite.ObjectStatus{ReservedCount: 1, ReservedBytes: 3, UnresolvedCount: 1, UnresolvedBytes: 4, GarbageCount: 1, GarbageBytes: 5}
		if err != nil || savedStatus != want {
			t.Fatalf("first pending state=%+v, error=%v", savedStatus, err)
		}
		savedBarrier, err = first.Barrier(t.Context(), 1024)
		if err != nil || savedBarrier.Incarnation == "" || savedBarrier.Position == 0 {
			t.Fatalf("first barrier=%+v, error=%v", savedBarrier, err)
		}
		savedNeighbourBarrier, err = first.neighbour.Barrier(t.Context(), 1024)
		if err != nil || savedNeighbourBarrier.Position == 0 {
			t.Fatalf("first neighbour barrier=%+v, error=%v", savedNeighbourBarrier, err)
		}
	})
	if t.Failed() {
		return
	}
	if !first.Store.Terminal() || !first.neighbour.Terminal() {
		t.Fatal("first child retained an open Store after cleanup")
	}
	before, err := sqlite.InspectDurableState(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("fresh namespace", func(t *testing.T) {
		second = factory(t, 256).(withNeighbour)
		if children, err := second.List(t.Context(), ""); err != nil || len(children) != 0 {
			t.Fatalf("fresh namespace lists %+v, error=%v", children, err)
		}
		if _, err := second.Stat(t.Context(), "same"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("fresh namespace inherited a name: %v", err)
		}
		if space, err := second.Space(t.Context()); err != nil || space != (storage.Space{Total: 256, Avail: 256}) {
			t.Fatalf("fresh quota=%+v, error=%v", space, err)
		}
		if status, err := second.ObjectStatus(t.Context()); err != nil || status != (sqlite.ObjectStatus{}) {
			t.Fatalf("fresh pending state=%+v, error=%v", status, err)
		}
		if garbage, err := second.Garbage(t.Context(), 16); err != nil || len(garbage) != 0 {
			t.Fatalf("fresh garbage=%+v, error=%v", garbage, err)
		}
		if barrier, err := second.Barrier(t.Context(), 1024); err != nil || barrier.Position != 0 || barrier.Incarnation == "" || barrier.Incarnation == savedBarrier.Incarnation {
			t.Fatalf("fresh barrier=%+v, error=%v", barrier, err)
		}
		picture, at, err := second.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := picture.Close(); err != nil {
				t.Error(err)
			}
		}()
		result, err := metastore.NewRowResult(1024, 0, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
			return 128 + lengths.Name + lengths.Content, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		done, err := picture.Next(t.Context(), 16, result)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := result.Rows()
		if err != nil || at != 0 || !done || len(rows) != 1 || rows[0].Parent != 0 || rows[0].Name != nil || !rows[0].Node.IsDir() || rows[0].Node.ID <= before.NodeHighWater {
			t.Fatalf("fresh snapshot at=%d done=%v rows=%+v, error=%v", at, done, rows, err)
		}
		if err := picture.Close(); err != nil {
			t.Fatal(err)
		}
		key, err := second.Reserve(t.Context(), "same", 9)
		if err != nil {
			t.Fatal(err)
		}
		if err := second.Commit(t.Context(), "same", metastore.Object{Key: key, Size: 9, ModTime: time.Unix(200, 0)}); err != nil {
			t.Fatal(err)
		}
		if node, err := second.Stat(t.Context(), "same"); err != nil || node.Content != key || node.Content == savedNode.Content || node.Size != 9 {
			t.Fatalf("second node=%+v, error=%v", node, err)
		}
	})
	if t.Failed() {
		return
	}
	if !second.Store.Terminal() || !second.neighbour.Terminal() {
		t.Fatal("second child retained an open Store after cleanup")
	}
	after, err := sqlite.InspectDurableState(t.Context(), path)
	if err != nil || after.DatabaseID != before.DatabaseID || after.Generation <= before.Generation || after.NodeHighWater <= before.NodeHighWater || after.ChangeHighWater <= before.ChangeHighWater {
		t.Fatalf("shared database state before=%+v after=%+v, error=%v", before, after, err)
	}
	reopened := open(t, path, "workspace-1", 128)
	if node, err := reopened.Stat(t.Context(), "same"); err != nil || node != savedNode {
		t.Fatalf("second child changed first node: %+v, error=%v; want %+v", node, err, savedNode)
	}
	if space, err := reopened.Space(t.Context()); err != nil || space != savedSpace {
		t.Fatalf("second child changed first quota: %+v, error=%v; want %+v", space, err, savedSpace)
	}
	if status, err := reopened.ObjectStatus(t.Context()); err != nil || status != savedStatus {
		t.Fatalf("second child changed first pending state: %+v, error=%v; want %+v", status, err, savedStatus)
	}
	if barrier, err := reopened.Barrier(t.Context(), 1024); err != nil || barrier != savedBarrier {
		t.Fatalf("second child changed first log: %+v, error=%v; want %+v", barrier, err, savedBarrier)
	}
	neighbour := open(t, path, "neighbour-1", 0)
	if barrier, err := neighbour.Barrier(t.Context(), 1024); err != nil || barrier != savedNeighbourBarrier {
		t.Fatalf("second child changed first neighbour log: %+v, error=%v; want %+v", barrier, err, savedNeighbourBarrier)
	}
}
