package sqlite_test

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

// A copy is exactly as correct as the log it is fed, and nothing behind it revalidates
// anything: a change applied wrongly, or quietly not applied, stays wrong for as long as the
// copy exists. These are the tests of that one obligation.

// source is a namespace to be copied, and copy is the copy of it.
func source(t *testing.T) *sqlite.Store {
	t.Helper()

	store, err := sqlite.Open(t.Context(), path.Join(t.TempDir(), "source.db"), "ws", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the namespace: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func copyOf(t *testing.T) *sqlite.Replica {
	t.Helper()

	replica, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	t.Cleanup(func() { replica.Close() })
	return replica
}

// fill puts one picture of the source into the copy, in pages, the way the transport delivers
// one.
func fill(t *testing.T, from *sqlite.Store, into *sqlite.Replica, page int) {
	t.Helper()

	snap, at, err := from.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("taking a picture of the namespace: %v", err)
	}
	defer snap.Close()

	seeding, err := into.Reseed(t.Context())
	if err != nil {
		t.Fatalf("emptying the copy: %v", err)
	}
	defer seeding.Close()

	for {
		rows, done, err := snap.Next(t.Context(), page)
		if err != nil {
			t.Fatalf("reading the picture: %v", err)
		}
		if err := seeding.Add(t.Context(), rows); err != nil {
			t.Fatalf("filling the copy: %v", err)
		}
		if done {
			break
		}
	}
	if err := seeding.Complete(t.Context(), at); err != nil {
		t.Fatalf("completing the copy: %v", err)
	}
}

// replay applies everything the source recorded after a position.
func replay(t *testing.T, from *sqlite.Store, into *sqlite.Replica) {
	t.Helper()

	changes, _, err := from.Since(t.Context(), into.Position(), 1000)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	for _, change := range changes {
		if err := into.Apply(t.Context(), change); err != nil {
			t.Fatalf("applying the change at position %d: %v", change.Position, err)
		}
	}
}

// tree reads a whole tree out of a store or a copy of one, so that the two can be compared
// node for node.
func tree(t *testing.T,
	stat func(context.Context, string) (metastore.Node, error),
	list func(context.Context, string) ([]metastore.Child, error),
) map[string]metastore.Node {
	t.Helper()

	root, err := stat(t.Context(), "")
	if err != nil {
		t.Fatalf("reading the root: %v", err)
	}
	nodes := map[string]metastore.Node{"": root}
	for pending := []string{""}; len(pending) > 0; {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		children, err := list(t.Context(), dir)
		if err != nil {
			t.Fatalf("listing %q: %v", dir, err)
		}
		for _, child := range children {
			at := path.Join(dir, string(child.Name))
			nodes[at] = child.Node
			if child.Node.IsDir() {
				pending = append(pending, at)
			}
		}
	}
	return nodes
}

// requireSame compares a copy against its source node for node, including the ids: a copy
// that renamed the nodes would need a translation table beside it, and every change naming a
// directory by id would have to go through it correctly, forever.
func requireSame(t *testing.T, from *sqlite.Store, into *sqlite.Replica) {
	t.Helper()

	want := tree(t, from.Stat, from.List)
	got := tree(t, into.Stat, into.List)
	if len(want) != len(got) {
		t.Fatalf("the namespace holds %d nodes and the copy holds %d:\n namespace %v\n copy      %v",
			len(want), len(got), names(want), names(got))
	}
	for at, node := range want {
		mirrored, present := got[at]
		if !present {
			t.Fatalf("the copy does not hold %q, which the namespace does", at)
		}
		// The content key is the one thing a copy does not hold: it never reaches an object
		// store, so a key here would name bytes nothing has.
		if mirrored.ID != node.ID || mirrored.Mode != node.Mode || mirrored.Size != node.Size ||
			!mirrored.ModTime.Equal(node.ModTime) || !mirrored.AccessTime.Equal(node.AccessTime) {
			t.Fatalf("the copy holds %q as %+v, the namespace holds it as %+v", at, mirrored, node)
		}
		if mirrored.Content != "" {
			t.Fatalf("the copy holds a content key for %q, and it has no object store to use one against", at)
		}
	}
}

func names(nodes map[string]metastore.Node) []string {
	var all []string
	for at := range nodes {
		all = append(all, at)
	}
	return all
}

// TestACopyIsFilledFromAPictureAndHoldsTheSourcesIds.
func TestACopyIsFilledFromAPictureAndHoldsTheSourcesIds(t *testing.T) {
	from := source(t)
	build(t, from)

	into := copyOf(t)
	fill(t, from, into, 1024)
	requireSame(t, from, into)
}

// TestAPictureIsAcceptedWhateverOrderItsRowsArriveIn.
//
// A page of one row delivers the tree in as many frames as it has nodes, and a child may
// reach the copy before the directory holding it. The references between the rows are checked
// in full — they are checked at the commit rather than at each statement — so the order the
// picture yields its rows in is the picture's business rather than an agreement the two sides
// have to keep.
func TestAPictureIsAcceptedWhateverOrderItsRowsArriveIn(t *testing.T) {
	from := source(t)
	build(t, from)

	into := copyOf(t)
	fill(t, from, into, 1)
	requireSame(t, from, into)
}

// TestEveryKindOfChangeIsAppliedAsTheNamespaceRecordedIt replays a log against a copy of the
// tree the log began from, and compares what comes out against the namespace itself.
//
// The operations below are chosen so that every kind of change is recorded at least once: a
// creation, a modification, a removal, a rename, and a rename onto something that was already
// there — which is the one that records a removal and a rename together.
func TestEveryKindOfChangeIsAppliedAsTheNamespaceRecordedIt(t *testing.T) {
	from := source(t)
	into := copyOf(t)
	// The copy starts from a picture of an empty namespace, so everything below reaches it as
	// a change rather than as part of the picture.
	fill(t, from, into, 1024)

	build(t, from)
	mode := fs.FileMode(0o600)
	for _, done := range []struct {
		what string
		run  func() error
	}{
		{"changing a mode", func() error { return from.SetAttr(t.Context(), "d/f", storage.AttrChange{Mode: &mode}) }},
		{"removing a file", func() error { return from.Remove(t.Context(), "g") }},
		{"renaming a file", func() error { return from.Rename(t.Context(), "d/f", "d/moved") }},
		{"renaming a directory", func() error { return from.Rename(t.Context(), "d", "e") }},
		{"creating over a name that was taken", func() error {
			if err := from.Create(t.Context(), "displaced"); err != nil {
				return err
			}
			return from.Rename(t.Context(), "e/moved", "displaced")
		}},
		{"emptying a directory", func() error { return from.Remove(t.Context(), "e/inner/deep") }},
		{"removing a directory", func() error { return from.RemoveDir(t.Context(), "e/inner") }},
	} {
		if err := done.run(); err != nil {
			t.Fatalf("%s in the namespace: %v", done.what, err)
		}
		replay(t, from, into)
		requireSame(t, from, into)
	}
}

// TestARenamedDirectoryMovesInTheCopyWithoutItsSubtreeBeingTouched. One row in the log is one
// row here: everything beneath keeps the identity it had, which is what makes a directory
// rename cost the same in a copy as it does in the namespace.
func TestARenamedDirectoryMovesInTheCopyWithoutItsSubtreeBeingTouched(t *testing.T) {
	from := source(t)
	build(t, from)

	into := copyOf(t)
	fill(t, from, into, 1024)
	before := tree(t, into.Stat, into.List)

	if err := from.Rename(t.Context(), "d", "moved"); err != nil {
		t.Fatalf("renaming the directory: %v", err)
	}
	replay(t, from, into)

	after := tree(t, into.Stat, into.List)
	for _, moved := range []struct{ was, is string }{
		{"d", "moved"}, {"d/f", "moved/f"}, {"d/inner", "moved/inner"}, {"d/inner/deep", "moved/inner/deep"},
	} {
		if after[moved.is].ID != before[moved.was].ID {
			t.Fatalf("%q was node %d and %q is node %d; a rename is one row and nothing beneath it moves",
				moved.was, before[moved.was].ID, moved.is, after[moved.is].ID)
		}
	}
	if _, err := into.Stat(t.Context(), "d"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the copy still answers about the old name: %v", err)
	}
}

// TestAChangeThatDoesNotFindWhatItDescribesIsRefused.
//
// Every one of these is impossible against a copy that was filled from a consistent picture
// and has applied everything since, in order — which is exactly why none of them is tolerated.
// A rule that let a rename with no source pass quietly would be depended upon within a week of
// existing, and the copy has no way back from having not applied something: nothing
// revalidates it and no timeout repairs it.
func TestAChangeThatDoesNotFindWhatItDescribesIsRefused(t *testing.T) {
	from := source(t)
	build(t, from)

	into := copyOf(t)
	fill(t, from, into, 1024)

	root, err := into.Stat(t.Context(), "")
	if err != nil {
		t.Fatalf("reading the root of the copy: %v", err)
	}
	filled := into.Position()
	absent := metastore.Node{ID: 9999, Mode: 0o644, ModTime: time.Now(), AccessTime: time.Now()}

	// Each case carries a position of its own. A copy that wrongly applied one of them would
	// stand at that position afterwards, and every later case would then be discarded as
	// already held — so one defect would read as several, and the ones it hid would read as
	// passes on the day it was fixed.
	for at, c := range []struct {
		name   string
		change metastore.Change
	}{
		{"a rename whose source is not there", metastore.Change{
			Position: 100, Kind: metastore.Renamed, Parent: root.ID, Name: []byte("arrived"),
			From: &metastore.Location{Parent: root.ID, Name: []byte("never-existed")}, Node: &absent,
		}},
		{"a modification of a node the copy does not hold", metastore.Change{
			Position: 100, Kind: metastore.Modified, Parent: root.ID, Name: []byte("g"), Node: &absent,
		}},
		{"a removal of a name that is not there", metastore.Change{
			Position: 100, Kind: metastore.Removed, Parent: root.ID, Name: []byte("never-existed"),
		}},
		{"a creation at a name that is taken", metastore.Change{
			Position: 100, Kind: metastore.Created, Parent: root.ID, Name: []byte("g"), Node: &absent,
		}},
		{"a change of a kind this build has no meaning for", metastore.Change{
			Position: 100, Kind: metastore.ChangeKind(42), Parent: root.ID, Name: []byte("g"), Node: &absent,
		}},
		{"a change that says what a name holds and carries no node", metastore.Change{
			Position: 100, Kind: metastore.Created, Parent: root.ID, Name: []byte("arrived"),
		}},
		{"a rename that does not say where the node came from", metastore.Change{
			Position: 100, Kind: metastore.Renamed, Parent: root.ID, Name: []byte("arrived"), Node: &absent,
		}},
	} {
		t.Run(c.name+" is refused", func(t *testing.T) {
			c.change.Position = filled + metastore.Position(at) + 1
			before := into.Position()
			err := into.Apply(t.Context(), c.change)
			if err == nil {
				t.Fatal("the change was applied, and the copy now holds something the namespace never recorded")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("applying it failed with %v, want EIO", err)
			}
			if into.Position() != before {
				t.Fatalf("the copy stands at position %d after a change it could not apply, and stood at %d before",
					into.Position(), before)
			}
			t.Logf("%v", err)
		})
	}
	// Nothing above changed the copy, which is the other half of the refusal: a change that
	// was refused half way through would leave the copy holding a tree the namespace never had.
	requireSame(t, from, into)
}

// TestAChangeAtAPositionTheCopyAlreadyHoldsIsDiscarded, which is what makes a replica's own
// echo free: the change it caused arrives on the stream like any other.
func TestAChangeAtAPositionTheCopyAlreadyHoldsIsDiscarded(t *testing.T) {
	from := source(t)
	into := copyOf(t)
	fill(t, from, into, 1024)

	build(t, from)
	changes, _, err := from.Since(t.Context(), 0, 1000)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	replay(t, from, into)
	at := into.Position()

	// Every change again, in order. A copy that applied any of them a second time would fail
	// on the first creation, and one that took the position back would replay the rest.
	for _, change := range changes {
		if err := into.Apply(t.Context(), change); err != nil {
			t.Fatalf("applying the change at position %d a second time: %v", change.Position, err)
		}
	}
	if into.Position() != at {
		t.Fatalf("the copy stands at position %d after being told everything a second time, and stood at %d before", into.Position(), at)
	}
	requireSame(t, from, into)
}

// TestAPictureWithNoRootIsNotATree. A picture that lost the one node with no parent would
// leave a copy with no root, and every path resolved through it would answer about a tree
// that has no beginning.
func TestAPictureWithNoRootIsNotATree(t *testing.T) {
	into := copyOf(t)

	seeding, err := into.Reseed(t.Context())
	if err != nil {
		t.Fatalf("emptying the copy: %v", err)
	}
	defer seeding.Close()

	if err := seeding.Complete(t.Context(), 7); err == nil {
		t.Fatal("a picture with no root was accepted")
	} else if !errors.Is(err, syscall.EIO) {
		t.Fatalf("completing it failed with %v, want EIO", err)
	} else {
		t.Logf("%v", err)
	}
}

// TestAFillingThatWasNotCompletedLeavesTheCopyAsItWas. A picture that stops half way through
// is one the copy must not be left holding: what it had before is at least a tree the
// namespace once had, and what a half-delivered picture leaves is a tree nobody ever had.
func TestAFillingThatWasNotCompletedLeavesTheCopyAsItWas(t *testing.T) {
	from := source(t)
	build(t, from)

	into := copyOf(t)
	fill(t, from, into, 1024)
	at := into.Position()

	seeding, err := into.Reseed(t.Context())
	if err != nil {
		t.Fatalf("emptying the copy: %v", err)
	}
	if err := seeding.Add(t.Context(), []metastore.Row{{Node: metastore.Node{ID: 4242, Mode: fs.ModeDir | 0o755}}}); err != nil {
		t.Fatalf("filling the copy: %v", err)
	}
	if err := seeding.Close(); err != nil {
		t.Fatalf("discarding the filling: %v", err)
	}

	if into.Position() != at {
		t.Fatalf("the copy stands at position %d after a filling that was discarded, and stood at %d before", into.Position(), at)
	}
	requireSame(t, from, into)
}

// build puts a small tree into a namespace: a directory with a file and a subtree, and a file
// beside it.
func build(t *testing.T, store *sqlite.Store) {
	t.Helper()

	for _, made := range []struct {
		what string
		run  func() error
	}{
		{"d", func() error { return store.Mkdir(t.Context(), "d") }},
		{"d/f", func() error { return store.Create(t.Context(), "d/f") }},
		{"d/inner", func() error { return store.Mkdir(t.Context(), "d/inner") }},
		{"d/inner/deep", func() error { return store.Create(t.Context(), "d/inner/deep") }},
		{"g", func() error { return store.Create(t.Context(), "g") }},
	} {
		if err := made.run(); err != nil {
			t.Fatalf("making %s: %v", made.what, err)
		}
	}
}
