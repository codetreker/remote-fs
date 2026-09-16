package integration_test

import (
	"bytes"
	"context"
	"errors"
	"path"
	"reflect"
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

// source is a volume to be copied, and copy is the copy of it.
func source(t *testing.T) *sqlite.Store {
	t.Helper()

	store, err := sqlite.Open(t.Context(), path.Join(t.TempDir(), "source.db"), "ws", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the volume: %v", err)
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
		t.Fatalf("taking a picture of the volume: %v", err)
	}
	defer snap.Close()

	seeding, err := into.Reseed(t.Context())
	if err != nil {
		t.Fatalf("emptying the copy: %v", err)
	}
	defer seeding.Close()

	for {
		rows, done, err := readRows(t.Context(), snap, page)
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

	changes, _, err := readChanges(t.Context(), from, into.Position(), 1000)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	for _, change := range changes {
		applied, err := into.Apply(t.Context(), change)
		if err != nil {
			t.Fatalf("applying the change at position %d: %v", change.Position, err)
		}
		if !applied {
			t.Fatalf("the change at position %d was discarded, and the copy stands at %d", change.Position, into.Position())
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
		t.Fatalf("the volume holds %d nodes and the copy holds %d:\n volume %v\n copy      %v",
			len(want), len(got), names(want), names(got))
	}
	for at, node := range want {
		mirrored, present := got[at]
		if !present {
			t.Fatalf("the copy does not hold %q, which the volume does", at)
		}
		// The content key is the one thing a copy does not hold: it never reaches an object
		// store, so a key here would name bytes nothing has.
		if mirrored.ID != node.ID || mirrored.Kind != node.Kind || mirrored.Size != node.Size ||
			!mirrored.ModTime.Equal(node.ModTime) || !mirrored.AccessTime.Equal(node.AccessTime) ||
			!replicaTimeEqual(mirrored.CreationTime, node.CreationTime) || !replicaTimeEqual(mirrored.ChangeTime, node.ChangeTime) ||
			mirrored.MetadataRevision != node.MetadataRevision || mirrored.DirectoryRevision != node.DirectoryRevision ||
			!reflect.DeepEqual(mirrored.Metadata, node.Metadata) || !bytes.Equal(mirrored.LinkTarget, node.LinkTarget) {
			t.Fatalf("the copy holds %q as %+v, the volume holds it as %+v", at, mirrored, node)
		}
		if mirrored.Content != "" {
			t.Fatalf("the copy holds a content key for %q, and it has no object store to use one against", at)
		}
	}
}

func replicaTimeEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func replicaNotificationChange(t *testing.T, kind metastore.ChangeKind, root, node metastore.Node, entry storage.EntryID, name, from string) metastore.Change {
	t.Helper()
	image := func(name string) *metastore.EventImage {
		return &metastore.EventImage{Attr: node.Attr(), LinkTarget: bytes.Clone(node.LinkTarget), Location: storage.EntryLocation{
			State: storage.LocationLinked, RootNodeID: uint64(root.ID), NodeID: uint64(node.ID),
			Ancestors: []storage.EntryCondition{{ParentID: uint64(root.ID), DirectoryRevision: root.DirectoryRevision,
				EntryID: entry, NodeID: uint64(node.ID), Name: []byte(name)}},
		}}
	}
	notification := &metastore.Notification{SubjectID: node.ID, SubjectKind: node.Kind, ChangeMask: metastore.ChangeName}
	owned := node.Clone()
	change := metastore.Change{Kind: kind, Parent: root.ID, Name: []byte(name), Node: &owned, Notification: notification}
	switch kind {
	case metastore.Created:
		notification.After = image(name)
	case metastore.Removed:
		change.Node = nil
		notification.Before = image(name)
	case metastore.Modified:
		notification.ChangeMask = 0
		notification.Before, notification.After = image(name), image(name)
	case metastore.Renamed:
		change.From = &metastore.Location{Parent: root.ID, Name: []byte(from)}
		notification.Before, notification.After = image(from), image(name)
	default:
		t.Fatalf("unsupported replica notification fixture kind %d", kind)
	}
	encoded, err := metastore.EncodeNotification(change)
	if err != nil {
		t.Fatalf("encoding replica notification fixture: %v", err)
	}
	change.Notification, err = metastore.DecodeNotification(change, encoded)
	if err != nil {
		t.Fatalf("decoding replica notification fixture: %v", err)
	}
	return change
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

func TestReplicaListBoundedPreservesCompleteResultsAndFailures(t *testing.T) {
	from := source(t)
	build(t, from)
	into := copyOf(t)
	fill(t, from, into, 1024)

	newResult := func() *storage.ListResult {
		result, err := storage.NewListResult(1<<20, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			return 64 + nameBytes + metadataBytes, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	complete := newResult()
	if err := into.ListBounded(t.Context(), "", complete); err != nil {
		t.Fatalf("listing the replica root: %v", err)
	}
	entries, err := complete.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("the bounded replica listing omitted every root entry")
	}

	failed := newResult()
	if err := into.ListBounded(t.Context(), "missing", failed); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("listing a missing replica directory: %v, want ENOENT", err)
	}
	if entries, err := failed.Entries(); !errors.Is(err, syscall.ENOENT) || entries != nil {
		t.Fatalf("the failed replica listing exposed %+v, %v", entries, err)
	}
	if err := into.ListBounded(t.Context(), "", nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("listing into a nil result: %v, want EINVAL", err)
	}
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

// TestEveryKindOfChangeIsAppliedAsTheVolumeRecordedIt replays a log against a copy of the
// tree the log began from, and compares what comes out against the volume itself.
//
// The operations below are chosen so that every kind of change is recorded at least once: a
// creation, a modification, a removal, a rename, and a rename onto something that was already
// there — which is the one that records a removal and a rename together.
func TestEveryKindOfChangeIsAppliedAsTheVolumeRecordedIt(t *testing.T) {
	from := source(t)
	into := copyOf(t)
	// The copy starts from a picture of an empty volume, so everything below reaches it as
	// a change rather than as part of the picture.
	fill(t, from, into, 1024)

	build(t, from)
	metadata := storage.Metadata{{Key: "test", Version: 1, Data: []byte{0, 0xff, 6}}}
	for _, done := range []struct {
		what string
		run  func() error
	}{
		{"changing opaque metadata", func() error {
			node, err := from.Stat(t.Context(), "d/f")
			if err != nil {
				return err
			}
			return from.SetAttr(t.Context(), "d/f", storage.AttrChange{ExpectedRevision: node.MetadataRevision, Metadata: &metadata})
		}},
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
			t.Fatalf("%s in the volume: %v", done.what, err)
		}
		replay(t, from, into)
		requireSame(t, from, into)
	}
}

func TestReplicaRefusesAReusedNodeIdentity(t *testing.T) {
	from := source(t)
	into := copyOf(t)
	fill(t, from, into, 1024)
	root, err := into.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	node := metastore.Node{
		ID: root.ID + 1, Kind: storage.NodeRegular, MetadataRevision: 1,
		Metadata:   storage.Metadata{{Key: "test", Version: 1, Data: []byte{1, 0xff}}},
		AccessTime: time.Unix(1, 0), ModTime: time.Unix(1, 0),
	}
	created := replicaNotificationChange(t, metastore.Created, root, node, storage.EntryID(root.ID+2), "first", "")
	created.Position = 1
	if applied, err := into.Apply(t.Context(), created); err != nil || !applied {
		t.Fatalf("applying the initial creation returned applied=%v, err=%v", applied, err)
	}
	removed := replicaNotificationChange(t, metastore.Removed, root, node, storage.EntryID(root.ID+2), "first", "")
	removed.Position = 2
	if applied, err := into.Apply(t.Context(), removed); err != nil || !applied {
		t.Fatalf("applying the removal returned applied=%v, err=%v", applied, err)
	}
	reused := replicaNotificationChange(t, metastore.Created, root, node, storage.EntryID(root.ID+3), "replacement", "")
	reused.Position = 3
	if applied, err := into.Apply(t.Context(), reused); !errors.Is(err, syscall.EIO) || applied {
		t.Fatalf("applying a reused node identity returned applied=%v, err=%v, want false and EIO", applied, err)
	}
	if into.Position() != 2 {
		t.Fatalf("refused reuse advanced the replica to %d, want 2", into.Position())
	}
	if _, err := into.Stat(t.Context(), "replacement"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused reuse left a replacement node: %v, want ENOENT", err)
	}
}

// TestARenamedDirectoryMovesInTheCopyWithoutItsSubtreeBeingTouched. One row in the log is one
// row here: everything beneath keeps the identity it had, which is what makes a directory
// rename cost the same in a copy as it does in the volume.
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
	absent := metastore.Node{ID: 9999, Kind: storage.NodeRegular, MetadataRevision: 1,
		Metadata: storage.Metadata{{Key: "test", Version: 1, Data: []byte{9}}}, ModTime: time.Now(), AccessTime: time.Now()}
	fixture := func(kind metastore.ChangeKind, name, from string) metastore.Change {
		return replicaNotificationChange(t, kind, root, absent, 10000, name, from)
	}
	unknownKind := fixture(metastore.Modified, "g", "")
	unknownKind.Kind = metastore.ChangeKind(42)
	missingNode := fixture(metastore.Created, "arrived", "")
	missingNode.Node = nil
	missingFrom := fixture(metastore.Renamed, "arrived", "never-existed")
	missingFrom.From = nil

	// Distinct positions keep one erroneous application from making later cases
	// look like successful duplicate suppression.
	for at, c := range []struct {
		name   string
		change metastore.Change
	}{
		{"a rename whose source is not there", fixture(metastore.Renamed, "arrived", "never-existed")},
		{"a modification of a node the copy does not hold", fixture(metastore.Modified, "g", "")},
		{"a removal of a name that is not there", fixture(metastore.Removed, "never-existed", "")},
		{"a creation at a name that is taken", fixture(metastore.Created, "g", "")},
		{"a change of a kind this build has no meaning for", unknownKind},
		{"a change that says what a name holds and carries no node", missingNode},
		{"a rename that does not say where the node came from", missingFrom},
	} {
		t.Run(c.name+" is refused", func(t *testing.T) {
			c.change.Position = filled + metastore.Position(at) + 1
			before := into.Position()
			applied, err := into.Apply(t.Context(), c.change)
			if applied {
				t.Fatal("the change was reported as applied")
			}
			if err == nil {
				t.Fatal("the change was applied, and the copy now holds something the volume never recorded")
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
	// was refused half way through would leave the copy holding a tree the volume never had.
	requireSame(t, from, into)
}

// TestAChangeAtAPositionTheCopyAlreadyHoldsIsDiscarded covers stream changes already included
// in the snapshot that built the copy.
func TestAChangeAtAPositionTheCopyAlreadyHoldsIsDiscarded(t *testing.T) {
	from := source(t)
	into := copyOf(t)
	fill(t, from, into, 1024)

	build(t, from)
	changes, _, err := readChanges(t.Context(), from, 0, 1000)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	replay(t, from, into)
	at := into.Position()

	// Every change again, in order. A copy that applied any of them a second time would fail
	// on the first creation, and one that took the position back would replay the rest. Each
	// one says it was discarded, which is what keeps the caller's own account of the copy
	// from being moved by a change the copy did not take.
	for _, change := range changes {
		applied, err := into.Apply(t.Context(), change)
		if err != nil {
			t.Fatalf("applying the change at position %d a second time: %v", change.Position, err)
		}
		if applied {
			t.Fatalf("the change at position %d was applied a second time", change.Position)
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
// volume once had, and what a half-delivered picture leaves is a tree nobody ever had.
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
	if err := seeding.Add(t.Context(), []metastore.Row{{Node: metastore.Node{ID: 4242, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1,
		Metadata: storage.Metadata{{Key: "test", Version: 1, Data: []byte{4, 2}}}}}}); err != nil {
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

func TestReseedReportsAClosedReplicaAsEIO(t *testing.T) {
	into := copyOf(t)
	if err := into.Close(); err != nil {
		t.Fatal(err)
	}
	seeding, err := into.Reseed(t.Context())
	if seeding != nil {
		seeding.Close()
		t.Fatal("a closed replica returned a seeding transaction")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("reseeding a closed replica returned %v, want EIO", err)
	}
}

func TestReseedWaitingForAnotherPictureHonorsCancellation(t *testing.T) {
	into := copyOf(t)
	first, err := into.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	second, err := into.Reseed(ctx)
	if second != nil {
		second.Close()
		t.Fatal("a canceled reseed returned a second seeding transaction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling a reseed behind an active picture returned %v", err)
	}
}

// build puts a small tree into a volume: a directory with a file and a subtree, and a file
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
	node, err := store.Stat(t.Context(), "d/f")
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "test", Version: 1, Data: []byte{0xff, 0, 4}}}
	created := time.Unix(123, 456789).UTC()
	if err := store.SetAttr(t.Context(), "d/f", storage.AttrChange{
		ExpectedRevision: node.MetadataRevision, Metadata: &metadata, CreationTime: &created,
	}); err != nil {
		t.Fatalf("setting snapshot metadata: %v", err)
	}
}
