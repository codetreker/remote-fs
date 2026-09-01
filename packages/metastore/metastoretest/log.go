package metastoretest

import (
	"bytes"
	"fmt"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// logCases are the obligations of metastore.Log: that every change to the tree is recorded,
// that a picture of the tree is one instant, and that the two together are enough to build a
// replica that agrees with the store node for node.
//
// The last of those is the one worth stating twice. A replica applies these events and has
// nothing behind them — no revalidation, no timeout that repairs a record that was wrong.
// So the cases here do not check that events look plausible; they build the replica and
// compare it against the store, which is the only question that matters.
//
// What is not here, because it cannot be asked without holding an implementation to a
// particular retention window: falling out of that window, and which dimension pushed the
// caller out. Those live beside the implementation that has the window to set.
var logCases = []testCase{
	{name: "a fresh log holds nothing and is caught up at zero", run: func(t *testing.T, s metastore.Store) {
		changes, retention, err := s.Since(ctx(t), 0, 10)
		mustSucceed(t, err)
		if len(changes) != 0 {
			t.Fatalf("a namespace nobody has written to has recorded %d changes, want none", len(changes))
		}
		if retention.Tail != 0 || retention.Oldest != 0 {
			t.Fatalf("a fresh log holds %+v, want a tail and an oldest of 0", retention)
		}

		at, err := s.CommittedPosition(ctx(t))
		mustSucceed(t, err)
		if at != 0 {
			t.Fatalf("a fresh namespace was last changed at position %d, want 0 — the position before every change there has ever been", at)
		}

		// A log with no incarnation cannot be resumed against: the pair a caller returns with
		// is (incarnation, position), and an empty half of it matches everything.
		incarnation, err := s.Incarnation(ctx(t))
		mustSucceed(t, err)
		if incarnation == "" {
			t.Fatal("the log names its run of history with the empty string, which every other log would match")
		}
	}},

	// Zero is before every change there has ever been, so no change may be recorded at it. A
	// log that handed the first one out at 0 would be invisible to the caller that has applied
	// nothing: that caller resumes at 0 and takes only what is strictly greater.
	{name: "no change is recorded at position zero", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "first"))
		changes := drain(t, s, 0)
		if len(changes) == 0 {
			t.Fatal("the first write to a namespace recorded nothing")
		}
		if changes[0].Position <= 0 {
			t.Fatalf("the first change is at position %d; a caller that has applied nothing resumes at 0 and would never see it",
				changes[0].Position)
		}
	}},

	// It does not change because a process asked again, and it does not change because the
	// namespace was written to. Only a log that is no longer a continuation of what a caller
	// saw changes it.
	{name: "the incarnation is stable across reads and writes", run: func(t *testing.T, s metastore.Store) {
		first, err := s.Incarnation(ctx(t))
		mustSucceed(t, err)

		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		put(t, s, "d/f", 100)
		mustSucceed(t, s.Remove(ctx(t), "d/f"))

		second, err := s.Incarnation(ctx(t))
		mustSucceed(t, err)
		if first != second {
			t.Fatalf("the incarnation moved from %q to %q over ordinary writes, which would make every replica rebuild", first, second)
		}
	}},

	{name: "every operation that changes the tree records it", run: func(t *testing.T, s metastore.Store) {
		for _, step := range []struct {
			what string
			do   func()
			want metastore.ChangeKind
		}{
			{"create", func() { mustSucceed(t, s.Create(ctx(t), "f")) }, metastore.Created},
			{"mkdir", func() { mustSucceed(t, s.Mkdir(ctx(t), "d")) }, metastore.Created},
			{"setattr", func() { mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o600)})) }, metastore.Modified},
			{"commit", func() { put(t, s, "f", 40) }, metastore.Modified},
			{"rename", func() { mustSucceed(t, s.Rename(ctx(t), "f", "d/g")) }, metastore.Renamed},
			{"unlink", func() { mustSucceed(t, s.Remove(ctx(t), "d/g")) }, metastore.Removed},
			{"rmdir", func() { mustSucceed(t, s.RemoveDir(ctx(t), "d")) }, metastore.Removed},
		} {
			before, err := s.CommittedPosition(ctx(t))
			mustSucceed(t, err)
			step.do()
			changes := drain(t, s, before)
			if len(changes) == 0 {
				t.Fatalf("%s recorded nothing, so no replica would ever learn of it", step.what)
			}
			if !slices.ContainsFunc(changes, func(c metastore.Change) bool { return c.Kind == step.want }) {
				t.Fatalf("%s recorded %v, want one of them to be %v", step.what, kinds(changes), step.want)
			}
		}
	}},

	// A position is never reused and only ever increases, because that is the whole of what a
	// caller is allowed to do with one: apply an event only when its position is strictly
	// greater than the one already applied.
	{name: "positions only ever increase", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		for i := range 40 {
			put(t, s, "d/f", int64(i))
			mustSucceed(t, s.SetAttr(ctx(t), "d/f", storage.AttrChange{Mode: mode(0o600)}))
		}
		mustSucceed(t, s.Rename(ctx(t), "d/f", "moved"))
		mustSucceed(t, s.Remove(ctx(t), "moved"))

		changes := drain(t, s, 0)
		if len(changes) == 0 {
			t.Fatal("a namespace written to forty times recorded nothing")
		}
		for i, c := range changes {
			if i > 0 && c.Position <= changes[i-1].Position {
				t.Fatalf("change %d is at position %d, which is not after the %d before it",
					i, c.Position, changes[i-1].Position)
			}
		}

		// The tail is the newest position the tree was changed at, and it is the same fact
		// CommittedPosition reports for a log kept beside the tree.
		_, retention, err := s.Since(ctx(t), 0, 1)
		mustSucceed(t, err)
		at, err := s.CommittedPosition(ctx(t))
		mustSucceed(t, err)
		if retention.Tail != at || at != changes[len(changes)-1].Position {
			t.Fatalf("the log's tail is %d, the tree was last changed at %d, and the last change is at %d; want all three the same",
				retention.Tail, at, changes[len(changes)-1].Position)
		}
	}},
}

// renameCases above prove the tree moves. These prove the log says so in one row.
var renameLogCases = []testCase{
	// The reason the events carry a node rather than an invalidation. A directory rename is one
	// row here and one row in a replica; an invalidation would force the replica to discard
	// everything beneath the directory and walk it again, and renaming directories is what
	// build tools, version control and package managers do constantly.
	{name: "renaming a directory records one rename and nothing beneath it", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b/c"))
		mustSucceed(t, s.Create(ctx(t), "a/b/c/deep"))
		mustSucceed(t, s.Create(ctx(t), "a/shallow"))

		moving := mustStat(t, s, "a")
		beneath := map[int64]string{}
		for _, path := range []string{"a/b", "a/b/c", "a/b/c/deep", "a/shallow"} {
			beneath[mustStat(t, s, path).ID] = path
		}

		before, err := s.CommittedPosition(ctx(t))
		mustSucceed(t, err)
		mustSucceed(t, s.Rename(ctx(t), "a", "moved"))
		changes := drain(t, s, before)

		var renames []metastore.Change
		for _, c := range changes {
			if c.Kind == metastore.Renamed {
				renames = append(renames, c)
			}
			if path, under := beneath[c.Parent]; under {
				t.Fatalf("the rename recorded a %v under %q, which did not move", c.Kind, path)
			}
			if c.Node == nil {
				continue
			}
			if path, under := beneath[c.Node.ID]; under {
				t.Fatalf("the rename recorded a %v carrying %q, which did not change", c.Kind, path)
			}
		}
		if len(renames) != 1 {
			t.Fatalf("renaming a directory recorded %d renames, want exactly one: %v", len(renames), kinds(changes))
		}

		renamed := renames[0]
		if renamed.Node == nil || renamed.Node.ID != moving.ID {
			t.Fatalf("the rename carries %v, want the directory that moved, node %d", renamed.Node, moving.ID)
		}
		if !bytes.Equal(renamed.Name, []byte("moved")) {
			t.Fatalf("the rename landed at %q, want moved", renamed.Name)
		}
		if renamed.From == nil || !bytes.Equal(renamed.From.Name, []byte("a")) {
			t.Fatalf("the rename came from %v, want the name a", renamed.From)
		}
		if renamed.From.Parent != renamed.Parent {
			t.Fatalf("the rename came from parent %d and landed in %d, want both to be the root it stayed in",
				renamed.From.Parent, renamed.Parent)
		}
	}},

	// A rename that lands on an occupied name destroys the node that was there, and a replica
	// that is not told so keeps it. The replica below is what asks the question: its tree
	// refuses to render while it holds a node no name reaches.
	{name: "a rename that replaces a file leaves no replica holding the replaced node",
		run: func(t *testing.T, s metastore.Store) {
			put(t, s, "from", 10)
			put(t, s, "onto", 20)
			replaced := mustStat(t, s, "onto")

			mirror, at := picture(t, s, 8, nil)
			mustSucceed(t, s.Rename(ctx(t), "from", "onto"))
			mirror.apply(t, drain(t, s, at))

			if _, held := mirror.nodes[replaced.ID]; held {
				t.Fatalf("the replica still holds node %d, which the rename destroyed", replaced.ID)
			}
			mustAgree(t, mirror, s)
		}},

	// The same for a directory: an empty one replaced by a rename is a node that stops
	// existing, and stops existing in the replica too.
	{name: "a rename that replaces an empty directory leaves no replica holding it",
		run: func(t *testing.T, s metastore.Store) {
			mustSucceed(t, s.Mkdir(ctx(t), "from"))
			mustSucceed(t, s.Create(ctx(t), "from/held"))
			mustSucceed(t, s.Mkdir(ctx(t), "onto"))
			replaced := mustStat(t, s, "onto")

			mirror, at := picture(t, s, 8, nil)
			mustSucceed(t, s.Rename(ctx(t), "from", "onto"))
			mirror.apply(t, drain(t, s, at))

			if _, held := mirror.nodes[replaced.ID]; held {
				t.Fatalf("the replica still holds node %d, which the rename destroyed", replaced.ID)
			}
			mustAgree(t, mirror, s)
		}},
}

var snapshotCases = []testCase{
	{name: "a picture of a fresh namespace is the root alone", run: func(t *testing.T, s metastore.Store) {
		snap, at, err := s.Snapshot(ctx(t))
		mustSucceed(t, err)
		defer func() { mustSucceed(t, snap.Close()) }()

		if at != 0 {
			t.Fatalf("a picture of a namespace nobody has written to is at position %d, want 0", at)
		}
		rows, done, err := snap.Next(ctx(t), 16)
		mustSucceed(t, err)
		if !done {
			t.Fatal("a picture of an empty namespace is not complete after sixteen rows")
		}
		if len(rows) != 1 {
			t.Fatalf("a picture of an empty namespace holds %d rows, want just the root", len(rows))
		}
		// The root has no name and no parent, and it is named that way rather than left out:
		// a replica needs the node its whole tree hangs from.
		root := rows[0]
		if root.Parent != 0 || root.Name != nil {
			t.Fatalf("the root is named (%d, %q), want parent 0 and no name", root.Parent, root.Name)
		}
		if !root.Node.IsDir() {
			t.Fatalf("the root of the picture has mode %v, want a directory", root.Node.Mode)
		}
	}},

	{name: "a picture holds the whole tree, however it is paged", run: func(t *testing.T, s metastore.Store) {
		build(t, s)
		for _, page := range []int{1, 2, 3, 7, 1000} {
			mirror, _ := picture(t, s, page, nil)
			mustAgree(t, mirror, s)
		}
	}},

	// The reason a picture is a cut rather than a scan with a position stamped on it. Writes
	// land while the pages are being read; the picture must show one instant, and the changes
	// after its position must carry a replica built from it the rest of the way.
	//
	// A stamp taken after the scan is the version of this that is easier to write and silently
	// wrong: a node read early and changed during the scan would have its event discarded for
	// not being newer than the stamp, and nothing afterwards would ever correct it.
	{name: "a picture taken while writes continue is one instant, and the changes after it catch up",
		run: func(t *testing.T, s metastore.Store) {
			build(t, s)
			before, err := s.CommittedPosition(ctx(t))
			mustSucceed(t, err)

			// One write between every page, so that the pages cannot all have been read before
			// the first of them landed.
			written := 0
			mirror, at := picture(t, s, 1, func(step int) {
				switch step % 6 {
				case 0:
					mustSucceed(t, s.SetAttr(ctx(t), "a/b/c/deep", storage.AttrChange{Mode: mode(0o600)}))
				case 1:
					put(t, s, "a/b/c/deep", int64(100+step))
				case 2:
					mustSucceed(t, s.Create(ctx(t), name("late", step)))
				case 3:
					mustSucceed(t, s.Mkdir(ctx(t), name("dir", step)))
				case 4:
					mustSucceed(t, s.Rename(ctx(t), name("late", step-2), name("moved", step)))
				case 5:
					// A commit onto a name nothing holds yet, which makes the file. It is here
					// rather than only before the picture because a store that recorded it as a
					// modification would be telling a replica to change a name it has never held.
					put(t, s, name("fresh", step), int64(step))
				}
				written++
			})

			// Without writes landing inside the paging this case checks nothing at all: it would
			// be a picture of a tree nobody touched, which every implementation gets right.
			if written < 6 {
				t.Fatalf("only %d writes landed while the picture was being read, so nothing was interleaved", written)
			}
			after, err := s.CommittedPosition(ctx(t))
			mustSucceed(t, err)
			if after <= at {
				t.Fatalf("the tree was last changed at %d and the picture was taken at %d; the writes did not land during it",
					after, at)
			}
			if at != before {
				t.Fatalf("the picture is at position %d and the tree was at %d when it was taken; a picture stamped with anything else describes a moment its rows did not come from",
					at, before)
			}

			// The picture on its own is a tree: every name in it reaches a node in it, and every
			// node in it is reachable from its root. Half of a rename would fail this.
			mirror.tree(t)

			mirror.apply(t, drain(t, s, at))
			mustAgree(t, mirror, s)
		}},

	// A picture holds a resource for as long as it is open — here a read transaction — so the
	// contract has a caller close it whether or not it read to the end.
	{name: "a picture is closed once, or twice, and answers nothing afterwards",
		run: func(t *testing.T, s metastore.Store) {
			build(t, s)
			snap, _, err := s.Snapshot(ctx(t))
			mustSucceed(t, err)

			if _, _, err := snap.Next(ctx(t), 2); err != nil {
				t.Fatalf("reading the first page: %v", err)
			}
			mustSucceed(t, snap.Close())
			mustSucceed(t, snap.Close())

			_, _, err = snap.Next(ctx(t), 2)
			mustFail(t, err, syscall.EINVAL)
		}},

	{name: "a page of no rows is refused", run: func(t *testing.T, s metastore.Store) {
		snap, _, err := s.Snapshot(ctx(t))
		mustSucceed(t, err)
		defer func() { mustSucceed(t, snap.Close()) }()
		for _, limit := range []int{0, -1} {
			_, _, err := snap.Next(ctx(t), limit)
			mustFail(t, err, syscall.EINVAL)
		}
	}},
}

var sinceCases = []testCase{
	// The two answers a caller must be able to tell apart without asking anything else. They
	// differ by a full rebuild of the tree, and an implementation that could not distinguish
	// them would deliver the worse of the two in silence.
	{name: "since tells caught up from resumable", run: func(t *testing.T, s metastore.Store) {
		build(t, s)
		at, err := s.CommittedPosition(ctx(t))
		mustSucceed(t, err)

		// Caught up: the position is the tail, and there is nothing after it.
		changes, retention, err := s.Since(ctx(t), at, 100)
		mustSucceed(t, err)
		if len(changes) != 0 {
			t.Fatalf("a caller at the tail is offered %d changes, want none", len(changes))
		}
		if retention.Tail != at {
			t.Fatalf("the log's tail is %d for a caller at %d, want them equal", retention.Tail, at)
		}

		// Resumable: a position inside what the log still holds, and the changes after it.
		mustSucceed(t, s.Create(ctx(t), "later"))
		mustSucceed(t, s.Mkdir(ctx(t), "later-still"))
		changes, retention, err = s.Since(ctx(t), at, 100)
		mustSucceed(t, err)
		if len(changes) == 0 {
			t.Fatalf("two writes after position %d are offered as nothing, want the changes", at)
		}
		if retention.TrimmedThrough > at {
			t.Fatalf("the log has discarded through %d, which is past a caller resuming at %d, and nothing has been trimmed", retention.TrimmedThrough, at)
		}
		if retention.Tail <= at {
			t.Fatalf("the log's tail is %d after two writes past %d", retention.Tail, at)
		}
		if got := changes[len(changes)-1].Position; got != retention.Tail {
			t.Fatalf("the last change offered is at %d and the tail is %d, want them equal", got, retention.Tail)
		}
	}},

	// What separates a caller that can carry on from one that cannot is what the log threw
	// away, and never how far the caller sits from the oldest entry that survived. Those are
	// the same number only where a namespace's positions have no gaps in them, which this
	// contract does not promise and an implementation numbering every namespace in one
	// database from a single sequence does not provide. Reading resumability off that distance
	// sends callers that had missed nothing away to rebuild a whole tree.
	{name: "what the log discarded is what decides resuming, not what survived it",
		run: func(t *testing.T, s metastore.Store) {
			build(t, s)
			_, retention, err := s.Since(ctx(t), 0, 0)
			mustSucceed(t, err)

			// A log that has discarded nothing admits every caller, including one that has
			// applied nothing at all — whatever position its first entry happens to sit on.
			if retention.TrimmedThrough != 0 {
				t.Fatalf("a log that has discarded nothing reports having discarded through %d", retention.TrimmedThrough)
			}
			if retention.Oldest == 0 {
				t.Fatalf("the log holds nothing after a tree was built in it")
			}

			// Every position the log holds is resumable from, and so is everything before the
			// oldest of them, because nothing has been thrown away.
			for _, from := range []metastore.Position{0, retention.Oldest - 1, retention.Oldest} {
				changes, again, err := s.Since(ctx(t), from, 100)
				mustSucceed(t, err)
				if again.TrimmedThrough > from {
					t.Fatalf("resuming at %d is refused by a log that has discarded nothing", from)
				}
				if from < again.Tail && len(changes) == 0 {
					t.Fatalf("resuming at %d before the tail at %d is offered nothing", from, again.Tail)
				}
			}
		}},

	{name: "since returns at most the limit it was given, oldest first", run: func(t *testing.T, s metastore.Store) {
		build(t, s)
		for _, limit := range []int{0, 1, 3, 50} {
			changes, _, err := s.Since(ctx(t), 0, limit)
			mustSucceed(t, err)
			if len(changes) > limit {
				t.Fatalf("a limit of %d returned %d changes", limit, len(changes))
			}
			if !slices.IsSortedFunc(changes, func(a, b metastore.Change) int { return int(a.Position - b.Position) }) {
				t.Fatalf("a limit of %d returned changes out of order", limit)
			}
		}
		// A limit of one, walked forward, reaches the same changes as one large page: paging is
		// not allowed to skip anything.
		var walked []metastore.Change
		for after := metastore.Position(0); ; {
			page, _, err := s.Since(ctx(t), after, 1)
			mustSucceed(t, err)
			if len(page) == 0 {
				break
			}
			walked = append(walked, page...)
			after = page[len(page)-1].Position
		}
		whole := drain(t, s, 0)
		if len(walked) != len(whole) {
			t.Fatalf("walking one change at a time reached %d of them, want the %d one page holds", len(walked), len(whole))
		}
	}},

	// A limit of zero asks what the log holds without asking for any of it, which is how a
	// caller subscribing from now learns the tail to start from and how a reconnecting one
	// decides between resuming and rebuilding before a single change is sent. The retention it
	// gets back has to be the whole of it — a limit read as "unset, use some default" would
	// hand back changes nobody asked for, and one that refused zero would leave the tail
	// unreachable without them.
	{name: "a limit of zero reports what the log holds and none of it", run: func(t *testing.T, s metastore.Store) {
		build(t, s)
		none, reported, err := s.Since(ctx(t), 0, 0)
		mustSucceed(t, err)
		if len(none) != 0 {
			t.Fatalf("a limit of zero returned %d changes", len(none))
		}
		all, whole, err := s.Since(ctx(t), 0, 1000)
		mustSucceed(t, err)
		if len(all) == 0 {
			t.Fatal("the log recorded nothing for a tree that was just built")
		}
		if reported != whole {
			t.Fatalf("a limit of zero reports %+v and a full read reports %+v", reported, whole)
		}
		if reported.Tail != all[len(all)-1].Position {
			t.Fatalf("a limit of zero reports a tail of %d and the newest change is at %d",
				reported.Tail, all[len(all)-1].Position)
		}
	}},

	{name: "since refuses a position and a limit that are not ones", run: func(t *testing.T, s metastore.Store) {
		_, _, err := s.Since(ctx(t), -1, 10)
		mustFail(t, err, syscall.EINVAL)
		_, _, err = s.Since(ctx(t), 0, -1)
		mustFail(t, err, syscall.EINVAL)
	}},
}

// --- the replica -----------------------------------------------------------------------

// replica is what a mount keeps: the nodes of a tree and the names that reach them, built
// from one picture and carried forward by the changes after it.
//
// It is a plain map rather than a second metastore because the question being asked is
// whether the events say enough, and a second implementation of the tree would answer a
// different question — whether two implementations agree — while hiding the first one behind
// its own corrections.
type replica struct {
	root    int64
	nodes   map[int64]metastore.Node
	entries map[where]int64

	// at is the position everything applied so far was recorded at.
	at metastore.Position
}

// where is a name in a directory. The name is held as a string because a map key cannot be a
// slice; it is still the bytes of the name and is compared as such, not as text.
type where struct {
	parent int64
	name   string
}

// picture takes a snapshot, reads it in pages of the given size, and returns the replica it
// builds. between runs after every page but the last, which is where a case puts the writes
// that must not disturb the picture.
func picture(t *testing.T, s metastore.Store, page int, between func(step int)) (*replica, metastore.Position) {
	t.Helper()
	snap, at, err := s.Snapshot(ctx(t))
	mustSucceed(t, err)
	defer func() { mustSucceed(t, snap.Close()) }()

	r := &replica{nodes: map[int64]metastore.Node{}, entries: map[where]int64{}, at: at}
	for step := 0; ; step++ {
		rows, done, err := snap.Next(ctx(t), page)
		mustSucceed(t, err)
		if len(rows) > page {
			t.Fatalf("a page of %d rows was asked for and %d came back", page, len(rows))
		}
		for _, row := range rows {
			if (row.Parent == 0) != (row.Name == nil) {
				t.Fatalf("a row is named (%d, %q); the root has both a parent of 0 and no name, and every other row has neither",
					row.Parent, row.Name)
			}
			r.nodes[row.Node.ID] = row.Node
			if row.Name == nil {
				if r.root != 0 {
					t.Fatalf("the picture holds two roots, nodes %d and %d", r.root, row.Node.ID)
				}
				r.root = row.Node.ID
				continue
			}
			r.entries[where{row.Parent, string(row.Name)}] = row.Node.ID
		}
		if done {
			break
		}
		if between != nil {
			between(step)
		}
		if step > 100000 {
			t.Fatal("the picture never reported itself complete")
		}
	}
	if r.root == 0 {
		t.Fatal("the picture never named the root, so a replica has nothing to hang the tree from")
	}
	return r, at
}

// apply carries the replica forward, and refuses an event that does not fit what it holds.
//
// The refusals are the point. A replica has nothing behind these events — no revalidation and
// no timeout that repairs a record that was wrong — so an event landing on a name that is
// already taken, or moving a node from a name that is not there, is a defect in the producer
// that a tolerant applier would absorb and never report.
func (r *replica) apply(t *testing.T, changes []metastore.Change) {
	t.Helper()
	for _, c := range changes {
		// The picture is a cut, not a lower bound: everything up to its position is already in
		// it. This is what makes a rename always able to see its own source.
		if c.Position <= r.at {
			continue
		}
		at := where{c.Parent, string(c.Name)}
		root := c.Parent == 0 && c.Name == nil

		switch c.Kind {
		case metastore.Created:
			if _, taken := r.entries[at]; taken {
				t.Fatalf("position %d creates %q under %d, which the replica already holds", c.Position, c.Name, c.Parent)
			}
			r.hold(t, c, at, root)
		case metastore.Modified:
			if _, held := r.entries[at]; !held && !root {
				t.Fatalf("position %d modifies %q under %d, which the replica does not hold", c.Position, c.Name, c.Parent)
			}
			r.hold(t, c, at, root)
		case metastore.Removed:
			if c.Node != nil {
				t.Fatalf("position %d removes %q and carries a node, which is what a name no longer holds", c.Position, c.Name)
			}
			id, held := r.entries[at]
			if !held {
				t.Fatalf("position %d removes %q under %d, which the replica does not hold", c.Position, c.Name, c.Parent)
			}
			delete(r.entries, at)
			delete(r.nodes, id)
		case metastore.Renamed:
			if c.From == nil {
				t.Fatalf("position %d renames %q and says nothing about where it came from", c.Position, c.Name)
			}
			from := where{c.From.Parent, string(c.From.Name)}
			if _, held := r.entries[from]; !held {
				t.Fatalf("position %d moves %q out of %d, where the replica holds nothing", c.Position, c.From.Name, c.From.Parent)
			}
			if _, taken := r.entries[at]; taken {
				t.Fatalf("position %d moves a node onto %q under %d, which the replica still holds — the node that was there was destroyed and never reported",
					c.Position, c.Name, c.Parent)
			}
			delete(r.entries, from)
			r.hold(t, c, at, root)
		default:
			t.Fatalf("position %d is a change of kind %v, which is not one of the four", c.Position, c.Kind)
		}
		r.at = c.Position
	}
}

// hold records the node a change carries at the name it carries it for.
func (r *replica) hold(t *testing.T, c metastore.Change, at where, root bool) {
	t.Helper()
	if c.Node == nil {
		t.Fatalf("position %d is a %v carrying no node, so a replica has nothing to record", c.Position, c.Kind)
	}
	r.nodes[c.Node.ID] = *c.Node
	if !root {
		r.entries[at] = c.Node.ID
	}
}

// tree renders the replica as the paths a caller would see, and refuses to render one that is
// not a tree: a name reaching a node it does not hold, or a node no name reaches.
//
// The second of those is the check a leaked node fails. A rename that replaced a file without
// the log saying the replaced node was destroyed leaves exactly that behind.
func (r *replica) tree(t *testing.T) map[string]metastore.Node {
	t.Helper()
	paths := map[string]metastore.Node{}
	var walk func(id int64, path string)
	walk = func(id int64, path string) {
		node, held := r.nodes[id]
		if !held {
			t.Fatalf("the replica holds the name %q, which reaches node %d, which it does not hold", path, id)
		}
		paths[path] = node
		for at, child := range r.entries {
			if at.parent != id {
				continue
			}
			under := at.name
			if path != "" {
				under = path + "/" + at.name
			}
			walk(child, under)
		}
	}
	walk(r.root, "")
	if len(paths) != len(r.nodes) {
		t.Fatalf("the replica holds %d nodes and %d of them are reachable from its root; the rest are nodes that stopped existing without the log saying so",
			len(r.nodes), len(paths))
	}
	return paths
}

// --- helpers ---------------------------------------------------------------------------

// mustAgree checks that a replica and the store hold the same tree, node by node. It is the
// question the whole of metastore.Log exists to answer, so it compares every field a caller
// can observe rather than the names alone.
func mustAgree(t *testing.T, r *replica, s metastore.Store) {
	t.Helper()
	want := walkStore(t, s)
	got := r.tree(t)

	for path, node := range want {
		mirrored, held := got[path]
		if !held {
			t.Fatalf("the replica does not hold %q, which the store does", path)
		}
		if !sameNode(mirrored, node) {
			t.Fatalf("the replica holds %q as %+v, want %+v", path, mirrored, node)
		}
	}
	for path := range got {
		if _, held := want[path]; !held {
			t.Fatalf("the replica holds %q, which the store does not", path)
		}
	}
}

// walkStore reads the whole tree out of the store, by path.
func walkStore(t *testing.T, s metastore.Store) map[string]metastore.Node {
	t.Helper()
	paths := map[string]metastore.Node{}
	var walk func(path string)
	walk = func(path string) {
		node := mustStat(t, s, path)
		paths[path] = node
		if !node.IsDir() {
			return
		}
		for _, child := range mustList(t, s, path) {
			under := string(child.Name)
			if path != "" {
				under = path + "/" + under
			}
			walk(under)
		}
	}
	walk("")
	return paths
}

// sameNode compares two nodes the way a caller observes them. The times go through Equal
// rather than ==, because two instants that are the same moment may carry different monotonic
// readings and different locations.
func sameNode(a, b metastore.Node) bool {
	return a.ID == b.ID && a.Mode == b.Mode && a.Size == b.Size &&
		a.AccessTime.Equal(b.AccessTime) && a.ModTime.Equal(b.ModTime) && a.Content == b.Content
}

// drain reads every change after a position, and fails if the log no longer holds them or if
// it stops before its own tail. A case that meant to read the whole of a run and silently got
// the first page of it would otherwise pass while checking a fraction of what it named.
func drain(t *testing.T, s metastore.Store, after metastore.Position) []metastore.Change {
	t.Helper()
	var all []metastore.Change
	for {
		changes, retention, err := s.Since(ctx(t), after, 64)
		mustSucceed(t, err)
		if retention.TrimmedThrough > after {
			t.Fatalf("the log has discarded through %d, so a caller at %d has lost changes it needed", retention.TrimmedThrough, after)
		}
		if len(changes) == 0 {
			if after != retention.Tail {
				t.Fatalf("the log offers nothing after position %d but its tail is %d", after, retention.Tail)
			}
			return all
		}
		all = append(all, changes...)
		after = changes[len(changes)-1].Position
	}
}

// build makes a tree with enough shape in it that paging, renaming and depth all have
// something to act on.
func build(t *testing.T, s metastore.Store) {
	t.Helper()
	mustSucceed(t, s.Mkdir(ctx(t), "a"))
	mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
	mustSucceed(t, s.Mkdir(ctx(t), "a/b/c"))
	mustSucceed(t, s.Create(ctx(t), "a/b/c/deep"))
	mustSucceed(t, s.Create(ctx(t), "a/shallow"))
	mustSucceed(t, s.Mkdir(ctx(t), "empty"))
	put(t, s, "sized", 4096)
	// A name that is not valid UTF-8 is still a name, and a picture that renamed one on the
	// way through would have lost the file it described.
	mustSucceed(t, s.Create(ctx(t), string([]byte{0xff, 0xfe})))
	changed := time.Date(2400, 6, 1, 12, 0, 0, 500000000, time.UTC)
	mustSucceed(t, s.SetAttr(ctx(t), "a/b", storage.AttrChange{Mode: mode(0o700), ModTime: &changed}))
}

// name builds a distinct path for a step of a case that writes while a picture is open.
func name(prefix string, step int) string {
	return fmt.Sprintf("%s-%d", prefix, step)
}

func mustStat(t *testing.T, s metastore.Store, path string) metastore.Node {
	t.Helper()
	node, err := s.Stat(ctx(t), path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return node
}

// kinds renders the kinds of a run of changes, for a failure message.
func kinds(changes []metastore.Change) []metastore.ChangeKind {
	got := make([]metastore.ChangeKind, 0, len(changes))
	for _, c := range changes {
		got = append(got, c.Kind)
	}
	return got
}
