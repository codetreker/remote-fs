package replicated_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/locked"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// TestAWalkOfACopiedTreeAsksTheServerNothing is what the whole feature is for, and it is a
// count rather than an impression.
//
// A mount that keeps no metadata turns every one of these calls into a request: one per
// listing, one per stat, and one for every name a path search asks about and does not find.
// That is what makes a toolchain unusable over a 20 ms link, and the number below is the
// evidence that it is no longer what happens.
func TestAWalkOfACopiedTreeAsksTheServerNothing(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mkdir(t, s, "src")
	mkdir(t, s, "src/inner")
	write(t, s, "src/a.go", "package a\n")
	write(t, s, "src/inner/b.go", "package b\n")
	write(t, s, "README", "hello\n")

	mounted, _ := mount(t, s)

	before := s.calls.snapshot()
	walked := walkThrough(t, mounted)
	if len(walked) != 5 {
		t.Fatalf("the walk saw %v, want the five nodes the namespace holds", walked)
	}
	// Every name that is not there is asked about too, which is the shape of a path search
	// and the case a copy answers without the server ever hearing about it.
	for _, absent := range []string{"src/missing.go", "src/inner/missing.go", "missing"} {
		if _, err := mounted.Stat(t.Context(), absent); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %q gave %v, want ENOENT from the copy", absent, err)
		}
	}

	if arrived := s.calls.since(before); arrived != "" {
		t.Fatalf("walking a copied tree sent %s to the server, and the point of the copy is that it sends nothing", arrived)
	}
	t.Logf("walked %d nodes and asked about 3 absent names: %d requests", len(walked), s.calls.total()-total(before))
}

// TestTheCopyIsTheNamespaceNodeForNode. A walk that asks nothing is worth nothing unless what
// it answers is what the namespace holds, read from the namespace's own metastore rather than
// from anything under test.
func TestTheCopyIsTheNamespaceNodeForNode(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mkdir(t, s, "d")
	write(t, s, "d/f", "contents")
	write(t, s, "g", "")

	_, replica := mount(t, s)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAChangeMadeElsewhereReachesTheCopyWithNoIntervalToWaitFor is R-CON-1 and R-CON-2
// together: what a second machine writes becomes visible here, and not because anything came
// round again.
//
// The delay is measured rather than slept through. Nothing in this system polls, so the
// interval between the write landing on the server and the copy holding it is the network's
// own; a implementation that waited for a period would report a delay near that period, and
// this fails long before a second (R-CON-1) has passed.
func TestAChangeMadeElsewhereReachesTheCopyWithNoIntervalToWaitFor(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	write(t, s, "a.txt", "first")
	mounted, _ := mount(t, s)

	write(t, s, "b.txt", "second")
	written := time.Now()

	var visible time.Duration
	for {
		_, err := mounted.Stat(t.Context(), "b.txt")
		visible = time.Since(written)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat b.txt through the copy: %v", err)
		}
		if visible >= time.Second {
			t.Fatalf("a file written on the server was not visible in the copy after %v; R-CON-1 allows one second", visible)
		}
	}
	t.Logf("written on the server → visible in the copy: %v", visible)
}

// TestAWriteIsVisibleToTheStatThatFollowsIt is R-CON-4, which is the requirement a copy is
// most likely to break: the write goes to the server, its event comes back on the stream, and
// a caller that read the copy in between would be told the file's previous size and previous
// modification time.
//
// It is stated as "immediately" rather than "within a second", so there is nothing to wait
// for here and nothing to retry: one write, one stat, one answer.
func TestAWriteIsVisibleToTheStatThatFollowsIt(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)

	for _, content := range []string{"first", "a second version, considerably longer", "third"} {
		before := time.Now()
		if err := mounted.Write(t.Context(), "a.txt", []byte(content)); err != nil {
			t.Fatalf("writing %q: %v", content, err)
		}
		attr, err := mounted.Stat(t.Context(), "a.txt")
		if err != nil {
			t.Fatalf("stat a.txt straight after writing %q: %v", content, err)
		}
		if attr.Size != int64(len(content)) {
			t.Fatalf("the copy reports a.txt as %d bytes straight after %d were written", attr.Size, len(content))
		}
		if attr.ModTime.Before(before) {
			t.Fatalf("the copy reports a.txt as modified at %v, before the write that happened at %v",
				attr.ModTime, before)
		}
	}

	// The same for the operations that make and unmake names: what the caller did has to be
	// what the next call sees.
	if err := mounted.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir d: %v", err)
	}
	if attr, err := mounted.Stat(t.Context(), "d"); err != nil || !attr.IsDir() {
		t.Fatalf("stat d straight after making it gave %+v, %v", attr, err)
	}
	if err := mounted.Create(t.Context(), "d/f"); err != nil {
		t.Fatalf("create d/f: %v", err)
	}
	if entries, err := mounted.List(t.Context(), "d"); err != nil || len(entries) != 1 {
		t.Fatalf("listing d straight after creating d/f gave %v, %v", entries, err)
	}
	if err := mounted.Remove(t.Context(), "d/f"); err != nil {
		t.Fatalf("removing d/f: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "d/f"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat d/f straight after removing it gave %v, want ENOENT", err)
	}
	if err := mounted.Rename(t.Context(), "a.txt", "b.txt"); err != nil {
		t.Fatalf("renaming a.txt: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "b.txt"); err != nil {
		t.Fatalf("stat b.txt straight after renaming onto it: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "a.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat a.txt straight after renaming it away gave %v, want ENOENT", err)
	}
}

// TestARenameOntoAnOccupiedNameIsNeverSeenAsAGap. A rename that replaces something is two
// changes — the destination emptied, then the node arriving there — and a caller released by
// the first of them would stat the destination and be told nothing is there.
func TestARenameOntoAnOccupiedNameIsNeverSeenAsAGap(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)

	for round := range 20 {
		if err := mounted.Write(t.Context(), "from", []byte(strings.Repeat("x", round+1))); err != nil {
			t.Fatalf("writing from: %v", err)
		}
		if err := mounted.Write(t.Context(), "onto", []byte("displaced")); err != nil {
			t.Fatalf("writing onto: %v", err)
		}
		if err := mounted.Rename(t.Context(), "from", "onto"); err != nil {
			t.Fatalf("renaming from onto: %v", err)
		}
		attr, err := mounted.Stat(t.Context(), "onto")
		if err != nil {
			t.Fatalf("round %d: stat onto straight after the rename: %v", round, err)
		}
		if attr.Size != int64(round+1) {
			t.Fatalf("round %d: the copy reports onto as %d bytes, and what was moved there holds %d",
				round, attr.Size, round+1)
		}
	}
}

// TestADirectoryRenameMovesTheSubtreeWithoutAskingForItAgain is the case that decided the
// shape of a change: a directory rename is one row in the log and one row here, so the nodes
// beneath it keep their identities and none of them is fetched again.
//
// An event that only said "something under this name changed" would force the copy to discard
// the whole subtree and walk it again, and renaming directories is what build tools, version
// control and package managers do constantly.
func TestADirectoryRenameMovesTheSubtreeWithoutAskingForItAgain(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mkdir(t, s, "before")
	mkdir(t, s, "before/inner")
	write(t, s, "before/a", "a")
	write(t, s, "before/inner/b", "b")

	mounted, replica := mount(t, s)
	beneath := map[string]int64{}
	for _, n := range walkCopy(t, replica) {
		beneath[n.Path] = n.ID
	}

	before := s.calls.snapshot()
	if err := mounted.Rename(t.Context(), "before", "after"); err != nil {
		t.Fatalf("renaming the directory: %v", err)
	}
	after := map[string]int64{}
	for _, n := range walkCopy(t, replica) {
		after[n.Path] = n.ID
	}

	for _, moved := range []struct{ was, is string }{
		{"before", "after"}, {"before/a", "after/a"},
		{"before/inner", "after/inner"}, {"before/inner/b", "after/inner/b"},
	} {
		if after[moved.is] == 0 {
			t.Fatalf("the copy does not hold %q after the rename", moved.is)
		}
		if after[moved.is] != beneath[moved.was] {
			t.Fatalf("%q was node %d before the rename and %q is node %d after it; the subtree was rebuilt rather than moved",
				moved.was, beneath[moved.was], moved.is, after[moved.is])
		}
	}
	if _, err := mounted.Stat(t.Context(), "before"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat of the old name gave %v, want ENOENT", err)
	}

	// One request, and it is the rename itself. Nothing beneath was asked about again.
	if arrived := s.calls.since(before); arrived != "rename×1" {
		t.Fatalf("renaming a directory sent %q to the server, want the rename alone", arrived)
	}
}

func total(counts map[string]int) int {
	sum := 0
	for _, count := range counts {
		sum += count
	}
	return sum
}

// TestAnEventChannelThatBrokeMakesEveryOperationFailRatherThanAnswer is the test that matters
// more than every one above it.
//
// The copy is worth exactly what the stream behind it is worth. Once that stream is gone,
// what is here is a picture of a moment that has passed, and there is nothing that says how
// long ago: a listing served from it says a directory holds these names and no others, and a
// stat says a file is not there. Whatever runs on top acts on both — it regenerates, it
// propagates the deletion, it overwrites — and there is no way to notice afterwards
// (R-ERR-1, R-ERR-2).
//
// Every operation gets a case of its own, because each of them resolves its own failure and
// one standing in for the rest is a bet that the other ten were written by somebody thinking
// the same thing.
func TestAnEventChannelThatBrokeMakesEveryOperationFailRatherThanAnswer(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mkdir(t, s, "existing")
	write(t, s, "a.txt", "hello")

	mounted, replica := mount(t, s)
	if _, err := mounted.Stat(t.Context(), "a.txt"); err != nil {
		t.Fatalf("stat a.txt while the stream is alive: %v", err)
	}

	s.events.cut()
	requireUnusable(t, mounted)

	t.Run("a listing fails rather than coming back empty", func(t *testing.T) {
		entries, err := mounted.List(t.Context(), "")
		if err == nil {
			t.Fatalf("listing succeeded with %d entries; this copy is no longer being fed and that answer is invented", len(entries))
		}
		requireErrno(t, "listing the root", err, syscall.EIO)
		t.Logf("List: %v", err)
	})
	t.Run("a stat of a name that is there does not come back as absent", func(t *testing.T) {
		_, err := mounted.Stat(t.Context(), "a.txt")
		requireErrno(t, "stat of a name that exists", err, syscall.EIO)
		t.Logf("Stat: %v", err)
	})

	for _, c := range []struct {
		name string
		run  func() error
	}{
		{"stat of a name that does not exist", func() error { _, err := mounted.Stat(t.Context(), "absent"); return err }},
		{"reading a file", func() error { _, err := mounted.Read(t.Context(), "a.txt"); return err }},
		{"writing a file", func() error { return mounted.Write(t.Context(), "a.txt", []byte("x")) }},
		{"creating a file", func() error { return mounted.Create(t.Context(), "new.txt") }},
		{"making a directory", func() error { return mounted.Mkdir(t.Context(), "d") }},
		{"removing a file", func() error { return mounted.Remove(t.Context(), "a.txt") }},
		{"removing a directory", func() error { return mounted.RemoveDir(t.Context(), "existing") }},
		{"renaming", func() error { return mounted.Rename(t.Context(), "a.txt", "b.txt") }},
		{"changing attributes", func() error {
			mode := os.FileMode(0o600)
			return mounted.SetAttr(t.Context(), "a.txt", storage.AttrChange{Mode: &mode})
		}},
		{"asking how much room there is", func() error { _, err := mounted.Space(t.Context()); return err }},
	} {
		t.Run(c.name+" fails", func(t *testing.T) {
			requireErrno(t, c.name, c.run(), syscall.EIO)
			t.Logf("%v", c.run())
		})
	}

	// And it comes back. A stream that can be picked up where it left off is what makes the
	// refusals above a pause rather than the end of the mount.
	write(t, s, "while-away.txt", "arrived while the copy was not being fed")
	s.events.mend()
	t.Logf("unusable → holding what it missed: %v", requireHolding(t, mounted, "while-away.txt"))
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAStreamPickedUpAgainIsResumedRatherThanRebuilt. Rebuilding costs a scan of the whole
// tree, so a break that the log can still cover must not cause one — and the evidence is that
// no second picture was asked for.
func TestAStreamPickedUpAgainIsResumedRatherThanRebuilt(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	write(t, s, "before.txt", "1")

	mounted, replica := mount(t, s)
	if pictures := s.calls.of(httprest.OpSnapshot); pictures != 1 {
		t.Fatalf("building the copy took %d pictures of the tree, want one", pictures)
	}

	s.events.cut()
	requireUnusable(t, mounted)
	write(t, s, "while-away.txt", "2")
	s.events.mend()

	requireHolding(t, mounted, "while-away.txt")
	if pictures := s.calls.of(httprest.OpSnapshot); pictures != 1 {
		t.Fatalf("picking the stream up again took %d pictures of the tree; the log could still supply what was missed", pictures)
	}
	if resumed := s.calls.of(httprest.OpResubscribe); resumed == 0 {
		t.Fatal("the stream came back without a resume, so it was not picked up where it left off")
	}
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestALogThatCannotCarryOnMakesTheCopyBeBuiltAgain. When the log answers that it cannot
// supply what the copy is missing, everything here is a copy of nothing: the changes between
// what it holds and what the log still has are gone, and no change arriving afterwards would
// put them back. The one honest answer is an expensive rebuild.
func TestALogThatCannotCarryOnMakesTheCopyBeBuiltAgain(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	write(t, s, "before.txt", "1")

	mounted, replica := mount(t, s)
	s.events.cut()
	requireUnusable(t, mounted)

	// The namespace moves on while the copy is not being fed, and the log then refuses to
	// carry on from where the copy stands.
	write(t, s, "while-away.txt", "2")
	s.events.refuseResume(true)
	s.events.mend()

	requireHolding(t, mounted, "while-away.txt")
	if pictures := s.calls.of(httprest.OpSnapshot); pictures != 2 {
		t.Fatalf("the log refused to carry on and the copy took %d pictures of the tree in all, want two", pictures)
	}
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestANamespaceWrittenToThroughoutThePictureIsCopiedExactly is the acceptance test for the
// picture being a consistent cut.
//
// The scan takes time, and the tree changes while it runs. What makes that safe is that the
// picture is one instant and carries the position of that instant, so every change recorded
// after it is applied on top and every change recorded before it is already in it. A picture
// stamped with a position newer than itself — the tempting implementation — would have the
// copy discard the very changes that would have corrected the nodes scanned early, and
// nothing afterwards would ever correct them.
func TestANamespaceWrittenToThroughoutThePictureIsCopiedExactly(t *testing.T) {
	limits := httprest.DefaultLimits()
	// One row per frame, and every frame held back, so that the picture takes long enough for
	// the writer below to get a good many changes in during it.
	limits.SnapshotPage = 1
	s := serve(t, limits)
	s.events.slowSnapshot(2 * time.Millisecond)

	for i := range 20 {
		write(t, s, fmt.Sprintf("before-%02d.txt", i), "before the picture")
	}

	writing := make(chan struct{})
	written := make(chan int, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			select {
			case <-writing:
				written <- count
				return
			default:
			}
			if err := s.elsewhere.Write(t.Context(), fmt.Sprintf("during-%03d.txt", count), []byte("during the picture")); err != nil {
				t.Errorf("writing during the picture: %v", err)
				written <- count
				return
			}
			count++
		}
	}()

	duringMount := s.calls.of(httprest.OpWrite)
	_, replica := mount(t, s)
	duringMount = s.calls.of(httprest.OpWrite) - duringMount

	close(writing)
	wg.Wait()
	total := <-written

	// A run in which nothing was written while the picture was being taken would pass this
	// test without testing anything, so it says so instead.
	if duringMount == 0 {
		t.Fatalf("no write landed while the picture was being taken, so this run proves nothing (%d writes in all)", total)
	}
	t.Logf("%d of %d writes landed while the picture was being taken", duringMount, total)

	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestTheMountFailsWhenThePictureCannotBeTaken. There is no degraded mode: a copy that could
// not be built is a mount that does not come up, and the failure says which step failed
// because "it did not come up" is not something an operator can act on.
func TestTheMountFailsWhenThePictureCannotBeTaken(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	write(t, s, "a.txt", "hello")
	s.events.refuseSnapshot(true)

	err := buildFailure(t, s)
	requireErrno(t, "building the copy", err, syscall.EIO)
	if !strings.Contains(err.Error(), "picture") {
		t.Fatalf("building the copy failed with %v, and it does not say that taking the picture is the step that failed", err)
	}
	t.Logf("%v", err)
}

// TestTheMountFailsWhenTheNamespaceCannotBeWatched, which is the other step, and it has to be
// distinguishable from the one above.
func TestTheMountFailsWhenTheNamespaceCannotBeWatched(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	s.events.cut()

	err := buildFailure(t, s)
	requireErrno(t, "building the copy", err, syscall.EIO)
	if !strings.Contains(err.Error(), "watching") {
		t.Fatalf("building the copy failed with %v, and it does not say that watching for changes is the step that failed", err)
	}
	t.Logf("%v", err)
}

// TestANamespaceThatKeepsNoLogRefusesToBeCopied, under its own errno.
//
// A namespace without a change log answers ENOSYS, allowing a mount without a copy.
// EIO instead reports a namespace that cannot currently be reached. The fixture exposes
// only the enforcing backend interface so optional log capabilities cannot enable copying.
func TestANamespaceThatKeepsNoLogRefusesToBeCopied(t *testing.T) {
	_, namespace := memoryfixture.New(t, "ws", 0, locking.DefaultOptions())
	backing := struct{ locked.Backend }{namespace}
	handler, err := httprest.NewHandler(backing, nil)
	if err != nil {
		t.Fatalf("building the handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	replica, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	defer replica.Close()

	remote, err := httprest.Dial(server.URL, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dialling the namespace: %v", err)
	}
	mounted, err := replicated.New(t.Context(), replica, remote)
	if err == nil {
		mounted.Close()
		t.Fatal("a copy was built of a namespace that keeps no log, which is a claim that it can be kept current")
	}
	if !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("building a copy of a namespace with no log failed with %v, want ENOSYS", err)
	}
	if errors.Is(err, syscall.EIO) {
		t.Fatalf("building a copy of a namespace with no log failed with %v, which reads as a server that could not be reached", err)
	}
	t.Logf("%v", err)
}

// TestAStreamThatStopsArrivingStopsTheCopyBeingAnsweredFrom is the failure with no event to
// it, and the one a copy is most dangerous under.
//
// Everything else that goes wrong here arrives as something: a stream that ends, a status
// that refuses, a connection that is reset. A flow that is simply no longer carried — a
// machine that vanished, a firewall that dropped an idle connection, a partition — arrives
// as nothing at all, and nothing at all is exactly what a namespace that nobody is writing
// to looks like. A mount that could not tell those apart would go on answering `Stat` and
// `List` from a copy that stopped being fed, with no bound on how long: not until some probe
// window elapsed, but for as long as the mount lived (R-ERR-1, R-ERR-2).
//
// What makes them distinguishable is the server saying it is still there when it has nothing
// else to say, and this side giving up on a stream that has said nothing at all. Both are
// configurable and both are wound right down here so that the test is quick; the defaults
// and the relationship between them are asserted in packages/transport/httprest.
func TestAStreamThatStopsArrivingStopsTheCopyBeingAnsweredFrom(t *testing.T) {
	const silence = 200 * time.Millisecond

	limits := httprest.DefaultLimits()
	limits.Keepalive = 20 * time.Millisecond
	s := serve(t, limits)
	mkdir(t, s, "existing")
	write(t, s, "a.txt", "hello")

	relay := s.interpose(t, silence)
	mounted, replica := mount(t, s)
	if _, err := mounted.Stat(t.Context(), "a.txt"); err != nil {
		t.Fatalf("stat a.txt while the stream is being carried: %v", err)
	}

	// The bytes stop. Nothing is closed, nothing is refused, and the server is still there
	// and still being written to by somebody else.
	relay.freeze()
	started := time.Now()
	requireUnusable(t, mounted)
	t.Logf("bytes stopped arriving → the copy stopped being answered from after %v (allowed %v)",
		time.Since(started), silence)

	t.Run("a listing fails rather than coming back empty", func(t *testing.T) {
		entries, err := mounted.List(t.Context(), "")
		if err == nil {
			t.Fatalf("listing succeeded with %d entries; the stream stopped arriving and that answer is invented", len(entries))
		}
		requireErrno(t, "listing the root", err, syscall.EIO)
		t.Logf("List: %v", err)
	})
	t.Run("a stat of a name that is there does not come back as absent", func(t *testing.T) {
		_, err := mounted.Stat(t.Context(), "a.txt")
		requireErrno(t, "stat of a name that exists", err, syscall.EIO)
		t.Logf("Stat: %v", err)
	})
	t.Run("a stat of a name that is not there fails rather than answering", func(t *testing.T) {
		_, err := mounted.Stat(t.Context(), "absent")
		requireErrno(t, "stat of a name that does not exist", err, syscall.EIO)
	})

	// The namespace moves on meanwhile, which is what makes the answers above wrong rather
	// than merely unjustified: a copy answering from what it holds would be answering about
	// a tree that no longer exists.
	write(t, s, "arrived-while-cut-off.txt", "written while the mount could not hear")
	if _, err := mounted.Stat(t.Context(), "existing"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the copy answered about a name while the stream was not arriving: %v", err)
	}

	// And it comes back: the flow is carried again, the mount picks the stream up, and what
	// it missed is there.
	relay.thaw()
	t.Logf("carried again → holding what it missed: %v", requireHolding(t, mounted, "arrived-while-cut-off.txt"))
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAPictureMayTakeLongerThanTheStreamIsAllowedToBeQuiet.
//
// While the picture is being taken the change stream is not read at all: what arrives on it
// queues on the connection and is read once the picture is in place. The bound on a quiet
// stream is therefore a bound on a read that is waiting, not on the connection — a mount
// that timed the connection out would fail every first sync of a tree big enough to take
// longer than the bound, which is precisely the trees this feature exists for.
func TestAPictureMayTakeLongerThanTheStreamIsAllowedToBeQuiet(t *testing.T) {
	const silence = 200 * time.Millisecond

	limits := httprest.DefaultLimits()
	limits.Keepalive = 20 * time.Millisecond
	limits.SnapshotPage = 1
	s := serve(t, limits)
	for i := range 60 {
		write(t, s, fmt.Sprintf("f%02d.txt", i), "contents")
	}
	// One row per frame, each held back, so that the picture takes several times as long as
	// the stream beside it is allowed to say nothing.
	s.events.slowSnapshot(5 * time.Millisecond)
	s.interpose(t, silence)

	started := time.Now()
	mounted, replica := mount(t, s)
	took := time.Since(started)

	if took < silence {
		t.Fatalf("the picture took %v, which is less than the %v a stream may be quiet for, so this run proves nothing", took, silence)
	}
	if _, err := mounted.Stat(t.Context(), "f00.txt"); err != nil {
		t.Fatalf("stat through a copy built by a picture that took %v: %v", took, err)
	}
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
	t.Logf("the picture took %v, and the stream beside it was quiet for all of it (allowed %v)", took, silence)
}

// TestACallerThatCannotConfirmItsChangeLeavesTheMountWorking.
//
// A mutation waits for the copy to reach the returned barrier, and it gives up after a while.
// What it gives up on is that one call: the change happened, this side could not confirm it in time,
// and it says so. What it must not do is declare the stream broken — a stream working through
// a backlog looks exactly like this from here, and a mount that broke its own healthy stream
// would reconnect to the same backlog and break it again. Worse, nothing would put it back:
// the goroutine following the stream is inside a read that has not returned, so it never
// reaches the reconnect that is the only thing that clears the failure, and the mount answers
// EIO for the rest of its life.
func TestACallerThatCannotConfirmItsChangeLeavesTheMountWorking(t *testing.T) {
	const grace = 2 * time.Second

	s := serve(t, httprest.DefaultLimits())
	write(t, s, "before.txt", "here before the mount")
	gate := s.events.gateEvents()
	defer gate.release()

	mounted, replica := mountWithGrace(t, s, grace)
	gate.arm()

	result := make(chan error, 1)
	go func() { result <- mounted.Write(t.Context(), "a.txt", []byte("written while the stream is slow")) }()
	gate.requireEntered(t)
	err := <-result
	if err == nil {
		t.Fatal("the write reported success, and its change had not come back")
	}
	requireErrno(t, "a write whose change could not be confirmed", err, syscall.EIO)
	if !strings.Contains(err.Error(), "could not confirm") {
		t.Fatalf("the write failed with %v, which does not say that the change was made and could not be confirmed", err)
	}
	t.Logf("Write: %v", err)

	// The mount is still a mount. This is the assertion the whole test is for: everything
	// below fails, forever, if giving up on one confirmation is taken for a broken stream.
	if _, err := mounted.Stat(t.Context(), "before.txt"); err != nil {
		t.Fatalf("stat after a write that could not be confirmed: %v", err)
	}
	if entries, err := mounted.List(t.Context(), ""); err != nil {
		t.Fatalf("listing after a write that could not be confirmed: %v", err)
	} else if len(entries) == 0 {
		t.Fatal("the listing came back empty")
	}

	// And the change it could not confirm was real, and arrives.
	gate.release()
	t.Logf("unconfirmed → held by the copy: %v", requireHolding(t, mounted, "a.txt"))
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))

	// Confirmation must also remain usable after the stream reconnects.
	s.events.cut()
	requireUnusable(t, mounted)
	s.events.mend()
	requireHolding(t, mounted, "a.txt")

	if err := mounted.Write(t.Context(), "b.txt", []byte("after")); err != nil {
		t.Fatalf("writing on a stream that is not being held back: %v", err)
	}
	if attr, err := mounted.Stat(t.Context(), "b.txt"); err != nil || attr.Size != 5 {
		t.Fatalf("stat straight after a confirmed write gave %+v, %v", attr, err)
	}
}

func TestConfirmationAdmissionRefusesBeforeSendingAndReleasesCapacity(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	gate := s.events.gateEvents()
	defer gate.release()
	options := replicated.DefaultOptions()
	options.ConfirmationGrace = 5 * time.Second
	options.MaxActiveConfirmations = 1
	options.MaxWaitingConfirmations = 0
	mounted, _ := mountWithOptions(t, s, options)
	gate.arm()

	at, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatalf("reading the starting position: %v", err)
	}
	first := make(chan error, 1)
	go func() { first <- mounted.Create(t.Context(), "first") }()
	requireRecordedPast(t, s, at)
	gate.requireEntered(t)

	before := s.calls.of(httprest.OpCreate)
	if err := mounted.Create(t.Context(), "second"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a mutation beyond the active bound returned %v, want EAGAIN", err)
	}
	if after := s.calls.of(httprest.OpCreate); after != before {
		t.Fatalf("the refused mutation reached the server: create calls moved from %d to %d", before, after)
	}
	if _, err := s.meta.Stat(t.Context(), "second"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the pre-send refusal changed the namespace: %v", err)
	}
	gate.release()
	if err := <-first; err != nil {
		t.Fatalf("the admitted mutation failed: %v", err)
	}
	if err := mounted.Create(t.Context(), "second"); err != nil {
		t.Fatalf("released confirmation capacity was not reusable: %v", err)
	}
}

func TestInvalidMutationPathsAreRejectedBeforeConfirmationAdmission(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	options := replicated.DefaultOptions()
	mounted, _ := mountWithOptions(t, s, options)

	before := s.calls.of(httprest.OpCreate)
	for _, invalid := range []string{"/absolute-path", "../escaping-path"} {
		if err := mounted.Create(t.Context(), invalid); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("Create(%q) returned %v, want EINVAL", invalid, err)
		} else if errors.Is(err, syscall.EAGAIN) {
			t.Errorf("Create(%q) reported confirmation admission saturation for an invalid path: %v", invalid, err)
		}
	}
	if after := s.calls.of(httprest.OpCreate); after != before {
		t.Fatalf("invalid paths reached the server: create calls moved from %d to %d", before, after)
	}
}

func TestCancelledConfirmationAdmissionWaiterNeverSendsAMutation(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	gate := s.events.gateEvents()
	defer gate.release()
	options := replicated.DefaultOptions()
	options.ConfirmationGrace = 5 * time.Second
	options.MaxActiveConfirmations = 1
	options.MaxWaitingConfirmations = 1
	mounted, _ := mountWithOptions(t, s, options)
	gate.arm()

	at, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatalf("reading the starting position: %v", err)
	}
	first := make(chan error, 1)
	go func() { first <- mounted.Create(t.Context(), "active") }()
	requireRecordedPast(t, s, at)
	gate.requireEntered(t)

	ctx, cancel := context.WithCancel(t.Context())
	waiting := make(chan error, 1)
	go func() { waiting <- mounted.Create(ctx, "cancelled") }()
	time.Sleep(20 * time.Millisecond)
	before := s.calls.of(httprest.OpCreate)
	if before != 1 {
		t.Fatalf("the waiting mutation reached the server before admission: create calls=%d", before)
	}
	cancel()
	if err := <-waiting; !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EINTR) || storage.ErrnoOf(err) != syscall.EINTR {
		t.Fatalf("the cancelled pre-send waiter returned %v, want cancellation classified EINTR", err)
	}
	if after := s.calls.of(httprest.OpCreate); after != before {
		t.Fatalf("the cancelled waiter reached the server: create calls moved from %d to %d", before, after)
	}
	gate.release()
	if err := <-first; err != nil {
		t.Fatalf("the active mutation failed: %v", err)
	}
}

func TestCancellationAfterServerSuccessReportsAmbiguousEIOAndReleasesCapacity(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	gate := s.events.gateEvents()
	defer gate.release()
	options := replicated.DefaultOptions()
	options.ConfirmationGrace = 5 * time.Second
	options.MaxActiveConfirmations = 1
	options.MaxWaitingConfirmations = 0
	mounted, _ := mountWithOptions(t, s, options)
	gate.arm()

	at, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatalf("reading the starting position: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mounted.Create(ctx, "committed") }()
	requireRecordedPast(t, s, at)
	gate.requireEntered(t)
	cancel()
	if err := <-done; !errors.Is(err, syscall.EIO) {
		t.Fatalf("cancellation after server success returned %v, want EIO", err)
	}
	if _, err := s.meta.Stat(t.Context(), "committed"); err != nil {
		t.Fatalf("the mutation reported as ambiguous did not reach the namespace: %v", err)
	}

	gate.release()
	if err := mounted.Create(t.Context(), "after-cancel"); err != nil {
		t.Fatalf("the cancelled confirmation did not release its capacity: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "committed"); err != nil {
		t.Fatalf("the replica was invalidated by a caller cancellation: %v", err)
	}
}

// TestACopyIsNotAnsweredFromWhileItIsCatchingUp.
//
// Getting the stream back is not the same as being current again. The log says how far it had
// reached when the stream was picked up, and everything between what the copy holds and that
// is on its way but not here — so a copy answered from in between reports what a name held
// before somebody else changed it, as fact. That is the stale answer R-ERR-1 and R-ERR-2
// forbid, and it differs from ordinary steady state in the one way that matters: the gap is
// known, not merely possible.
func TestACopyIsNotAnsweredFromWhileItIsCatchingUp(t *testing.T) {
	const stale = "old"
	const current = "a great deal newer, and a different length"

	s := serve(t, httprest.DefaultLimits())
	write(t, s, "watched.txt", stale)
	mounted, replica := mount(t, s)
	if attr, err := mounted.Stat(t.Context(), "watched.txt"); err != nil || attr.Size != int64(len(stale)) {
		t.Fatalf("stat before the outage gave %+v, %v", attr, err)
	}

	s.events.cut()
	requireUnusable(t, mounted)

	// The namespace moves on, and the last thing it does is change the name being watched.
	for i := range 5 {
		write(t, s, fmt.Sprintf("during-%d.txt", i), "written during the outage")
	}
	write(t, s, "watched.txt", current)

	// Slowly enough that catching up is something a caller can be caught in the middle of.
	s.events.slowEvents(100 * time.Millisecond)
	s.events.mend()

	// The first answer that is not a refusal has to be the current one. A copy that believed
	// itself the moment it was attached again would answer with what it holds, which is what
	// the name held before the outage.
	refusals := 0
	deadline := time.Now().Add(10 * time.Second)
	for {
		attr, err := mounted.Stat(t.Context(), "watched.txt")
		if err == nil {
			if attr.Size != int64(len(current)) {
				t.Fatalf("the copy answered with %d bytes while it was still catching up; the name holds %d",
					attr.Size, len(current))
			}
			break
		}
		requireErrno(t, "stat while the copy is catching up", err, syscall.EIO)
		refusals++
		if time.Now().After(deadline) {
			t.Fatal("the copy never came back")
		}
		time.Sleep(time.Millisecond)
	}
	// A run in which the copy was never behind proves nothing about what it does when it is.
	if refusals == 0 {
		t.Fatal("the copy answered on the first attempt, so it was never seen catching up")
	}
	if resumed := s.calls.of(httprest.OpResubscribe); resumed == 0 {
		t.Fatal("the stream came back without a resume, so this is not the catching-up path")
	}
	t.Logf("refused %d times while catching up, then answered with the current contents", refusals)

	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAChangeToTheRootIsConfirmedLikeAnyOther.
//
// The root is the one node a log names differently: it has no parent and no name, so a change
// to it arrives under parent zero and no name, while every other node arrives under the id of
// the directory holding it. A caller that named the root the way it names everything else
// would wait for an event that had already arrived, give up after its whole grace, and report
// EIO for a chmod of the mountpoint — an ordinary thing to do to a mountpoint.
func TestAChangeToTheRootIsConfirmedLikeAnyOther(t *testing.T) {
	const grace = 2 * time.Second

	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mountWithGrace(t, s, grace)

	mode := fs.FileMode(0o711)
	started := time.Now()
	if err := mounted.SetAttr(t.Context(), "", storage.AttrChange{Mode: &mode}); err != nil {
		t.Fatalf("changing the root's mode: %v", err)
	}
	took := time.Since(started)

	// It has to be confirmed by the event, not by the grace running out — and the grace here
	// is long enough that waiting it out is unmistakable.
	if took >= grace {
		t.Fatalf("changing the root's mode took %v, which is the whole grace: its barrier was not reached", took)
	}
	attr, err := mounted.Stat(t.Context(), "")
	if err != nil {
		t.Fatalf("stat of the root straight after changing its mode: %v", err)
	}
	if attr.Mode.Perm() != mode.Perm() {
		t.Fatalf("the copy reports the root as %v straight after it was set to %v", attr.Mode, mode)
	}
	t.Logf("the root's mode was changed and confirmed in %v", took)
}

// TestSameTargetReplayCannotConfirmBeforeTheMutationBarrier.
//
// A stream is attached before the picture is taken, so everything the picture already covered
// arrives afterwards and is discarded. Those frames must not move the applied position backwards.
// If they did, a later mutation at the same name could be confirmed before its barrier was reached,
// exposing the contents that preceded the mutation.
func TestSameTargetReplayCannotConfirmBeforeTheMutationBarrier(t *testing.T) {
	const written = "the contents this caller wrote, of a length nothing else here has"
	const frame = 10 * time.Millisecond

	s := serve(t, httprest.DefaultLimits())
	write(t, s, "hot.txt", "before the picture")
	// The picture is not taken until this test says so, and the stream is delivered slowly —
	// so everything written in between is replayed and discarded, and is still being
	// discarded while the write below waits.
	release := s.events.holdBackPicture()
	s.events.slowEvents(frame)

	// One name, changed over and over, and every one of those changes lands before the
	// picture is taken: nothing at that name is recorded after it, so what is replayed at
	// that name is entirely changes the copy already holds. There are more of them than the
	// stream carries in that time, so a good many are still on their way afterwards.
	// Recorded straight into the namespace rather than through a request, so that how many of
	// them there are does not depend on how fast requests happen to be: what has to be true is
	// that there are more of them than the stream carries before the write below is made. The
	// one request at the end is what tells the stream to read the log at all.
	const changes = 60
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer release()
		for s.calls.of(httprest.OpSubscribe) == 0 {
			time.Sleep(time.Millisecond)
		}
		for round := range changes {
			if err := s.storage.Write(t.Context(), "hot.txt", []byte(strings.Repeat("x", round%17+1))); err != nil {
				t.Errorf("writing before the picture: %v", err)
				return
			}
		}
		if err := s.elsewhere.Mkdir(t.Context(), "poke"); err != nil {
			t.Errorf("waking the stream: %v", err)
		}
	}()

	mounted, _ := mountWithGrace(t, s, 30*time.Second)
	wg.Wait()

	// Long enough that the replay is under way, so that a caller registering now reads
	// whatever the replay has done to the account of where the copy stands.
	time.Sleep(5 * frame)

	if err := mounted.Write(t.Context(), "hot.txt", []byte(written)); err != nil {
		t.Fatalf("writing hot.txt: %v", err)
	}

	attr, err := mounted.Stat(t.Context(), "hot.txt")
	if err != nil {
		t.Fatalf("stat hot.txt straight after writing it: %v", err)
	}
	if attr.Size != int64(len(written)) {
		t.Fatalf("the copy reports hot.txt as %d bytes straight after %d were written: the write was released by a change the copy discarded",
			attr.Size, len(written))
	}
	t.Log("the write returned only after its contents replaced the same-target replay in the copy")
}

// TestADirectoryRemovedThroughTheCopyIsGoneFromItAtOnce is R-CON-4 for the operations whose
// change empties a name rather than filling it.
//
// A barrier position confirms removals without interpreting whether a target name should be
// present or absent. Both the removed child and its parent must therefore be absent immediately
// after their calls return.
func TestADirectoryRemovedThroughTheCopyIsGoneFromItAtOnce(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, replica := mount(t, s)

	for _, at := range []string{"d", "d/inner"} {
		if err := mounted.Mkdir(t.Context(), at); err != nil {
			t.Fatalf("mkdir %s: %v", at, err)
		}
	}

	if err := mounted.RemoveDir(t.Context(), "d/inner"); err != nil {
		t.Fatalf("rmdir d/inner: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "d/inner"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat d/inner straight after removing it gave %v, want ENOENT", err)
	}
	if entries, err := mounted.List(t.Context(), "d"); err != nil || len(entries) != 0 {
		t.Fatalf("listing d straight after removing the only thing in it gave %v (%v)", entries, err)
	}

	// And the directory that held it, so that the name being emptied is one something else was
	// listing rather than a leaf at the bottom of the tree.
	if err := mounted.RemoveDir(t.Context(), "d"); err != nil {
		t.Fatalf("rmdir d: %v", err)
	}
	if entries, err := mounted.List(t.Context(), ""); err != nil || len(entries) != 0 {
		t.Fatalf("listing the root straight after removing the only thing in it gave %v (%v)", entries, err)
	}

	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAMutationThatRecordsNothingIsNotWaitedFor.
//
// Two operations succeed while the namespace records nothing: an attribute change that names no
// attribute, and a rename of a name onto itself, which POSIX has "return successfully and
// perform no other action". No event is coming for either, so a copy that waited for one would
// hold its caller for the whole grace and then report EIO for something that succeeded.
//
// Both are still sent, and the names that are not there are how that half is held: whether a
// name exists at all is the server's answer and never this copy's to invent.
func TestAMutationThatRecordsNothingIsNotWaitedFor(t *testing.T) {
	// Short enough that accidentally waiting for a no-op barrier is a prompt test failure.
	const grace = 2 * time.Second

	s := serve(t, httprest.DefaultLimits())
	write(t, s, "a.txt", "some contents")
	mkdir(t, s, "d")
	mounted, replica := mountWithGrace(t, s, grace)

	before, err := mounted.Stat(t.Context(), "a.txt")
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}

	if err := mounted.SetAttr(t.Context(), "a.txt", storage.AttrChange{}); err != nil {
		t.Fatalf("an attribute change that names no attribute: %v", err)
	}
	if err := mounted.SetAttr(t.Context(), "nowhere", storage.AttrChange{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an attribute change naming nothing, at a name that is not there, gave %v, want ENOENT: it is sent for exactly this answer", err)
	}

	// Three spellings of one name. What decides whether anything was recorded is what the paths
	// mean rather than how they were typed.
	for _, onto := range []string{"a.txt", "./a.txt", "d/../a.txt"} {
		if err := mounted.Rename(t.Context(), "a.txt", onto); err != nil {
			t.Fatalf("renaming a.txt onto %q, which is the same name: %v", onto, err)
		}
	}
	if err := mounted.Rename(t.Context(), "nowhere", "nowhere"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renaming a name that is not there onto itself gave %v, want ENOENT: it is sent for exactly this answer", err)
	}

	if after, err := mounted.Stat(t.Context(), "a.txt"); err != nil || after != before {
		t.Fatalf("a.txt is now %+v (%v), and nothing here changed it from %+v", after, err, before)
	}
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestAMutationTheServerRefusesIsReportedAsTheServerRefusedIt.
//
// A refused mutation never happened, so no event is coming and there is nothing to wait for.
// The refusal is the answer, and it has to arrive under the errno the namespace chose: a copy
// that swallowed it and waited would turn "that name is taken" into "this copy could not
// confirm it", which sends whoever reads it looking for a broken mount instead of for the file
// they tried to create — and would do it after a delay as long as the grace, every time.
func TestAMutationTheServerRefusesIsReportedAsTheServerRefusedIt(t *testing.T) {
	const grace = 2 * time.Second
	const allowance = 4096

	s := serveWithAllowance(t, httprest.DefaultLimits(), allowance)
	mounted, replica := mountWithGrace(t, s, grace)

	if err := mounted.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir d: %v", err)
	}
	if err := mounted.Create(t.Context(), "d/f"); err != nil {
		t.Fatalf("create d/f: %v", err)
	}

	for _, c := range []struct {
		what string
		do   func() error
		want syscall.Errno
	}{
		{"a name that is already taken", func() error { return mounted.Create(t.Context(), "d/f") }, syscall.EEXIST},
		{"a name that is not there", func() error { return mounted.Remove(t.Context(), "d/gone") }, syscall.ENOENT},
		{"a directory with something in it", func() error { return mounted.RemoveDir(t.Context(), "d") }, syscall.ENOTEMPTY},
		{"more bytes than the namespace may hold", func() error {
			return mounted.Write(t.Context(), "big", make([]byte, allowance+1))
		}, syscall.EDQUOT},
	} {
		t.Run(c.what, func(t *testing.T) {
			err := c.do()
			if errors.Is(err, syscall.EIO) {
				t.Fatalf("%s was answered with %v: EIO would replace the namespace's definitive refusal with a confirmation failure", c.what, err)
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("%s was answered with %v, want %v", c.what, err, c.want)
			}
		})
	}

	// Nothing a refusal touched is in the copy, and nothing it did not touch has gone missing.
	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}

// TestLosingReplicationWhileAMutationIsOutstandingNeverReportsSuccess.
//
// The response carrying the barrier and the stream carrying changes use different connections.
// Losing both after the server commits can make either side fail first, but neither ordering may
// report success without proof that the copy reached the barrier.
func TestLosingReplicationWhileAMutationIsOutstandingNeverReportsSuccess(t *testing.T) {
	const frame = 300 * time.Millisecond
	const grace = 3 * time.Second

	s := serve(t, httprest.DefaultLimits())
	write(t, s, "before.txt", "here before the mount")
	// Every frame held back for long enough that the change made below cannot come back before
	// the stream is cut, while the grace is ten times that: what ends the wait has to be the
	// stream going, and running out of patience has to be ruled out as the explanation.
	s.events.slowEvents(frame)
	mounted, _ := mountWithGrace(t, s, grace)

	at, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatalf("reading the position the namespace stands at: %v", err)
	}

	made := make(chan error, 1)
	go func() { made <- mounted.Mkdir(t.Context(), "d") }()

	// Cut only once the namespace itself holds the change. Before that the mutation has not
	// been sent, and what would be under test is the refusal to send it at all.
	//
	// The connections go with it. A stream merely told to end can still deliver the frame already
	// being written and reach the barrier, so cutting alone would race the failure arranged here.
	requireRecordedPast(t, s, at)
	started := time.Now()
	s.events.cut()
	s.sever()

	err = <-made
	took := time.Since(started)
	requireErrno(t, "a mutation that lost both its barrier response and change stream", err, syscall.EIO)
	if took >= grace {
		t.Fatalf("the caller was held for %v, which is the whole grace: it was told by the wait running out rather than by the stream going", took)
	}
	t.Logf("told in %v, of a grace of %v: %v", took.Round(time.Millisecond), grace, err)

	// And the change is real. The namespace holds it, read from the namespace's own tree rather
	// than through anything that just failed.
	if _, err := s.meta.Stat(t.Context(), "d"); err != nil {
		t.Fatalf("the namespace does not hold d, and the caller was told its change was made: %v", err)
	}
	requireUnusable(t, mounted)
}

func TestClosingReleasesAServerMutationStillWaitingForItsBarrier(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	gate := s.events.gateEvents()
	defer gate.release()
	options := replicated.DefaultOptions()
	options.ConfirmationGrace = 10 * time.Second
	options.MaxActiveConfirmations = 1
	options.MaxWaitingConfirmations = 1
	mounted, _ := mountWithOptions(t, s, options)
	gate.arm()

	at, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatalf("reading the starting position: %v", err)
	}
	mutation := make(chan error, 1)
	go func() { mutation <- mounted.Create(t.Context(), "made-before-close") }()
	requireRecordedPast(t, s, at)
	gate.requireEntered(t)
	waiting := make(chan error, 1)
	go func() { waiting <- mounted.Create(t.Context(), "never-sent") }()
	time.Sleep(20 * time.Millisecond)
	before := s.calls.of(httprest.OpCreate)
	if before != 1 {
		t.Fatalf("the admission waiter reached the server before Close: create calls=%d", before)
	}

	closed := make(chan error, 1)
	go func() { closed <- mounted.Close() }()
	select {
	case err := <-mutation:
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("the mutation whose server success could not be confirmed returned %v, want EIO", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closing did not release the mutation waiting for its barrier")
	}
	select {
	case err := <-waiting:
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("the pre-send admission waiter returned %v on Close, want EAGAIN", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not release the confirmation admission waiter")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("closing the replicated storage: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited on a barrier the copy can no longer reach")
	}
	if _, err := s.meta.Stat(t.Context(), "made-before-close"); err != nil {
		t.Fatalf("the server did not retain the mutation whose confirmation became ambiguous: %v", err)
	}
	if _, err := s.meta.Stat(t.Context(), "never-sent"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the admission waiter changed the namespace during Close: %v", err)
	}
}

// TestAChangeUnderADirectoryTheCopyHasNotSeenYetReachesItsBarrier.
//
// A barrier is independent of path resolution. A mutation below a directory that has not yet
// reached the copy must wait until both the parent and the mutation's position are applied.
func TestAChangeUnderADirectoryTheCopyHasNotSeenYetReachesItsBarrier(t *testing.T) {
	const frame = 300 * time.Millisecond

	s := serve(t, httprest.DefaultLimits())
	s.events.slowEvents(frame)
	mounted, replica := mount(t, s)

	// Made elsewhere, so the copy can learn of it only from the stream — which is holding every
	// frame back for long enough that it cannot have arrived by the time the file below is made
	// inside it.
	mkdir(t, s, "d")
	if _, err := replica.Stat(t.Context(), "d"); err == nil {
		t.Fatal("the copy already holds d, so nothing here is waiting for a parent that has not arrived: this case tests nothing as written")
	}

	if err := mounted.Create(t.Context(), "d/f"); err != nil {
		t.Fatalf("creating d/f while the copy has not seen d yet: %v", err)
	}
	if _, err := mounted.Stat(t.Context(), "d/f"); err != nil {
		t.Fatalf("stat d/f straight after creating it: %v", err)
	}

	requireCaughtUp(t, s, replica)
	requireSameTree(t, walkSource(t, s), walkCopy(t, replica))
}
