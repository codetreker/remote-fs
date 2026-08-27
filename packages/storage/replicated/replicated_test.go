package replicated_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
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
// A namespace held in a local directory has no metastore and therefore no ordered record of
// what changed in it. That is a standing property of that namespace rather than a failure to
// reach anything, and the two call for opposite actions: ENOSYS means mount it without a copy
// and never ask again, EIO means the server may be there in a moment. Answering one for the
// other means either a mount that never comes up or a mount that quietly gave up on being
// current.
func TestANamespaceThatKeepsNoLogRefusesToBeCopied(t *testing.T) {
	backing, err := localdir.New(t.TempDir())
	if err != nil {
		t.Fatalf("opening a local directory: %v", err)
	}
	// A nil log, which is what a namespace with no metastore is served with.
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
