package cmd_test

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The tests here are about the copy of the namespace's metadata each mountpoint keeps: what
// it costs to walk a tree that has been copied, what a directory rename costs, and what a
// namespace that keeps no change log gets instead.
//
// They are at this level rather than beside the copy because what they assert is what a
// program on the machine sees: a real mountpoint, real system calls, and the kernel's own
// path resolution in between. The layer beneath has its own tests, and neither set answers
// for the other — the mount is where "one stat is one request" was true, and it is where it
// has stopped being true.

// TestWalkingAMountedTreeSendsNothingToTheServer is the whole point of the feature, measured
// at the mountpoint rather than under it.
//
// `ls -R` over a source tree is thousands of these calls. Every one of them was a request to
// the server, and every name a path search asked about and did not find was one too, which is
// what made a workspace on a 20 ms link unusable. The number below is zero.
//
// The kernel is not caching any of it: packages/fuse leaves its three timeouts at zero, so
// every one of these lookups, listings and stats arrives at the copy. That is what makes this
// a measurement of the copy rather than of the kernel.
func TestWalkingAMountedTreeSendsNothingToTheServer(t *testing.T) {
	s := serveNamespace(t)
	a := mountpointOn(t, s)

	// A tree with a couple of levels, made through the mountpoint so that what is walked is
	// what a program would have put there.
	for _, dir := range []string{"src", "src/inner", "docs"} {
		if err := os.Mkdir(filepath.Join(a, dir), 0o755); err != nil {
			t.Fatalf("making %s: %v", dir, err)
		}
	}
	for _, file := range []string{"README", "src/a.go", "src/b.go", "src/inner/c.go", "docs/d.md"} {
		if err := os.WriteFile(filepath.Join(a, file), []byte("contents of "+file), 0o644); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}

	before := s.calls.snapshot()

	// filepath.Walk rather than WalkDir: it stats every name it finds, which is what a build
	// tool does and what a directory entry alone would not have proved.
	var walked []string
	if err := filepath.Walk(a, func(at string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if at != a {
			walked = append(walked, at[len(a)+1:])
		}
		return nil
	}); err != nil {
		t.Fatalf("walking the mountpoint: %v", err)
	}
	slices.Sort(walked)
	if want := []string{"README", "docs", "docs/d.md", "src", "src/a.go", "src/b.go", "src/inner", "src/inner/c.go"}; !slices.Equal(walked, want) {
		t.Fatalf("the walk saw %v, want %v", walked, want)
	}

	// And the names that are not there, which is what a toolchain spends most of its calls
	// asking about: an executable search, an import path, a linker's search list.
	for _, absent := range []string{"src/missing.go", "src/inner/missing.go", "docs/missing.md", "missing"} {
		if _, err := os.Lstat(filepath.Join(a, absent)); !os.IsNotExist(err) {
			t.Fatalf("stat of %q gave %v, want ENOENT", absent, err)
		}
	}

	if arrived := s.calls.since(before); arrived != "" {
		t.Fatalf("walking a copied tree sent %s to the server; the copy exists so that it sends nothing", arrived)
	}
	t.Logf("walked %d nodes and asked about 4 absent names, and the server heard nothing", len(walked))
}

// TestADirectoryRenameKeepsTheIdentitiesBeneathIt.
//
// A directory rename is one row in the log and one row in the copy: the nodes beneath keep
// the parent they always had, so nothing under the renamed directory is fetched again and
// nothing under it changes identity. An event that had only said "something under this name
// changed" would have forced the copy to discard the subtree and walk it again — and renaming
// directories is what build tools, version control and package managers do constantly.
//
// The inode numbers are what a program sees of that identity. Anything holding a file open
// across the rename, and anything that remembers what it has already visited, reads them.
func TestADirectoryRenameKeepsTheIdentitiesBeneathIt(t *testing.T) {
	s := serveNamespace(t)
	a := mountpointOn(t, s)

	for _, dir := range []string{"before", "before/inner"} {
		if err := os.Mkdir(filepath.Join(a, dir), 0o755); err != nil {
			t.Fatalf("making %s: %v", dir, err)
		}
	}
	for _, file := range []string{"before/f", "before/inner/g"} {
		if err := os.WriteFile(filepath.Join(a, file), []byte("contents"), 0o644); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}

	beneath := []string{"", "/inner", "/f", "/inner/g"}
	was := map[string]uint64{}
	for _, at := range beneath {
		was[at] = inodeOf(t, filepath.Join(a, "before"+at))
	}

	before := s.calls.snapshot()
	if err := os.Rename(filepath.Join(a, "before"), filepath.Join(a, "after")); err != nil {
		t.Fatalf("renaming the directory: %v", err)
	}

	for _, at := range beneath {
		if is := inodeOf(t, filepath.Join(a, "after"+at)); is != was[at] {
			t.Fatalf("%q was inode %d before the rename and %q is inode %d after it", "before"+at, was[at], "after"+at, is)
		}
	}
	if _, err := os.Lstat(filepath.Join(a, "before")); !os.IsNotExist(err) {
		t.Fatalf("the old name is still there after the rename: %v", err)
	}
	if got := namesIn(t, filepath.Join(a, "after", "inner")); !slices.Equal(got, []string{"g"}) {
		t.Fatalf("the moved subtree lists %v, want [g]", got)
	}

	// One request, and it is the rename. Everything read afterwards came out of the copy.
	if arrived := s.calls.since(before); arrived != "rename×1" {
		t.Fatalf("renaming a directory and reading the subtree back sent %q to the server, want the rename alone", arrived)
	}
}

// TestANamespaceThatKeepsNoLogIsMountedWithoutACopy.
//
// A namespace held in a local directory has no metastore and therefore no ordered record of
// what changed in it, so there is nothing to feed a copy from and the server says so under
// its own errno. The mount is the one that was delivered before any of this existed: it works
// in full, and every operation is a request.
//
// The distinction being kept here is between ENOSYS and EIO. One says this namespace will
// never be replicable and the mount should get on with it; the other says the server may
// answer in a moment. Answering either for the other gives a mount that never comes up, or a
// mount that quietly stopped being current.
func TestANamespaceThatKeepsNoLogIsMountedWithoutACopy(t *testing.T) {
	s := serveDirectory(t)
	a := mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt: %v", err)
	}
	if got := namesIn(t, a); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("the mountpoint lists %v, want [a.txt]", got)
	}

	// Every operation is a request, which is the other half of the claim: this mount is not
	// answering from a copy, because there is no copy. One stat by a program is more than one
	// request here — the kernel resolves the name and then asks about the node, and with all
	// three of its timeouts at zero it caches neither — which is exactly the traffic the other
	// test in this file counts as zero.
	before := s.calls.snapshot()
	if _, err := os.Lstat(filepath.Join(a, "a.txt")); err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	arrived := s.calls.since(before)
	if !strings.HasPrefix(arrived, "stat×") {
		t.Fatalf("a stat on a namespace that keeps no log sent %q to the server, and with no copy behind it it has nowhere else to come from", arrived)
	}
	t.Logf("one stat through the mountpoint: %s", arrived)
	// And the bytes are in the directory the server was given, read with nothing of this
	// system in the way.
	if got, err := os.ReadFile(filepath.Join(s.backing, "a.txt")); err != nil || string(got) != "hello\n" {
		t.Fatalf("the backing directory holds %q, %v", got, err)
	}
}

func inodeOf(t *testing.T, at string) uint64 {
	t.Helper()

	info, err := os.Lstat(at)
	if err != nil {
		t.Fatalf("stat %s: %v", at, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s reports no inode number", at)
	}
	return stat.Ino
}

// A descriptor reads the file it was opened on, even after another mountpoint has renamed a
// different file over that name. R-FS-5 is what makes this possible and R-CON-3 is what it
// delivers: the reader sees the old contents or the new ones, never a piece of each.
//
// The sequence is the ordinary atomic save — write a temp file, rename it over the target —
// which is how editors, compilers, package managers and git all replace a file. Before the
// mount had node identity this returned the old file's bytes cut to the new file's length,
// silently, with no error at either end: the name kept the inode number the kernel already
// held attributes against, so the size came from the node that arrived and the bytes from
// the node the descriptor was opened on.
//
// It is here rather than beside the identity record because every layer has to hold for the
// program to be right: the namespace has to report which node is at a name, the wire has to
// carry it, the record has to compare it, and an open descriptor has to describe the file it
// holds rather than the path it came from. A case under any one of them passes with the
// other three broken.
func TestADescriptorKeepsReadingTheFileItOpened(t *testing.T) {
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	const (
		old = "the contents this descriptor was opened on\n"
		new = "shorter\n"
	)
	if err := os.WriteFile(filepath.Join(a, "doc.txt"), []byte(old), 0o644); err != nil {
		t.Fatalf("writing doc.txt through A: %v", err)
	}

	// B opens it and reads it once, so the descriptor is established and the name has an
	// identity on that mount before anything changes.
	held, err := os.Open(filepath.Join(b, "doc.txt"))
	if err != nil {
		t.Fatalf("opening doc.txt through B: %v", err)
	}
	defer held.Close()
	if first, err := io.ReadAll(held); err != nil || string(first) != old {
		t.Fatalf("B first read %q (%v), want %q", first, err, old)
	}
	before, err := held.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// A saves over the name the way every tool does.
	if err := os.WriteFile(filepath.Join(a, "doc.tmp"), []byte(new), 0o644); err != nil {
		t.Fatalf("writing the staging file through A: %v", err)
	}
	if err := os.Rename(filepath.Join(a, "doc.tmp"), filepath.Join(a, "doc.txt")); err != nil {
		t.Fatalf("renaming over doc.txt through A: %v", err)
	}

	// Wait for B to see the replacement at the name, so that what is checked afterwards is a
	// descriptor held across a change B has already applied rather than one that arrived early.
	replaced := time.Now()
	for {
		got, err := os.ReadFile(filepath.Join(b, "doc.txt"))
		if err != nil {
			t.Fatalf("reading doc.txt through B: %v", err)
		}
		if string(got) == new {
			break
		}
		if waited := time.Since(replaced); waited >= time.Second {
			t.Fatalf("B still read the old contents %v after the rename returned on A; R-CON-1 allows one second", waited)
		}
	}

	// The descriptor still reads what it was opened on, whole.
	if _, err := held.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	again, err := io.ReadAll(held)
	if err != nil {
		t.Fatalf("reading through the held descriptor: %v", err)
	}
	if string(again) != old {
		t.Fatalf("a descriptor held across a rename over its name read %q, want the %q it was opened on", again, old)
	}

	// And its own attributes describe that file rather than the one that took its place. The
	// length is what the kernel clips a read at, so a length from the wrong node is how the
	// bytes above went wrong.
	after, err := held.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != int64(len(old)) {
		t.Fatalf("fstat on the held descriptor reports %d bytes, want the %d it holds", after.Size(), len(old))
	}

	// The name now refers to a different node, and must not have kept the number the kernel
	// still holds for the one the descriptor has open.
	arrived, err := os.Stat(filepath.Join(b, "doc.txt"))
	if err != nil {
		t.Fatal(err)
	}
	opened, arrivedIno := before.Sys().(*syscall.Stat_t).Ino, arrived.Sys().(*syscall.Stat_t).Ino
	if opened == arrivedIno {
		t.Fatalf("the node that arrived and the one the descriptor holds are both inode %d, so the kernel has two nodes under one number", opened)
	}
}
