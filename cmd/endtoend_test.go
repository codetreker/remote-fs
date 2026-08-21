package cmd_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// TestWhatOneMountpointWritesAnotherReads is the claim the project exists to make:
//
//	machine A:   echo hello > /mnt/ws/a.txt
//	machine B:   cat /mnt/ws/a.txt        →  hello      (within one second)
//
// Here the two machines are two mountpoints with a client each, against one server over
// one directory. What that arrangement leaves out is the network between two hosts; what
// it keeps is every piece of this system that stands between the write and the read.
//
// The second in R-CON-1 is measured from close() returning on A to a successful read on
// B. That is the interval a person waits: the write is finished when close() returns, and
// the read is answered when the bytes come back. The scope note never said where the
// second is measured from, so this is where this suite puts it.
//
// The read is attempted once and is not retried. Retrying would turn the assertion into
// "it becomes visible eventually", and R-CON-2 is the requirement that visibility does
// not wait for an interval to come round.
func TestWhatOneMountpointWritesAnotherReads(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	content := []byte("hello\n")
	file, err := os.Create(filepath.Join(a, "a.txt"))
	if err != nil {
		t.Fatalf("creating a.txt through A: %v", err)
	}
	if _, err := file.Write(content); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("closing a.txt through A: %v", err)
	}
	written := time.Now()

	got, err := os.ReadFile(filepath.Join(b, "a.txt"))
	visible := time.Since(written)
	if err != nil {
		t.Fatalf("reading a.txt through B %v after close() returned on A: %v", visible, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("B read %q, A wrote %q", got, content)
	}
	if visible >= time.Second {
		t.Fatalf("B saw the contents %v after close() returned on A; R-CON-1 allows one second", visible)
	}
	t.Logf("close() returned on A → contents read on B: %v", visible)
}

// TestTheBytesReachTheBackingDirectory reads the file the server was given, with plain os
// calls and nothing of this system in the way.
//
// Both mountpoints and the server could agree with each other and all be wrong. The
// backing directory is the one path to the bytes that does not run through the code under
// test, which is what makes it evidence.
func TestTheBytesReachTheBackingDirectory(t *testing.T) {
	s := serveDirectory(t)
	a := mountpointOn(t, s)

	content := []byte("hello\n")
	if err := os.WriteFile(filepath.Join(a, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(s.backing, "a.txt"))
	if err != nil {
		t.Fatalf("the backing directory has no a.txt: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("the backing directory holds %q, A wrote %q", got, content)
	}
}

// TestACreationOnOneMountpointAppearsInAListingOnTheOther covers the case the previous
// test does not: reading a file by name proves the name resolves, not that the directory
// reports it. Anything that walks a tree finds files this way.
func TestACreationOnOneMountpointAppearsInAListingOnTheOther(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	if got := namesIn(t, b); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("B lists %v, want [a.txt]", got)
	}
}

// TestARemovalOnOneMountpointDisappearsFromTheOther. A deletion that does not propagate
// is how a mount starts serving files that are gone.
func TestARemovalOnOneMountpointDisappearsFromTheOther(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	if got := namesIn(t, b); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("B lists %v before the removal, want [a.txt]", got)
	}

	if err := os.Remove(filepath.Join(a, "a.txt")); err != nil {
		t.Fatalf("removing a.txt through A: %v", err)
	}
	if got := namesIn(t, b); len(got) != 0 {
		t.Fatalf("B still lists %v after the removal", got)
	}
	if _, err := os.Stat(filepath.Join(b, "a.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat through B after the removal gave %v, want ENOENT", err)
	}
}

// TestADirectoryMadeOnOneMountpointIsADirectoryOnTheOther. A directory that arrives as a
// file is worse than one that does not arrive: everything that walks the tree stops
// there and reports the subtree as absent.
func TestADirectoryMadeOnOneMountpointIsADirectoryOnTheOther(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.Mkdir(filepath.Join(a, "d"), 0o755); err != nil {
		t.Fatalf("making d through A: %v", err)
	}
	if err := os.WriteFile(filepath.Join(a, "d", "inner.txt"), []byte("inside\n"), 0o644); err != nil {
		t.Fatalf("writing d/inner.txt through A: %v", err)
	}

	info, err := os.Stat(filepath.Join(b, "d"))
	if err != nil {
		t.Fatalf("stat d through B: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("B reports d as mode %v, want a directory", info.Mode())
	}
	if got := namesIn(t, filepath.Join(b, "d")); !slices.Equal(got, []string{"inner.txt"}) {
		t.Fatalf("B lists %v inside d, want [inner.txt]", got)
	}
}

// TestARenameOnOneMountpointIsSeenAsARename. What separates a rename from a copy is
// observable from the other side: after a rename the old name is gone. A mount that let
// the old name survive would leave whatever is watching the directory with two files
// where the writer left one, and no way to tell which is current.
func TestARenameOnOneMountpointIsSeenAsARename(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	content := []byte("hello\n")
	if err := os.WriteFile(filepath.Join(a, "before.txt"), content, 0o644); err != nil {
		t.Fatalf("writing before.txt through A: %v", err)
	}
	if err := os.Rename(filepath.Join(a, "before.txt"), filepath.Join(a, "after.txt")); err != nil {
		t.Fatalf("renaming through A: %v", err)
	}

	if got := namesIn(t, b); !slices.Equal(got, []string{"after.txt"}) {
		t.Fatalf("B lists %v, want [after.txt] alone — the old name surviving would make this a copy", got)
	}
	if _, err := os.Stat(filepath.Join(b, "before.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat before.txt through B gave %v, want ENOENT", err)
	}
	got, err := os.ReadFile(filepath.Join(b, "after.txt"))
	if err != nil {
		t.Fatalf("reading after.txt through B: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("B reads %q at the new name, A wrote %q at the old one", got, content)
	}
}

// TestAnOverwriteOnOneMountpointIsSeenWhole. The shortening case is the one that catches
// a replacement done in place: contents written over a longer file leave the old tail
// behind, and the result reads as a file that was never written by anyone (R-CON-3).
func TestAnOverwriteOnOneMountpointIsSeenWhole(t *testing.T) {
	s := serveDirectory(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	path := filepath.Join(a, "a.txt")
	for _, content := range []string{
		"first",
		"a second version, considerably longer than the first",
		"third",
	} {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("writing %q through A: %v", content, err)
		}
		got, err := os.ReadFile(filepath.Join(b, "a.txt"))
		if err != nil {
			t.Fatalf("reading through B after writing %q: %v", content, err)
		}
		if string(got) != content {
			t.Fatalf("B reads %q, A wrote %q", got, content)
		}
	}
}

// TestASymbolicLinkSurvivesTheWholeChain. A node's kind is the one attribute every layer
// has to agree on, and a symbolic link is the kind that is lost by default: it is the one
// the operating system resolves unless it is told not to, and the one whose mode bit has
// to be carried across the wire rather than inferred.
//
// Both halves are proved apart — the mount against a plain directory, the transport
// against the contract suite — and neither of them carries fs.ModeSymlink through both. A
// chain that dropped it would present the link as an ordinary file, and everything reading
// under that name would be reading a file nobody named.
//
// The namespace has no operation that makes a link (R-FS-1 asks for them eventually), so
// the link is put into the served directory the way one gets there in practice: by
// something else that reaches that directory.
func TestASymbolicLinkSurvivesTheWholeChain(t *testing.T) {
	s := serveDirectory(t)
	a := mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(s.backing, "target"), []byte("payload\n"), 0o644); err != nil {
		t.Fatalf("writing the file the link points at: %v", err)
	}
	if err := os.Symlink("target", filepath.Join(s.backing, "link")); err != nil {
		t.Fatalf("planting the link: %v", err)
	}

	got, err := os.Lstat(filepath.Join(a, "link"))
	if err != nil {
		t.Fatalf("lstat the link through the mount: %v", err)
	}
	if got.Mode().Type() != fs.ModeSymlink {
		t.Errorf("the mount reports the link as %v, want a symbolic link", got.Mode())
	}
	// A link's length is the length of the name it holds, so the file it points at being a
	// different length is what makes this an assertion rather than a coincidence.
	if got.Size() != int64(len("target")) {
		t.Errorf("the mount reports the link as %d bytes, want %d — the length of the name it holds",
			got.Size(), len("target"))
	}

	// The listing has to say the same thing, because it travels as its own message and
	// anything pairing a listing with a lookup would otherwise see the name change kind.
	entries, err := os.ReadDir(a)
	if err != nil {
		t.Fatalf("listing the mountpoint: %v", err)
	}
	listed := map[string]fs.FileMode{}
	for _, entry := range entries {
		listed[entry.Name()] = entry.Type()
	}
	if listed["link"] != fs.ModeSymlink {
		t.Errorf("the listing reports the link as %v, and a lookup reports %v", listed["link"], got.Mode().Type())
	}

	// Where it points is the one thing that cannot be answered: no operation the namespace
	// offers could produce it, and EOPNOTSUPP says that rather than saying this is not a
	// link.
	if target, err := os.Readlink(filepath.Join(a, "link")); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Errorf("readlink through the mount gave %q with error %v, want EOPNOTSUPP", target, err)
	}
}

// TestAnUnreachableServerFailsRatherThanAnswering is the test that matters more than the
// happy path.
//
// A directory listing that comes back empty says the namespace has nothing in it, and
// whatever is watching acts on that: it regenerates, it propagates the deletion, it
// overwrites. There is no way to notice afterwards and no way back. The same goes for
// "no such file" in place of "I could not ask" (R-ERR-1, R-ERR-2).
//
// Every operation the contract offers resolves its own failure, so each gets a case of its
// own here rather than one standing in for the rest.
func TestAnUnreachableServerFailsRatherThanAnswering(t *testing.T) {
	s := serveDirectory(t)
	a := mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	if err := os.Mkdir(filepath.Join(a, "existing"), 0o755); err != nil {
		t.Fatalf("making existing through A: %v", err)
	}
	if got := namesIn(t, a); !slices.Equal(got, []string{"a.txt", "existing"}) {
		t.Fatalf("A lists %v while the server is up, want [a.txt existing]", got)
	}

	s.stop()

	t.Run("a listing fails rather than coming back empty", func(t *testing.T) {
		entries, err := os.ReadDir(a)
		if err == nil {
			t.Fatalf("listing succeeded with %d entries; the server is gone and this answer is invented", len(entries))
		}
		if errno := errnoOf(err); errno != syscall.EIO {
			t.Fatalf("listing failed with %v (errno %v), want EIO", err, errno)
		}
		t.Logf("os.ReadDir: %v", err)
	})

	for _, c := range []struct {
		name string
		run  func() error
	}{
		{"reading a file that exists", func() error { _, err := os.ReadFile(filepath.Join(a, "a.txt")); return err }},
		{"stat on a file that exists", func() error { _, err := os.Stat(filepath.Join(a, "a.txt")); return err }},
		{"stat on a name that does not exist", func() error { _, err := os.Stat(filepath.Join(a, "absent.txt")); return err }},
		{"creating a file", func() error { return os.WriteFile(filepath.Join(a, "new.txt"), []byte("x"), 0o644) }},
		{"changing a file's mode", func() error { return os.Chmod(filepath.Join(a, "a.txt"), 0o600) }},
		// Removal goes through the system calls rather than os.Remove, which tries unlink
		// and then rmdir and reports whichever of the two it decides on: one case would
		// then answer for both operations, and neither for itself.
		{"removing a file", func() error { return syscall.Unlink(filepath.Join(a, "a.txt")) }},
		{"making a directory", func() error { return os.Mkdir(filepath.Join(a, "d"), 0o755) }},
		{"removing a directory", func() error { return syscall.Rmdir(filepath.Join(a, "existing")) }},
		{"renaming", func() error { return os.Rename(filepath.Join(a, "a.txt"), filepath.Join(a, "b.txt")) }},
	} {
		t.Run(c.name+" fails", func(t *testing.T) {
			err := c.run()
			if err == nil {
				t.Fatal("the operation succeeded; the server is gone and this outcome is invented")
			}
			if errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the operation failed with %v; \"no such file\" is a claim about the namespace, and the truth is that it could not be reached", err)
			}
			if errno := errnoOf(err); errno != syscall.EIO {
				t.Fatalf("the operation failed with %v (errno %v), want EIO", err, errno)
			}
			t.Logf("%v", err)
		})
	}
}

func namesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}
