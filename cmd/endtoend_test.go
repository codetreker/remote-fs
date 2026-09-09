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

	"github.com/codetreker/remote-fs/packages/storage/replicated"
)

// TestWhatOneMountpointWritesAnotherReads is the claim the project exists to make:
//
//	machine A:   echo hello > /mnt/ws/a.txt
//	machine B:   cat /mnt/ws/a.txt        →  hello      (within one second)
//
// Here the two machines are two mountpoints with a client each, against one server over one
// namespace. What that arrangement leaves out is the network between two hosts; what it keeps
// is every piece of this system that stands between the write and the read — including the
// copy of the metadata each mountpoint keeps, and the stream of changes that feeds it.
//
// Visibility starts when Write returns. The writer remains open while another mount
// reads the completed change, so neither Close nor Fsync can trigger its publication.
func TestWhatOneMountpointWritesAnotherReads(t *testing.T) {
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	content := []byte("hello\n")
	file, err := os.Create(filepath.Join(a, "a.txt"))
	if err != nil {
		t.Fatalf("creating a.txt through A: %v", err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("closing writer through A: %v", err)
		}
	})
	if _, err := file.Write(content); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	written := time.Now()

	var (
		got     []byte
		visible time.Duration
	)
	for {
		got, err = os.ReadFile(filepath.Join(b, "a.txt"))
		visible = time.Since(written)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reading a.txt through B %v after Write returned on A: %v", visible, err)
		}
		if visible >= time.Second {
			t.Fatalf("B could not read a.txt %v after Write returned on A; R-CON-1 allows one second", visible)
		}
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("B read %q, A wrote %q", got, content)
	}
	if visible >= time.Second {
		t.Fatalf("B read the completed write after %v; R-CON-1 allows one second", visible)
	}
	t.Logf("Write returned on A → contents read on B: %v", visible)
}

// Reading the authoritative storage bypasses the mount and its metadata replica.
func TestTheBytesReachAuthoritativeStorage(t *testing.T) {
	s := serveNamespace(t)
	a := mountpointOn(t, s)
	content := []byte("hello\n")
	if err := os.WriteFile(filepath.Join(a, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	got, err := s.authoritative.Read(t.Context(), "a.txt")
	if err != nil {
		t.Fatalf("read authoritative a.txt: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("the authoritative namespace holds %q, A wrote %q", got, content)
	}
}

// TestACreationOnOneMountpointAppearsInAListingOnTheOther covers the case the previous
// test does not: reading a file by name proves the name resolves, not that the directory
// reports it. Anything that walks a tree finds files this way.
func TestACreationOnOneMountpointAppearsInAListingOnTheOther(t *testing.T) {
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	settled(t, "a.txt reaching B", present(filepath.Join(b, "a.txt")))
	if got := namesIn(t, b); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("B lists %v, want [a.txt]", got)
	}
}

// TestARemovalOnOneMountpointDisappearsFromTheOther. A deletion that does not propagate
// is how a mount starts serving files that are gone.
func TestARemovalOnOneMountpointDisappearsFromTheOther(t *testing.T) {
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.WriteFile(filepath.Join(a, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("writing a.txt through A: %v", err)
	}
	settled(t, "a.txt reaching B", present(filepath.Join(b, "a.txt")))
	if got := namesIn(t, b); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("B lists %v before the removal, want [a.txt]", got)
	}

	if err := os.Remove(filepath.Join(a, "a.txt")); err != nil {
		t.Fatalf("removing a.txt through A: %v", err)
	}
	settled(t, "the removal reaching B", absent(filepath.Join(b, "a.txt")))
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
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	if err := os.Mkdir(filepath.Join(a, "d"), 0o755); err != nil {
		t.Fatalf("making d through A: %v", err)
	}
	if err := os.WriteFile(filepath.Join(a, "d", "inner.txt"), []byte("inside\n"), 0o644); err != nil {
		t.Fatalf("writing d/inner.txt through A: %v", err)
	}

	settled(t, "d/inner.txt reaching B", present(filepath.Join(b, "d", "inner.txt")))
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
	s := serveNamespace(t)
	a, b := mountpointOn(t, s), mountpointOn(t, s)

	content := []byte("hello\n")
	if err := os.WriteFile(filepath.Join(a, "before.txt"), content, 0o644); err != nil {
		t.Fatalf("writing before.txt through A: %v", err)
	}
	if err := os.Rename(filepath.Join(a, "before.txt"), filepath.Join(a, "after.txt")); err != nil {
		t.Fatalf("renaming through A: %v", err)
	}

	settled(t, "the rename reaching B", present(filepath.Join(b, "after.txt")))
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
	s := serveNamespace(t)
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
		settled(t, "the new contents reaching B", func() bool {
			info, err := os.Stat(filepath.Join(b, "a.txt"))
			return err == nil && info.Size() == int64(len(content))
		})
		got, err := os.ReadFile(filepath.Join(b, "a.txt"))
		if err != nil {
			t.Fatalf("reading through B after writing %q: %v", content, err)
		}
		if string(got) != content {
			t.Fatalf("B reads %q, A wrote %q", got, content)
		}
	}
}

// A symbolic-link kind and target length must survive both HTTP and FUSE. The storage
// interface describes links but provides no operation to resolve their targets.
func TestASymbolicLinkSurvivesTheWholeChain(t *testing.T) {
	namespace, _ := namespaceFixture(t)
	for name, content := range map[string]string{"target": "payload\n", "link": "target"} {
		if err := namespace.Write(t.Context(), name, []byte(content)); err != nil {
			t.Fatalf("seed symbolic-link fixture: %v", err)
		}
	}
	link, err := namespace.Stat(t.Context(), "link")
	if err != nil {
		t.Fatalf("stat symbolic-link fixture: %v", err)
	}
	s := serveStorage(t, &symlinkMetadata{Storage: namespace, linkID: link.ID}, nil)
	a := mountpointOn(t, s)

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
	s := serveNamespace(t)
	a, served := mountNamespaceOn(t, s)
	if _, ok := served.(*replicated.Storage); !ok {
		t.Fatalf("mount storage is %T, want a metadata replica", served)
	}

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

	// Inode attributes use authoritative StatNode requests and can fail before the
	// event follower invalidates the copy. Wait on the mounted replica itself so
	// every OS assertion starts after the stream failure is observed.
	settled(t, "the metadata replica noticing that the server is gone", func() bool {
		_, err := served.Stat(t.Context(), "")
		return errnoOf(err) == syscall.EIO
	})

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

// settled waits for something to become true through a second mountpoint, and fails if it
// has not within the second R-CON-1 allows.
//
// It is not an assertion and nothing turns on it. A mountpoint holds a change when its own
// event arrives, which is a moment after the server recorded it rather than at the instant
// the write on the other mountpoint returned — so this decides when to look, and what is
// asserted afterwards is asserted exactly, once, with everything it has to say about a
// failure.
func settled(t *testing.T, what string, holds func() bool) {
	t.Helper()

	started := time.Now()
	for !holds() {
		if waited := time.Since(started); waited >= time.Second {
			t.Fatalf("%s has not happened %v after the change was made on the other mountpoint; R-CON-1 allows one second", what, waited)
		}
	}
	t.Logf("%s: %v", what, time.Since(started))
}

func present(at string) func() bool {
	return func() bool { _, err := os.Lstat(at); return err == nil }
}

func absent(at string) func() bool {
	return func() bool { _, err := os.Lstat(at); return errors.Is(err, os.ErrNotExist) }
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
