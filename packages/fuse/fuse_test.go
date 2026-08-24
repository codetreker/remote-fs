// The tests in this file need a real mount, and therefore /dev/fuse. They are kept
// apart from the rest of the package's tests so that a machine without FUSE still runs
// everything else. See docs/testing.md.
package fuse_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/fuse/fusetest"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
)

// TestMain runs the tests beneath a temporary directory of this run's own, so that a
// mountpoint still attached afterwards is one this run attached — see
// packages/fuse/fusetest — and fixes the umask so that the modes a plain directory gives
// new files and directories are the same ones the storage hands out, and the comparison
// below is therefore comparing filesystems rather than comparing this process against
// itself.
func TestMain(m *testing.M) {
	os.Exit(fusetest.Run("remote-fs-fuse", func() int {
		previous := syscall.Umask(0o022)
		defer syscall.Umask(previous)
		return m.Run()
	}))
}

func requireFUSE(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("skipping: this test mounts a filesystem and /dev/fuse is not available (%v)", err)
	}
	if _, err := exec.LookPath("fusermount3"); err != nil {
		if _, err := exec.LookPath("fusermount"); err != nil {
			t.Skipf("skipping: this test mounts a filesystem and no fusermount binary is on PATH (%v)", err)
		}
	}
}

// mountBacking mounts a namespace held in one directory at a fresh mountpoint.
func mountBacking(t *testing.T, backing string) string {
	t.Helper()
	s, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	return mountStorage(t, s, fuse.Options{Logger: testLogger(t)})
}

// mountStorage mounts s at a fresh mountpoint. The mountpoint is torn down whether or
// not the test passes; a test that left one behind would wedge the machine it ran on.
func mountStorage(t *testing.T, s storage.Storage, opts fuse.Options) string {
	t.Helper()
	requireFUSE(t)

	mountpoint := t.TempDir()
	m, err := fuse.New(mountpoint, s, opts)
	if err != nil {
		t.Fatalf("mounting at %s: %v", mountpoint, err)
	}
	t.Cleanup(func() { unmount(t, m, mountpoint) })
	return mountpoint
}

func unmount(t *testing.T, m *fuse.Mount, mountpoint string) {
	t.Helper()
	var err error
	for attempt := range 20 {
		if err = m.Unmount(); err == nil {
			m.Wait()
			return
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	// Leaving a mountpoint behind wedges every later run on this machine, so fall back
	// to detaching it even though the test is already failing.
	lazy := exec.Command("fusermount3", "-uz", mountpoint)
	out, lazyErr := lazy.CombinedOutput()
	t.Errorf("unmounting %s failed: %v; lazy unmount said %v %s", mountpoint, err, lazyErr, out)
}

// mountedPair returns a mountpoint backed by one namespace, and a plain directory. The
// differential test applies the same operations to both.
func mountedPair(t *testing.T) (mountpoint, plain, backing string) {
	t.Helper()
	backing = t.TempDir()
	plain = t.TempDir()
	mountpoint = mountBacking(t, backing)
	return mountpoint, plain, backing
}

func testLogger(t *testing.T) *log.Logger {
	return log.New(testWriter{t}, "go-fuse: ", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// --- the differential test -----------------------------------------------------------

// A step is one operation applied to both filesystems. It reports what the operation
// observed, so that a difference in the answer is caught as well as a difference in the
// tree the operation left behind.
type step struct {
	name string
	run  func(root string) (string, error)
}

// TestMountBehavesLikeAPlainDirectory applies the identical sequence of operations to a
// mountpoint and to a plain directory, and compares what each operation observed and
// what the two trees look like afterwards. Nothing here states what the answer should
// be: the plain directory is the answer.
func TestMountBehavesLikeAPlainDirectory(t *testing.T) {
	mountpoint, plain, _ := mountedPair(t)

	for _, s := range differentialSteps {
		gotMount, errMount := s.run(mountpoint)
		gotPlain, errPlain := s.run(plain)

		if describeError(errMount) != describeError(errPlain) {
			t.Fatalf("%s: the mount failed with %s, the plain directory with %s",
				s.name, describeError(errMount), describeError(errPlain))
		}
		if gotMount != gotPlain {
			t.Fatalf("%s: the mount observed %s, the plain directory observed %s",
				s.name, gotMount, gotPlain)
		}
		if diff := treeDiff(t, mountpoint, plain); diff != "" {
			t.Fatalf("after %s the trees differ:\n%s", s.name, diff)
		}
	}
}

// describeError reduces an error to the part both filesystems must agree on: whether
// the call failed, and with which errno. The surrounding *os.PathError text names the
// root directory, which necessarily differs between the two.
func describeError(err error) string {
	if err == nil {
		return "no error"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return "errno " + errno.Error()
	}
	return "a non-errno error: " + err.Error()
}

// entry is one node of a tree, in a form that can be compared across two filesystems:
// the absolute paths and the inode numbers cannot be, but these can.
type entry struct {
	path string
	mode fs.FileMode
	size int64
	sum  string
}

func (e entry) String() string {
	if e.mode.IsDir() {
		return fmt.Sprintf("%s  %v", e.path, e.mode)
	}
	return fmt.Sprintf("%s  %v  %d bytes  %s", e.path, e.mode, e.size, e.sum)
}

func walkTree(t *testing.T, root string) []entry {
	t.Helper()
	var entries []entry
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		e := entry{path: filepath.ToSlash(rel), mode: info.Mode()}
		if !d.IsDir() {
			body, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(body)
			e.size, e.sum = info.Size(), hex.EncodeToString(sum[:8])
		}
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return entries
}

func treeDiff(t *testing.T, mountpoint, plain string) string {
	t.Helper()
	got, want := walkTree(t, mountpoint), walkTree(t, plain)
	var lines []string
	for i := range max(len(got), len(want)) {
		var g, w string
		if i < len(got) {
			g = got[i].String()
		}
		if i < len(want) {
			w = want[i].String()
		}
		if g != w {
			lines = append(lines, fmt.Sprintf("  mount: %s\n  plain: %s", g, w))
		}
	}
	return strings.Join(lines, "\n")
}

// --- the operations the differential test applies ------------------------------------

// The sequence builds on itself, so later steps operate on what earlier ones left. Each
// step reports what it observed; the tree is compared after every one of them.
//
// A statfs step does not belong here, tempting as the symmetry is. Both roots sit on the
// same host filesystem, so the two answers would agree by construction and say nothing
// about the conversion into blocks; and the host's own free-block counters move between
// the two calls, so the comparison would fail at random as well.
// TestSpaceIsReportedInWholeBlocks drives that conversion from figures chosen for it.
var differentialSteps = []step{
	{"list the empty root", func(root string) (string, error) {
		return listDir(root)
	}},
	{"stat the root", func(root string) (string, error) {
		info, err := os.Stat(root)
		if err != nil {
			return "", err
		}
		return describeInfo(info), nil
	}},

	{"make a directory", func(root string) (string, error) {
		return "", os.Mkdir(filepath.Join(root, "d"), 0o755)
	}},
	{"make the same directory again", func(root string) (string, error) {
		return "", os.Mkdir(filepath.Join(root, "d"), 0o755)
	}},
	{"make a directory under a missing parent", func(root string) (string, error) {
		return "", os.Mkdir(filepath.Join(root, "missing", "d"), 0o755)
	}},
	{"make a nested directory", func(root string) (string, error) {
		return "", os.Mkdir(filepath.Join(root, "d", "sub"), 0o755)
	}},

	{"create a file and write to it", func(root string) (string, error) {
		f, err := os.Create(filepath.Join(root, "a.txt"))
		if err != nil {
			return "", err
		}
		n, err := f.Write([]byte("hello"))
		if err != nil {
			f.Close()
			return "", err
		}
		return fmt.Sprintf("wrote %d bytes", n), f.Close()
	}},
	{"read the file back", func(root string) (string, error) {
		body, err := os.ReadFile(filepath.Join(root, "a.txt"))
		return string(body), err
	}},
	{"stat the file", func(root string) (string, error) {
		info, err := os.Stat(filepath.Join(root, "a.txt"))
		if err != nil {
			return "", err
		}
		return describeInfo(info), nil
	}},
	{"list the root now that it has entries", func(root string) (string, error) {
		return listDir(root)
	}},

	{"see the written size through the handle that wrote it", func(root string) (string, error) {
		f, err := os.Create(filepath.Join(root, "fresh.txt"))
		if err != nil {
			return "", err
		}
		defer f.Close()
		if _, err := f.Write([]byte("0123456789")); err != nil {
			return "", err
		}
		info, err := f.Stat()
		if err != nil {
			return "", err
		}
		return describeInfo(info), nil
	}},

	{"truncate an existing file to nothing by opening it", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "a.txt"), os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return "", err
		}
		return "", f.Close()
	}},
	{"rewrite the truncated file", func(root string) (string, error) {
		return "", os.WriteFile(filepath.Join(root, "a.txt"), []byte("second value"), 0o644)
	}},
	{"overwrite part of a file in place", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "a.txt"), os.O_WRONLY, 0o644)
		if err != nil {
			return "", err
		}
		if _, err := f.WriteAt([]byte("FIRST!"), 0); err != nil {
			f.Close()
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
		body, err := os.ReadFile(filepath.Join(root, "a.txt"))
		return string(body), err
	}},
	{"append to a file", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "a.txt"), os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return "", err
		}
		if _, err := f.Write([]byte(" and more")); err != nil {
			f.Close()
			return "", err
		}
		return "", f.Close()
	}},
	{"write past the end of a file", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "sparse.bin"), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return "", err
		}
		if _, err := f.WriteAt([]byte("tail"), 4096); err != nil {
			f.Close()
			return "", err
		}
		return "", f.Close()
	}},
	{"read part of a file at an offset", func(root string) (string, error) {
		f, err := os.Open(filepath.Join(root, "sparse.bin"))
		if err != nil {
			return "", err
		}
		defer f.Close()
		buf := make([]byte, 6)
		n, err := f.ReadAt(buf, 4094)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%q", buf[:n]), nil
	}},

	{"write a file larger than one kernel request", func(root string) (string, error) {
		return "", os.WriteFile(filepath.Join(root, "big.bin"), pattern(3<<20), 0o644)
	}},
	{"read the large file back", func(root string) (string, error) {
		body, err := os.ReadFile(filepath.Join(root, "big.bin"))
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(body)
		return fmt.Sprintf("%d bytes %x", len(body), sum[:8]), nil
	}},

	{"shorten a file with truncate", func(root string) (string, error) {
		return "", os.Truncate(filepath.Join(root, "big.bin"), 100)
	}},
	{"lengthen a file with truncate", func(root string) (string, error) {
		return "", os.Truncate(filepath.Join(root, "big.bin"), 200)
	}},
	{"read what truncate left", func(root string) (string, error) {
		body, err := os.ReadFile(filepath.Join(root, "big.bin"))
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(body)
		return fmt.Sprintf("%d bytes %x", len(body), sum[:8]), nil
	}},
	{"truncate through an open handle", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "big.bin"), os.O_RDWR, 0o644)
		if err != nil {
			return "", err
		}
		if err := f.Truncate(8); err != nil {
			f.Close()
			return "", err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return "", err
		}
		return describeInfo(info), f.Close()
	}},
	{"truncate a missing file", func(root string) (string, error) {
		return "", os.Truncate(filepath.Join(root, "absent.bin"), 0)
	}},

	{"write into a nested directory", func(root string) (string, error) {
		return "", os.WriteFile(filepath.Join(root, "d", "sub", "deep.txt"), []byte("deep"), 0o644)
	}},
	{"list a nested directory", func(root string) (string, error) {
		return listDir(filepath.Join(root, "d", "sub"))
	}},

	{"rename a file within a directory", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "a.txt"), filepath.Join(root, "renamed.txt"))
	}},
	{"rename a file across directories", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "renamed.txt"), filepath.Join(root, "d", "moved.txt"))
	}},
	{"rename over an existing file", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "d", "moved.txt"), filepath.Join(root, "fresh.txt"))
	}},
	{"rename a directory", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "d", "sub"), filepath.Join(root, "sub"))
	}},
	{"rename a missing source", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "absent"), filepath.Join(root, "wherever"))
	}},
	{"rename into a missing directory", func(root string) (string, error) {
		return "", os.Rename(filepath.Join(root, "fresh.txt"), filepath.Join(root, "missing", "x"))
	}},

	{"read a missing file", func(root string) (string, error) {
		body, err := os.ReadFile(filepath.Join(root, "absent.txt"))
		return string(body), err
	}},
	{"stat a missing file", func(root string) (string, error) {
		info, err := os.Stat(filepath.Join(root, "absent.txt"))
		if err != nil {
			return "", err
		}
		return describeInfo(info), nil
	}},
	{"stat below a file", func(root string) (string, error) {
		info, err := os.Stat(filepath.Join(root, "fresh.txt", "below"))
		if err != nil {
			return "", err
		}
		return describeInfo(info), nil
	}},
	{"create a file that already exists, exclusively", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "fresh.txt"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return "", err
		}
		return "", f.Close()
	}},
	{"create a file under a missing directory", func(root string) (string, error) {
		f, err := os.Create(filepath.Join(root, "missing", "f"))
		if err != nil {
			return "", err
		}
		return "", f.Close()
	}},
	{"open a directory as a file and read it", func(root string) (string, error) {
		f, err := os.Open(filepath.Join(root, "d"))
		if err != nil {
			return "", err
		}
		defer f.Close()
		body := make([]byte, 16)
		n, err := f.Read(body)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%q", body[:n]), nil
	}},
	{"list a file", func(root string) (string, error) {
		return listDir(filepath.Join(root, "fresh.txt"))
	}},
	{"list a missing directory", func(root string) (string, error) {
		return listDir(filepath.Join(root, "absent"))
	}},

	{"remove a directory that still has entries", func(root string) (string, error) {
		return "", syscall.Rmdir(filepath.Join(root, "sub"))
	}},
	{"remove a file", func(root string) (string, error) {
		return "", syscall.Unlink(filepath.Join(root, "sub", "deep.txt"))
	}},
	{"remove the now empty directory", func(root string) (string, error) {
		return "", syscall.Rmdir(filepath.Join(root, "sub"))
	}},
	{"remove a directory with unlink", func(root string) (string, error) {
		return "", syscall.Unlink(filepath.Join(root, "d"))
	}},
	{"remove a file with rmdir", func(root string) (string, error) {
		return "", syscall.Rmdir(filepath.Join(root, "fresh.txt"))
	}},
	{"remove a missing file", func(root string) (string, error) {
		return "", syscall.Unlink(filepath.Join(root, "absent.txt"))
	}},
	{"remove a missing directory", func(root string) (string, error) {
		return "", syscall.Rmdir(filepath.Join(root, "absent"))
	}},

	{"list what is left", func(root string) (string, error) {
		return listDir(root)
	}},

	// Attributes. The tree comparison after every step already checks the mode of every
	// node, so these report the times, which it leaves out: two files made at different
	// instants have different times, but two files set to the same instant do not.
	{"change a file's mode", func(root string) (string, error) {
		p := filepath.Join(root, "attr.txt")
		if err := os.WriteFile(p, []byte("attributes"), 0o644); err != nil {
			return "", err
		}
		return "", os.Chmod(p, 0o600)
	}},
	{"take every permission away and put them back", func(root string) (string, error) {
		p := filepath.Join(root, "attr.txt")
		if err := os.Chmod(p, 0o000); err != nil {
			return "", err
		}
		return "", os.Chmod(p, 0o644)
	}},
	{"change a directory's mode", func(root string) (string, error) {
		p := filepath.Join(root, "attrdir")
		if err := os.Mkdir(p, 0o755); err != nil {
			return "", err
		}
		return "", os.Chmod(p, 0o700)
	}},
	// Before the setuid step rather than after it. A chown of a file that is setuid and
	// sticky at once is where the two filesystems genuinely part company, for a reason
	// neither of them decides: the kernel attaches the mode it wants left behind, and for
	// a FUSE inode the mode it attaches has lost the sticky bit as well as the setuid one
	// (measured on Linux 6.8; a chown of a file that is only sticky carries no mode at all,
	// and one that is only setuid carries the same mode both filesystems end up with).
	// TestOwnershipIsRefusedUnlessItIsAlreadyTheAnswer covers what is left of the question.
	{"give ownership to the user who already has it", func(root string) (string, error) {
		return "", os.Chown(filepath.Join(root, "attr.txt"), os.Getuid(), os.Getgid())
	}},
	{"set the setuid, setgid and sticky bits", func(root string) (string, error) {
		p := filepath.Join(root, "attr.txt")
		return "", os.Chmod(p, 0o755|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)
	}},
	{"set a chosen modification time", func(root string) (string, error) {
		p := filepath.Join(root, "attr.txt")
		accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
		changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
		if err := os.Chtimes(p, accessed, changed); err != nil {
			return "", err
		}
		return describeTimes(p)
	}},
	{"set only the access time", func(root string) (string, error) {
		p := filepath.Join(root, "attr.txt")
		accessed := time.Date(1987, time.May, 4, 3, 2, 1, 0, time.UTC)
		if err := os.Chtimes(p, accessed, time.Time{}); err != nil {
			return "", err
		}
		return describeTimes(p)
	}},
	{"set only the modification time", func(root string) (string, error) {
		// Both first, so that the access time this reports is one this step set rather
		// than whichever instant the walk between two steps last read the file at.
		p := filepath.Join(root, "attr.txt")
		accessed := time.Date(1993, time.August, 7, 6, 5, 4, 3, time.UTC)
		if err := os.Chtimes(p, accessed, accessed); err != nil {
			return "", err
		}
		changed := time.Date(2038, time.January, 19, 3, 14, 8, 0, time.UTC)
		if err := os.Chtimes(p, time.Time{}, changed); err != nil {
			return "", err
		}
		return describeTimes(p)
	}},
	{"set a directory's times", func(root string) (string, error) {
		p := filepath.Join(root, "attrdir")
		moment := time.Date(2011, time.November, 11, 11, 11, 11, 0, time.UTC)
		if err := os.Chtimes(p, moment, moment); err != nil {
			return "", err
		}
		return describeTimes(p)
	}},
	{"change the mode of something that is not there", func(root string) (string, error) {
		return "", os.Chmod(filepath.Join(root, "absent.txt"), 0o600)
	}},
	{"change the times of something that is not there", func(root string) (string, error) {
		moment := time.Unix(1_000_000, 0)
		return "", os.Chtimes(filepath.Join(root, "absent.txt"), moment, moment)
	}},
	{"change the mode below a file", func(root string) (string, error) {
		return "", os.Chmod(filepath.Join(root, "attr.txt", "below"), 0o600)
	}},

	// Making a node with permissions of its own is one step to the caller, whatever it
	// takes underneath. A tar or an unzip that never chmods afterwards depends on it.
	{"make a file with permissions of its own", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "own.txt"), os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return "", err
		}
		return "", f.Close()
	}},
	{"make an executable file", func(root string) (string, error) {
		f, err := os.OpenFile(filepath.Join(root, "runnable"), os.O_CREATE|os.O_WRONLY, 0o755)
		if err != nil {
			return "", err
		}
		return "", f.Close()
	}},
	{"make a directory with permissions of its own", func(root string) (string, error) {
		return "", os.Mkdir(filepath.Join(root, "owndir"), 0o700)
	}},
}

// describeTimes reports both of a node's times, to the nanosecond. Set explicitly, they
// are the same on both filesystems; left alone they are not, which is why the differential
// steps that report them set them first.
func describeTimes(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	accessed := info.Sys().(*syscall.Stat_t).Atim
	return fmt.Sprintf("accessed %v changed %v",
		time.Unix(accessed.Unix()).UTC(), info.ModTime().UTC()), nil
}

func listDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, fmt.Sprintf("%s(%v)", e.Name(), e.Type()))
	}
	slices.Sort(names)
	return strings.Join(names, " "), nil
}

// describeInfo leaves out the modification time, which is a different instant on each
// of the two filesystems, and the inode number, which is a different namespace.
// TestModificationTimeAdvancesWithAWrite covers the one property of the time that both
// filesystems must share.
func describeInfo(info fs.FileInfo) string {
	if info.IsDir() {
		return fmt.Sprintf("directory mode=%v", info.Mode())
	}
	return fmt.Sprintf("file mode=%v size=%d", info.Mode(), info.Size())
}

// pattern is a byte sequence long enough to span many kernel requests and varied enough
// that a chunk delivered at the wrong offset changes the checksum.
func pattern(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = byte(i*7 + i/251)
	}
	return body
}

// --- crossing the boundary the other way ---------------------------------------------

// The differential test can only see what the mount chooses to report back. These cases
// act through the mountpoint and check the namespace underneath it with plain os calls,
// and act on the namespace and check what the mountpoint reports. A mount that kept
// everything in memory would pass the differential test and fail here.

func TestWritesReachTheNamespaceUnderneath(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	if err := os.Mkdir(filepath.Join(mountpoint, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "d", "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(filepath.Join(backing, "d", "f"))
	if err != nil {
		t.Fatalf("the file never reached the namespace: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("the namespace holds %q, want %q", body, "payload")
	}
}

// A change made underneath the mount is visible through it with no invalidation step in
// between, whether it adds, replaces, or removes. TestEveryLookAtTheNamespaceReachesIt
// covers the mechanism that makes this true; this covers the result.
func TestChangesUnderneathAreVisibleImmediately(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(mountpoint, "f")); err != nil || string(body) != "first" {
		t.Fatalf("read %q, %v through the mount, want %q", body, err, "first")
	}

	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("second value"), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len("second value")) {
		t.Fatalf("the mount reports size %d, want %d — a stale attribute survived",
			info.Size(), len("second value"))
	}
	if body, err := os.ReadFile(filepath.Join(mountpoint, "f")); err != nil || string(body) != "second value" {
		t.Fatalf("read %q, %v through the mount, want %q", body, err, "second value")
	}

	if err := os.Remove(filepath.Join(backing, "f")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(mountpoint, "f")); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("stat of the removed file failed with %v, want ENOENT", err)
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the mount still lists %v after the namespace was emptied", entries)
	}
}

func TestModificationTimeAdvancesWithAWrite(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	path := filepath.Join(mountpoint, "f")

	before := time.Now().Add(-time.Minute)
	if err := os.WriteFile(path, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.ModTime().Before(before) {
		t.Fatalf("modification time %v predates the write", first.ModTime())
	}

	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(path, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !second.ModTime().After(first.ModTime()) {
		t.Fatalf("modification time stayed at %v after a second write", second.ModTime())
	}
}

// Reporting that an fsync succeeded while the contents sat in this process's memory
// would be the false report this filesystem exists to avoid.
func TestFsyncCommits(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	f, err := os.Create(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(filepath.Join(backing, "f"))
	if err != nil {
		t.Fatalf("fsync reported success and the file is not in the namespace: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("the namespace holds %q after fsync, want %q", body, "payload")
	}
}

func TestReadingPastTheEndOfAFile(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	path := filepath.Join(mountpoint, "f")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 4)
	if n, err := f.ReadAt(buf, 100); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("reading past the end gave %d bytes and %v, want 0 and EOF", n, err)
	}
}

// --- identity --------------------------------------------------------------------------

// The number this mount reports for a node is what the kernel identifies that node by, and
// it holds the number against everything else it knows: a number two live nodes share is a
// number the kernel maps one node's pages against the other node's length, and the program
// that asked takes SIGBUS.

// A rename moves a file rather than replacing it, so the identity a program observes has
// to survive the move. Inode numbers cannot be compared between two filesystems, but
// this relation between two of them can.
func TestIdentitySurvivesARename(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)

	if err := os.WriteFile(filepath.Join(mountpoint, "before"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(mountpoint, "before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(mountpoint, "before"), filepath.Join(mountpoint, "after")); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(mountpoint, "after"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("the renamed file reports a different identity than it had before the rename")
	}
}

// A handle that is open across a rename keeps writing to the file it opened, not to
// whatever now sits at the old name.
func TestAnOpenHandleFollowsItsFileThroughARename(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	f, err := os.Create(filepath.Join(mountpoint, "before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(mountpoint, "before"), filepath.Join(mountpoint, "after")); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("payload")); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(filepath.Join(backing, "after"))
	if err != nil {
		t.Fatalf("the renamed file is not in the namespace: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("the renamed file holds %q, want %q", body, "payload")
	}
	if _, err := os.Stat(filepath.Join(backing, "before")); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the old name is still in the namespace (%v); the write went to the wrong file", err)
	}
}

// Listing a directory nobody has looked inside yet is the cold case: every name in it is
// new to the mount, and the numbers the listing reports for them are the numbers a later
// stat has to agree with.
func TestListingADirectoryNothingHasLookedInsideYet(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(backing, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	listed := listedInodes(t, mountpoint)
	if len(listed) != 3 {
		t.Fatalf("the listing holds %v, want three names", listed)
	}
	for name, number := range listed {
		if want := ino(t, filepath.Join(mountpoint, name)); number != want {
			t.Fatalf("the listing reports %s as %d and a stat reports it as %d", name, number, want)
		}
	}
}

// listedInodes reads a directory the way the kernel delivers it. os.ReadDir cannot be used
// here: an entry's Info is a second lookup, so comparing it against a stat would compare
// two stats and never observe the numbers the listing itself carries.
func listedInodes(t *testing.T, dir string) map[string]uint64 {
	t.Helper()
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	listed := map[string]uint64{}
	buf := make([]byte, 8192)
	for {
		n, err := unix.Getdents(fd, buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return listed
		}
		for offset := 0; offset < n; {
			entry := (*unix.Dirent)(unsafe.Pointer(&buf[offset]))
			offset += int(entry.Reclen)
			name := make([]byte, 0, len(entry.Name))
			for _, c := range entry.Name {
				if c == 0 {
					break
				}
				name = append(name, byte(c))
			}
			if string(name) == "." || string(name) == ".." {
				continue
			}
			listed[string(name)] = entry.Ino
		}
	}
}

// ino is the number the mount reports for one path. It does not follow a link: the number
// wanted is the one belonging to the node at the name.
func ino(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Sys().(*syscall.Stat_t).Ino
}

// A rename frees the name it moved away from, and the node created at that name afterwards
// is a different node from the one that left it — both live, so neither may be given the
// other's number. `git init` writes a file, renames it into place, and writes the next one
// at the name it just freed; against a mount that hands the second the first's number it
// dies with SIGBUS.
func TestANodeAtANameARenameFreedIsANodeOfItsOwn(t *testing.T) {
	mountpoint, plain, _ := mountedPair(t)

	for _, root := range []string{mountpoint, plain} {
		if err := os.WriteFile(filepath.Join(root, "f"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "f.lock"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(root, "f.lock"), filepath.Join(root, "f")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "f.lock"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if moved, fresh := ino(t, filepath.Join(root, "f")), ino(t, filepath.Join(root, "f.lock")); moved == fresh {
			t.Fatalf("%s reports the renamed file and the new one at the freed name as the same node, %d",
				root, moved)
		}
	}
}

// A directory rename moves everything beneath it, so a file's identity has to survive its
// parent moving as much as it survives its own move — and the names underneath the old
// directory are freed by that move just as the old directory's own name is.
func TestRenamingADirectoryCarriesTheIdentitiesBeneathIt(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)

	if err := os.Mkdir(filepath.Join(mountpoint, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "d", "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := ino(t, filepath.Join(mountpoint, "d", "f"))

	if err := os.Rename(filepath.Join(mountpoint, "d"), filepath.Join(mountpoint, "moved")); err != nil {
		t.Fatal(err)
	}
	if after := ino(t, filepath.Join(mountpoint, "moved", "f")); after != before {
		t.Fatalf("the file reports %d after its directory moved and %d before it", after, before)
	}

	if err := os.Mkdir(filepath.Join(mountpoint, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "d", "f"), []byte("other"), 0o644); err != nil {
		t.Fatal(err)
	}
	if fresh := ino(t, filepath.Join(mountpoint, "d", "f")); fresh == before {
		t.Fatalf("the new file under the freed directory name reports %d, the number the moved file still holds", fresh)
	}
}

// A number outlives nothing. Once the node it named is gone, a node made at the same name
// is a new node, and a program that remembered the old number must not find it again.
func TestAnIdentityIsNotHandedOutASecondTime(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	path := filepath.Join(mountpoint, "f")

	if err := os.WriteFile(path, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	first := ino(t, path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}
	if second := ino(t, path); second == first {
		t.Fatalf("the file made after the first was removed reports the first's number, %d", second)
	}
}

// Two lookups of one name have to agree even when they run at the same time: a name that
// resolved to two numbers would be two nodes, and the kernel would hold each one's contents
// against the other's.
func TestConcurrentLookupsOfOneNameAgreeOnItsIdentity(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	const lookups = 16
	numbers := make([]uint64, lookups)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(lookups)
	done.Add(lookups)
	for i := range numbers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			info, err := os.Stat(filepath.Join(mountpoint, "f"))
			if err != nil {
				t.Error(err)
				return
			}
			numbers[i] = info.Sys().(*syscall.Stat_t).Ino
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for _, number := range numbers[1:] {
		if number != numbers[0] {
			t.Fatalf("concurrent lookups of one name reported %v", numbers)
		}
	}
}

// A node removed by somebody this mount never heard from is gone all the same, and the
// node that appears at its name afterwards is a new one. This is also what keeps the
// mount's record of names from being a list of everything the namespace ever held.
func TestANodeReplacedUnderneathTheMountIsANewNode(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	first := ino(t, filepath.Join(mountpoint, "f"))

	if err := os.Remove(filepath.Join(backing, "f")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(mountpoint, "f")); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("stat of the removed file failed with %v, want ENOENT", err)
	}
	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("second"), 0o644); err != nil {
		t.Fatal(err)
	}
	if second := ino(t, filepath.Join(mountpoint, "f")); second == first {
		t.Fatalf("the file that appeared at the name reports %d, the number the removed one had", second)
	}
}

// A name that becomes a directory names a different node from the file it named before,
// and the file is live for as long as anything still has it open.
func TestANameThatBecomesADirectoryIsADifferentNode(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	if err := os.WriteFile(filepath.Join(backing, "x"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	open, err := os.Open(filepath.Join(mountpoint, "x"))
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	asFile := ino(t, filepath.Join(mountpoint, "x"))

	if err := os.Remove(filepath.Join(backing, "x")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(backing, "x"), 0o755); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(mountpoint, "x"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("the mount reports x as %v, want a directory", info.Mode())
	}
	if asDir := info.Sys().(*syscall.Stat_t).Ino; asDir == asFile {
		t.Fatalf("the directory reports %d, the number the file it replaced still holds", asDir)
	}
}

// A listing is the whole truth about one directory, so it is where a name removed by
// somebody this mount never heard from is noticed without anything asking for it by name.
func TestAListingNoticesWhatTheDirectoryNoLongerHas(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(backing, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	first := listedInodes(t, mountpoint)

	if err := os.Remove(filepath.Join(backing, "a")); err != nil {
		t.Fatal(err)
	}
	if listed := listedInodes(t, mountpoint); len(listed) != 1 {
		t.Fatalf("the listing holds %v after one of the two was removed", listed)
	}
	if err := os.WriteFile(filepath.Join(backing, "a"), []byte("again"), 0o644); err != nil {
		t.Fatal(err)
	}

	if again := ino(t, filepath.Join(mountpoint, "a")); again == first["a"] {
		t.Fatalf("the file that appeared at the name reports %d, the number the removed one had", again)
	}
	if kept := ino(t, filepath.Join(mountpoint, "b")); kept != first["b"] {
		t.Fatalf("the file the listing still held reports %d, and reported %d before", kept, first["b"])
	}
}

// --- symbolic links --------------------------------------------------------------------

// The namespace has no operation that makes a symbolic link or reads one back, so this
// version does not provide them (R-FS-1 asks for them eventually). A directory the server
// was given can hold one all the same, put there by whatever else reaches that directory,
// and what the mount owes such a link is an honest report: it is a link, and where it
// points is something this mount cannot say.
//
// plantLink puts one, and the file it points at, into a directory.
func plantLink(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "target"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nowhere", filepath.Join(dir, "dangling")); err != nil {
		t.Fatal(err)
	}
}

// A link is reported as a link, with its own length rather than the length of what it
// points at, and a link to nothing is reported as being there. Reporting either as an
// ordinary file would put a different object under that name: reads and writes aimed at
// the link would land on its target, and a program that copies the tree would replace the
// link with a copy of the file.
//
// The plain directory is the answer here as everywhere: a link is what lstat(2) says it is.
func TestASymbolicLinkIsReportedAsALink(t *testing.T) {
	mountpoint, plain, backing := mountedPair(t)
	plantLink(t, backing)
	plantLink(t, plain)

	for _, name := range []string{"link", "dangling"} {
		got, err := os.Lstat(filepath.Join(mountpoint, name))
		if err != nil {
			t.Fatalf("lstat %s through the mount: %v", name, err)
		}
		want, err := os.Lstat(filepath.Join(plain, name))
		if err != nil {
			t.Fatal(err)
		}
		if got.Mode().Type() != want.Mode().Type() {
			t.Errorf("the mount reports %s as %v, the plain directory as %v", name, got.Mode(), want.Mode())
		}
		if got.Size() != want.Size() {
			t.Errorf("the mount reports %s as %d bytes, the plain directory as %d — a link's length is the length of the name it holds",
				name, got.Size(), want.Size())
		}
	}
}

// A symbolic link is one node, and one node holds one number for as long as it is there. A
// listing and a lookup each report what is at the name, so a mount whose two answers named
// different kinds would conclude on every alternation that the node had been replaced, and
// hand what is in fact one live node a fresh number each time.
func TestASymbolicLinkKeepsOneIdentity(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	plantLink(t, backing)

	first := listedInodes(t, mountpoint)["link"]
	for range 3 {
		if listed := listedInodes(t, mountpoint)["link"]; listed != first {
			t.Fatalf("a listing reports link as %d, and the first listing reported %d", listed, first)
		}
		if looked := ino(t, filepath.Join(mountpoint, "link")); looked != first {
			t.Fatalf("a lookup reports link as %d, and the listing reports %d", looked, first)
		}
	}
}

// Where a link points cannot be said, so nothing resolves through one — and the refusal
// says that rather than saying the name is not a link, or answering with the target.
//
// Reading the target's contents under the link's name is the failure that matters: it
// succeeds, so nothing above notices, and the bytes belong to a file the caller did not
// name. EOPNOTSUPP is the FUSE library's answer for a node that cannot be read back as a
// link, and it is the true one: the namespace has no operation that could answer.
func TestNothingResolvesThroughASymbolicLink(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	plantLink(t, backing)
	link := filepath.Join(mountpoint, "link")

	for _, c := range []struct {
		name string
		run  func() error
	}{
		{"readlink", func() error { _, err := os.Readlink(link); return err }},
		{"stat, which follows", func() error { _, err := os.Stat(link); return err }},
		{"open", func() error {
			f, err := os.Open(link)
			if err == nil {
				f.Close()
			}
			return err
		}},
		{"read", func() error { _, err := os.ReadFile(link); return err }},
		{"write", func() error { return os.WriteFile(link, []byte("elsewhere"), 0o644) }},
	} {
		t.Run(c.name+" is refused", func(t *testing.T) {
			err := c.run()
			if err == nil {
				t.Fatal("the operation succeeded; it can only have been answered by the file the link points at")
			}
			if !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("the operation failed with %v, want EOPNOTSUPP", err)
			}
		})
	}

	// Nothing reached the target under the link's name.
	body, err := os.ReadFile(filepath.Join(backing, "target"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "payload" {
		t.Fatalf("the file the link points at now holds %q, want %q", body, "payload")
	}
}

// Removing a link removes the link. The file it points at is a different node and is not
// the caller's to lose here, and a removal that followed the link would take it silently:
// the name the caller gave would still be gone, so nothing would look wrong.
func TestRemovingASymbolicLinkLeavesWhatItPointsAt(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	plantLink(t, backing)

	if err := os.Remove(filepath.Join(mountpoint, "link")); err != nil {
		t.Fatalf("removing the link: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(backing, "link")); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the link is still in the namespace (%v)", err)
	}
	body, err := os.ReadFile(filepath.Join(backing, "target"))
	if err != nil {
		t.Fatalf("the file the link pointed at is gone: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("the file the link pointed at now holds %q, want %q", body, "payload")
	}
}

// --- nothing is cached ---------------------------------------------------------------

// countingStorage records how many times each operation was asked for.

type countingStorage struct {
	storage.Storage
	mu     sync.Mutex
	counts map[string]int
}

func (s *countingStorage) record(operation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[operation]++
}

func (s *countingStorage) count(operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[operation]
}

func (s *countingStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	s.record("Stat")
	return s.Storage.Stat(ctx, path)
}

func (s *countingStorage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	s.record("List")
	return s.Storage.List(ctx, path)
}

func (s *countingStorage) Read(ctx context.Context, path string) ([]byte, error) {
	s.record("Read")
	return s.Storage.Read(ctx, path)
}

// Nothing is held locally, so nothing can go stale, and that is why this filesystem needs
// no way of being told that something changed. This is the executable form of it: every
// look at the namespace reaches the namespace. A kernel allowed to remember an answer,
// for however short a time, is a kernel that can hand out a stale one.
func TestEveryLookAtTheNamespaceReachesIt(t *testing.T) {
	backing := t.TempDir()
	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	inner, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	counted := &countingStorage{Storage: inner, counts: map[string]int{}}
	mountpoint := mountStorage(t, counted, fuse.Options{Logger: testLogger(t)})
	path := filepath.Join(mountpoint, "f")

	for _, c := range []struct {
		name      string
		operation string
		act       func() error
	}{
		{"a stat", "Stat", func() error { _, err := os.Stat(path); return err }},
		{"a listing", "List", func() error { _, err := os.ReadDir(mountpoint); return err }},
		{"a read", "Read", func() error { _, err := os.ReadFile(path); return err }},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Twice, because the first of anything reaches the namespace whether or not
			// the answer is then kept.
			for range 2 {
				before := counted.count(c.operation)
				if err := c.act(); err != nil {
					t.Fatal(err)
				}
				if counted.count(c.operation) == before {
					t.Fatalf("%s was answered without a %s reaching the namespace",
						c.name, c.operation)
				}
			}
		})
	}
}

// --- error paths ---------------------------------------------------------------------

// unreachable is the storage error a network implementation produces when it cannot
// reach the server: an ordinary error carrying no errno at all.
var unreachable = errors.New("the namespace is unreachable")

// faultyStorage runs every call past a fault before passing it through. Each operation
// resolves its own failure, so each one is a separate chance to turn a failure into a
// plausible-looking answer.
type faultyStorage struct {
	storage.Storage
	mu    sync.Mutex
	fault func(operation, path string) error
}

func (s *faultyStorage) check(operation, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fault(operation, path)
}

func (s *faultyStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	if err := s.check("Stat", path); err != nil {
		return storage.Attr{}, err
	}
	return s.Storage.Stat(ctx, path)
}

func (s *faultyStorage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := s.check("SetAttr", path); err != nil {
		return err
	}
	return s.Storage.SetAttr(ctx, path, change)
}

func (s *faultyStorage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	if err := s.check("List", path); err != nil {
		return nil, err
	}
	return s.Storage.List(ctx, path)
}

func (s *faultyStorage) Read(ctx context.Context, path string) ([]byte, error) {
	if err := s.check("Read", path); err != nil {
		return nil, err
	}
	return s.Storage.Read(ctx, path)
}

func (s *faultyStorage) Write(ctx context.Context, path string, content []byte) error {
	if err := s.check("Write", path); err != nil {
		return err
	}
	return s.Storage.Write(ctx, path, content)
}

func (s *faultyStorage) Create(ctx context.Context, path string) error {
	if err := s.check("Create", path); err != nil {
		return err
	}
	return s.Storage.Create(ctx, path)
}

func (s *faultyStorage) Mkdir(ctx context.Context, path string) error {
	if err := s.check("Mkdir", path); err != nil {
		return err
	}
	return s.Storage.Mkdir(ctx, path)
}

func (s *faultyStorage) Remove(ctx context.Context, path string) error {
	if err := s.check("Remove", path); err != nil {
		return err
	}
	return s.Storage.Remove(ctx, path)
}

func (s *faultyStorage) RemoveDir(ctx context.Context, path string) error {
	if err := s.check("RemoveDir", path); err != nil {
		return err
	}
	return s.Storage.RemoveDir(ctx, path)
}

func (s *faultyStorage) Rename(ctx context.Context, from, to string) error {
	if err := s.check("Rename", from); err != nil {
		return err
	}
	return s.Storage.Rename(ctx, from, to)
}

// failing produces a fault that fails one named operation, whatever it is applied to.
func failing(operation string, err error) func(string, string) error {
	return func(candidate, _ string) error {
		if candidate == operation {
			return err
		}
		return nil
	}
}

// afterTheLookup produces a fault that lets the first Stat of one path through and fails
// every Stat of it after that. The lookup that precedes an open, a creation or a
// truncation is a Stat too, and it has to succeed for the operation itself to be
// attempted at all; this reaches the Stat the operation makes on its own behalf.
func afterTheLookup(path string, err error) func(string, string) error {
	looked := false
	return func(operation, candidate string) error {
		if operation != "Stat" || candidate != path {
			return nil
		}
		if looked {
			return err
		}
		looked = true
		return nil
	}
}

// mountFaulty mounts a namespace prepared by prepare, with fault deciding what fails.
func mountFaulty(t *testing.T, fault func(operation, path string) error, prepare func(backing string)) string {
	t.Helper()
	backing := t.TempDir()
	prepare(backing)
	inner, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	s := &faultyStorage{Storage: inner, fault: fault}
	return mountStorage(t, s, fuse.Options{Logger: testLogger(t)})
}

func nothingIsThere(string) {}

// A storage failure that carries no errno must arrive as EIO. ENOENT would say "that
// file is not there" when the truth is "I could not find out", and whatever runs on top
// acts on those two very differently.
func TestAnUnreachableNamespaceIsNotFileNotFound(t *testing.T) {
	mountpoint := mountFaulty(t, failing("Stat", unreachable), func(backing string) {
		if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	})

	_, err := os.Stat(filepath.Join(mountpoint, "f"))
	if errors.Is(err, syscall.ENOENT) {
		t.Fatal("stat reported that the file does not exist; the namespace was unreachable")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("stat failed with %v, want EIO", err)
	}
}

// The same for a directory listing: an empty listing reads as established fact, and
// acting on it deletes things.
func TestAnUnreachableNamespaceIsNotAnEmptyDirectory(t *testing.T) {
	mountpoint := mountFaulty(t, failing("List", unreachable), func(backing string) {
		if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	})

	entries, err := os.ReadDir(mountpoint)
	if err == nil {
		t.Fatalf("listing succeeded with %v; the namespace was unreachable", entries)
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("listing failed with %v, want EIO", err)
	}
}

func TestAnUnreachableNamespaceFailsEveryOperation(t *testing.T) {
	for _, c := range []struct {
		operation string
		act       func(root string) error
	}{
		{"Read", func(root string) error { _, err := os.ReadFile(filepath.Join(root, "f")); return err }},
		{"Write", func(root string) error { return os.WriteFile(filepath.Join(root, "f"), []byte("x"), 0o644) }},
		{"Create", func(root string) error {
			f, err := os.OpenFile(filepath.Join(root, "new"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			return f.Close()
		}},
		{"Mkdir", func(root string) error { return os.Mkdir(filepath.Join(root, "d"), 0o755) }},
		{"Remove", func(root string) error { return syscall.Unlink(filepath.Join(root, "f")) }},
		{"RemoveDir", func(root string) error { return syscall.Rmdir(filepath.Join(root, "existing")) }},
		{"Rename", func(root string) error {
			return os.Rename(filepath.Join(root, "f"), filepath.Join(root, "g"))
		}},
		{"SetAttr", func(root string) error { return os.Chmod(filepath.Join(root, "f"), 0o600) }},
	} {
		t.Run(c.operation, func(t *testing.T) {
			mountpoint := mountFaulty(t, failing(c.operation, unreachable), func(backing string) {
				if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(backing, "existing"), 0o755); err != nil {
					t.Fatal(err)
				}
			})
			if err := c.act(mountpoint); !errors.Is(err, syscall.EIO) {
				t.Fatalf("with %s failing, the operation returned %v, want EIO", c.operation, err)
			}
		})
	}
}

// A storage that does carry an errno keeps it. Collapsing every failure to EIO would
// hide EACCES, ENOSPC and EDQUOT, which callers do act on.
func TestAStorageErrnoReachesTheCallerUnchanged(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.ENOSPC, syscall.EDQUOT} {
		t.Run(errno.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("reaching the namespace: %w", errno)
			mountpoint := mountFaulty(t, failing("Stat", wrapped), nothingIsThere)
			_, err := os.Stat(filepath.Join(mountpoint, "f"))
			if !errors.Is(err, errno) {
				t.Fatalf("stat failed with %v, want %v", err, errno)
			}
		})
	}
}

// Setting a time on a file whose buffer is still uncommitted commits it first, so that the
// write does not stamp its own time over the one being asked for. That commit can fail,
// and a failure there is the whole request failing: reporting the time as set while the
// contents it belongs to never arrived would describe a file that does not exist.
func TestAFailureCommittingBeforeATimeIsSetIsReported(t *testing.T) {
	mountpoint := mountFaulty(t, failing("Write", unreachable), nothingIsThere)

	f, err := os.Create(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}

	moment := time.Date(2004, time.July, 6, 1, 2, 3, 0, time.UTC)
	if err := futimens(f, moment, moment); !errors.Is(err, syscall.EIO) {
		t.Fatalf("setting the times through the handle returned %v, want EIO", err)
	}
}

// The commit happens on close, because that is the last moment a failure can still be
// reported to whoever caused it. Release cannot report anything.
func TestACommitFailureIsReportedToClose(t *testing.T) {
	mountpoint := mountFaulty(t, failing("Write", unreachable), nothingIsThere)

	f, err := os.Create(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("payload")); err != nil {
		f.Close()
		t.Fatalf("the write itself failed with %v; it should only fail on close", err)
	}
	if err := f.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("close returned %v, want EIO", err)
	}
}

// The namespace can fail after a create has already succeeded. Reading back what was
// just made is the only way to know its attributes, so a failure there has to be a
// failure; inventing the attributes of a file we could not look at would be a guess
// presented as fact.
func TestAFailureReadingBackWhatWasJustMadeIsReported(t *testing.T) {
	for _, c := range []struct {
		name string
		act  func(root string) error
	}{
		{"a created file", func(root string) error {
			f, err := os.Create(filepath.Join(root, "new"))
			if err != nil {
				return err
			}
			return f.Close()
		}},
		{"a created directory", func(root string) error {
			return os.Mkdir(filepath.Join(root, "new"), 0o755)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mountpoint := mountFaulty(t, afterTheLookup("new", unreachable), nothingIsThere)
			if err := c.act(mountpoint); !errors.Is(err, syscall.EIO) {
				t.Fatalf("the operation returned %v, want EIO", err)
			}
		})
	}
}

// Giving a node just made the permissions the caller asked for is a step of its own, and
// it can fail on its own. Reporting the node as made, with permissions nobody asked for,
// leaves the caller acting on a file that is not the one they asked for.
func TestAFailureGivingANewNodeItsModeIsReported(t *testing.T) {
	for _, c := range []struct {
		name string
		act  func(root string) error
	}{
		{"a created file", func(root string) error {
			f, err := os.OpenFile(filepath.Join(root, "new"), os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			return f.Close()
		}},
		{"a created directory", func(root string) error {
			return os.Mkdir(filepath.Join(root, "new"), 0o700)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mountpoint := mountFaulty(t, failing("SetAttr", unreachable), nothingIsThere)
			if err := c.act(mountpoint); !errors.Is(err, syscall.EIO) {
				t.Fatalf("the operation returned %v, want EIO", err)
			}
		})
	}
}

// Shortening a file with no handle open asks the namespace two questions: how large the
// file is, which decides whether this mount can hold it at all, and what it holds. Either
// can fail, and neither can be answered with a guess.
func TestAFailureShorteningAFileWithNoHandleOpenIsReported(t *testing.T) {
	for _, c := range []struct {
		name  string
		fault func(operation, path string) error
	}{
		{"the size cannot be learned", afterTheLookup("f", unreachable)},
		{"the contents cannot be fetched", failing("Read", unreachable)},
	} {
		t.Run(c.name, func(t *testing.T) {
			mountpoint := mountFaulty(t, c.fault, func(backing string) {
				if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
					t.Fatal(err)
				}
			})
			if err := os.Truncate(filepath.Join(mountpoint, "f"), 2); !errors.Is(err, syscall.EIO) {
				t.Fatalf("truncate returned %v, want EIO", err)
			}
		})
	}
}

// Opening a file asks the namespace how large it is before fetching it, because a file
// this mount cannot hold has to be refused rather than fetched. That question can fail on
// its own, and a size we could not find out is not a size to guess at.
func TestAFailureLearningTheSizeOfAFileBeingOpenedIsReported(t *testing.T) {
	mountpoint := mountFaulty(t, afterTheLookup("f", unreachable), func(backing string) {
		if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	})

	f, err := os.Open(filepath.Join(mountpoint, "f"))
	if err == nil {
		f.Close()
		t.Fatal("the file opened; the namespace could not say how large it is")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening the file returned %v, want EIO", err)
	}
}

// A namespace can become unreachable while a file is open. Reporting the attributes it
// had at the open, or any other plausible set, would tell the program something we no
// longer know.
func TestAFailureStattingAnOpenFileIsReported(t *testing.T) {
	// Opening the file stats it too, to find out whether it fits under the ceiling, so
	// the failure is switched on after the open rather than counted out by call number.
	var unreachableNow atomic.Bool
	mountpoint := mountFaulty(t, func(operation, path string) error {
		if operation == "Stat" && path == "f" && unreachableNow.Load() {
			return unreachable
		}
		return nil
	}, func(backing string) {
		if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	})

	f, err := os.Open(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	unreachableNow.Store(true)
	if _, err := f.Stat(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("stat of the open file returned %v, want EIO", err)
	}
}

// --- what this filesystem refuses to answer ------------------------------------------

// oddStorage reports the nodes odd chooses as a kind the mount has no way to present.
type oddStorage struct {
	storage.Storage
	odd func(path string) bool
}

func (s *oddStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	attr, err := s.Storage.Stat(ctx, path)
	if err == nil && s.odd(path) {
		attr.Mode |= fs.ModeIrregular
	}
	return attr, err
}

func (s *oddStorage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	entries, err := s.Storage.List(ctx, path)
	for i := range entries {
		if s.odd(path + "/" + entries[i].Name) {
			entries[i].Attr.Mode |= fs.ModeIrregular
		}
	}
	return entries, err
}

// mountOdd mounts a namespace holding one ordinary file, with odd deciding which nodes
// are reported as a kind that cannot be presented.
func mountOdd(t *testing.T, odd func(path string) bool) string {
	t.Helper()
	backing := t.TempDir()
	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	inner, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	return mountStorage(t, &oddStorage{Storage: inner, odd: odd}, fuse.Options{Logger: testLogger(t)})
}

// A node whose kind we cannot name is refused rather than presented as an ordinary file.
// Presenting it would invite reads and writes that cannot mean what they appear to. Every
// operation that reports attributes has to refuse it, including the ones that report the
// attributes of something they have just made.
func TestANodeOfAnUnknownKindIsRefused(t *testing.T) {
	everythingBelowTheRoot := func(path string) bool { return path != "" }
	mountpoint := mountOdd(t, everythingBelowTheRoot)

	for _, c := range []struct {
		name string
		act  func() error
	}{
		{"stat", func() error { _, err := os.Stat(filepath.Join(mountpoint, "f")); return err }},
		{"list", func() error { _, err := os.ReadDir(mountpoint); return err }},
		{"create", func() error {
			f, err := os.Create(filepath.Join(mountpoint, "new"))
			if err != nil {
				return err
			}
			return f.Close()
		}},
		{"mkdir", func() error { return os.Mkdir(filepath.Join(mountpoint, "d"), 0o755) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := c.act(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("%s returned %v, want EIO", c.name, err)
			}
		})
	}
}

// A file's kind can change under an open handle. Reporting the attributes it had at the
// open would describe something that is no longer there.
func TestANodeThatBecomesUnknownWhileOpenIsRefused(t *testing.T) {
	var oddNow atomic.Bool
	mountpoint := mountOdd(t, func(path string) bool { return path == "f" && oddNow.Load() })

	f, err := os.Open(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	oddNow.Store(true)
	if _, err := f.Stat(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("stat of the open file returned %v, want EIO", err)
	}
}

// The mode reaches the namespace, which is checked underneath the mount rather than
// through it: a mount that remembered the mode without passing it on would answer every
// question about it correctly and store nothing.
func TestAModeChangeReachesTheNamespace(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	path := filepath.Join(mountpoint, "f")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, want := range []fs.FileMode{
		0o600, 0o755, 0o000, 0o777,
		0o755 | fs.ModeSetuid, 0o755 | fs.ModeSetgid, 0o777 | fs.ModeSticky,
		0o700 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky,
		0o644,
	} {
		if err := os.Chmod(path, want); err != nil {
			t.Fatalf("chmod to %v: %v", want, err)
		}
		underneath, err := os.Stat(filepath.Join(backing, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if underneath.Mode() != want {
			t.Fatalf("the namespace holds mode %v after a chmod to %v", underneath.Mode(), want)
		}
		through, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if through.Mode() != want {
			t.Fatalf("the mount reports mode %v where the namespace holds %v",
				through.Mode(), underneath.Mode())
		}
	}

	// A directory's mode too, since a mount could plausibly handle only files.
	dir := filepath.Join(mountpoint, "d")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	underneath, err := os.Stat(filepath.Join(backing, "d"))
	if err != nil {
		t.Fatal(err)
	}
	if underneath.Mode() != fs.ModeDir|0o700 {
		t.Fatalf("the namespace holds mode %v for the directory, want %v",
			underneath.Mode(), fs.ModeDir|0o700)
	}
}

// The two times are separate instants, so setting them apart is where a mount that keeps
// one field for both, or reports the modification time as the access time, gives itself
// away. cp -p and tar -x both set them apart.
func TestTimeChangesReachTheNamespace(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	path := filepath.Join(mountpoint, "f")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
	changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
	if err := os.Chtimes(path, accessed, changed); err != nil {
		t.Fatalf("setting the times: %v", err)
	}

	underneath, err := os.Stat(filepath.Join(backing, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if !underneath.ModTime().Equal(changed) {
		t.Fatalf("the namespace is dated %v, want %v", underneath.ModTime(), changed)
	}
	if got := accessTimeOf(underneath); !got.Equal(accessed) {
		t.Fatalf("the namespace was accessed %v, want %v", got, accessed)
	}

	through, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !through.ModTime().Equal(changed) {
		t.Fatalf("the mount reports the file dated %v, want %v", through.ModTime(), changed)
	}
	if got := accessTimeOf(through); !got.Equal(accessed) {
		t.Fatalf("the mount reports the file accessed %v, want %v; the access time is not the modification time",
			got, accessed)
	}

	// A time before the epoch crosses the kernel boundary as an unsigned field, which is
	// where it would come back as a date tens of billions of years away.
	early := time.Date(1902, time.March, 4, 5, 6, 7, 8, time.UTC)
	if err := os.Chtimes(path, early, early); err != nil {
		t.Fatalf("setting a time before the epoch: %v", err)
	}
	if got, err := os.Stat(path); err != nil || !got.ModTime().Equal(early) {
		t.Fatalf("the mount reports the file dated %v, %v, want %v", got.ModTime(), err, early)
	}
}

// futimens sets both of a file's times through the descriptor rather than through its
// name, which is the case the mount can put in order and the case tools that write a file
// and then date it produce.
//
// Linux reaches it through utimensat with a null path, and no wrapper in the standard
// library or in golang.org/x/sys/unix exposes that: unix.Futimes goes by way of
// /proc/self/fd, which is a name, and a name does not carry the descriptor to the
// filesystem underneath it.
func futimens(f *os.File, accessed, changed time.Time) error {
	var times [2]unix.Timespec
	for i, t := range [2]time.Time{accessed, changed} {
		ts, err := unix.TimeToTimespec(t)
		if err != nil {
			return err
		}
		times[i] = ts
	}
	if _, _, errno := unix.Syscall6(unix.SYS_UTIMENSAT, f.Fd(), 0,
		uintptr(unsafe.Pointer(&times)), 0, 0, 0); errno != 0 {
		return errno
	}
	return nil
}

func accessTimeOf(info fs.FileInfo) time.Time {
	accessed := info.Sys().(*syscall.Stat_t).Atim
	return time.Unix(accessed.Unix())
}

// A file with a handle open holds its contents in this process until the commit, and the
// commit stamps the namespace's own modification time over whatever was asked for. Setting
// a time through that handle therefore has to commit first, or a program that writes a file
// and dates it — tar and cp among them — gets the time of the close instead.
func TestATimeSetThroughAnOpenHandleSurvivesTheCommit(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)

	f, err := os.Create(filepath.Join(mountpoint, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("payload")); err != nil {
		f.Close()
		t.Fatal(err)
	}
	changed := time.Date(2004, time.July, 6, 1, 2, 3, 0, time.UTC)
	if err := futimens(f, changed, changed); err != nil {
		f.Close()
		t.Fatalf("setting the times through the open handle: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	underneath, err := os.Stat(filepath.Join(backing, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if !underneath.ModTime().Equal(changed) {
		t.Fatalf("the namespace is dated %v, want %v — the commit landed after the time was set",
			underneath.ModTime(), changed)
	}
	body, err := os.ReadFile(filepath.Join(backing, "f"))
	if err != nil || string(body) != "payload" {
		t.Fatalf("the namespace holds %q, %v; want the contents committed as well", body, err)
	}
}

// The namespace carries no ownership, and this mount reports every node as belonging to
// whoever made it. A request for that same owner asks for what is already the case, so
// answering it claims nothing untrue; a request for anyone else is a change with nowhere
// to go.
func TestOwnershipIsRefusedUnlessItIsAlreadyTheAnswer(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	path := filepath.Join(mountpoint, "f")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Chown(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chown to the user the mount already reports: %v", err)
	}
	if err := os.Chown(path, -1, os.Getgid()); err != nil {
		t.Fatalf("chgrp to the group the mount already reports: %v", err)
	}

	// A chown drops the setuid bit, on a plain directory and here alike. It is the kernel
	// that decides so: what reaches this filesystem is an ordinary mode change alongside
	// the ownership, and applying it is how a request nobody could refuse gets carried out.
	if err := os.Chmod(path, 0o755|fs.ModeSetuid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&fs.ModeSetuid != 0 {
		t.Fatalf("the file is still %v after a chown; the kernel asked for the setuid bit to go", info.Mode())
	}

	// Running as root, a chown to anyone would succeed on a plain directory too, so there
	// is nothing to compare against.
	if os.Getuid() == 0 {
		t.Log("running as root; a chown to another user is not refused by a plain directory either")
		return
	}
	if err := os.Chown(path, os.Getuid()+1, os.Getgid()); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("chown to another user returned %v, want EPERM", err)
	}
	if err := os.Chown(path, -1, os.Getgid()+1); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("chgrp to another group returned %v, want EPERM", err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if owner := info.Sys().(*syscall.Stat_t); owner.Uid != uint32(os.Getuid()) || owner.Gid != uint32(os.Getgid()) {
		t.Fatalf("the file is owned by %d:%d after the refusals, want %d:%d",
			owner.Uid, owner.Gid, os.Getuid(), os.Getgid())
	}
}

// Rename replaces whatever is at the destination. The renameat2 flags ask for something
// else, and both of them ask for it atomically, which nothing here can promise. Reporting
// success while having done the plain rename would be worse than refusing.
func TestRenameFlagsThatCannotBeHonouredAreRefused(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	from, to := filepath.Join(mountpoint, "from"), filepath.Join(mountpoint, "to")
	for _, p := range []string{from, to} {
		if err := os.WriteFile(p, []byte(filepath.Base(p)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// RENAME_NOREPLACE is checked against an existing destination by the kernel before
	// it reaches a filesystem, so the case that gets here is the one with no destination.
	absent := filepath.Join(mountpoint, "absent")
	for _, c := range []struct {
		name        string
		flag        uint
		destination string
	}{
		{"RENAME_NOREPLACE", unix.RENAME_NOREPLACE, absent},
		{"RENAME_EXCHANGE", unix.RENAME_EXCHANGE, to},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, c.destination, c.flag)
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("renaming with %s returned %v, want EINVAL", c.name, err)
			}
		})
	}

	for _, p := range []string{from, to} {
		body, err := os.ReadFile(p)
		if err != nil || string(body) != filepath.Base(p) {
			t.Fatalf("%s now reads %q, %v; a refused rename moved something", p, body, err)
		}
	}
	if _, err := os.Stat(absent); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("stat of the destination that should not exist gave %v", err)
	}
}

// The namespace has no extended attributes. The answer has to be about the filesystem —
// "there are none here" — and not about the file, because "this file has no such
// attribute" invites the caller to try setting one.
func TestExtendedAttributesAreRefusedAsUnsupported(t *testing.T) {
	mountpoint, _, _ := mountedPair(t)
	path := filepath.Join(mountpoint, "f")
	if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string
		act  func() error
	}{
		{"set", func() error { return unix.Setxattr(path, "user.thing", []byte("x"), 0) }},
		{"get", func() error { _, err := unix.Getxattr(path, "user.thing", make([]byte, 16)); return err }},
		{"list", func() error { _, err := unix.Listxattr(path, make([]byte, 64)); return err }},
		{"remove", func() error { return unix.Removexattr(path, "user.thing") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.act()
			if errors.Is(err, syscall.ENODATA) {
				t.Fatalf("%s reported that the file has no such attribute; "+
					"the truth is that the filesystem has no attributes at all", c.name)
			}
			if !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("%s returned %v, want EOPNOTSUPP", c.name, err)
			}
		})
	}
}

// --- the mount as a library ----------------------------------------------------------

// R-INT-2: linked into somebody else's process, this package may not write to that
// process's own output. The caller supplies a destination or gets none. Mounting with no
// destination given is the case where a careless implementation falls back to the
// standard logger, so that is the case this exercises.
func TestNothingIsWrittenToTheProcessOutput(t *testing.T) {
	requireFUSE(t)

	backing, mountpoint := t.TempDir(), t.TempDir()
	s, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}

	restore := captureProcessOutput(t)
	m, mountErr := fuse.New(mountpoint, s, fuse.Options{})
	if mountErr == nil {
		exerciseTheMount(t, mountpoint)
		unmount(t, m, mountpoint)
	}
	written := restore()

	if mountErr != nil {
		t.Fatalf("mounting at %s: %v", mountpoint, mountErr)
	}
	if written != "" {
		t.Fatalf("the package wrote this to the process's own output:\n%s", written)
	}
}

// Diagnostics go where the caller asked and nowhere else. The trace is the case where
// there is enough of it for a wrong destination to show.
func TestDiagnosticsGoOnlyWhereTheCallerAsked(t *testing.T) {
	requireFUSE(t)

	backing, mountpoint := t.TempDir(), t.TempDir()
	s, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	collected := &safeBuffer{}

	restore := captureProcessOutput(t)
	m, mountErr := fuse.New(mountpoint, s, fuse.Options{Logger: log.New(collected, "", 0), Debug: true})
	if mountErr == nil {
		exerciseTheMount(t, mountpoint)
		unmount(t, m, mountpoint)
	}
	written := restore()

	if mountErr != nil {
		t.Fatalf("mounting at %s: %v", mountpoint, mountErr)
	}
	if written != "" {
		t.Errorf("the package wrote this to the process's own output:\n%s", written)
	}
	if !strings.Contains(collected.String(), "LOOKUP") {
		t.Errorf("the trace the caller asked for never reached the caller's logger; it holds:\n%s",
			collected.String())
	}
}

func TestMountingSomewhereItCannotBeDone(t *testing.T) {
	requireFUSE(t)
	backing := t.TempDir()
	s, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}

	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// A directory that can be named but not entered. Nothing here can refuse it; the
	// mount is attempted and fails.
	unusable := filepath.Join(t.TempDir(), "unusable")
	if err := os.Mkdir(unusable, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(unusable, 0o755) })

	for _, mountpoint := range []string{filepath.Join(t.TempDir(), "absent"), file, unusable} {
		m, err := fuse.New(mountpoint, s, fuse.Options{Logger: testLogger(t)})
		if err == nil {
			m.Unmount()
			t.Errorf("mounting on %s succeeded, want an error", mountpoint)
			continue
		}
		t.Logf("mounting on %s failed with %v", mountpoint, err)
	}
}

// exerciseTheMount runs enough traffic through a mountpoint to reach every operation,
// including the ones that fail.
func exerciseTheMount(t *testing.T, mountpoint string) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(mountpoint, "d"), 0o755); err != nil {
		t.Error(err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "d", "f"), []byte("payload"), 0o644); err != nil {
		t.Error(err)
	}
	if _, err := os.ReadFile(filepath.Join(mountpoint, "d", "f")); err != nil {
		t.Error(err)
	}
	if _, err := os.ReadDir(filepath.Join(mountpoint, "d")); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(filepath.Join(mountpoint, "absent")); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("stat of a missing file failed with %v, want ENOENT", err)
	}
	if err := os.Rename(filepath.Join(mountpoint, "d", "f"), filepath.Join(mountpoint, "g")); err != nil {
		t.Error(err)
	}
	if err := os.Remove(filepath.Join(mountpoint, "g")); err != nil {
		t.Error(err)
	}
	if err := os.Remove(filepath.Join(mountpoint, "d")); err != nil {
		t.Error(err)
	}
}

// captureProcessOutput redirects the process's standard output and error, and the
// standard logger that writes to them, into a buffer. The returned function puts them
// back and reports everything that was written meanwhile.
func captureProcessOutput(t *testing.T) func() string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	collected := make(chan string, 1)
	go func() {
		var buf strings.Builder
		io.Copy(&buf, read)
		collected <- buf.String()
	}()

	stdout, stderr, logOut := os.Stdout, os.Stderr, log.Writer()
	os.Stdout, os.Stderr = write, write
	log.SetOutput(write)

	return func() string {
		os.Stdout, os.Stderr = stdout, stderr
		log.SetOutput(logOut)
		write.Close()
		written := <-collected
		read.Close()
		return written
	}
}

// safeBuffer collects what the FUSE library writes, which it does from many goroutines.
type safeBuffer struct {
	mu      sync.Mutex
	written strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.String()
}

// --- the ceiling on a single file ------------------------------------------------------

// mountCeiling mounts a namespace holding one file of the given contents, under a chosen
// ceiling. The counter comes back with it, because for the paths that refuse a file the
// namespace already holds, what has to be shown is not only the errno but that the
// contents were never fetched: fetching them is the allocation the ceiling exists to
// prevent, and an errno returned after it would be too late.
func mountCeiling(t *testing.T, contents []byte, maxFileSize int64) (path string, counted *countingStorage) {
	t.Helper()
	backing := t.TempDir()
	if err := os.WriteFile(filepath.Join(backing, "f"), contents, 0o644); err != nil {
		t.Fatal(err)
	}
	inner, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	counted = &countingStorage{Storage: inner, counts: map[string]int{}}
	mountpoint := mountStorage(t, counted, fuse.Options{Logger: testLogger(t), MaxFileSize: maxFileSize})
	return filepath.Join(mountpoint, "f"), counted
}

// An open file is held whole in memory, so the size of one file is the size of one
// allocation a caller can ask this process to make. Go answers an allocation it cannot
// satisfy with a fatal error rather than a panic: neither recover nor the FUSE library's
// panic handling catches it, and the process this package is linked into dies (R-INT-1,
// R-INT-2). Every path that allocates in proportion to a size has to refuse before
// allocating, and this covers each of them.
//
// The ceiling is small so that no case here allocates anything large, including on a run
// where the ceiling has stopped working.
func TestNoOperationGrowsAFilePastTheCeiling(t *testing.T) {
	const ceiling = 64

	t.Run("truncate(2) past the ceiling", func(t *testing.T) {
		path, _ := mountCeiling(t, []byte("payload"), ceiling)
		if err := os.Truncate(path, ceiling+1); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("truncate returned %v, want EFBIG", err)
		}
		if body, err := os.ReadFile(path); err != nil || string(body) != "payload" {
			t.Fatalf("the file reads %q, %v after a refused truncate", body, err)
		}
	})

	t.Run("ftruncate(2) past the ceiling", func(t *testing.T) {
		path, _ := mountCeiling(t, []byte("payload"), ceiling)
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(ceiling + 1); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("ftruncate returned %v, want EFBIG", err)
		}
		// The handle holds the buffer the truncation would have grown, so what it reports
		// afterwards is the evidence that nothing grew.
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != int64(len("payload")) {
			t.Fatalf("the open file is %d bytes after a refused ftruncate, want %d",
				info.Size(), len("payload"))
		}
	})

	t.Run("a write ending past the ceiling", func(t *testing.T) {
		path, _ := mountCeiling(t, []byte("payload"), ceiling)
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteAt([]byte("tail"), ceiling-2); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("a write ending past the ceiling returned %v, want EFBIG", err)
		}
	})

	t.Run("a create, seek and write past the ceiling", func(t *testing.T) {
		path, _ := mountCeiling(t, []byte("payload"), ceiling)
		f, err := os.Create(filepath.Join(filepath.Dir(path), "new"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteAt([]byte("tail"), ceiling); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("a write past the ceiling into a new file returned %v, want EFBIG", err)
		}
	})

	t.Run("a file the namespace holds above the ceiling cannot be opened", func(t *testing.T) {
		path, counted := mountCeiling(t, pattern(ceiling+1), ceiling)
		before := counted.count("Read")
		if _, err := os.Open(path); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("opening a file above the ceiling returned %v, want EFBIG", err)
		}
		if counted.count("Read") != before {
			t.Fatal("the contents were fetched before the open was refused; " +
				"the fetch is the allocation the ceiling exists to prevent")
		}
	})

	t.Run("a file above the ceiling cannot be shortened to a prefix", func(t *testing.T) {
		path, counted := mountCeiling(t, pattern(ceiling+1), ceiling)
		before := counted.count("Read")
		// Keeping a prefix means fetching what is there, which is the file this mount
		// cannot hold, so the shorter length being asked for does not make it possible.
		if err := os.Truncate(path, 8); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("shortening a file above the ceiling returned %v, want EFBIG", err)
		}
		if counted.count("Read") != before {
			t.Fatal("the contents were fetched before the truncation was refused")
		}
	})

	t.Run("a file above the ceiling can still be emptied", func(t *testing.T) {
		path, counted := mountCeiling(t, pattern(ceiling+1), ceiling)
		before := counted.count("Read")
		// Nothing that is there survives a truncation to nothing, so this one needs no
		// fetch, and a file too large to hold is still a file that can be emptied.
		if err := os.Truncate(path, 0); err != nil {
			t.Fatalf("emptying a file above the ceiling returned %v, want it to succeed", err)
		}
		if counted.count("Read") != before {
			t.Fatal("the contents were fetched in order to be discarded")
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Fatalf("the file is %d bytes after being emptied", info.Size())
		}
	})

	t.Run("a file at exactly the ceiling is served", func(t *testing.T) {
		path, _ := mountCeiling(t, pattern(ceiling), ceiling)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading a file of exactly the ceiling failed with %v", err)
		}
		if len(body) != ceiling {
			t.Fatalf("read %d bytes, want %d", len(body), ceiling)
		}
		if err := os.Truncate(path, ceiling); err != nil {
			t.Fatalf("truncating to exactly the ceiling failed with %v", err)
		}
	})
}

// A caller who never thought about the ceiling still gets one, because the alternative is
// that a zero Options leaves the process it is linked into one ordinary command away from
// dying. The size asked for here is a gibibyte, but nothing allocates it: the refusal
// happens before the allocation, which is the whole point of the ceiling.
func TestAMountConfiguredWithNothingStillHasACeiling(t *testing.T) {
	backing := t.TempDir()
	if err := os.WriteFile(filepath.Join(backing, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(mountStorage(t, s, fuse.Options{}), "f")

	if err := os.Truncate(path, fuse.DefaultMaxFileSize+1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("truncate to one byte above the default ceiling returned %v, want EFBIG", err)
	}
	if err := os.Truncate(path, 3); err != nil {
		t.Fatalf("truncate to a size the default ceiling allows returned %v", err)
	}
}

// The command that prompted the ceiling, run for real. It is only safe to run because the
// ceiling refuses it before anything is allocated: without one, this asks the runtime for
// a terabyte, and Go answers an allocation it cannot satisfy with a fatal error rather
// than a panic. Nothing recovers from that — not this test, not the FUSE library's panic
// handling — so the test binary would die with its mountpoint still attached, wedging the
// machine it ran on. The ceiling is therefore shown to hold at a harmless size first, and
// the terabyte is attempted only after that.
func TestTheTerabyteTruncateIsRefusedRatherThanAllocated(t *testing.T) {
	truncateBinary, err := exec.LookPath("truncate")
	if err != nil {
		t.Skipf("skipping: this test runs truncate(1) and it is not on PATH (%v)", err)
	}

	const ceiling = 64
	path, counted := mountCeiling(t, []byte("payload"), ceiling)

	if err := os.Truncate(path, ceiling+1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("the ceiling returned %v at %d bytes, want EFBIG; not attempting a terabyte",
			err, ceiling+1)
	}

	before := counted.count("Write")
	out, err := exec.Command(truncateBinary, "-s", "1T", path).CombinedOutput()
	t.Logf("truncate -s 1T %s\n%s", path, out)
	if err == nil {
		t.Fatal("truncate -s 1T succeeded")
	}
	// "File too large" is what strerror gives for EFBIG, which is what coreutils prints.
	if !strings.Contains(string(out), "File too large") {
		t.Fatalf("truncate -s 1T failed with %v and said %q, want EFBIG", err, out)
	}
	if counted.count("Write") != before {
		t.Fatal("the refused truncation still wrote to the namespace")
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "payload" {
		t.Fatalf("the file reads %q, %v after the refused truncation", body, err)
	}
}

// --- the room the namespace has --------------------------------------------------------

// Two facts about statfs belong here as a record rather than as a case, because neither is
// this filesystem's to decide.
//
// The kernel answers statfs itself, with a zeroed struct, for any caller that is not the
// user who made the mount, and never forwards the request. `sudo df` therefore sees a
// mount of zero blocks whatever the namespace would have said, and the zeroes it prints
// are the kernel's answer rather than one of ours.
//
// The ceiling on a single file applies whatever Avail says, so a tool that reads statfs,
// sees room, and starts copying can still be refused with EFBIG partway through. The two
// limits answer different questions — how large a file this mount will hold in memory, and
// how much the workspace may hold in all — and neither stands in for the other.

// reportedBlock is the unit the mount reports space in. The figures below are written as
// multiples of it so that what each case is checking stays legible.
const reportedBlock = 4096

// tellsSpace answers Space with a figure of the test's choosing. The arithmetic has to be
// checked against exact numbers, and the room a real filesystem has left is neither exact
// nor still.
type tellsSpace struct {
	storage.Storage
	space storage.Space
	err   error
}

func (s *tellsSpace) Space(context.Context) (storage.Space, error) {
	if s.err != nil {
		return storage.Space{}, s.err
	}
	return s.space, nil
}

// mountTelling mounts a namespace held in a fresh directory that reports the given room.
func mountTelling(t *testing.T, space storage.Space, err error) (mountpoint, backing string) {
	t.Helper()
	backing = t.TempDir()
	inner, openErr := localdir.New(backing)
	if openErr != nil {
		t.Fatal(openErr)
	}
	s := &tellsSpace{Storage: inner, space: space, err: err}
	return mountStorage(t, s, fuse.Options{Logger: testLogger(t)}), backing
}

// A namespace with no room of its own to report answers ENOSYS, and that reaches the caller
// as it stands: df says "Function not implemented". The answer that may never be given is
// the FUSE library's own default, a zeroed reply, which reads as a filesystem with no space
// left — a program that checks for room before writing would believe it (R-ERR-2).
func TestSpaceThatCannotBeSeenIsNotDescribed(t *testing.T) {
	mountpoint, _ := mountTelling(t, storage.Space{}, syscall.ENOSYS)

	var described unix.Statfs_t
	if err := unix.Statfs(mountpoint, &described); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("statfs returned %v and reported %d blocks of %d bytes, want ENOSYS",
			err, described.Blocks, described.Bsize)
	}
}

// A namespace that does report its room has that report converted into blocks, and the
// conversion floors: a block that cannot be filled is not offered.
func TestSpaceIsReportedInWholeBlocks(t *testing.T) {
	for _, c := range []struct {
		name                  string
		space                 storage.Space
		blocks, bfree, bavail uint64
	}{
		{"a workspace with nothing in it",
			storage.Space{Total: 100 * reportedBlock, Avail: 100 * reportedBlock}, 100, 100, 100},
		{"a workspace part spent",
			storage.Space{Total: 100 * reportedBlock, Used: 40 * reportedBlock, Avail: 60 * reportedBlock}, 100, 60, 60},
		// A filesystem keeps a reserve only the superuser may spend, which makes what may
		// still be written smaller than what the limit leaves. Reporting either as the other
		// would state a quantity nobody measured.
		{"a reserve that only the superuser may spend",
			storage.Space{Total: 100 * reportedBlock, Used: 40 * reportedBlock, Avail: 55 * reportedBlock}, 100, 60, 55},
		// What an allowance lowered underneath content already written looks like. Nothing
		// is repaired and nothing goes negative: what is left is none.
		{"more spent than the limit allows",
			storage.Space{Total: 100 * reportedBlock, Used: 140 * reportedBlock}, 100, 0, 0},
		{"a limit that is not a whole number of blocks",
			storage.Space{Total: 100*reportedBlock + 4095, Avail: 100*reportedBlock + 4095}, 100, 100, 100},
		{"a part-filled block counts as spent",
			storage.Space{Total: 100 * reportedBlock, Used: 1, Avail: 100*reportedBlock - 1}, 100, 99, 99},
	} {
		t.Run(c.name, func(t *testing.T) {
			mountpoint, _ := mountTelling(t, c.space, nil)

			var described unix.Statfs_t
			if err := unix.Statfs(mountpoint, &described); err != nil {
				t.Fatalf("statfs failed with %v", err)
			}
			if described.Bsize != reportedBlock || described.Frsize != reportedBlock {
				t.Fatalf("the mount reports blocks of %d and fragments of %d bytes, want %d",
					described.Bsize, described.Frsize, reportedBlock)
			}
			if described.Blocks != c.blocks || described.Bfree != c.bfree || described.Bavail != c.bavail {
				t.Fatalf("the mount reports %d blocks, %d free, %d available; want %d, %d, %d",
					described.Blocks, described.Bfree, described.Bavail, c.blocks, c.bfree, c.bavail)
			}
			if described.Bavail > described.Bfree || described.Bfree > described.Blocks {
				t.Fatalf("the mount reports %d available of %d free of %d blocks, which cannot all be true",
					described.Bavail, described.Bfree, described.Blocks)
			}
			// This system charges bytes and counts no inodes, so there is no figure to give.
			if described.Files != 0 || described.Ffree != 0 {
				t.Fatalf("the mount reports %d inodes and %d free; it counts none",
					described.Files, described.Ffree)
			}
			if described.Namelen != 255 {
				t.Fatalf("the mount says a name may be %d bytes, want 255", described.Namelen)
			}
		})
	}
}

// An answer that cannot be true of anything is a failure to report, not a set of numbers
// to repair. These fields cross into the kernel unsigned, where a negative arrives as an
// enormous positive and offers room no disk anywhere holds (R-ERR-2).
func TestASpaceThatCannotBeTrueIsRefused(t *testing.T) {
	for _, c := range []struct {
		name  string
		space storage.Space
	}{
		{"a negative limit", storage.Space{Total: -reportedBlock}},
		{"more spent than can be spent", storage.Space{Total: reportedBlock, Used: -1}},
		{"a negative amount left", storage.Space{Total: reportedBlock, Avail: -1}},
		{"more left than the limit leaves",
			storage.Space{Total: 100 * reportedBlock, Used: 40 * reportedBlock, Avail: 61 * reportedBlock}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mountpoint, _ := mountTelling(t, c.space, nil)

			var described unix.Statfs_t
			if err := unix.Statfs(mountpoint, &described); !errors.Is(err, syscall.EIO) {
				t.Fatalf("statfs returned %v and reported %d blocks, %d available; want EIO",
					err, described.Blocks, described.Bavail)
			}
		})
	}
}

// A write that would carry the namespace past its limit is refused at the write(2) that
// asked for it, with EDQUOT: the disk is not full, an allowance is spent.
//
// Which call reports it is the whole point. The contents of an open file are held until
// the commit, and the commit happens at close(2), so a refusal discovered only there
// reaches a caller that in a large share of programs never looks at what close(2)
// returned — and the bytes go missing in silence.
func TestAWriteWithNoRoomForItIsRefusedAtTheWrite(t *testing.T) {
	const room = 10
	full := storage.Space{Total: 1 << 20, Used: 1<<20 - room, Avail: room}

	t.Run("a write larger than the room left", func(t *testing.T) {
		mountpoint, backing := mountTelling(t, full, nil)
		f, err := os.Create(filepath.Join(mountpoint, "f"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		if _, err := f.Write(make([]byte, room+1)); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("writing %d bytes into a workspace with %d left returned %v, want EDQUOT",
				room+1, room, err)
		}
		// Nothing was buffered, so there is nothing to commit and close has nothing to
		// report. This is where the refusal would have surfaced had it waited for the
		// commit, which is the arrival nobody would have seen.
		if err := f.Close(); err != nil {
			t.Fatalf("close returned %v; the refusal already reached the write", err)
		}
		if body, err := os.ReadFile(filepath.Join(backing, "f")); err != nil || len(body) != 0 {
			t.Fatalf("the namespace holds %d bytes, %v; a refused write left something behind",
				len(body), err)
		}
	})

	t.Run("a write that exactly fills the room left", func(t *testing.T) {
		mountpoint, backing := mountTelling(t, full, nil)
		if err := os.WriteFile(filepath.Join(mountpoint, "f"), make([]byte, room), 0o644); err != nil {
			t.Fatalf("writing exactly the %d bytes left returned %v", room, err)
		}
		if body, err := os.ReadFile(filepath.Join(backing, "f")); err != nil || len(body) != room {
			t.Fatalf("the namespace holds %d bytes, %v; want %d", len(body), err, room)
		}
	})

	// What a write needs is the room its growth asks for, not the room the whole file would
	// take. A mount that weighed the file's length would refuse every change to a large file
	// in a workspace near its limit, including one that shortens it.
	t.Run("appending to a file the namespace already holds", func(t *testing.T) {
		mountpoint, backing := mountTelling(t, full, nil)
		const held = 1 << 16
		if err := os.WriteFile(filepath.Join(backing, "big"), pattern(held), 0o644); err != nil {
			t.Fatal(err)
		}

		f, err := os.OpenFile(filepath.Join(mountpoint, "big"), os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.Write([]byte("tail")); err != nil {
			t.Fatalf("appending 4 bytes to a file of %d bytes returned %v, with %d bytes of room left",
				held, err, room)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Stat(filepath.Join(backing, "big")); err != nil || info.Size() != held+4 {
			t.Fatalf("the namespace holds %v bytes, %v; want %d", info.Size(), err, held+4)
		}
	})
}

// mountLimited mounts a namespace held in backing under an allowance of that many bytes.
// The allowance is enforced by the storage rather than described by a fixture, because the
// cases below turn on one operation reaching the namespace by two routes and having to be
// answered the same way on both.
func mountLimited(t *testing.T, backing string, allowance int64) string {
	t.Helper()
	inner, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	held, err := limited.New(context.Background(), inner, allowance)
	if err != nil {
		t.Fatal(err)
	}
	return mountStorage(t, held, fuse.Options{Logger: testLogger(t)})
}

// A truncation that would carry the namespace past its allowance is refused at the call
// that asked for it, and is refused there whether or not the caller holds the file open.
//
// ftruncate(2) arrives as a size change on an open handle, whose contents are held until
// the commit at close(2); truncate(2) on the same file arrives with no handle and goes
// straight to the namespace, which refuses it at the syscall. Weighing only the ceiling on
// a single file's size in the first would leave the allowance to be discovered at the
// commit — a refusal that reaches only close(2), which a large share of programs never
// look at (R-WS-5) — and one operation would then be answered in two different places.
func TestATruncationWithNoRoomForItIsRefusedAtTheTruncation(t *testing.T) {
	const allowance = 64 << 10

	t.Run("growing through an open descriptor", func(t *testing.T) {
		backing := t.TempDir()
		mountpoint := mountLimited(t, backing, allowance)

		f, err := os.Create(filepath.Join(mountpoint, "f"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		if err := f.Truncate(allowance + 1); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("ftruncate to %d bytes under an allowance of %d returned %v, want EDQUOT",
				allowance+1, allowance, err)
		}
		// Nothing was lengthened, so there is nothing to commit and close has nothing to
		// report. This is where the refusal would have surfaced had it waited for the
		// commit, which is the arrival nobody would have seen.
		if err := f.Close(); err != nil {
			t.Fatalf("close returned %v; the refusal already reached the ftruncate", err)
		}
		held, err := os.Stat(filepath.Join(backing, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if held.Size() != 0 {
			t.Fatalf("the namespace holds %d bytes; a refused truncation lengthened the file",
				held.Size())
		}
	})

	t.Run("growing with no descriptor", func(t *testing.T) {
		backing := t.TempDir()
		mountpoint := mountLimited(t, backing, allowance)
		path := filepath.Join(mountpoint, "f")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}

		if err := os.Truncate(path, allowance+1); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("truncate to %d bytes under an allowance of %d returned %v, want EDQUOT",
				allowance+1, allowance, err)
		}
		held, err := os.Stat(filepath.Join(backing, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if held.Size() != 0 {
			t.Fatalf("the namespace holds %d bytes; a refused truncation lengthened the file",
				held.Size())
		}
	})

	// A workspace past its allowance has to have a way back under it, and shortening a file
	// is that way. There is no room left at all here, and the truncation is carried out
	// regardless, because what it needs is the room its growth asks for and it grows by
	// nothing.
	t.Run("shrinking from over the allowance", func(t *testing.T) {
		const stands = allowance + (16 << 10)
		backing := t.TempDir()
		if err := os.WriteFile(filepath.Join(backing, "f"), pattern(stands), 0o644); err != nil {
			t.Fatal(err)
		}
		mountpoint := mountLimited(t, backing, allowance)

		var described unix.Statfs_t
		if err := unix.Statfs(mountpoint, &described); err != nil || described.Bavail != 0 {
			t.Fatalf("the mount reports %d blocks available, %v; the workspace holds %d of an allowance of %d and has none",
				described.Bavail, err, stands, allowance)
		}

		f, err := os.OpenFile(filepath.Join(mountpoint, "f"), os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(1024); err != nil {
			t.Fatalf("ftruncate to 1024 bytes of a file of %d returned %v, with the workspace over its allowance of %d",
				stands, err, allowance)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("committing the truncation returned %v", err)
		}
		held, err := os.Stat(filepath.Join(backing, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if held.Size() != 1024 {
			t.Fatalf("the namespace holds %d bytes, want 1024", held.Size())
		}
	})
}
