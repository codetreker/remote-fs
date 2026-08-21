package localdir_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
)

func TestContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		return newStorage(t, t.TempDir())
	})
}

// The contract suite can only see what the implementation chooses to report back. These
// cases cross the boundary the other way: they act through the storage and check the
// directory with plain os calls, and act on the directory and check what the storage
// reports. A storage that quietly kept everything in memory would pass the suite and
// fail here.

func TestWritesLandInTheDirectory(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root)

	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(t.Context(), "d/f", []byte("payload")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(root, "d", "f"))
	if err != nil {
		t.Fatalf("the file is not in the directory: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("the directory holds %q, want %q", got, "payload")
	}
}

func TestReadsReportWhatTheDirectoryHolds(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root)

	if err := os.WriteFile(filepath.Join(root, "f"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := s.Read(t.Context(), "f")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "payload" {
		t.Fatalf("read %q, want %q", got, "payload")
	}
}

// --- symbolic links --------------------------------------------------------------------

// A symbolic link is a node in its own right, and every operation acts on the node at the
// name it was given rather than on whatever that node refers to. The contract suite cannot
// reach any of this — it has no operation that makes a link — so the links are planted
// with plain os calls, and what happened to them is read back the same way.
//
// One case per operation, because following is decided by each operation for itself: a
// single case saying "links are refused" would pass while the rest of them still followed.

// plantLinks lays out a namespace holding a file, a link to it, and a link to nothing.
func plantLinks(t *testing.T) (storage.Storage, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "target"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nowhere", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	return newStorage(t, root), root
}

// untouchedTarget checks that the file the link points at is exactly as plantLinks left
// it. Every case below calls it, because the damage a followed link does lands there and
// nowhere else: the operation returns whatever it would have returned anyway.
func untouchedTarget(t *testing.T, root string) {
	t.Helper()
	info, err := os.Lstat(filepath.Join(root, "target"))
	if err != nil {
		t.Fatalf("the file the link points at: %v", err)
	}
	if info.Mode() != 0o644 {
		t.Errorf("the file the link points at has mode %v, want %v — the operation followed the link", info.Mode(), fs.FileMode(0o644))
	}
	body, err := os.ReadFile(filepath.Join(root, "target"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "payload" {
		t.Errorf("the file the link points at holds %q, want %q — the operation followed the link", body, "payload")
	}
}

func TestNeitherStatNorListFollowsASymbolicLink(t *testing.T) {
	s, _ := plantLinks(t)

	stat, err := s.Stat(t.Context(), "link")
	if err != nil {
		t.Fatalf("stat link: %v", err)
	}
	if stat.Mode.Type() != fs.ModeSymlink {
		t.Errorf("Stat reports link as mode %v, want a symbolic link — it followed the link and described the file it points at", stat.Mode)
	}

	listed := entryNamed(t, s, "link")
	if listed.Attr.Mode.Type() != fs.ModeSymlink {
		t.Errorf("List reports link as mode %v, want a symbolic link", listed.Attr.Mode)
	}

	// The two disagreeing is worse than either being wrong on its own: whatever pairs a
	// listing with a lookup then sees the name change kind under it.
	if stat.Mode != listed.Attr.Mode || stat.Size != listed.Attr.Size {
		t.Errorf("Stat reports link as %v of %d bytes and List reports it as %v of %d bytes",
			stat.Mode, stat.Size, listed.Attr.Mode, listed.Attr.Size)
	}
}

// A link to a name that holds nothing is still a name that holds something. Following it
// would turn "there is a link here" into "there is nothing here", which is the answer this
// system exists not to give.
func TestALinkToNothingIsReportedRatherThanResolved(t *testing.T) {
	s, _ := plantLinks(t)

	attr, err := s.Stat(t.Context(), "dangling")
	if err != nil {
		t.Fatalf("stat dangling: %v — the link is there, and a listing of the directory reports it", err)
	}
	if attr.Mode.Type() != fs.ModeSymlink {
		t.Errorf("Stat reports dangling as mode %v, want a symbolic link", attr.Mode)
	}
}

// Handing back the target's bytes under the link's name is the failure that hides itself:
// it succeeds, so nothing above notices, and the contents belong to a file the caller
// never named.
func TestReadRefusesASymbolicLinkRatherThanReturningWhatItPointsAt(t *testing.T) {
	s, root := plantLinks(t)

	for _, name := range []string{"link", "dangling"} {
		body, err := s.Read(t.Context(), name)
		if !errors.Is(err, syscall.ELOOP) {
			t.Errorf("reading %s gave %q with error %v, want ELOOP", name, body, err)
		}
	}
	untouchedTarget(t, root)
}

// A write that follows destroys twice over: the contents the caller wrote land in a file
// they did not name, and the file they did name is left holding a pointer to it.
func TestWriteRefusesASymbolicLinkRatherThanWritingThroughIt(t *testing.T) {
	s, root := plantLinks(t)

	for _, name := range []string{"link", "dangling"} {
		if err := s.Write(t.Context(), name, []byte("elsewhere")); !errors.Is(err, syscall.ELOOP) {
			t.Errorf("writing %s failed with %v, want ELOOP", name, err)
		}
	}
	untouchedTarget(t, root)

	// The link is still a link pointing where it pointed. A write that replaced it would
	// leave a regular file here, having taken the name from the node that held it.
	for name, points := range map[string]string{"link": "target", "dangling": "nowhere"} {
		got, err := os.Readlink(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("%s is no longer a link: %v", name, err)
		}
		if got != points {
			t.Errorf("%s points at %q, want %q", name, got, points)
		}
	}
}

// Linux has no lchmod, so a mode cannot be put on a link at all — and applying it to the
// target instead is the one answer that must not be given, because the caller is told the
// node they named now has permissions it does not have.
func TestSetAttrRefusesAModeOnASymbolicLink(t *testing.T) {
	s, root := plantLinks(t)

	for _, name := range []string{"link", "dangling"} {
		err := s.SetAttr(t.Context(), name, storage.AttrChange{Mode: mode(0o600)})
		if !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Errorf("setting the mode of %s failed with %v, want EOPNOTSUPP", name, err)
		}
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Type() != fs.ModeSymlink {
			t.Errorf("%s is now %v, want a symbolic link", name, info.Mode())
		}
	}
	untouchedTarget(t, root)
}

// Times are the one attribute a link can carry of its own: utimensat with
// AT_SYMLINK_NOFOLLOW sets them on the link. So this succeeds, and what it changes is the
// link — which is why the link to nothing is here, a name a following utimensat could only
// answer ENOENT for.
func TestSetAttrSetsASymbolicLinkOwnTimes(t *testing.T) {
	s, root := plantLinks(t)

	accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
	changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
	for _, name := range []string{"link", "dangling"} {
		if err := s.SetAttr(t.Context(), name, storage.AttrChange{AccessTime: &accessed, ModTime: &changed}); err != nil {
			t.Errorf("setting the times of %s: %v", name, err)
			continue
		}
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(changed) {
			t.Errorf("%s is dated %v, want %v — the time landed on what the link points at", name, info.ModTime().UTC(), changed)
		}
		attr, err := s.Stat(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if !attr.AccessTime.Equal(accessed) || !attr.ModTime.Equal(changed) {
			t.Errorf("%s is accessed %v and changed %v, want %v and %v",
				name, attr.AccessTime.UTC(), attr.ModTime.UTC(), accessed, changed)
		}
	}
	untouchedTarget(t, root)
}

// A change naming a mode and times at once is partly applicable to a link: the times go
// on and the mode has nowhere to go. The contract allows that — a change naming more than
// one attribute is not applied atomically — and what must not happen is the mode reaching
// the target instead.
func TestAChangeNamingAModeAndTimesIsPartlyApplicableToALink(t *testing.T) {
	s, root := plantLinks(t)

	changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
	err := s.SetAttr(t.Context(), "link", storage.AttrChange{Mode: mode(0o600), ModTime: &changed})
	if !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Errorf("the change failed with %v, want EOPNOTSUPP — the mode has nowhere to go", err)
	}
	info, err := os.Lstat(filepath.Join(root, "link"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(changed) {
		t.Errorf("the link is dated %v, want the %v the same change asked for", info.ModTime().UTC(), changed)
	}
	untouchedTarget(t, root)
}

// A change naming nothing asks whether the node is there, and the node is the link. The
// link to nothing is the whole case: a probe that followed would report the namespace has
// no such name, when a listing of the directory reports it.
func TestSetAttrWithNothingToChangeAnswersForTheLinkItself(t *testing.T) {
	s, _ := plantLinks(t)

	for _, name := range []string{"link", "dangling"} {
		if err := s.SetAttr(t.Context(), name, storage.AttrChange{}); err != nil {
			t.Errorf("a change naming nothing on %s failed with %v, want it to succeed — the link is there", name, err)
		}
	}
}

// Removing and renaming take the link itself. Neither syscall can follow, so what these
// guard against is an implementation reaching for a call that does — os.Remove over
// unlink, say, or a stat of the target on the way.
func TestRemovingAndRenamingActOnTheLinkItself(t *testing.T) {
	s, root := plantLinks(t)

	if err := s.Rename(t.Context(), "link", "moved"); err != nil {
		t.Fatalf("renaming the link: %v", err)
	}
	if points, err := os.Readlink(filepath.Join(root, "moved")); err != nil || points != "target" {
		t.Errorf("the renamed node points at %q with error %v, want a link to %q", points, err, "target")
	}

	if err := s.Remove(t.Context(), "moved"); err != nil {
		t.Fatalf("removing the link: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "moved")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the link is still there (%v)", err)
	}
	untouchedTarget(t, root)
}

// mode is the address of a mode, which is what an AttrChange takes.
func mode(m fs.FileMode) *fs.FileMode { return &m }

func entryNamed(t *testing.T, s storage.Storage, name string) storage.Entry {
	t.Helper()
	entries, err := s.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("the listing holds %v, and none of them is %q", entries, name)
	return storage.Entry{}
}

// --- rename ------------------------------------------------------------------------------

// Rename calls rename(2) rather than os.Rename, and these are the spellings that settle
// which of the two answered. os.Rename lstats the destination and returns an EEXIST of its
// own making whenever a directory is there, before any syscall is made, so each case below
// turns red the moment the call is routed back through the standard library.
//
// The contract suite cannot pin any of this. It has to hold on whatever filesystem an
// implementation sits on, so it permits either errno for a destination that still has
// entries and says nothing at all about the other two. Here the host is Linux and the
// answers are the kernel's own.

// A destination directory is not in the way of a rename by being a directory. rename(2)
// takes an empty one's place, and a program written for a local directory relies on it
// (R-FS-2) — that is how a build tool swaps a staging tree for the one it replaces.
func TestRenameTakesAnEmptyDirectorysPlace(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root)
	plantDir(t, root, "from", "held")
	plantDir(t, root, "to")

	if err := s.Rename(t.Context(), "from", "to"); err != nil {
		t.Fatalf("renaming a directory onto an empty one failed with %v, want it to succeed", err)
	}

	got, err := os.ReadFile(filepath.Join(root, "to", "held"))
	if err != nil {
		t.Fatalf("what the moved directory held: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("the moved directory holds %q, want %q", got, "payload")
	}
	if _, err := os.Lstat(filepath.Join(root, "from")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the source is still there (%v)", err)
	}
}

// The errnos rename(2) gives for a destination it will not replace. Each one names what is
// actually in the way, where the EEXIST os.Rename hands back for both says only that
// something is there.
func TestRenameReportsTheKernelsErrnoForADestinationItCannotReplace(t *testing.T) {
	for _, c := range []struct {
		name  string
		plant func(t *testing.T, root string)
		want  syscall.Errno
	}{
		{"a directory onto a directory that still has entries", func(t *testing.T, root string) {
			plantDir(t, root, "from")
			plantDir(t, root, "to", "held")
		}, syscall.ENOTEMPTY},
		{"a file onto a directory", func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "from"), []byte("payload"), 0o644); err != nil {
				t.Fatal(err)
			}
			plantDir(t, root, "to")
		}, syscall.EISDIR},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			s := newStorage(t, root)
			c.plant(t, root)

			err := s.Rename(t.Context(), "from", "to")
			if !errors.Is(err, c.want) {
				t.Fatalf("the rename failed with %v, want %v", err, c.want)
			}
			// The operands are the namespace's, not the directory the namespace happens to
			// be held in: this error travels to a caller on another machine, and the host
			// paths are the server's own layout for it to know nothing about.
			var link *os.LinkError
			if !errors.As(err, &link) {
				t.Fatalf("the rename failed with %T, want an *os.LinkError", err)
			}
			if link.Old != "from" || link.New != "to" {
				t.Errorf("the error names %q and %q, want %q and %q", link.Old, link.New, "from", "to")
			}
			if _, err := os.Lstat(filepath.Join(root, "from")); err != nil {
				t.Errorf("the source is gone after a rename that failed: %v", err)
			}
		})
	}
}

// plantDir makes a directory holding one file per name given, each of them "payload".
func plantDir(t *testing.T, root, dir string, holds ...string) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range holds {
		if err := os.WriteFile(filepath.Join(root, dir, name), []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriteLeavesNoLitterBehind(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root)

	for range 3 {
		if err := s.Write(t.Context(), "f", []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "f" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the directory holds %v, want just the one file — a temporary file survived", names)
	}
}

// A path that climbs out of the root must be refused rather than followed. Refusing it
// in the contract is not enough on its own; what matters is that nothing outside the
// root is reachable, so this checks the neighbouring file is still there afterwards.
func TestPathsCannotReachOutsideTheRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside")
	if err := os.WriteFile(outside, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := newStorage(t, root)

	if _, err := s.Read(t.Context(), "../outside"); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("reading outside the root failed with %v, want EINVAL", err)
	}
	if err := s.Write(t.Context(), "../outside", []byte("clobbered")); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("writing outside the root failed with %v, want EINVAL", err)
	}
	if err := s.Remove(t.Context(), "../outside"); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("removing outside the root failed with %v, want EINVAL", err)
	}

	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("the neighbouring file is gone: %v", err)
	}
	if string(got) != "untouched" {
		t.Fatalf("the neighbouring file now holds %q, want %q", got, "untouched")
	}
}

// A namespace whose root is missing must fail loudly. Reporting ENOENT per operation
// would be a lie of exactly the kind this system exists to avoid: it says "that file is
// not there" when the truth is "the whole namespace is not there".
func TestAMissingRootIsRefusedUpFront(t *testing.T) {
	if _, err := localdir.New(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("a missing root was accepted, want an error")
	}
}

// The same lie, arrived at from the other end: an empty path names the root, and rmdir(2)
// removes the served directory as readily as any other. The contract case can only ask
// the storage whether its root is still there; this one asks the filesystem, which is
// where the damage lands.
func TestTheServedDirectorySurvivesOperationsAimedAtTheRoot(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root)

	if err := s.RemoveDir(t.Context(), ""); !errors.Is(err, syscall.EBUSY) {
		t.Errorf("removing the root failed with %v, want EBUSY", err)
	}
	if err := s.Rename(t.Context(), "", "moved"); !errors.Is(err, syscall.EBUSY) {
		t.Errorf("moving the root failed with %v, want EBUSY", err)
	}

	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("the served directory is gone: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("the served directory has mode %v, want a directory", info.Mode())
	}
}

func TestARootThatIsAFileIsRefusedUpFront(t *testing.T) {
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := localdir.New(file); err == nil {
		t.Fatal("a file was accepted as a root, want an error")
	}
}

func newStorage(t *testing.T, root string) storage.Storage {
	t.Helper()
	s, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
