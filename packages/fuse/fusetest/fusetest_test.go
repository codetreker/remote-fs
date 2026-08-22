package fusetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A mount table line, assembled from the pieces a case varies. The fixed prefix is the
// shape the kernel writes: mount id, parent id, device, the filesystem's own root, the
// mountpoint, then the per-mount options.
func line(device, point, tags, kind string) string {
	return "36 35 " + device + " / " + point + " rw,nosuid,nodev,relatime " + tags + "- " + kind +
		" remote-fs rw,user_id=1000,group_id=1000,max_read=131072"
}

const root = "/tmp/remote-fs-cmd-1234"

// A leak is a mount beneath this run's own root and nothing else. The case that matters
// most is the second one: it is the line the CI failure reported, a mount made by the
// test binary of another package, and it has to produce nothing at all.
func TestOnlyWhatIsBeneathTheRootIsALeak(t *testing.T) {
	for _, c := range []struct {
		name string
		line string
		want string
	}{
		{"a mount at the root itself", line("0:48", root, "shared:29 ", "fuse.remote-fs"),
			"a mount: " + root + " (fuse.remote-fs, holding /sys/fs/fuse/connections/48); " +
				"detach it with `fusermount3 -u " + root + "`"},
		{"another package's mount, which is what this exists to stop reporting",
			line("0:48", "/tmp/TestSpaceIsReportedInWholeBlocks123/002", "shared:29 ", "fuse.remote-fs"), ""},
		{"a directory whose name merely starts with the root's",
			line("0:48", root+"-2/mnt", "shared:29 ", "fuse.remote-fs"), ""},
		{"a mount beneath the root", line("0:7", root+"/TestX/001", "shared:29 ", "fuse.remote-fs"),
			"a mount: " + root + "/TestX/001 (fuse.remote-fs, holding /sys/fs/fuse/connections/7); " +
				"detach it with `fusermount3 -u " + root + "/TestX/001`"},

		// Optional tag fields are zero or more, so the filesystem type is found by the
		// separator rather than counted to.
		{"a line with no optional tag fields", line("0:7", root+"/m", "", "fuse.remote-fs"),
			"a mount: " + root + "/m (fuse.remote-fs, holding /sys/fs/fuse/connections/7); " +
				"detach it with `fusermount3 -u " + root + "/m`"},
		{"a line with several", line("0:7", root+"/m", "shared:29 master:4 propagate_from:2 ", "fuse.remote-fs"),
			"a mount: " + root + "/m (fuse.remote-fs, holding /sys/fs/fuse/connections/7); " +
				"detach it with `fusermount3 -u " + root + "/m`"},

		// Nothing else this project mounts is FUSE, but a leak is a leak: what is reported
		// follows from the line rather than from an expectation about the type.
		{"something that is not FUSE", line("0:52", root+"/m", "shared:29 ", "tmpfs"),
			"a mount: " + root + "/m (tmpfs); detach it with `umount " + root + "/m`"},
		{"a fuseblk mount, whose device number is not a connection",
			line("8:1", root+"/m", "shared:29 ", "fuseblk"),
			"a mount: " + root + "/m (fuseblk); detach it with `umount " + root + "/m`"},
	} {
		t.Run(c.name, func(t *testing.T) {
			leaks, err := leaksBeneath(c.line+"\n"+aMountEveryMachineHas, root)
			if err != nil {
				t.Fatal(err)
			}
			if c.want == "" {
				if len(leaks) != 0 {
					t.Fatalf("reported %q; that mount was not made by this run", leaks)
				}
				return
			}
			if len(leaks) != 1 || leaks[0] != c.want {
				t.Fatalf("reported %q\nwant     [%q]", leaks, c.want)
			}
		})
	}
}

// So that a case exercising one line is still a table with something in it, and the
// "nothing at all" cases are answering "none of these are mine" rather than "the table
// was empty".
const aMountEveryMachineHas = "25 30 0:23 / /proc rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw"

// The kernel escapes what would otherwise be unreadable in a table whose fields are
// separated by spaces. A path this misread would be attributed to the wrong run, or to
// none.
func TestAnEscapedMountpointIsReadBack(t *testing.T) {
	leaks, err := leaksBeneath(
		line("0:9", root+`/mnt\0400\011tab\134back#hash`, "shared:29 ", "fuse.remote-fs"), root)
	if err != nil {
		t.Fatal(err)
	}
	want := root + "/mnt 0\ttab\\back#hash"
	if len(leaks) != 1 || !strings.Contains(leaks[0], want) {
		t.Fatalf("reported %q, want a mountpoint reading %q", leaks, want)
	}
}

// A line this cannot read has to be an error. Skipping it would answer "nothing is
// mounted" on evidence that says "the table said something unexpected", and a leak check
// that invents that answer is one that cannot fail.
func TestATableThisCannotReadIsNotAnEmptyTable(t *testing.T) {
	for _, c := range []struct {
		name  string
		table string
	}{
		{"a line with no separator", "36 35 0:48 / /tmp/x rw shared:29 fuse.remote-fs remote-fs rw"},
		{"a line naming no source", "36 35 0:48 / /tmp/x rw shared:29 - fuse.remote-fs"},
		{"a separator with nothing after it", "36 35 0:48 / /tmp/x rw shared:29 -"},
		{"a truncated line", "36 35 0:48 /"},
		{"an escape that ends the field", line("0:9", `/tmp/x\04`, "", "fuse.remote-fs")},
		{"an escape that is not octal", line("0:9", `/tmp/x\09z`, "", "fuse.remote-fs")},
		{"a device that is not major:minor", line("48", root+"/m", "", "fuse.remote-fs")},
		{"nothing at all", ""},
		{"blank lines", "\n\n \n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if leaks, err := leaksBeneath(c.table, root); err == nil {
				t.Fatalf("read %q as a mount table and reported %q", c.table, leaks)
			}
		})
	}
}

// A root is a directory of this run's own, and TMPDIR points at it so that everything a
// test can reach for a mountpoint from lands inside it. That is the whole of the
// attribution: without it, Leaks would be reading the machine's table and guessing.
func TestTheRootIsWhereTemporaryFilesGo(t *testing.T) {
	keepTMPDIR(t)

	root, err := NewTempRoot("fusetest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root.Path()) })

	if got := os.Getenv("TMPDIR"); got != root.Path() {
		t.Fatalf("TMPDIR is %q, want the root %q", got, root.Path())
	}
	// The property the package promises, asserted rather than assumed: this is the call
	// every t.TempDir and every child process's temporary file goes through.
	if got := os.TempDir(); got != root.Path() {
		t.Fatalf("os.TempDir() is %q, want the root %q", got, root.Path())
	}
	if inside := t.TempDir(); !strings.HasPrefix(inside, root.Path()+string(filepath.Separator)) {
		t.Fatalf("t.TempDir() gave %q, which is not beneath the root %q", inside, root.Path())
	}
	if err := root.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root.Path()); !os.IsNotExist(err) {
		t.Fatalf("the root is still there after Remove: %v", err)
	}
}

// A root whose name is reached through a symbolic link still matches what the kernel
// records, which is the resolved path. Getting this wrong would leave a check that can
// never fire on a machine whose TMPDIR is a link.
func TestTheRootIsRecordedAsTheKernelWouldRecordIt(t *testing.T) {
	keepTMPDIR(t)

	real := t.TempDir()
	linked := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, linked); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", linked)

	root, err := NewTempRoot("fusetest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root.Path()) })

	if strings.HasPrefix(root.Path(), linked) {
		t.Fatalf("the root is %q, which is the path as asked for rather than as resolved", root.Path())
	}
	if !strings.HasPrefix(root.Path(), real+string(filepath.Separator)) {
		t.Fatalf("the root is %q, want it beneath the resolved %q", root.Path(), real)
	}
}

func TestARootReportsWhatIsMountedBeneathIt(t *testing.T) {
	keepTMPDIR(t)

	root, err := NewTempRoot("fusetest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root.Path()) })

	if leaks, err := root.Leaks(); err != nil || len(leaks) != 0 {
		t.Fatalf("a root nothing was ever mounted under reports %q, %v", leaks, err)
	}

	mountedAt(t, root.Path()+"/TestX/001")
	leaks, err := root.Leaks()
	if err != nil {
		t.Fatal(err)
	}
	if len(leaks) != 1 || !strings.Contains(leaks[0], root.Path()+"/TestX/001") {
		t.Fatalf("reported %q, want the mount beneath the root", leaks)
	}
}

// Mounted answers about one path, and it answers about the path the kernel would name
// rather than the one the caller happened to spell.
func TestMountedResolvesThePathBeforeAsking(t *testing.T) {
	real := t.TempDir()
	linked := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, linked); err != nil {
		t.Fatal(err)
	}

	if mounted, err := Mounted(linked); err != nil || mounted {
		t.Fatalf("Mounted(%s) = %v, %v before anything is mounted there", linked, mounted, err)
	}

	mountedAt(t, real)
	if mounted, err := Mounted(linked); err != nil || !mounted {
		t.Fatalf("Mounted(%s) = %v, %v; the same directory named through a link", linked, mounted, err)
	}
}

// Neither question may be answered from a table that could not be read. "I could not find
// out" is not "nothing is mounted".
func TestATableThatCannotBeReadIsNotAnAnswer(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "no-such-table")
	restore := mountTable
	mountTable = absent
	t.Cleanup(func() { mountTable = restore })

	root := &TempRoot{path: "/tmp/whatever"}
	if leaks, err := root.Leaks(); err == nil {
		t.Fatalf("Leaks reported %q with no mount table to read", leaks)
	}
	if mounted, err := Mounted(t.TempDir()); err == nil {
		t.Fatalf("Mounted reported %v with no mount table to read", mounted)
	}
	// A path that is not there cannot be resolved, and so cannot be answered about.
	if mounted, err := Mounted(absent); err == nil {
		t.Fatalf("Mounted(%s) reported %v for a path that does not exist", absent, mounted)
	}

	// Nor from a table that was read and could not be understood.
	unreadable := filepath.Join(t.TempDir(), "mountinfo")
	if err := os.WriteFile(unreadable, []byte("36 35 0:48 /\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mountTable = unreadable
	if mounted, err := Mounted(t.TempDir()); err == nil {
		t.Fatalf("Mounted reported %v from a table it could not read", mounted)
	}
}

// mountedAt puts a mount table in front of the package that says point is mounted, without
// mounting anything: these cases are about reading the table, and they have to run on a
// machine with no /dev/fuse.
func mountedAt(t *testing.T, point string) {
	t.Helper()
	table := filepath.Join(t.TempDir(), "mountinfo")
	contents := aMountEveryMachineHas + "\n" + line("0:48", point, "shared:29 ", "fuse.remote-fs") + "\n"
	if err := os.WriteFile(table, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	restore := mountTable
	mountTable = table
	t.Cleanup(func() { mountTable = restore })
}

// keepTMPDIR restores the environment these cases change. NewTempRoot points TMPDIR at a
// directory of its own, which is process-wide and would otherwise outlive the case.
func keepTMPDIR(t *testing.T) {
	t.Helper()
	before, had := os.LookupEnv("TMPDIR")
	t.Cleanup(func() {
		if had {
			os.Setenv("TMPDIR", before)
			return
		}
		os.Unsetenv("TMPDIR")
	})
}

// A fusermount left running is this run's business only when this run started it: the
// helper is setuid and anything on the machine may be running one. The name is varied
// rather than the relationship, because a process this test can start and stop on demand
// is the only way to have the relationship be true of something.
func TestOnlyThisProcessesOwnChildrenAreReported(t *testing.T) {
	if found := childrenNamed("sleep"); len(found) != 0 {
		t.Fatalf("this process has children named sleep before one is started: %v", found)
	}

	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		child.Process.Kill()
		child.Wait()
	}()

	found := childrenNamed("sleep")
	if len(found) != 1 || found[0] != strconv.Itoa(child.Process.Pid) {
		t.Fatalf("reported %v, want only this process's own child %d", found, child.Process.Pid)
	}
	if unrelated := childrenNamed("systemd"); len(unrelated) != 0 {
		t.Fatalf("reported %v for a name no child of this process has", unrelated)
	}
}

// Run hands back what the tests decided when nothing was left behind, and it puts the run
// beneath a root of its own on the way.
func TestRunLeavesTheTestsVerdictAloneWhenNothingLeaks(t *testing.T) {
	keepTMPDIR(t)

	outside := os.TempDir()
	for _, want := range []int{0, 3} {
		var beneath string
		if got := Run("fusetest", func() int {
			beneath = os.TempDir()
			return want
		}); got != want {
			t.Fatalf("Run returned %d for tests that returned %d", got, want)
		}
		if beneath == outside {
			t.Fatalf("the tests ran with TMPDIR still at %s, so nothing would be attributable", outside)
		}
		if _, err := os.Stat(beneath); !os.IsNotExist(err) {
			t.Fatalf("the root %s was not removed after a run that left nothing behind: %v", beneath, err)
		}
	}
}

// The other half: a run that left something attached fails, and its root is kept so that
// what it left is still there to look at. A guard that has never been seen to fail is not
// a guard, so this is that demonstration rather than a note that one was done by hand.
func TestRunFailsAndKeepsTheEvidenceWhenSomethingIsLeftBehind(t *testing.T) {
	keepTMPDIR(t)
	restore := mountTable
	t.Cleanup(func() { mountTable = restore })

	var beneath string
	code := Run("fusetest", func() int {
		beneath = os.TempDir()
		mountedAt(t, filepath.Join(beneath, "TestX", "001"))
		return 0
	})
	t.Cleanup(func() { os.RemoveAll(beneath) })

	if code != 1 {
		t.Fatalf("Run returned %d for a run that left a mount attached, want 1", code)
	}
	if _, err := os.Stat(beneath); err != nil {
		t.Fatalf("the root was removed although a mount was still attached beneath it: %v", err)
	}
}

// The name fusermount is matched against is the one the setuid helper is installed under,
// and fusermount3 has to match it too.
func TestAFusermountChildIsReported(t *testing.T) {
	if leaks := fusermountChildren(); len(leaks) != 0 {
		t.Fatalf("this process has a fusermount child before one is started: %v", leaks)
	}

	// A process of our own, named as the helper is, because the helper itself exits at
	// once and a test needs one that is still there to be found.
	named := filepath.Join(t.TempDir(), "fusermount3")
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skipf("skipping: this test needs a program it can keep alive (%v)", err)
	}
	contents, err := os.ReadFile(sleep)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(named, contents, 0o755); err != nil {
		t.Fatal(err)
	}

	child := exec.Command(named, "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		child.Process.Kill()
		child.Wait()
	}()

	leaks := fusermountChildren()
	want := "a fusermount process: pid " + strconv.Itoa(child.Process.Pid)
	if len(leaks) != 1 || leaks[0] != want {
		t.Fatalf("reported %q, want [%q]", leaks, want)
	}
}
