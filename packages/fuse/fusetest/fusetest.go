// Package fusetest tells a mount this run left attached from a mount that was never ours.
//
// The two are indistinguishable in the mount table. That table is machine-wide, and a
// `fuse.remote-fs` line in it reads the same whether this test binary attached it, the
// test binary of another package running beside it — `go test` runs one per package, in
// parallel — or a person who has this filesystem mounted for real work. A check that
// reads that table and concludes something about its own run is answering a question the
// table cannot answer, and it answers it wrong in the direction that costs most: a red
// build with nothing behind it teaches the next reader to scroll past the check.
//
// Attribution here is structural rather than recorded. TestMain points TMPDIR at a
// directory whose name no other process knows; `t.TempDir` builds beneath it and child
// processes inherit it, so every path a test can take a mountpoint from is beneath that
// directory, and anything mounted beneath it was mounted by this run. Nothing has to be
// registered at the call sites, so no test written later can forget to register itself
// and quietly stop being covered.
package fusetest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// mountTable is where the kernel publishes what is attached. It is a variable so that the
// branch taken when it cannot be read is reachable from a test; nothing else assigns to it.
var mountTable = "/proc/self/mountinfo"

// report is where a leak is announced. It is a variable for the case that drives Run into
// announcing one: a rehearsal that printed a real-looking `left behind:` line into the
// run's own output would put a line in every CI log that names a mount nobody ever made,
// and a signal that cries wolf once is one people learn to grep past.
var report io.Writer = os.Stderr

// A TempRoot is the directory this run puts its temporary files in, and the answer to
// whether it left anything mounted.
type TempRoot struct{ path string }

// NewTempRoot creates the directory and points this process's TMPDIR at it.
//
// Call it from TestMain before m.Run. That is the one moment the environment can be
// changed without racing a test that reads it, and it is early enough for every
// t.TempDir and every child process to land beneath the result.
func NewTempRoot(name string) (*TempRoot, error) {
	path, err := os.MkdirTemp("", name+"-")
	if err != nil {
		return nil, fmt.Errorf("making a temporary directory for %s: %w", name, err)
	}
	// The kernel records the mountpoint it resolved, so hold the resolved form: a TMPDIR
	// reached through a symbolic link would otherwise match no line in the table, and the
	// check would be one that cannot fail.
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolving %s: %w", path, err)
	}
	if err := os.Setenv("TMPDIR", resolved); err != nil {
		return nil, fmt.Errorf("pointing TMPDIR at %s: %w", resolved, err)
	}
	return &TempRoot{path: resolved}, nil
}

// Path is the directory itself.
func (r *TempRoot) Path() string { return r.path }

// Leaks reports everything still attached beneath the root, one line each, in the order
// the mount table holds them. Nothing reported means this run detached everything it
// attached.
func (r *TempRoot) Leaks() ([]string, error) {
	table, err := os.ReadFile(mountTable)
	if err != nil {
		return nil, fmt.Errorf("reading %s, so a mount left behind cannot be ruled out: %w", mountTable, err)
	}
	return leaksBeneath(string(table), r.path)
}

// Remove deletes the root.
//
// Only once Leaks reports nothing. Removing a tree walks into a mountpoint that is still
// attached and deletes what is served through it, so a run that did this while a leak was
// live would destroy both the evidence and the namespace behind it.
func (r *TempRoot) Remove() error { return os.RemoveAll(r.path) }

// Mounted reports whether anything is attached at path.
func Mounted(path string) (bool, error) {
	// The comparison is against what the kernel recorded, which is the resolved path.
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, fmt.Errorf("resolving %s, so whether it is mounted cannot be told: %w", path, err)
	}
	table, err := os.ReadFile(mountTable)
	if err != nil {
		return false, fmt.Errorf("reading %s, so whether %s is mounted cannot be told: %w", mountTable, path, err)
	}
	mounts, err := parse(string(table))
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(mounts, func(m mount) bool { return m.point == resolved }), nil
}

func leaksBeneath(table, root string) ([]string, error) {
	mounts, err := parse(table)
	if err != nil {
		return nil, err
	}
	var leaks []string
	for _, m := range mounts {
		if m.point != root && !strings.HasPrefix(m.point, root+string(filepath.Separator)) {
			continue
		}
		leaks = append(leaks, m.describe())
	}
	return leaks, nil
}

// A mount is one line of the mount table, reduced to what a leak has to be reported by.
type mount struct {
	point      string // where it is attached, as the kernel resolved it
	kind       string // the filesystem type, as "fuse.remote-fs"
	connection string // the FUSE connection this holds open, empty for anything not FUSE
}

func (m mount) describe() string {
	detach := "umount"
	held := ""
	if m.connection != "" {
		detach = "fusermount3 -u"
		held = ", holding " + m.connection
	}
	return fmt.Sprintf("a mount: %s (%s%s); detach it with `%s %s`", m.point, m.kind, held, detach, m.point)
}

// parse reads the mount table.
//
// The layout is fixed for six fields and variable after them: zero or more optional tag
// fields end at a lone "-", and the filesystem type is the field following that
// separator. Both halves are documented in proc_pid_mountinfo(5),
// https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html, and the kernel writes
// them in fs/proc_namespace.c show_mountinfo:
// https://github.com/torvalds/linux/blob/v6.8/fs/proc_namespace.c#L136-L175
//
// The older /proc/self/mounts is not a substitute: it is fstab format, with no device
// number in it at all, so a line from it cannot be tied to a FUSE connection.
//
// A line this cannot read is an error rather than a line skipped. Skipping it would turn
// "the table said something unexpected" into "nothing is mounted", which is the one
// answer a leak check must never invent.
func parse(table string) ([]mount, error) {
	var mounts []mount
	for line := range strings.SplitSeq(strings.TrimSpace(table), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		// Six fixed fields, then zero or more optional tags, then the separator, the
		// filesystem type and the source. Nine fields is the shortest a line can be, and
		// the separator is looked for past the fixed six so that a path among them cannot
		// be mistaken for it.
		if len(fields) < 9 {
			return nil, fmt.Errorf("%q is not a mount table line, so what is mounted cannot be told from it", line)
		}
		tail := slices.Index(fields[6:], "-")
		if tail < 0 || 6+tail+2 >= len(fields) {
			return nil, fmt.Errorf("%q names no filesystem after its separator, so what is mounted cannot be told from it", line)
		}
		kind := fields[6+tail+1]
		point, err := unescape(fields[4])
		if err != nil {
			return nil, err
		}
		connection, err := connectionOf(fields[2], kind)
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, mount{point: point, kind: kind, connection: connection})
	}
	if len(mounts) == 0 {
		return nil, fmt.Errorf("%s named nothing at all; a machine always has something mounted, so this is not the table", mountTable)
	}
	return mounts, nil
}

// connectionOf names the control directory a FUSE mount holds open.
//
// A FUSE mount sits on an anonymous block device: major 0, and a minor drawn from the
// same pool tmpfs, proc and overlay draw from. That minor is the number the connection's
// directory is named by, which is what makes the pair worth printing together — the
// directory outlives a lazy unmount, and it is where an operator goes to abort one. The
// chain is `fc->dev = sb->s_dev` in fs/fuse/inode.c, printed whole by fuse_ctl_add_conn
// and, with major 0, equal to the minor:
// https://github.com/torvalds/linux/blob/v6.8/fs/fuse/control.c#L256-L269
// https://github.com/torvalds/linux/blob/v6.8/fs/super.c#L1190-L1219
//
// Both the type and the major decide whether there is a connection to name. An anonymous
// device alone does not distinguish a FUSE mount from a tmpfs, and a fuseblk mount sits
// on a real block device whose number is not a connection at all.
func connectionOf(device, kind string) (string, error) {
	major, minor, found := strings.Cut(device, ":")
	if !found {
		return "", fmt.Errorf("%q is not a major:minor device, so this line was misread", device)
	}
	if major != "0" || (kind != "fuse" && !strings.HasPrefix(kind, "fuse.")) {
		return "", nil
	}
	return "/sys/fs/fuse/connections/" + minor, nil
}

// unescape reads back the form the kernel writes a path in: a backslash and three octal
// digits, for the characters that would otherwise be unreadable in a table whose fields
// are separated by spaces. The mountpoint is escaped for space, tab, newline and
// backslash — the filesystem type and source add '#', which is why they are not put
// through this:
// https://github.com/torvalds/linux/blob/v6.8/fs/proc_namespace.c#L87-L90
// https://github.com/torvalds/linux/blob/v6.8/fs/seq_file.c#L440-L458
//
// Any escape is read rather than only those four, so that a kernel which starts escaping
// a fifth character is understood instead of misread.
func unescape(field string) (string, error) {
	var path strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] != '\\' {
			path.WriteByte(field[i])
			continue
		}
		if i+3 >= len(field) {
			return "", fmt.Errorf("%q ends in an incomplete escape, so the path it names is unknown", field)
		}
		value, err := strconv.ParseUint(field[i+1:i+4], 8, 8)
		if err != nil {
			return "", fmt.Errorf("%q holds an escape this cannot read, so the path it names is unknown: %w", field, err)
		}
		path.WriteByte(byte(value))
		i += 3
	}
	return path.String(), nil
}

// Run runs a test binary's tests beneath a temporary root of its own, and reports what
// the run left attached to the machine.
//
// tests is whatever TestMain would otherwise do — m.Run, and whatever the binary tears
// down itself — and its result is the tests' own verdict. What comes back is the code to
// exit with: that verdict, or 1 when this run left a mount or a fusermount process
// behind. A mountpoint still attached wedges every path beneath it until somebody detaches
// it by hand, and the run that left it there has usually already reported success.
//
// This does not see a run that panicked or ran past its -timeout, because TestMain does
// not regain control after either. The machine-wide sweep in ci.yml is what covers those.
func Run(name string, tests func() int) int {
	before, had := os.LookupEnv("TMPDIR")
	root, err := NewTempRoot(name)
	if err != nil {
		fmt.Fprintln(report, err)
		return 1
	}
	// The root is removed below, and a TMPDIR still naming it would send whatever ran next
	// in this process at a directory that is no longer there.
	defer func() {
		if had {
			os.Setenv("TMPDIR", before)
			return
		}
		os.Unsetenv("TMPDIR")
	}()
	code := tests()

	leaks, err := root.Leaks()
	if err != nil {
		fmt.Fprintln(report, err)
		code = 1
	}
	for _, leak := range slices.Concat(leaks, fusermountChildren()) {
		fmt.Fprintf(report, "left behind: %s\n", leak)
		code = 1
	}

	// Removing the root walks into anything still attached beneath it and deletes what is
	// served through it, so it happens only once nothing is — and not at all when the
	// question could not be answered.
	if err == nil && len(leaks) == 0 {
		if err := root.Remove(); err != nil {
			fmt.Fprintln(report, err)
			code = 1
		}
	}
	return code
}

// fusermountChildren reports the fusermount processes this process started and did not
// reap. Only our own children are considered: fusermount is a setuid helper that anything
// on the machine may be running, and a stranger's is not evidence about this run.
func fusermountChildren() []string {
	var leaks []string
	for _, pid := range childrenNamed("fusermount") {
		leaks = append(leaks, "a fusermount process: pid "+pid)
	}
	return leaks
}

// childrenNamed reports the live children of this process whose executable name begins
// with the given one. fusermount and fusermount3 are both wanted, and both begin with it.
func childrenNamed(name string) []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var found []string
	self := strconv.Itoa(os.Getpid())
	for _, entry := range entries {
		if _, isPID := strconv.Atoi(entry.Name()); isPID != nil {
			continue
		}
		status, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "status"))
		if err != nil {
			// The process ended between the listing and the read, which is what we want it
			// to have done.
			continue
		}
		text := string(status)
		if strings.Contains(text, "\nPPid:\t"+self+"\n") && strings.Contains(text, "Name:\t"+name) {
			found = append(found, entry.Name())
		}
	}
	return found
}
