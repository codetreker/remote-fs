// The workspace allowance, from the flag that sets it to the tools that see it.
//
// R-WS-5 asks for a limit given by the deployer, three measured figures reported to
// whatever runs on the mountpoint, and a refusal that lands on the write that caused it.
// Each layer below proves its own part — the count and its arithmetic in
// packages/storage/limited, the reply to statfs(2) in packages/fuse — and none of them
// says that a server started with -quota is a workspace df reports that allowance for.
package cmd_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// mountBlockSize is the unit a mount reports space in. The figures below are whole
// multiples of it so that nothing here has to allow for what a floor division drops.
const mountBlockSize = 4096

// TestAServerUnderAnAllowanceReportsItAndRefusesPastIt drives one workspace to its limit
// with the tools an operator uses: df to see the allowance, ordinary writes to spend it.
func TestAServerUnderAnAllowanceReportsItAndRefusesPastIt(t *testing.T) {
	requireFUSE(t)

	const allowance = 64 << 10
	backing := t.TempDir()
	srv := startDirectoryServerBinary(t, backing, "-quota", "64K")
	mountpoint := t.TempDir()
	startMountBinary(t, "-server", srv.url, "-mountpoint", mountpoint)

	total, used, avail := roomAt(t, mountpoint)
	if total != allowance || used != 0 || avail != allowance {
		t.Fatalf("df reports %d bytes with %d used and %d available, over an empty workspace under an allowance of %d",
			total, used, avail, allowance)
	}

	const half = allowance / 2
	if err := os.WriteFile(filepath.Join(mountpoint, "half.bin"), make([]byte, half), 0o644); err != nil {
		t.Fatalf("writing %d bytes into a workspace with %d free: %v", half, allowance, err)
	}
	total, used, avail = roomAt(t, mountpoint)
	if total != allowance || used != half || avail != allowance-half {
		t.Fatalf("after %d bytes were written, df reports %d bytes with %d used and %d available",
			half, total, used, avail)
	}

	// The refusal has to reach write(2). A great many programs never look at what close(2)
	// returned, so an allowance enforced only where the contents are committed loses those
	// bytes in silence (R-WS-5).
	//
	// More than the whole allowance is written rather than merely more than what is left,
	// because a mount weighs a write against a figure it may have measured up to a second
	// earlier: one taken before half.bin would let a write of what is left through here and
	// leave the commit to refuse it. That the figure accounts for what the workspace already
	// holds is the next test's business.
	written, err := writeThrough(t, filepath.Join(mountpoint, "big.bin"), allowance+mountBlockSize)
	if err == nil {
		t.Fatalf("write(2) took %d bytes into a workspace of %d holding %d", written, allowance, half)
	}
	if errno := errnoOf(err); errno != syscall.EDQUOT {
		t.Fatalf("write(2) failed with %v (errno %v), want EDQUOT: this workspace has spent its allowance, "+
			"and \"no space left on device\" would be a claim about the machine", err, errno)
	}
	t.Logf("write(2) past the allowance: %v", err)

	// The name may well be there — open(2) with O_CREAT made an empty file before anything
	// was written, and an empty file spends none of the allowance. What must not be there is
	// bytes, and the served directory is where that is settled without this system in the way.
	switch info, err := os.Stat(filepath.Join(backing, "big.bin")); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		t.Fatalf("stat big.bin in the served directory: %v", err)
	case info.Size() != 0:
		t.Fatalf("the served directory holds %d bytes at big.bin, and the write that would have put them there was refused",
			info.Size())
	}
	if _, used, _ = roomAt(t, mountpoint); used != half {
		t.Fatalf("df reports %d bytes used after a refused write, want the %d that were written before it", used, half)
	}
}

// TestAnAllowanceIsSpentAgainstWhatTheWorkspaceAlreadyHolds. The walk at startup is what
// makes the first write into a workspace that is already half full be weighed against what
// is left of the allowance rather than against the whole of it.
func TestAnAllowanceIsSpentAgainstWhatTheWorkspaceAlreadyHolds(t *testing.T) {
	requireFUSE(t)

	const allowance = 64 << 10
	const held = allowance / 2

	backing := t.TempDir()
	// Put there before the server starts, so that it is what the startup walk measures
	// rather than something the count was told about.
	if err := os.WriteFile(filepath.Join(backing, "held.bin"), make([]byte, held), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := startDirectoryServerBinary(t, backing, "-quota", "64K")
	srv.awaitLine(t, fmt.Sprintf("allowance of %d bytes, %d of them taken", allowance, held), startup)

	mountpoint := t.TempDir()
	startMountBinary(t, "-server", srv.url, "-mountpoint", mountpoint)

	// Under the allowance, over what is left of it. A workspace weighing this against the
	// allowance alone would take it and end up holding more than it may. Nothing has been
	// written through this mount yet, so the figure it weighs the write against is the one
	// the startup walk arrived at.
	tooMuch := held + mountBlockSize
	written, err := writeThrough(t, filepath.Join(mountpoint, "over.bin"), tooMuch)
	if errno := errnoOf(err); errno != syscall.EDQUOT {
		t.Fatalf("write(2) of %d bytes took %d of them and failed with %v (errno %v), into a workspace of %d holding %d already; want EDQUOT",
			tooMuch, written, err, errno, allowance, held)
	}

	// What is left is still there to be written, which is the difference between a
	// workspace that is full and one that refuses everything.
	fits := allowance - held - mountBlockSize
	if err := os.WriteFile(filepath.Join(mountpoint, "fits.bin"), make([]byte, fits), 0o644); err != nil {
		t.Fatalf("writing %d bytes into a workspace with %d left: %v", fits, allowance-held, err)
	}
	total, used, avail := roomAt(t, mountpoint)
	if total != allowance || used != int64(held+fits) || avail != int64(allowance-held-fits) {
		t.Fatalf("df reports %d bytes with %d used and %d available, over a workspace of %d holding %d and %d",
			total, used, avail, allowance, held, fits)
	}
}

// TestHangingUpRepairsACountMadeWrongBehindTheServer.
//
// The count is exact for everything that passes through the server, and drifts only when
// the served directory is modified behind its back — a use the spec does not support, and
// one no later traffic through the server can correct. SIGHUP is the way back: the
// namespace is measured again and the count replaced with what the walk found.
func TestHangingUpRepairsACountMadeWrongBehindTheServer(t *testing.T) {
	const held, planted = 5000, 3000

	backing := t.TempDir()
	if err := os.WriteFile(filepath.Join(backing, "held.bin"), make([]byte, held), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := startDirectoryServerBinary(t, backing, "-quota", "64K")
	client := dial(t, srv)

	if space := spaceOf(t, client); space.Used != held {
		t.Fatalf("the server reports %d bytes taken over a directory holding %d", space.Used, held)
	}

	// Behind the server's back, which is the one thing that makes the count wrong.
	if err := os.WriteFile(filepath.Join(backing, "planted.bin"), make([]byte, planted), 0o644); err != nil {
		t.Fatal(err)
	}
	if space := spaceOf(t, client); space.Used != held {
		t.Fatalf("the count moved to %d bytes without anything having been asked to measure the namespace", space.Used)
	}

	srv.hangup(t)
	line := srv.awaitLine(t, "recounted", startup)
	// Both figures have to be in it. A repair that reports nothing leaves the operator
	// unable to tell whether it was needed or whether it did anything.
	for _, figure := range []string{strconv.Itoa(held + planted), strconv.Itoa(held)} {
		if !strings.Contains(line, figure) {
			t.Fatalf("the recount says %q, and %s is not in it", line, figure)
		}
	}
	t.Log(line)

	if space := spaceOf(t, client); space.Used != held+planted {
		t.Fatalf("after the recount the server reports %d bytes taken, over a directory holding %d", space.Used, held+planted)
	}

	srv.interrupt(t)
	if err := srv.wait(t); err != nil {
		t.Fatalf("remote-fs-server exited with %v after SIGINT\n%s", err, srv.output())
	}
}

// TestHangingUpWithoutAnAllowanceSaysThereIsNothingToRecount. A signal that quietly did
// nothing would leave an operator waiting on a repair that was never going to happen.
func TestHangingUpWithoutAnAllowanceSaysThereIsNothingToRecount(t *testing.T) {
	srv := startDirectoryServerBinary(t, t.TempDir())

	srv.hangup(t)
	t.Log(srv.awaitLine(t, "no allowance", startup))

	// Still serving afterwards: SIGHUP asks for something this server survives, whether or
	// not it has anything to do about it.
	if _, err := dial(t, srv).List(context.Background(), ""); err != nil {
		t.Fatalf("listing the namespace after SIGHUP: %v", err)
	}

	srv.interrupt(t)
	if err := srv.wait(t); err != nil {
		t.Fatalf("remote-fs-server exited with %v after SIGINT\n%s", err, srv.output())
	}
}

// TestAServerWithoutAnAllowanceReportsTheHostFilesystemsFigures. Both ways of starting the
// server report measured facts and neither invents any: given -quota the figures are the
// allowance and what has been counted against it, and given none they are what the
// filesystem holding the served directory says about itself.
func TestAServerWithoutAnAllowanceReportsTheHostFilesystemsFigures(t *testing.T) {
	requireFUSE(t)

	backing := t.TempDir()
	srv := startDirectoryServerBinary(t, backing)
	mountpoint := t.TempDir()
	startMountBinary(t, "-server", srv.url, "-mountpoint", mountpoint)

	hostTotal, _, hostAvail := roomAt(t, backing)
	total, used, avail := roomAt(t, mountpoint)

	// The mount reports space in blocks, so the filesystem's total arrives floored to a
	// whole number of them. That is the only difference there may be: a total is a property
	// of the filesystem and does not move while it is mounted.
	if want := hostTotal / mountBlockSize * mountBlockSize; total != want {
		t.Fatalf("the mount reports a total of %d bytes over a filesystem of %d", total, hostTotal)
	}
	if avail <= 0 || avail > total {
		t.Fatalf("the mount reports %d bytes available of %d", avail, total)
	}
	// What is available moves under everything else on the machine between the two
	// readings, so the two are required to agree rather than to be equal. A figure that
	// came from anywhere but the filesystem would not be near this one at all.
	if slack := hostTotal / 100; hostAvail-avail > slack || avail-hostAvail > slack {
		t.Fatalf("the mount reports %d bytes available where the filesystem holding the served directory reports %d",
			avail, hostAvail)
	}
	t.Logf("mount: %d bytes, %d used, %d available; the filesystem underneath: %d, %d available",
		total, used, avail, hostTotal, hostAvail)

	// And it is a workspace, not only a set of figures: nothing about being under no
	// allowance stops anything being written.
	content := []byte("hello\n")
	if err := os.WriteFile(filepath.Join(mountpoint, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("writing through a mount that is under no allowance: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(backing, "a.txt")); err != nil || string(got) != string(content) {
		t.Fatalf("the served directory holds %q (%v), want %q", got, err, content)
	}
}

// writeThrough makes a file and writes size bytes into it in one write(2), and reports
// what that write answered.
//
// The write and the close are kept apart because R-WS-5 distinguishes them: an allowance
// spent has to be reported at the write, and a refusal that only surfaces at the close is
// the one this system may not give.
func writeThrough(t *testing.T, name string, size int) (int, error) {
	t.Helper()

	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("opening %s: %v", name, err)
	}
	// Closed whatever happens, including under a t.Fatal from the caller: an open file
	// inside a mountpoint keeps it busy, and a run that ended holding one would leave the
	// mount attached to this machine.
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("closing %s: %v", name, err)
		}
	}()
	return file.Write(make([]byte, size))
}

// roomAt reports what df says about a path, in bytes.
//
// df is asked rather than statfs(2) being called directly, because df is what an operator
// runs and the answer it prints is the one they act on. --output pins the columns down;
// the default layout puts a device name of unknown width in front of them.
func roomAt(t *testing.T, path string) (total, used, avail int64) {
	t.Helper()

	out, err := exec.Command("df", "-B1", "--output=size,used,avail", path).CombinedOutput()
	if err != nil {
		t.Fatalf("df on %s: %v\n%s", path, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("df on %s answered with %d lines, want a header and one row:\n%s", path, len(lines), out)
	}
	fields := strings.Fields(lines[1])
	if len(fields) != 3 {
		t.Fatalf("df on %s answered %q, want three figures", path, lines[1])
	}
	figures := make([]int64, len(fields))
	for i, field := range fields {
		if figures[i], err = strconv.ParseInt(field, 10, 64); err != nil {
			t.Fatalf("df on %s answered %q, which is not a byte count: %v", path, field, err)
		}
	}
	return figures[0], figures[1], figures[2]
}

// dial reaches a running server the way the mount does, for the questions that are put to
// the server itself rather than to a mountpoint over it.
func dial(t *testing.T, srv *runningServer) storage.Storage {
	t.Helper()
	client, err := httprest.Dial(srv.url, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("reaching %s: %v", srv.url, err)
	}
	return client
}

func spaceOf(t *testing.T, s storage.Storage) storage.Space {
	t.Helper()
	space, err := s.Space(context.Background())
	if err != nil {
		t.Fatalf("asking the server what the namespace holds: %v", err)
	}
	return space
}
