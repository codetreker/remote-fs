// The workspace allowance, from the flag that sets it to the tools that see it.
//
// R-WS-5 asks for a limit given by the deployer, three measured figures reported to
// whatever runs on the mountpoint, and a refusal that lands on the write that caused it.
// Each layer below proves its own part — the count and its arithmetic in
// packages/metastore/sqlite, the reply to statfs(2) in packages/fuse — and none of them
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
	root := privateDirectory(t)
	srv := startLocalStoreServerBinary(t, root, "64K")
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

	// O_CREAT can publish an empty node before write(2) checks capacity.
	switch attr, err := dial(t, srv).Stat(t.Context(), "big.bin"); {
	case errors.Is(err, syscall.ENOENT):
	case err != nil:
		t.Fatalf("stat rejected file in the authoritative namespace: %v", err)
	case attr.Size != 0:
		t.Fatalf("the authoritative namespace published %d rejected bytes", attr.Size)
	}
	if _, used, _ = roomAt(t, mountpoint); used != half {
		t.Fatalf("df reports %d bytes used after a refused write, want the %d that were written before it", used, half)
	}
}

// A restarted server must charge against the persisted workspace usage before its first
// mounted write, including when the attempted write is smaller than the total allowance.
func TestAnAllowanceIsSpentAgainstWhatTheWorkspaceAlreadyHolds(t *testing.T) {
	requireFUSE(t)

	const allowance = 64 << 10
	const held = allowance / 2

	root := privateDirectory(t)
	seed := startLocalStoreServerBinary(t, root, "64K")
	if err := dial(t, seed).Write(t.Context(), "held.bin", make([]byte, held)); err != nil {
		t.Fatalf("seed existing workspace usage: %v", err)
	}
	seed.interrupt(t)
	if err := seed.wait(t); err != nil {
		t.Fatalf("stop seeded server: %v", err)
	}
	srv := startServerBinary(t, localStoreServerArgs(root, "64K")...)
	srv.awaitLine(t, fmt.Sprintf("allowance of %d bytes, %d of them taken", allowance, held), startup)

	mountpoint := t.TempDir()
	startMountBinary(t, "-server", srv.url, "-mountpoint", mountpoint)

	// Under the allowance, over what is left of it. A workspace weighing this against the
	// allowance alone would take it and end up holding more than it may. Nothing has been
	// written through this mount yet, so the figure it weighs the write against is the one
	// the reopened metastore reports.
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

func TestHangingUpReportsLocalStoreState(t *testing.T) {
	const held = 5000
	srv := startLocalStoreServerBinary(t, privateDirectory(t), "64K")
	client := dial(t, srv)
	if err := client.Write(t.Context(), "artifact", make([]byte, held)); err != nil {
		t.Fatalf("write before SIGHUP: %v", err)
	}
	before := spaceOf(t, client)
	srv.hangup(t)
	line := srv.awaitLine(t, "local-store status", startup)
	if !strings.Contains(line, fmt.Sprintf("%d of %d workspace bytes used", held, before.Total)) {
		t.Fatalf("status omitted the current workspace usage: %s", line)
	}
	if after := spaceOf(t, client); after != before {
		t.Fatalf("SIGHUP changed workspace accounting: before=%+v, after=%+v", before, after)
	}
	if _, err := client.List(t.Context(), ""); err != nil {
		t.Fatalf("list after SIGHUP: %v", err)
	}
	srv.interrupt(t)
	if err := srv.wait(t); err != nil {
		t.Fatalf("server exit after SIGHUP: %v", err)
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
