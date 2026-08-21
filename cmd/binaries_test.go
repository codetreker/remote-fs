// These tests run the two binaries as processes. Calling the packages proves the
// packages; only running the programs proves what is shipped — the flags, the messages,
// the exit codes, and the detaching that has to happen when a person presses Ctrl-C.
package cmd_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// startup is how long a binary is given to report that it is up. Generous: it covers a
// mount, and being slow is not the failure these tests are looking for.
const startup = 30 * time.Second

// TestTheBinariesServeAndMount drives the two commands from the scope note through a
// shell, against a mountpoint that a real remote-fs process is serving from a real
// remote-fs-server process.
func TestTheBinariesServeAndMount(t *testing.T) {
	requireFUSE(t)

	backing := t.TempDir()
	srv := startServerBinary(t, "-listen", "127.0.0.1:0", "-dir", backing)
	mountpoint := t.TempDir()
	mnt := startMountBinary(t, "-server", srv.url, "-mountpoint", mountpoint)

	shell(t, fmt.Sprintf("echo hello > %s/a.txt", mountpoint))
	if got := shell(t, fmt.Sprintf("cat %s/a.txt", mountpoint)); got != "hello\n" {
		t.Fatalf("cat gave %q, want %q", got, "hello\n")
	}

	// The same bytes, from the directory the server was given rather than through it.
	onDisk, err := os.ReadFile(filepath.Join(backing, "a.txt"))
	if err != nil {
		t.Fatalf("the served directory has no a.txt: %v", err)
	}
	if string(onDisk) != "hello\n" {
		t.Fatalf("the served directory holds %q, want %q", onDisk, "hello\n")
	}

	// Ctrl-C, and the mountpoint has to be gone afterwards. A stale mount left behind is
	// the defect R-ERR-5 names: every later access to that path fails until somebody
	// detaches it by hand.
	mnt.interrupt(t)
	if err := mnt.wait(t); err != nil {
		t.Fatalf("remote-fs exited with %v after SIGINT\n%s", err, mnt.output())
	}
	if stillMounted(t, mountpoint) {
		t.Fatalf("%s is still mounted after remote-fs exited", mountpoint)
	}

	srv.interrupt(t)
	if err := srv.wait(t); err != nil {
		t.Fatalf("remote-fs-server exited with %v after SIGINT\n%s", err, srv.output())
	}
}

// TestTheBinariesRefuseWhatTheyCannotDo. Every one of these is a mistake somebody will
// make on their first afternoon, and each has to say which mistake it was, exit non-zero,
// and not answer with a stack trace.
func TestTheBinariesRefuseWhatTheyCannotDo(t *testing.T) {
	occupied, free := oneAddressInUseAndOneFree(t)
	backing := t.TempDir()
	notADirectory := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(notADirectory, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// A server the mount binary can reach, so that the mountpoint failure below is
	// reported for the mountpoint rather than for the server.
	reachable := startServerBinary(t, "-listen", "127.0.0.1:0", "-dir", backing)

	for _, c := range []struct {
		name    string
		binary  string
		args    []string
		expects string
	}{
		{"a directory that is not there", serverBinary(t),
			[]string{"-listen", free, "-dir", filepath.Join(backing, "absent")}, "no such file or directory"},
		{"a directory that is a file", serverBinary(t),
			[]string{"-listen", free, "-dir", notADirectory}, "not a directory"},
		{"an address already in use", serverBinary(t),
			[]string{"-listen", occupied, "-dir", backing}, "address already in use"},
		{"no directory at all", serverBinary(t),
			[]string{"-listen", free}, "-dir is required"},
		{"a flag nobody defined", serverBinary(t),
			[]string{"-listen", free, "-dir", backing, "-nonsense"}, "not defined"},
		{"an allowance that is not a size", serverBinary(t),
			[]string{"-listen", free, "-dir", backing, "-quota", "banana"}, "is not a size"},
		{"an allowance spelled as a power of 1000", serverBinary(t),
			[]string{"-listen", free, "-dir", backing, "-quota", "5MB"}, "power of 1000"},
		{"an allowance below the smallest there is", serverBinary(t),
			[]string{"-listen", free, "-dir", backing, "-quota", "1K"}, "smallest allowance"},

		{"a mountpoint that is not a directory", mountBinary(t),
			[]string{"-server", reachable.url, "-mountpoint", notADirectory}, "not a directory"},
		{"a mountpoint that is not there", mountBinary(t),
			[]string{"-server", reachable.url, "-mountpoint", filepath.Join(backing, "absent")}, "no such file or directory"},
		{"a server that cannot be reached", mountBinary(t),
			[]string{"-server", "http://" + free, "-mountpoint", t.TempDir()}, "cannot be reached"},
		{"a server URL that is not a URL", mountBinary(t),
			[]string{"-server", "nowhere", "-mountpoint", t.TempDir()}, "needs a scheme and a host"},
		{"no server at all", mountBinary(t),
			[]string{"-mountpoint", t.TempDir()}, "-server is required"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, err := exec.Command(c.binary, c.args...).CombinedOutput()
			if err == nil {
				t.Fatalf("%s succeeded; it should have refused\n%s", filepath.Base(c.binary), out)
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("%s did not run: %v", filepath.Base(c.binary), err)
			}
			if !strings.Contains(string(out), c.expects) {
				t.Fatalf("the complaint does not mention %q:\n%s", c.expects, out)
			}
			// A panic reaching the operator means the program did not know this could
			// happen, and the operator has to read a traceback to find out what they typed
			// wrong.
			for _, trace := range []string{"panic:", "goroutine ", ".go:"} {
				if strings.Contains(string(out), trace) {
					t.Fatalf("the complaint reads as a crash (%q):\n%s", trace, out)
				}
			}
			t.Logf("exit %d: %s", exit.ExitCode(), strings.TrimSpace(string(out)))
		})
	}

	reachable.interrupt(t)
	if err := reachable.wait(t); err != nil {
		t.Fatalf("remote-fs-server exited with %v after SIGINT\n%s", err, reachable.output())
	}
}

// oneAddressInUseAndOneFree returns an address something is listening on for the whole
// test, and one nothing is listening on.
func oneAddressInUseAndOneFree(t *testing.T) (occupied, free string) {
	t.Helper()

	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })

	// Taken and released. Nothing guarantees the port stays free, but on a machine that
	// is not exhausting its ephemeral range the window is not one that reopens.
	released, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	free = released.Addr().String()
	released.Close()

	return held.Addr().String(), free
}

func shell(t *testing.T, script string) string {
	t.Helper()
	out, err := exec.Command("sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("`%s` failed: %v\n%s", script, err, out)
	}
	return string(out)
}

func stillMounted(t *testing.T, mountpoint string) bool {
	t.Helper()
	mounts, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(mounts), "\n") {
		if fields := strings.Fields(line); len(fields) > 1 && fields[1] == mountpoint {
			return true
		}
	}
	return false
}
