// The tests in this package stand up the whole system — a real HTTP listener over a real
// directory, and two independent mountpoints against it — and check the claim the project
// exists to make: what one machine writes, another reads.
//
// They are here rather than inside a package because there is no package to put them in.
// Every package below has been proved in halves: the contract suite runs through client →
// HTTP → server → localdir with no mount in sight, and the mount is compared against a
// plain directory with no network in sight. Joining the two halves is not a fact about
// either of them.
//
// These tests mount filesystems, so they need /dev/fuse and skip without it. See
// docs/testing.md.
package cmd_test

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// TestMain checks afterwards that nothing was left attached to this machine. A test that
// leaves a mountpoint behind wedges every path under it until somebody detaches it by
// hand, and the run that did it has usually already reported success.
func TestMain(m *testing.M) {
	before := fuseConnections()
	code := m.Run()
	removeBuiltBinaries()
	for _, leak := range mountLeaks(before) {
		fmt.Fprintf(os.Stderr, "left behind: %s\n", leak)
		code = 1
	}
	os.Exit(code)
}

// mountLeaks reports anything this run attached and did not detach.
//
// The FUSE connections present beforehand are subtracted rather than the directory being
// required to be empty, because it is shared with everything else on the machine that
// uses FUSE and a developer's desktop rarely has none.
func mountLeaks(connectionsBefore map[string]bool) []string {
	var leaks []string

	mounts, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		return []string{fmt.Sprintf("cannot read /proc/self/mounts, so leaks cannot be ruled out: %v", err)}
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(mounts)), "\n") {
		if strings.HasPrefix(line, "remote-fs ") {
			leaks = append(leaks, "a mount: "+line)
		}
	}

	for id := range fuseConnections() {
		if !connectionsBefore[id] {
			leaks = append(leaks, "a FUSE connection: /sys/fs/fuse/connections/"+id)
		}
	}

	for _, pid := range fusermountChildren() {
		leaks = append(leaks, "a fusermount process: pid "+pid)
	}
	return leaks
}

func fuseConnections() map[string]bool {
	present := map[string]bool{}
	entries, err := os.ReadDir("/sys/fs/fuse/connections")
	if err != nil {
		// Not mounted on this machine, so there is nothing to compare against and nothing
		// this check can claim either way.
		return present
	}
	for _, entry := range entries {
		present[entry.Name()] = true
	}
	return present
}

// fusermountChildren reports the fusermount processes this process started and did not
// reap. Only our own children are considered: fusermount is a setuid helper that anything
// on the machine may be running, and a stranger's is not evidence about this run.
func fusermountChildren() []string {
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
			// The process exited between the listing and the read, which is what we want
			// it to have done.
			continue
		}
		text := string(status)
		if strings.Contains(text, "\nPPid:\t"+self+"\n") && strings.Contains(text, "Name:\tfusermount") {
			found = append(found, entry.Name())
		}
	}
	return found
}

// --- the system under test -----------------------------------------------------------

// namespaceServer is one server over one directory, on a real TCP listener.
type namespaceServer struct {
	url     string
	backing string

	// stop makes the server unreachable, the way a machine going away makes it
	// unreachable: the listener closes and every connection is severed.
	stop func()
}

// serveDirectory starts a server over a fresh directory and returns once it is listening.
func serveDirectory(t *testing.T) *namespaceServer {
	t.Helper()

	backing := t.TempDir()
	namespace, err := localdir.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httprest.NewHandler(namespace)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	httpServer := &http.Server{Handler: handler}
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("the server stopped serving: %v", err)
		}
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			httpServer.Close()
			<-served
		})
	}
	t.Cleanup(stop)

	return &namespaceServer{url: "http://" + listener.Addr().String(), backing: backing, stop: stop}
}

// mountpointOn mounts the server's namespace at a fresh directory, through a storage of
// its own. Two calls produce two independent mounts of the same namespace, which is what
// "two machines" means here.
func mountpointOn(t *testing.T, s *namespaceServer) string {
	t.Helper()
	requireFUSE(t)

	namespace, err := httprest.Dial(s.url, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the mount so that it is removed after the unmount: cleanups run
	// in reverse, and removing a directory that is still mounted does not work.
	mountpoint := t.TempDir()

	m, err := fuse.New(mountpoint, namespace, fuse.Options{Logger: testLogger(t)})
	if err != nil {
		t.Fatalf("mounting %s at %s: %v", s.url, mountpoint, err)
	}
	t.Cleanup(func() { unmount(t, m, mountpoint) })
	return mountpoint
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

// unmount detaches a mountpoint whether or not the test passed. Detaching fails while
// anything still holds a file inside, which after a failed test it may briefly do.
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
	// The test has already failed by this point, but a mountpoint left attached would
	// break every later run on this machine, so detach it the blunt way.
	out, lazyErr := exec.Command("fusermount3", "-uz", mountpoint).CombinedOutput()
	t.Errorf("detaching %s failed: %v; the lazy attempt said %v %s", mountpoint, err, lazyErr, out)
}

func testLogger(t *testing.T) *log.Logger { return log.New(testWriter{t}, "go-fuse: ", 0) }

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// errnoOf reports the errno an operation on a mountpoint failed with, and 0 for anything
// carrying none. A failure with no errno in it is a failure to describe, which these
// tests need to be able to tell apart from a particular one.
func errnoOf(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return 0
}
