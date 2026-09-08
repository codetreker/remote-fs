// These tests join real HTTP servers, enforcing object namespaces and independent FUSE
// mounts. They require /dev/fuse; the strict test runner rejects unavailable mount tests.
package cmd_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/fuse/fusetest"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// TestMain runs the tests beneath a temporary directory of this run's own, so that a
// mountpoint still attached afterwards is one this run attached. See
// packages/fuse/fusetest for why the mount table cannot be read any other way.
func TestMain(m *testing.M) {
	os.Exit(fusetest.Run("remote-fs-cmd", func() int {
		code := m.Run()
		removeBuiltBinaries()
		return code
	}))
}

// --- the system under test -----------------------------------------------------------

// namespaceServer is one server over one namespace, on a real TCP listener.
type namespaceServer struct {
	url string

	authoritative storage.Storage

	// calls counts what crosses the wire, which is how "the copy answered this without
	// asking anybody" is a number rather than an impression.
	calls *calls

	// stop makes the server unreachable, the way a machine going away makes it
	// unreachable: the listener closes and every connection is severed.
	stop func()
}

func serveNamespace(t *testing.T) *namespaceServer {
	t.Helper()
	namespace, meta := namespaceFixture(t)
	return serveStorage(t, namespace, meta)
}

// The handler's explicit nil log keeps the ENOSYS replication path covered over a real
// enforcing namespace, independently of whether its backend retains a change log.
func serveUnreplicatedNamespace(t *testing.T) *namespaceServer {
	t.Helper()
	namespace, _ := namespaceFixture(t)
	return serveStorage(t, namespace, nil)
}

func namespaceFixture(t *testing.T) (*objectstore.Storage, *sqlite.LockingStore) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(privateDirectory(t), "namespace.db"), Namespace: "ws",
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatalf("opening the namespace's metastore: %v", err)
	}
	namespace := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})
	return namespace, meta
}

func privateDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("make private directory: %v", err)
	}
	return directory
}

func serveStorage(t *testing.T, namespace storage.Storage, log metastore.Log) *namespaceServer {
	t.Helper()

	handler, err := httprest.NewHandler(namespace, log)
	if err != nil {
		t.Fatal(err)
	}
	counted := &calls{handler: handler, counts: map[string]int{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	httpServer := &http.Server{Handler: counted}
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

	return &namespaceServer{url: "http://" + listener.Addr().String(), authoritative: namespace, calls: counted, stop: stop}
}

// calls counts the requests that reach the server, by operation.
type calls struct {
	handler http.Handler

	mu     sync.Mutex
	counts map[string]int
}

func (c *calls) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.counts[strings.TrimPrefix(r.URL.Path, httprest.Prefix)]++
	c.mu.Unlock()
	c.handler.ServeHTTP(w, r)
}

// snapshot is what has arrived so far, so that a later call can be compared against it.
func (c *calls) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	taken := make(map[string]int, len(c.counts))
	for op, count := range c.counts {
		taken[op] = count
	}
	return taken
}

// since renders what has arrived since a snapshot was taken, naming the operations rather
// than only counting them.
func (c *calls) since(before map[string]int) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var arrived []string
	for op, count := range c.counts {
		if extra := count - before[op]; extra > 0 {
			arrived = append(arrived, fmt.Sprintf("%s×%d", op, extra))
		}
	}
	return strings.Join(arrived, " ")
}

// mountpointOn mounts the server's namespace at a fresh directory, through a storage of
// its own. Two calls produce two independent mounts of the same namespace, which is what
// "two machines" means here.
//
// The mount is given a copy of the namespace's metadata where the namespace keeps a change
// log, and the namespace itself where it does not — which is what the binary does, decided
// the way the binary decides it: by asking, and by telling ENOSYS from a failure to reach
// anything.
func mountpointOn(t *testing.T, s *namespaceServer) string {
	t.Helper()
	requireFUSE(t)

	namespace, err := httprest.Dial(s.url, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	served := copyOf(t, namespace)
	// Registered before the mount so that it is removed after the unmount: cleanups run
	// in reverse, and removing a directory that is still mounted does not work.
	mountpoint := t.TempDir()

	m, err := fuse.New(mountpoint, served, fuse.Options{Logger: testLogger(t)})
	if err != nil {
		t.Fatalf("mounting %s at %s: %v", s.url, mountpoint, err)
	}
	t.Cleanup(func() { unmount(t, m, mountpoint) })
	return mountpoint
}

// copyOf builds the local copy the mount is served from, and reports the namespace itself for
// one that keeps no log.
func copyOf(t *testing.T, namespace *httprest.Storage) storage.Storage {
	t.Helper()

	replica, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	served, err := replicated.New(t.Context(), replica, namespace)
	switch {
	case errors.Is(err, syscall.ENOSYS):
		replica.Close()
		return namespace
	case err != nil:
		replica.Close()
		t.Fatalf("copying the namespace's metadata: %v", err)
	}
	t.Cleanup(func() { served.Close() })
	return served
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

// SQLite stores regular files and directories. This metadata decorator preserves real
// node identities and content lengths while exercising the public symbolic-link contract.
type symlinkMetadata struct {
	*objectstore.Storage
	linkID uint64
}

func (s *symlinkMetadata) describe(attr storage.Attr) storage.Attr {
	if attr.ID == s.linkID {
		attr.Mode = fs.ModeSymlink | 0o777
	}
	return attr
}

func (s *symlinkMetadata) Stat(ctx context.Context, name string) (storage.Attr, error) {
	attr, err := s.Storage.Stat(ctx, name)
	if err != nil {
		return storage.Attr{}, err
	}
	return s.describe(attr), nil
}

func (s *symlinkMetadata) List(ctx context.Context, name string) ([]storage.Entry, error) {
	entries, err := s.Storage.List(ctx, name)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].Attr = s.describe(entries[i].Attr)
	}
	return entries, nil
}

func (s *symlinkMetadata) ListBounded(ctx context.Context, name string, result *storage.ListResult) error {
	if result == nil {
		return s.Storage.ListBounded(ctx, name, result)
	}
	// Native enumeration remains bounded; the destination charges the transformed mode.
	captured, err := storage.NewListResult(result.MaxBytes(), 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + int64(unsafe.Sizeof(storage.Entry{})), nil
	})
	if err != nil {
		return result.Fail(err)
	}
	if err := s.Storage.ListBounded(ctx, name, captured); err != nil {
		return result.Fail(err)
	}
	entries, err := captured.Entries()
	if err != nil {
		return result.Fail(err)
	}
	for _, entry := range entries {
		entry.Attr = s.describe(entry.Attr)
		if err := result.Add(entry); err != nil {
			return result.Fail(err)
		}
	}
	return nil
}
