// These tests exercise replica ownership and cleanup through the command's startup path.
// The mount lifecycle case uses a real mount and signal; TestMain attributes any leaked
// mount to this run.
package main

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse/fusetest"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestMutationConfirmationFlagsAreValidatedBeforeTheServerIsContacted(t *testing.T) {
	tests := []struct {
		flag  string
		value string
	}{
		{"-confirmation-grace", "0s"},
		{"-max-active-mutation-confirmations", "0"},
		{"-max-active-mutation-confirmations", strconv.Itoa(math.MaxInt)},
		{"-max-waiting-mutation-confirmations", "-1"},
		{"-max-waiting-mutation-confirmations", strconv.Itoa(math.MaxInt)},
	}
	for _, test := range tests {
		t.Run(test.flag, func(t *testing.T) {
			err := run([]string{
				"-server", "http://127.0.0.1:1",
				"-mountpoint", t.TempDir(),
				test.flag, test.value,
			}, io.Discard)
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("run returned %v, want EINVAL", err)
			}
		})
	}
}

func TestHTTPFrameLimitIsValidatedBeforeTheServerIsContacted(t *testing.T) {
	for _, test := range []struct {
		value string
		want  string
	}{
		{"0", "positive"},
		{"-1", "whole-number"},
		{"1KB", "decimal suffix"},
		{"1", "DialOptions.MaxFrameBytes"},
		{strconv.FormatInt(math.MaxInt64, 10), "DialOptions.MaxFrameBytes"},
	} {
		var output strings.Builder
		err := run([]string{
			"-server", "http://127.0.0.1:1",
			"-mountpoint", t.TempDir(),
			"-http-max-frame-bytes", test.value,
		}, &output)
		diagnostic := output.String()
		if err != nil {
			diagnostic += err.Error()
		}
		if err == nil || !strings.Contains(diagnostic, test.want) {
			t.Fatalf("-http-max-frame-bytes %s returned %v and %q, want local validation containing %q", test.value, err, output.String(), test.want)
		}
	}
}

func TestMutationConfirmationLimitsAreInCommandHelp(t *testing.T) {
	var said strings.Builder
	if err := run([]string{"-h"}, &said); err != nil {
		t.Fatalf("asking for help: %v", err)
	}
	for _, flag := range []string{
		"-http-max-frame-bytes",
		"-confirmation-grace",
		"-max-active-mutation-confirmations",
		"-max-waiting-mutation-confirmations",
	} {
		if !strings.Contains(said.String(), flag) {
			t.Errorf("help does not describe %s", flag)
		}
	}
}

func TestHTTPFrameLimitIsForwardedToTheReplicationClient(t *testing.T) {
	url, namespace := serveNamespace(t)
	longName := strings.Repeat("x", 1500)
	if err := namespace.Create(t.Context(), longName); err != nil {
		t.Fatalf("creating a row larger than the minimum frame: %v", err)
	}

	small, err := dialNamespace(url, startup, 1024)
	if err != nil {
		t.Fatalf("dialling with the minimum frame bound: %v", err)
	}
	if _, release, err := replicate(t.Context(), small, t.TempDir(), io.Discard); err == nil {
		release()
		t.Fatal("a 1024-byte client accepted the larger replication frame")
	}

	large, err := dialNamespace(url, startup, 4096)
	if err != nil {
		t.Fatalf("dialling with a larger frame bound: %v", err)
	}
	_, release, err := replicate(t.Context(), large, t.TempDir(), io.Discard)
	if err != nil {
		t.Fatalf("the configured larger client frame bound was not forwarded: %v", err)
	}
	release()
}

func TestInvalidMutationConfirmationOptionsDoNotCreateTheReplicaDirectory(t *testing.T) {
	where := t.TempDir()
	options := replicated.DefaultOptions()
	options.MaxActiveConfirmations = 0
	if _, _, err := replicateWithOptions(t.Context(), nil, where, io.Discard, options); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("replicateWithOptions returned %v, want EINVAL", err)
	}
	if entries, err := os.ReadDir(where); err != nil || len(entries) != 0 {
		t.Fatalf("invalid options left %d entries in the replica directory (%v)", len(entries), err)
	}
}

func TestMain(m *testing.M) {
	os.Exit(fusetest.Run("remote-fs-mount", m.Run))
}

// startup is how long the program is given to mount and to stop again. Generous: being slow
// is not the failure any of this is looking for.
const startup = 30 * time.Second

// --- what the command is pointed at ---------------------------------------------------

// serveNamespace starts a server over a namespace that keeps a change log, and hands back the
// namespace itself so that a test can put something in it without going through anything the
// mount does.
func serveNamespace(t *testing.T) (url string, namespace storage.Storage) {
	t.Helper()
	namespace, handler := replicableNamespace(t)
	return listenOn(t, handler), namespace
}

func newTestNamespace(t *testing.T) (*objectstore.Storage, *sqlite.LockingStore) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "namespace.db"), Namespace: "ws",
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

func replicableNamespace(t *testing.T) (storage.Storage, *httprest.Handler) {
	t.Helper()
	namespace, meta := newTestNamespace(t)
	handler, err := httprest.NewHandler(namespace, meta)
	if err != nil {
		t.Fatal(err)
	}
	return namespace, handler
}

func serveWithoutReplication(t *testing.T) string {
	t.Helper()
	namespace, _ := newTestNamespace(t)
	handler, err := httprest.NewHandler(namespace, nil)
	if err != nil {
		t.Fatal(err)
	}
	return listenOn(t, handler)
}

func listenOn(t *testing.T, handler http.Handler) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("the server stopped serving: %v", err)
		}
	}()
	// Closed rather than shut down: a change stream never becomes idle, so waiting for one to
	// finish would wait for as long as the test holds it.
	t.Cleanup(func() {
		server.Close()
		<-served
	})
	return "http://" + listener.Addr().String()
}

// refusing answers one operation with a failure, and everything else as usual.
//
// It is how the copy is made to fail after the directory and the file for it have already been
// made. Injected rather than produced by pointing at an address nobody is listening on,
// because what is asserted is what is left on disk afterwards, and the copy has to get far
// enough to have left something.
type refusing struct {
	http.Handler
	op httprest.Op
}

func (r refusing) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == httprest.Prefix+string(r.op) {
		http.Error(w, "this namespace is not answering that today", http.StatusInternalServerError)
		return
	}
	r.Handler.ServeHTTP(w, req)
}

// dial reaches the namespace exactly as run does.
func dial(t *testing.T, url string) *httprest.Storage {
	t.Helper()
	namespace, err := httprest.Dial(url, callerClient(startup))
	if err != nil {
		t.Fatal(err)
	}
	return namespace
}

// --- the copy is nobody else's ---------------------------------------------------------

// TestTheCopyIsOutOfEverybodyElsesReach.
//
// The copy holds the names, the sizes and the times of a whole workspace, and R-SEC-3 says
// those stay with the person whose workspace it is. All four paths are checked rather than the
// two this command creates: SQLite makes the write-ahead log and the shared-memory file, and
// that they come out with the mode of the database file is the whole reason deciding it once
// is enough.
func TestTheCopyIsOutOfEverybodyElsesReach(t *testing.T) {
	url, namespace := serveNamespace(t)
	if err := namespace.Write(t.Context(), "a.txt", []byte("something for the copy to hold")); err != nil {
		t.Fatalf("putting something in the namespace: %v", err)
	}

	// None of the modes below may come from the umask this process happens to have been
	// started with: under a permissive one, a mode nothing decided arrives as 0666 and every
	// assertion here becomes one that could not have failed. Tests in this package do not run
	// in parallel, so nothing else is creating files while it is zero.
	defer syscall.Umask(syscall.Umask(0))

	where := t.TempDir()
	_, release, err := replicate(t.Context(), dial(t, url), where, io.Discard)
	if err != nil {
		t.Fatalf("building the copy: %v", err)
	}
	defer release()

	dir := theOnlyEntryIn(t, where)
	if mode := modeOf(t, dir); mode != 0o700 {
		t.Fatalf("the copy is kept in a directory with mode %#o, want 0700: nobody but its owner may enter it", mode)
	}
	for _, name := range []string{"tree.db", "tree.db-wal", "tree.db-shm"} {
		if mode := modeOf(t, filepath.Join(dir, name)); mode != 0o600 {
			t.Fatalf("%s has mode %#o, want 0600: it holds the shape of a workspace and only its owner may read it", name, mode)
		}
	}
}

// TestAPathPreparedBySomebodyElseIsRefused.
//
// The directory the copy is kept in has a name nothing could have taken first; this is the
// other half of that guard, on the file inside it. A symbolic link is the case worth naming:
// following one would write the shape of somebody's workspace wherever it pointed, and O_EXCL
// refuses it without ever looking at what it points to.
func TestAPathPreparedBySomebodyElseIsRefused(t *testing.T) {
	t.Run("a file that is already there", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tree.db")
		if err := os.WriteFile(path, []byte("planted"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := createPrivately(path); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("a path something was already at was answered with %v, want it refused as taken", err)
		}
		if content, err := os.ReadFile(path); err != nil || string(content) != "planted" {
			t.Fatalf("what was already at the path now reads %q (%v), and this writes into nothing it did not make", content, err)
		}
	})

	t.Run("a link to somewhere else", func(t *testing.T) {
		dir := t.TempDir()
		path, elsewhere := filepath.Join(dir, "tree.db"), filepath.Join(dir, "elsewhere")
		if err := os.Symlink(elsewhere, path); err != nil {
			t.Fatal(err)
		}
		if err := createPrivately(path); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("a path a link was at was answered with %v, want it refused as taken", err)
		}
		if _, err := os.Lstat(elsewhere); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("the copy was made at %s, where a link at the path pointed (%v): following one puts a workspace's shape wherever somebody else chose", elsewhere, err)
		}
	})
}

// TestTheCopyIsGoneOnceTheMountpointIsDetached.
//
// The copy is as large as the tree it copies and it is somebody's workspace, so leaving it
// behind is both a disk filling up and a workspace's shape left on a machine after the mount
// that needed it has gone. The flag's own promise is that it is removed when the mountpoint is
// detached.
//
// Nothing here is stubbed below the signal handler, because the removal is a deferred call in
// run and the question is precisely whether a signal reaches it.
func TestTheCopyIsGoneOnceTheMountpointIsDetached(t *testing.T) {
	requireFUSE(t)

	url, namespace := serveNamespace(t)
	if err := namespace.Write(t.Context(), "a.txt", []byte("hello\n")); err != nil {
		t.Fatalf("putting something in the namespace: %v", err)
	}
	where, mountpoint := t.TempDir(), t.TempDir()

	said := &transcript{}
	var exit error
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		exit = run([]string{"-server", url, "-mountpoint", mountpoint, "-replica-dir", where}, said)
	}()
	// A test that fails before it can stop the program would leave the mountpoint attached,
	// which breaks every later run on this machine — and registered after the mountpoint, so
	// that it runs before the mountpoint is removed.
	t.Cleanup(func() {
		select {
		case <-ended:
			return
		default:
		}
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case <-ended:
		case <-time.After(startup):
			t.Errorf("the program did not stop, and %s may still be mounted", mountpoint)
		}
	})
	said.await(t, "mounted at", startup)

	// The copy is there, and it is being answered from. Without this the removal below would
	// be the removal of whatever happened to be in the directory, which is nothing at all.
	copied := theOnlyEntryIn(t, where)
	if content, err := os.ReadFile(filepath.Join(mountpoint, "a.txt")); err != nil || string(content) != "hello\n" {
		t.Fatalf("the mountpoint gives %q (%v), want what the namespace holds", content, err)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling this process: %v", err)
	}
	select {
	case <-ended:
		if exit != nil {
			t.Fatalf("the program exited with %v\n%s", exit, said)
		}
	case <-time.After(startup):
		t.Fatalf("the program did not exit within %v of a signal\n%s", startup, said)
	}

	if _, err := os.Stat(copied); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the copy is still at %s (%v) after the mountpoint was detached", copied, err)
	}
	if left := entriesIn(t, where); len(left) != 0 {
		t.Fatalf("%s still holds %v after the mountpoint was detached", where, left)
	}
	if mounted, err := fusetest.Mounted(mountpoint); err != nil {
		t.Fatal(err)
	} else if mounted {
		t.Fatalf("%s is still mounted after the program exited", mountpoint)
	}
}

// TestNothingIsLeftBehindWhenTheCopyIsNotBuilt.
//
// Each of these gives up part way through making a copy, and what a mount that failed must not
// do is leave one: the next attempt would find a directory nobody is going to remove, holding
// a tree nobody is going to finish.
func TestNothingIsLeftBehindWhenTheCopyIsNotBuilt(t *testing.T) {
	t.Run("the namespace will not be copied", func(t *testing.T) {
		_, handler := replicableNamespace(t)
		// Refused after the subscription is established, so that the copy fails with its
		// directory, its database and its stream all already made.
		url := listenOn(t, refusing{Handler: handler, op: httprest.OpSnapshot})

		where := t.TempDir()
		served, release, err := replicate(t.Context(), dial(t, url), where, io.Discard)
		if err == nil {
			release()
			t.Fatalf("a namespace that would not be copied was mounted anyway, from %v", served)
		}
		if errors.Is(err, syscall.ENOSYS) {
			t.Fatalf("a namespace that refused one request was taken for one that keeps no log: %v", err)
		}
		if left := entriesIn(t, where); len(left) != 0 {
			t.Fatalf("a copy that was never built left %v behind in %s", left, where)
		}
	})

	t.Run("the server does not expose replication", func(t *testing.T) {
		namespace := dial(t, serveWithoutReplication(t))
		where := t.TempDir()
		said := &transcript{}

		served, release, err := replicate(t.Context(), namespace, where, said)
		if err != nil {
			t.Fatalf("a server without replication failed to mount: %v", err)
		}
		defer release()

		if served != namespace {
			t.Fatalf("a server without replication is served from %v, want the namespace itself", served)
		}
		if left := entriesIn(t, where); len(left) != 0 {
			t.Fatalf("a server without replication left %v behind in %s", left, where)
		}
		if !strings.Contains(said.String(), "keeps no record") {
			t.Fatalf("nothing said that this namespace is not being copied, so nobody watching would know every operation is a request:\n%s", said)
		}
	})

	t.Run("there is nowhere to keep it", func(t *testing.T) {
		where := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(where, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		url, _ := serveNamespace(t)

		_, _, err := replicate(t.Context(), dial(t, url), where, io.Discard)
		if err == nil {
			t.Fatal("a copy was kept somewhere that is not a directory")
		}
		if !strings.Contains(err.Error(), "making a directory for the copy") {
			t.Fatalf("the failure does not say what could not be made: %v", err)
		}
	})
}

// TestTheCopyGoesUnderTheSystemTemporaryDirectoryByDefault, which is what the flag says and
// what everybody who does not pass it gets.
func TestTheCopyGoesUnderTheSystemTemporaryDirectoryByDefault(t *testing.T) {
	url, _ := serveNamespace(t)
	before := copiesUnderTemp(t)

	_, release, err := replicate(t.Context(), dial(t, url), "", io.Discard)
	if err != nil {
		t.Fatalf("building the copy: %v", err)
	}

	made := added(before, copiesUnderTemp(t))
	if len(made) != 1 {
		release()
		t.Fatalf("asking for the default put %v under %s, want one copy", made, os.TempDir())
	}
	release()
	if _, err := os.Stat(made[0]); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the copy is still at %s (%v) after it was released", made[0], err)
	}
}

func copiesUnderTemp(t *testing.T) []string {
	t.Helper()
	// TMPDIR is this run's own directory, so what is under it was put there by this run.
	found, err := filepath.Glob(filepath.Join(os.TempDir(), "remote-fs-replica-*"))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func added(before, after []string) []string {
	var made []string
	for _, path := range after {
		if !slices.Contains(before, path) {
			made = append(made, path)
		}
	}
	return made
}

// --- reading the machine back ----------------------------------------------------------

func entriesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// theOnlyEntryIn is the path of what a copy was made at, and an assertion that exactly one
// was: a test that went looking through several would be a test that had already found a
// defect.
func theOnlyEntryIn(t *testing.T, dir string) string {
	t.Helper()
	names := entriesIn(t, dir)
	if len(names) != 1 {
		t.Fatalf("%s holds %v, want one copy of the namespace's metadata", dir, names)
	}
	return filepath.Join(dir, names[0])
}

func modeOf(t *testing.T, path string) fs.FileMode {
	t.Helper()
	// Not followed: a link's own mode says nothing about what it points at, and every path
	// here is one this command made itself.
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
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

// transcript is what the program said, kept so that a test can wait for a line of it and a
// failure can show all of it.
type transcript struct {
	mu      sync.Mutex
	written strings.Builder
}

func (s *transcript) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written.Write(p)
}

func (s *transcript) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written.String()
}

func (s *transcript) await(t *testing.T, line string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !strings.Contains(s.String(), line) {
		if time.Now().After(deadline) {
			t.Fatalf("the program did not say %q within %v\n%s", line, within, s)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
