//go:build linux

package fuse_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type signalNamespace struct {
	served *replicated.Storage
	remote *httprest.Storage
	writes atomic.Int32
}

func newSignalNamespace(t *testing.T, allowance int64, files map[string][]byte) *signalNamespace {
	t.Helper()
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "namespace.db"), "signal", allowance, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	backing := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Error(err)
		}
	})
	for name, body := range files {
		if err := backing.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
		if err := backing.Write(t.Context(), name, body); err != nil {
			t.Fatal(err)
		}
	}
	handler, err := httprest.NewHandler(backing, meta)
	if err != nil {
		t.Fatal(err)
	}
	namespace := &signalNamespace{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := httprest.ParseRequest(r.Method, r.URL)
		if err == nil && request.Op == httprest.OpWrite {
			namespace.writes.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	namespace.remote, err = httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	namespace.served, err = replicated.New(t.Context(), replica, namespace.remote)
	if err != nil {
		replica.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := namespace.served.Close(); err != nil {
			t.Error(err)
		}
	})
	return namespace
}

type heldCloseWrite struct {
	storage.Storage
	entered chan context.Context
	release chan struct{}
}

func (s *heldCloseWrite) Write(ctx context.Context, name string, body []byte) error {
	s.entered <- ctx
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return s.Storage.Write(ctx, name, body)
}

type flushInterruptTrace struct {
	mu          sync.Mutex
	flush       uint64
	target      uint64
	seen        bool
	interrupted chan struct{}
}

func (trace *flushInterruptTrace) Write(body []byte) (int, error) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	var request, target uint64
	if n, _ := fmt.Sscanf(string(body), "rx %d: FLUSH", &request); n == 1 && bytes.Contains(body, []byte(": FLUSH ")) {
		trace.flush = request
	}
	if n, _ := fmt.Sscanf(string(body), "rx %d: INTERRUPT n0 {ix %d}", &request, &target); n == 2 && target == trace.target && !trace.seen {
		trace.seen = true
		close(trace.interrupted)
	}
	return len(body), nil
}

func TestSignalDuringClosePreservesTheCommit(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runWriteSignalChild(t, mode)
		return
	}
	requireFUSE(t)
	namespace := newSignalNamespace(t, 0, map[string][]byte{"file": []byte("old body")})
	held := &heldCloseWrite{Storage: namespace.served, entered: make(chan context.Context, 1), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(held.release) })
	trace := &flushInterruptTrace{interrupted: make(chan struct{})}
	point := mountStorage(t, held, fuse.Options{Logger: log.New(trace, "", 0), Debug: true})
	runSignalProcess(t, "TestSignalDuringClosePreservesTheCommit", "close", filepath.Join(point, "file"), func(ctx context.Context, pid, tid int) {
		var completing context.Context
		select {
		case completing = <-held.entered:
		case <-ctx.Done():
			t.Fatal("Close did not enter the held commit")
		}
		trace.mu.Lock()
		trace.target = trace.flush
		target := trace.target
		trace.mu.Unlock()
		if target == 0 {
			t.Fatal("the held commit has no FLUSH request")
		}
		if err := unix.Tgkill(pid, tid, syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
		// Flush owns a completion context, so its cancellation channel cannot prove
		// that the original request was interrupted. The FUSE trace identifies that
		// request and the kernel's INTERRUPT before the held commit is released.
		select {
		case <-trace.interrupted:
		case <-ctx.Done():
			t.Fatalf("the kernel did not interrupt FLUSH %d", target)
		}
		if err := completing.Err(); err != nil {
			t.Fatalf("the signal canceled the commit context: %v", err)
		}
		release.Do(func() { close(held.release) })
	})
	if got := namespace.writes.Load(); got != 1 {
		t.Fatalf("Close sent %d writes, want exactly one", got)
	}
	body, err := namespace.remote.Read(t.Context(), "file")
	if err != nil || string(body) != "new body" {
		t.Fatalf("the completed close saved %q, %v", body, err)
	}
}

type interruptedSpace struct {
	storage.Storage
	entered  chan context.Context
	returned chan error
	release  chan struct{}
	calls    atomic.Int32
}

func (s *interruptedSpace) Space(ctx context.Context) (storage.Space, error) {
	if s.calls.Add(1) != 1 {
		return s.Storage.Space(ctx)
	}
	s.entered <- ctx
	select {
	case <-ctx.Done():
	case <-s.release:
	}
	space, err := s.Storage.Space(ctx)
	s.returned <- err
	return space, err
}

func TestSignalDuringQuotaCheckPreservesTheWriteLimit(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runWriteSignalChild(t, mode)
		return
	}
	requireFUSE(t)
	for _, mode := range []string{"quota-raw", "quota-go"} {
		t.Run(mode, func(t *testing.T) {
			namespace := newSignalNamespace(t, 64<<10, map[string][]byte{
				"used": bytes.Repeat([]byte("x"), 32<<10),
				"file": {},
			})
			held := &interruptedSpace{Storage: namespace.served, entered: make(chan context.Context, 1), returned: make(chan error, 1), release: make(chan struct{})}
			defer close(held.release)
			point := mountStorage(t, held, fuse.Options{Logger: testLogger(t)})
			runSignalProcess(t, "TestSignalDuringQuotaCheckPreservesTheWriteLimit", mode, filepath.Join(point, "file"), func(ctx context.Context, pid, tid int) {
				var interrupted context.Context
				select {
				case interrupted = <-held.entered:
				case <-ctx.Done():
					t.Fatal("Write did not enter the quota check")
				}
				if interrupted.Err() != nil {
					t.Fatalf("the quota check was already canceled: %v", interrupted.Err())
				}
				if err := unix.Tgkill(pid, tid, syscall.SIGUSR1); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-held.returned:
					if !errors.Is(interrupted.Err(), context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR {
						t.Fatalf("quota check returned %v with context %v, want EINTR after cancellation", err, interrupted.Err())
					}
				case <-ctx.Done():
					t.Fatal("the signal did not cancel the quota check")
				}
			})
			wantCalls := int32(1)
			if mode == "quota-go" {
				wantCalls = 2
			}
			if calls := held.calls.Load(); calls != wantCalls {
				t.Fatalf("Space was called %d times, want %d", calls, wantCalls)
			}
			if writes := namespace.writes.Load(); writes != 0 {
				t.Fatalf("the rejected write sent %d commits", writes)
			}
			body, err := namespace.remote.Read(t.Context(), "file")
			if err != nil || len(body) != 0 {
				t.Fatalf("the rejected write saved %d bytes: %v", len(body), err)
			}
		})
	}
}

func runWriteSignalChild(t *testing.T, mode string) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	defer signal.Stop(signals)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	report := os.NewFile(3, "thread ID")
	defer report.Close()
	file, err := os.OpenFile(os.Getenv("REMOTE_FS_SIGNAL_PATH"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fd := file.Fd()
	if mode == "close" {
		if _, err := file.Write([]byte("new body")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fmt.Fprintln(report, unix.Gettid()); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "close":
	case "quota-raw":
		n, err := unix.Write(int(fd), bytes.Repeat([]byte("n"), 36<<10))
		if n > 0 || !errors.Is(err, syscall.EINTR) {
			t.Fatalf("interrupted raw write returned %d, %v, want no bytes and EINTR", n, err)
		}
	case "quota-go":
		n, err := file.Write(bytes.Repeat([]byte("n"), 36<<10))
		if n != 0 || !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("Go write after interruption returned %d, %v, want no bytes and EDQUOT", n, err)
		}
	default:
		t.Fatalf("unknown write signal mode %q", mode)
	}
	// Close consumes the descriptor even when the filesystem reports an error.
	// https://github.com/golang/go/blob/d90b98e65320778f3b1f99a6951ab20f04d218b3/src/internal/poll/fd_unix.go#L76-L118
	if err := file.Close(); err != nil {
		t.Fatalf("Close after interruption returned %v", err)
	}
	if _, err := unix.FcntlInt(fd, unix.F_GETFD, 0); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("descriptor after Close returned %v, want EBADF", err)
	}
	select {
	case <-signals:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not handle SIGUSR1")
	}
}
