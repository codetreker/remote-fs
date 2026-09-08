//go:build linux

package fuse_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
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
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type signalNamespace struct {
	served storage.FileStorage
	remote *httprest.Storage
	writes atomic.Int32
}

func newSignalNamespace(t *testing.T, allowance int64, files map[string][]byte) *signalNamespace {
	t.Helper()
	meta, backing := memoryfixture.New(t, "signal", allowance, locking.DefaultOptions())
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
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Errorf("close signal handler: %v", err)
		}
	})
	namespace.remote, err = httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	served, err := replicated.New(t.Context(), replica, namespace.remote)
	if err != nil {
		replica.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := served.Close(); err != nil {
			t.Error(err)
		}
	})
	namespace.served = &signalFileStorage{FileStorage: served, wrap: func(file storage.File) storage.File {
		return &countedSignalFile{File: file, writes: &namespace.writes}
	}}
	return namespace
}

type signalFileStorage struct {
	storage.FileStorage
	wrap func(storage.File) storage.File
}

func (s *signalFileStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &signalFileSession{FileSession: session, wrap: s.wrap}, nil
}

type signalFileSession struct {
	storage.FileSession
	wrap func(storage.File) storage.File
}

func (s *signalFileSession) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return s.wrap(file), nil
}

func (s *signalFileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenNode(ctx, id, options)
	if err != nil {
		return nil, err
	}
	return s.wrap(file), nil
}

type countedSignalFile struct {
	storage.File
	writes *atomic.Int32
}

func (f *countedSignalFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	f.writes.Add(1)
	return f.File.WriteAt(ctx, offset, data)
}

type heldCloseCleanup struct {
	storage.File
	entered chan context.Context
	release chan struct{}
}

func (s *heldCloseCleanup) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	if family != storage.POSIX {
		return s.File.DropLocks(ctx, owner, family)
	}
	s.entered <- ctx
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return s.File.DropLocks(ctx, owner, family)
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

func TestSignalDuringClosePreservesOwnerCleanup(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runWriteSignalChild(t, mode)
		return
	}
	requireFUSE(t)
	namespace := newSignalNamespace(t, 0, map[string][]byte{"file": []byte("old body")})
	held := &heldCloseCleanup{entered: make(chan context.Context, 1), release: make(chan struct{})}
	wrapped := &signalFileStorage{FileStorage: namespace.served, wrap: func(file storage.File) storage.File {
		held.File = file
		return held
	}}
	var release sync.Once
	defer release.Do(func() { close(held.release) })
	trace := &flushInterruptTrace{interrupted: make(chan struct{})}
	point := mountStorage(t, wrapped, fuse.Options{Logger: log.New(trace, "", 0), Debug: true})
	runSignalProcess(t, "TestSignalDuringClosePreservesOwnerCleanup", "close", filepath.Join(point, "file"), func(ctx context.Context, pid, tid int) {
		var completing context.Context
		select {
		case completing = <-held.entered:
		case <-ctx.Done():
			t.Fatal("Close did not enter owner cleanup")
		}
		trace.mu.Lock()
		trace.target = trace.flush
		target := trace.target
		trace.mu.Unlock()
		if target == 0 {
			t.Fatal("owner cleanup has no FLUSH request")
		}
		if err := unix.Tgkill(pid, tid, syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
		// Flush owns a completion context, so its cancellation channel cannot prove
		// that the original request was interrupted. The FUSE trace identifies that
		// request and the kernel's INTERRUPT before owner cleanup is released.
		select {
		case <-trace.interrupted:
		case <-ctx.Done():
			t.Fatalf("the kernel did not interrupt FLUSH %d", target)
		}
		if err := completing.Err(); err != nil {
			t.Fatalf("the signal canceled owner cleanup: %v", err)
		}
		release.Do(func() { close(held.release) })
	})
	if got := namespace.writes.Load(); got != 1 {
		t.Fatalf("write followed by Close sent %d writes, want exactly one", got)
	}
	body, err := namespace.remote.Read(t.Context(), "file")
	if err != nil || string(body) != "new body" {
		t.Fatalf("close cleanup changed completed write contents %q, %v", body, err)
	}
}

type interruptedFileWrite struct {
	storage.File
	entered  chan context.Context
	returned chan error
	release  chan struct{}
	calls    atomic.Int32
}

func (s *interruptedFileWrite) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	if s.calls.Add(1) != 1 {
		return s.File.WriteAt(ctx, offset, data)
	}
	s.entered <- ctx
	select {
	case <-ctx.Done():
	case <-s.release:
	}
	err := ctx.Err()
	if err == nil {
		err = syscall.EIO
	}
	s.returned <- err
	return storage.Attr{}, err
}

func TestSignalDuringWritePreservesTheAuthoritativeQuotaLimit(t *testing.T) {
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
			held := &interruptedFileWrite{entered: make(chan context.Context, 1), returned: make(chan error, 1), release: make(chan struct{})}
			wrapped := &signalFileStorage{FileStorage: namespace.served, wrap: func(file storage.File) storage.File {
				held.File = file
				return held
			}}
			defer close(held.release)
			point := mountStorage(t, wrapped, fuse.Options{Logger: testLogger(t)})
			runSignalProcess(t, "TestSignalDuringWritePreservesTheAuthoritativeQuotaLimit", mode, filepath.Join(point, "file"), func(ctx context.Context, pid, tid int) {
				var interrupted context.Context
				select {
				case interrupted = <-held.entered:
				case <-ctx.Done():
					t.Fatal("Write did not enter the retained mutation")
				}
				if interrupted.Err() != nil {
					t.Fatalf("the retained write was already canceled: %v", interrupted.Err())
				}
				if err := unix.Tgkill(pid, tid, syscall.SIGUSR1); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-held.returned:
					if !errors.Is(interrupted.Err(), context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR {
						t.Fatalf("retained write returned %v with context %v, want EINTR after cancellation", err, interrupted.Err())
					}
				case <-ctx.Done():
					t.Fatal("the signal did not cancel the retained write")
				}
			})
			wantCalls := int32(1)
			if mode == "quota-go" {
				wantCalls = 2
			}
			if calls := held.calls.Load(); calls != wantCalls {
				t.Fatalf("retained WriteAt was called %d times, want %d", calls, wantCalls)
			}
			if writes := namespace.writes.Load(); writes != wantCalls-1 {
				t.Fatalf("the interrupted write sent %d authority requests, want %d", writes, wantCalls-1)
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
