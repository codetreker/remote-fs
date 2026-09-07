//go:build linux

package fuse_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

const signalChildMode = "REMOTE_FS_SIGNAL_CHILD"

type interruptedStat struct {
	name    string
	entered chan context.Context
	mu      sync.Mutex
	ctx     context.Context
	held    bool
}

type observedStatStorage struct {
	storage.BoundedStorage
	request *interruptedStat
}

func (s *observedStatStorage) Stat(ctx context.Context, name string) (storage.Attr, error) {
	if name == s.request.name {
		s.request.mu.Lock()
		if s.request.ctx == nil {
			s.request.ctx = ctx
		}
		s.request.mu.Unlock()
	}
	return s.BoundedStorage.Stat(ctx, name)
}

// A thread-directed signal reaches the thread blocked in the FUSE syscall. The
// request is held in a real HTTP exchange, so only the kernel's FUSE_INTERRUPT can
// cancel its context. A separate process keeps signal handling out of other tests.
// https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse.html#interrupting-filesystem-operations
func TestSignalInterruptsARequestWithoutBreakingTheMount(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runSignalChild(t, mode)
		return
	}
	requireFUSE(t)
	for _, mode := range []string{"raw", "go"} {
		t.Run(mode, func(t *testing.T) {
			backing := t.TempDir()
			if err := os.WriteFile(filepath.Join(backing, "file"), []byte("still readable"), 0o644); err != nil {
				t.Fatal(err)
			}
			config := localdir.Config{Root: backing, StateRoot: t.TempDir(), Locks: locking.DefaultOptions(), Limits: localdir.DefaultLimits()}
			if err := os.Chmod(config.StateRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := localdir.Init(t.Context(), config); err != nil {
				t.Fatal(err)
			}
			local, err := localdir.Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := local.Close(); err != nil {
					t.Error(err)
				}
			})
			handler, err := httprest.NewHandler(local, nil)
			if err != nil {
				t.Fatal(err)
			}
			request := &interruptedStat{name: "file", entered: make(chan context.Context, 1)}
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				op, err := httprest.ParseRequest(r.Method, r.URL)
				if err == nil && op.Op == httprest.OpStat && op.Path == request.name {
					request.mu.Lock()
					hold := !request.held
					request.held = true
					request.mu.Unlock()
					if hold {
						request.entered <- r.Context()
						select {
						case <-r.Context().Done():
							return
						case <-release:
						}
					}
				}
				handler.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			defer close(release)
			remote, err := httprest.Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			mountpoint := mountStorage(t, &observedStatStorage{remote, request}, fuse.Options{Logger: testLogger(t)})
			runInterruptedChild(t, mode, filepath.Join(mountpoint, request.name), request)
		})
	}
}

func runInterruptedChild(t *testing.T, mode, name string, request *interruptedStat) {
	t.Helper()
	runSignalProcess(t, "TestSignalInterruptsARequestWithoutBreakingTheMount", mode, name, func(ctx context.Context, pid, tid int) {
		var rpcContext context.Context
		select {
		case rpcContext = <-request.entered:
		case <-ctx.Done():
			t.Fatal("child did not reach the held stat RPC")
		}
		request.mu.Lock()
		fuseContext := request.ctx
		request.mu.Unlock()
		if fuseContext == nil || fuseContext.Err() != nil || rpcContext.Err() != nil {
			t.Fatal("the held RPC needs live FUSE and HTTP request contexts before the signal")
		}
		if err := unix.Tgkill(pid, tid, syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
		for what, canceled := range map[string]context.Context{"FUSE": fuseContext, "HTTP": rpcContext} {
			select {
			case <-canceled.Done():
				if !errors.Is(canceled.Err(), context.Canceled) {
					t.Fatalf("%s request ended with %v, want context.Canceled", what, canceled.Err())
				}
			case <-ctx.Done():
				t.Fatalf("the signal did not cancel the %s request", what)
			}
		}
	})
}

func runSignalProcess(t *testing.T, testName, mode, name string, interrupt func(context.Context, int, int)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	read, report, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer report.Close()
	var output bytes.Buffer
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+testName+"$", "-test.timeout=10s")
	child.Env = append(os.Environ(), signalChildMode+"="+mode, "REMOTE_FS_SIGNAL_PATH="+name)
	child.ExtraFiles = []*os.File{report}
	child.Stdout = &output
	child.Stderr = &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	report.Close()
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	waited := false
	defer func() {
		if waited {
			return
		}
		child.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("signal child did not exit after being killed")
		}
	}()
	thread := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(read)
		if scanner.Scan() {
			thread <- scanner.Text()
		} else {
			thread <- "missing child thread ID"
		}
	}()
	var tid int
	select {
	case line := <-thread:
		tid, err = strconv.Atoi(line)
		if err != nil {
			t.Fatalf("reading child thread ID: %q: %v", line, err)
		}
	case <-ctx.Done():
		t.Fatal("child did not report its thread ID")
	}
	interrupt(ctx, child.Process.Pid, tid)
	select {
	case err := <-done:
		waited = true
		if err != nil {
			t.Fatalf("signal child: %v\n%s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatal("child did not finish after interruption")
	}
}

func runSignalChild(t *testing.T, mode string) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	defer signal.Stop(signals)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	report := os.NewFile(3, "thread ID")
	defer report.Close()
	if _, err := fmt.Fprintln(report, unix.Gettid()); err != nil {
		t.Fatal(err)
	}
	name := os.Getenv("REMOTE_FS_SIGNAL_PATH")
	switch mode {
	case "raw":
		var attr unix.Stat_t
		if err := unix.Fstatat(unix.AT_FDCWD, name, &attr, 0); err != syscall.EINTR {
			t.Fatalf("interrupted raw stat returned %v, want EINTR", err)
		}
	case "go":
		// os.Stat retries EINTR itself; this checks the behavior ordinary Go callers see.
		// https://github.com/golang/go/blob/d90b98e65320778f3b1f99a6951ab20f04d218b3/src/os/stat_unix.go#L28-L39
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("ordinary Go stat after its request was interrupted: %v", err)
		}
	default:
		t.Fatalf("unknown signal child mode %q", mode)
	}
	select {
	case <-signals:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not handle SIGUSR1")
	}
	body, err := os.ReadFile(name)
	if err != nil || string(body) != "still readable" {
		t.Fatalf("reading after interruption: %q, %v", body, err)
	}
}
