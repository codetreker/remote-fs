//go:build linux

package fuse_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// Holding the complete authoritative reply exposes cancellation between an open
// reference being returned and its ACK. SIGURG keeps Go's normal signal handler.
func TestSignalDuringPlainOpenPreservesRetryableCancellation(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runOpenSignalChild(t, mode)
		return
	}
	requireFUSE(t)
	for _, mode := range []string{"raw", "go"} {
		t.Run(mode, func(t *testing.T) {
			gate, observed, backing := openSignalVolume(t)
			defer gate.unblock()
			before, err := backing.Stat(t.Context(), "artifact")
			if err != nil {
				t.Fatal(err)
			}
			trace := &openInterruptTrace{interrupted: make(chan struct{})}
			point := mountStorage(t, observed, fuse.Options{Logger: log.New(trace, "", 0), Debug: true})
			t.Cleanup(func() {
				if t.Failed() {
					_, _, output := trace.snapshot()
					t.Logf("open interruption trace:\n%s", output)
				}
			})
			runSignalProcess(t, "TestSignalDuringPlainOpenPreservesRetryableCancellation", mode, filepath.Join(point, "artifact"), func(ctx context.Context, pid, tid int) {
				original := receiveOpenContext(t, ctx, observed.entered, "FUSE Open")
				exchange := receiveOpenContext(t, ctx, gate.held, "complete HTTP Open reply")
				unique, count, _ := trace.snapshot()
				if unique == 0 || count != 1 || original.Err() != nil || exchange.Err() != nil {
					t.Fatalf("open entry: unique=%d count=%d caller=%v HTTP=%v", unique, count, original.Err(), exchange.Err())
				}
				if gate.acks.Load() != 0 || gate.opens.Load() != 1 {
					t.Fatalf("open reply gate: opens=%d ACKs=%d", gate.opens.Load(), gate.acks.Load())
				}
				if err := unix.Tgkill(pid, tid, syscall.SIGURG); err != nil {
					t.Fatal(err)
				}
				select {
				case <-trace.interrupted:
				case <-ctx.Done():
					t.Fatalf("kernel did not interrupt OPEN %d: %v", unique, ctx.Err())
				}
				select {
				case <-original.Done():
				case <-ctx.Done():
					t.Fatal("OPEN interrupt did not cancel FUSE context")
				}
				select {
				case <-exchange.Done():
				case <-ctx.Done():
					t.Fatal("OPEN interrupt did not reach HTTP context")
				}
				gate.unblock()
			})
			first := receiveOpenResult(t, observed.results)
			if storage.ErrnoOf(first.err) != syscall.EINTR || !errors.Is(first.err, context.Canceled) || first.acks != 0 || first.closed != 1 {
				t.Fatalf("interrupted open: err=%v ACKs=%d confirmed closes=%d", first.err, first.acks, first.closed)
			}
			wantOpens, wantAcks := int32(1), int32(0)
			if mode == "go" {
				wantOpens, wantAcks = 2, 1
				if second := receiveOpenResult(t, observed.results); second.err != nil {
					t.Fatalf("Go's retried open: %v", second.err)
				}
			}
			for range wantOpens {
				select {
				case <-gate.closedEvents:
				case <-time.After(5 * time.Second):
					t.Fatal("file reference cleanup did not finish")
				}
			}
			_, kernelOpens, _ := trace.snapshot()
			if gate.opens.Load() != wantOpens || gate.acks.Load() != wantAcks || gate.closes.Load() != wantOpens || gate.closed.Load() != wantOpens || kernelOpens != int(wantOpens) {
				t.Fatalf("mode %s: kernel opens=%d HTTP opens=%d ACKs=%d closes=%d confirmed closes=%d", mode, kernelOpens, gate.opens.Load(), gate.acks.Load(), gate.closes.Load(), gate.closed.Load())
			}
			after, err := backing.Stat(t.Context(), "artifact")
			if err != nil {
				t.Fatal(err)
			}
			body, err := backing.Read(t.Context(), "artifact")
			if err != nil {
				t.Fatal(err)
			}
			if after != before || string(body) != "original" {
				t.Fatalf("plain open changed file: before=%+v after=%+v body=%q", before, after, body)
			}
			if err := backing.Remove(t.Context(), "artifact"); err != nil {
				t.Fatal(err)
			}
			space, err := backing.Space(t.Context())
			if err != nil || space.Used != 0 {
				t.Fatalf("closed references retained bytes: %+v, %v", space, err)
			}
		})
	}
}

func runOpenSignalChild(t *testing.T, mode string) {
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
		fd, err := unix.Open(name, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		if err != syscall.EINTR {
			t.Fatalf("single interrupted open: %v, want EINTR", err)
		}
	case "go":
		// os.OpenFile's EINTR handling must work without an application retry loop.
		file, err := os.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown open signal mode %q", mode)
	}
}

func receiveOpenContext(t *testing.T, ctx context.Context, source <-chan context.Context, phase string) context.Context {
	t.Helper()
	select {
	case got := <-source:
		return got
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", phase, ctx.Err())
		return nil
	}
}
func receiveOpenResult(t *testing.T, source <-chan openSignalResult) openSignalResult {
	t.Helper()
	select {
	case result := <-source:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("open result was not observed")
		return openSignalResult{}
	}
}

type openInterruptTrace struct {
	mu          sync.Mutex
	output      bytes.Buffer
	first       uint64
	count       int
	interrupted chan struct{}
	once        sync.Once
}

func (p *openInterruptTrace) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.output.Write(b)
	var unique, target uint64
	// go-fuse opens its epoll sentinel during mounting; only the RDWR file is under test.
	if n, _ := fmt.Sscanf(string(b), "rx %d: OPEN", &unique); n == 1 && bytes.Contains(b, []byte(": OPEN ")) && bytes.Contains(b, []byte("RDWR")) {
		if p.first == 0 {
			p.first = unique
		}
		p.count++
	}
	if n, _ := fmt.Sscanf(string(b), "rx %d: INTERRUPT n0 {ix %d}", &unique, &target); n == 2 && p.first != 0 && target == p.first {
		p.once.Do(func() { close(p.interrupted) })
	}
	return len(b), nil
}
func (p *openInterruptTrace) snapshot() (uint64, int, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.first, p.count, p.output.String()
}

type openSignalResult struct {
	err          error
	acks, closed int32
}
type interruptedOpenStorage struct {
	storage.FileStorage
	gate    *openReplyGate
	entered chan context.Context
	results chan openSignalResult
}

func (s *interruptedOpenStorage) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, o)
	if err != nil {
		return nil, err
	}
	return &interruptedOpenSession{FileSession: session, observe: s}, nil
}

type interruptedOpenSession struct {
	storage.FileSession
	observe *interruptedOpenStorage
}

func (s *interruptedOpenSession) OpenFile(ctx context.Context, path string, o storage.FileOpenOptions) (storage.File, error) {
	s.observe.entered <- ctx
	file, err := s.FileSession.OpenFile(ctx, path, o)
	s.observe.results <- openSignalResult{err: err, acks: s.observe.gate.acks.Load(), closed: s.observe.gate.closed.Load()}
	return file, err
}

type openReplyGate struct {
	next                        http.RoundTripper
	held                        chan context.Context
	release                     chan struct{}
	holdOnce, releaseOnce       sync.Once
	opens, acks, closes, closed atomic.Int32
	closedEvents                chan struct{}
}

func (g *openReplyGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }
func (g *openReplyGate) RoundTrip(r *http.Request) (*http.Response, error) {
	var request struct {
		Op storage.Operation `json:"op"`
	}
	if r.GetBody != nil {
		body, err := r.GetBody()
		if err != nil {
			return nil, err
		}
		err = json.NewDecoder(io.LimitReader(body, 4096)).Decode(&request)
		closeErr := body.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	switch request.Op {
	case storage.OpFileOpen:
		g.opens.Add(1)
	case storage.OpFileAck:
		g.acks.Add(1)
	case storage.OpFileClose:
		g.closes.Add(1)
	}
	response, err := g.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if request.Op == storage.OpFileClose && response.StatusCode == http.StatusOK && response.Header.Get(httprest.HeaderProtocol) == httprest.Version {
		g.closed.Add(1)
		g.closedEvents <- struct{}{}
	}
	if request.Op != storage.OpFileOpen {
		return response, nil
	}
	hold := false
	g.holdOnce.Do(func() { hold = true })
	if !hold {
		return response, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	closeErr := response.Body.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(body) > 4096 {
		return nil, errors.New("open reply exceeds the test fixture bound")
	}
	var shape struct {
		File    string                    `json:"file"`
		Barrier *httprest.MutationBarrier `json:"barrier"`
	}
	if err := json.Unmarshal(body, &shape); err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK || shape.File == "" || shape.Barrier == nil {
		return nil, errors.New("open reply did not establish a file reference and barrier")
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	g.held <- r.Context()
	<-g.release
	return response, nil
}

func openSignalVolume(t *testing.T) (*openReplyGate, *interruptedOpenStorage, *localstore.Store) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	locks := locking.DefaultOptions()
	backing, err := localstore.Open(t.Context(), localstore.Config{Root: root, Volume: "open-signal", Quota: 8 << 20, Window: sqlite.DefaultWindow(), Maintenance: objectstore.DefaultOptions(), Locks: &locks, InitializeLocks: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := backing.Write(t.Context(), "artifact", []byte("original")); err != nil {
		t.Fatal(err)
	}
	handler, err := httprest.NewHandler(backing, backing.Log())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	gate := &openReplyGate{next: server.Client().Transport, held: make(chan context.Context, 1), release: make(chan struct{}), closedEvents: make(chan struct{}, 4)}
	client := &http.Client{Transport: gate, Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	remote, err := httprest.Dial(server.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	replica, err := replicated.New(t.Context(), local, remote)
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := replica.Close(); err != nil {
			t.Error(err)
		}
	})
	observed := &interruptedOpenStorage{FileStorage: replica, gate: gate, entered: make(chan context.Context, 4), results: make(chan openSignalResult, 4)}
	return gate, observed, backing
}
