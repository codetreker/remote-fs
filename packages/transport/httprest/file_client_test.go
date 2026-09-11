package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestRetainedHTTPCancelLockReconcilesPendingAttempt(t *testing.T) {
	ctx := t.Context()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	client, _ := filePair(t, backend)
	if err := client.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	firstSession := fileSession(t, client)
	secondSession := fileSession(t, client)
	first := openHTTPFile(t, firstSession, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	second := openHTTPFile(t, secondSession, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	newID := func(session storage.FileSession) storage.LockRequestID {
		t.Helper()
		status, err := session.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id, err := storage.NewLockRequestID(status.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	held, err := first.SetLock(ctx, 1, lock, newID(firstSession))
	if err != nil || held.State != storage.LockGranted {
		t.Fatalf("held = %+v, %v", held, err)
	}
	lock.Wait = true
	id := newID(secondSession)
	pending, err := second.SetLock(ctx, 2, lock, id)
	if err != nil || pending.State != storage.LockPending {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	for i := 0; i < 2; i++ {
		cancelled, err := second.CancelLock(ctx, 2, id)
		if err != nil || cancelled.Request != id || cancelled.State != storage.LockCancelled {
			t.Fatalf("cancel %d = %+v, %v", i, cancelled, err)
		}
	}
	if err := first.DropLocks(ctx, 1, storage.Flock); err != nil {
		t.Fatal(err)
	}
	observed, err := second.QueryLock(ctx, 2, id)
	if err != nil || observed.State != storage.LockCancelled {
		t.Fatalf("cancelled request was granted after release: %+v, %v", observed, err)
	}
	conflict, err := first.GetLock(ctx, 1, lock)
	if err != nil || conflict.Found {
		t.Fatalf("cancel left a grant: %+v, %v", conflict, err)
	}
	if _, err := second.CancelLock(ctx, 2, "invalid"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid cancellation = %v", err)
	}
}

func TestRetainedHTTPPlainOpenACKCancellationClosesExactReference(t *testing.T) {
	for _, test := range []struct {
		name        string
		node, write bool
	}{
		{"path read", false, false}, {"path read write", false, true}, {"node read write", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, client, wire := openACKFixture(t)
			if err := backend.Storage.Write(t.Context(), "file", []byte("original bytes")); err != nil {
				t.Fatal(err)
			}
			before, err := backend.Storage.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			session := fileSession(t, client)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wire.afterOpen = func() error { cancel(); return nil }
			options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: test.write}}
			var file storage.File
			if test.node {
				file, err = session.OpenNode(ctx, before.ID, options)
			} else {
				file, err = session.OpenFile(ctx, "file", options)
			}
			if file != nil {
				file.Close(context.Background())
			}
			if file != nil || storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) {
				t.Fatalf("plain open ACK cancellation returned file=%v err=%v", file != nil, err)
			}
			wire.requireExactCleanup(t, 0)
			if backend.opened.Load() != 1 || backend.closed.Load() != 1 {
				t.Fatalf("native open/close calls = %d/%d, want 1/1", backend.opened.Load(), backend.closed.Load())
			}
			after, err := backend.Storage.Stat(t.Context(), "file")
			if err != nil || before != after {
				t.Fatalf("plain cancelled open changed attributes: before=%+v after=%+v err=%v", before, after, err)
			}
			if content, err := backend.Storage.Read(t.Context(), "file"); err != nil || string(content) != "original bytes" {
				t.Fatalf("plain cancelled open changed content: %q, %v", content, err)
			}
			if err := backend.Storage.Remove(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			if used, err := backend.Storage.Usage(t.Context()); err != nil || used != 0 {
				t.Fatalf("confirmed exact Close left a retained native reference: usage=%d err=%v", used, err)
			}
		})
	}
}

func TestRetainedHTTPOpenACKFailuresRemainEIO(t *testing.T) {
	for _, name := range []string{"create", "truncate node", "reported deadline", "cleanup EIO", "cleanup ESTALE", "unknown ACK and failed reconciliation", "closed session"} {
		t.Run(name, func(t *testing.T) {
			backend, client, wire := openACKFixture(t)
			defer backend.closeError.Store(0)
			options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}}
			if name == "create" {
				options.Create, options.Exclusive, options.Mode = true, true, 0600
			} else if err := backend.Storage.Write(t.Context(), "file", []byte("original bytes")); err != nil {
				t.Fatal(err)
			}
			var id uint64
			if name == "truncate node" {
				options.Truncate = true
				attr, err := backend.Storage.Stat(t.Context(), "file")
				if err != nil {
					t.Fatal(err)
				}
				id = attr.ID
			}
			session := fileSession(t, client)
			ctx, cancel := context.WithCancel(t.Context())
			var deadline *reportedACKDeadline
			if name == "reported deadline" {
				cancel()
				deadline = &reportedACKDeadline{Context: context.WithoutCancel(t.Context()), done: make(chan struct{})}
				ctx, cancel = deadline, deadline.expire
			}
			defer cancel()
			wire.afterOpen = func() error {
				if name == "reported deadline" {
					deadline.expire()
					if ctx.Err() != context.DeadlineExceeded {
						t.Fatalf("reported deadline did not precede ACK: %v", ctx.Err())
					}
					return nil
				}
				if name == "closed session" {
					if err := session.Close(context.Background()); err != nil {
						return err
					}
				}
				cancel()
				return nil
			}
			switch name {
			case "cleanup EIO":
				backend.closeError.Store(int64(syscall.EIO))
			case "cleanup ESTALE":
				backend.closeError.Store(int64(syscall.ESTALE))
			case "unknown ACK and failed reconciliation":
				wire.afterOpen, wire.loseACK, wire.afterLostACK = nil, true, cancel
			}
			var file storage.File
			var err error
			if id != 0 {
				file, err = session.OpenNode(ctx, id, options)
			} else {
				file, err = session.OpenFile(ctx, "file", options)
			}
			if file != nil {
				file.Close(context.Background())
			}
			if file != nil || storage.ErrnoOf(err) != syscall.EIO {
				t.Fatalf("%s returned file=%v err=%v, want EIO", name, file != nil, err)
			}
			if name == "reported deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("ACK deadline identity lost: %v", err)
			}
			acks := 0
			if name == "unknown ACK and failed reconciliation" {
				acks = 2
			}
			wire.requireExactCleanup(t, acks)
			if name == "unknown ACK and failed reconciliation" && wire.sentACK.Load() != 1 {
				t.Fatalf("real ACK dispatches=%d, want 1 before failed reconciliation", wire.sentACK.Load())
			}
			content, err := backend.Storage.Read(t.Context(), "file")
			want := "original bytes"
			if options.Create || options.Truncate {
				want = ""
			}
			if err != nil || string(content) != want {
				t.Fatalf("open effect = %q, %v, want %q", content, err, want)
			}
			if err := backend.Storage.Write(t.Context(), "file", []byte("reference probe")); err != nil {
				t.Fatal(err)
			}
			if err := backend.Storage.Remove(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			used, err := backend.Storage.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if name == "cleanup EIO" || name == "cleanup ESTALE" {
				if used != int64(len("reference probe")) {
					t.Fatalf("failed native Close did not retain its reference: %d", used)
				}
				backend.closeError.Store(0)
				if err := session.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				used, err = backend.Storage.Usage(t.Context())
			}
			if err != nil || used != 0 {
				t.Fatalf("open failure left native reference after cleanup: usage=%d err=%v", used, err)
			}
		})
	}
}

type openACKBackend struct {
	*objectstore.Storage
	closeError atomic.Int64
	opened     atomic.Int64
	closed     atomic.Int64
}

func (b *openACKBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &openACKSession{FileSession: session, backend: b}, nil
}

type openACKSession struct {
	storage.FileSession
	backend *openACKBackend
}

func (s *openACKSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, path, options)
	return s.wrap(file, err)
}

func (s *openACKSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenNode(ctx, id, options)
	return s.wrap(file, err)
}

func (s *openACKSession) wrap(file storage.File, err error) (storage.File, error) {
	if err != nil {
		return nil, err
	}
	s.backend.opened.Add(1)
	return &openACKFile{File: file, backend: s.backend}, nil
}

type openACKFile struct {
	storage.File
	backend *openACKBackend
}

func (f *openACKFile) Close(ctx context.Context) error {
	f.backend.closed.Add(1)
	if errno := f.backend.closeError.Load(); errno != 0 {
		return syscall.Errno(errno)
	}
	return f.File.Close(ctx)
}

type openACKRequest struct {
	Op      storage.Operation `json:"op"`
	Session string            `json:"session"`
	File    string            `json:"file"`
}

type openACKTransport struct {
	base                      http.RoundTripper
	afterOpen                 func() error
	loseACK                   bool
	afterLostACK              func()
	sentACK                   atomic.Int64
	mu                        sync.Mutex
	requests                  []openACKRequest
	openedFile, openedSession string
	ackAttempts               int
}

func (w *openACKTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	var operation openACKRequest
	if err := json.Unmarshal(body, &operation); err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.requests = append(w.requests, operation)
	attempt := 0
	if operation.Op == storage.OpFileAck {
		w.ackAttempts++
		attempt = w.ackAttempts
	}
	w.mu.Unlock()
	if operation.Op == storage.OpFileAck && w.loseACK && attempt > 1 {
		return nil, io.ErrUnexpectedEOF
	}
	if operation.Op == storage.OpFileAck {
		w.sentACK.Add(1)
	}
	response, err := w.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if operation.Op == storage.OpFileAck && w.loseACK {
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		if w.afterLostACK != nil {
			w.afterLostACK()
		}
		return nil, io.ErrUnexpectedEOF
	}
	if operation.Op == storage.OpFileOpen || operation.Op == storage.OpFileOpenNode {
		encoded, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		var opened struct {
			File string `json:"file"`
		}
		if err := json.Unmarshal(encoded, &opened); err != nil {
			return nil, err
		}
		w.mu.Lock()
		w.openedFile, w.openedSession = opened.File, operation.Session
		w.mu.Unlock()
		response.Body = io.NopCloser(bytes.NewReader(encoded))
		if w.afterOpen != nil {
			if err := w.afterOpen(); err != nil {
				return nil, err
			}
		}
	}
	return response, nil
}

func (w *openACKTransport) requireExactCleanup(t *testing.T, wantACK int) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	opens, closes, acks := 0, 0, 0
	for _, req := range w.requests {
		switch req.Op {
		case storage.OpFileOpen, storage.OpFileOpenNode:
			opens++
		case storage.OpFileAck:
			acks++
		case storage.OpFileClose:
			closes++
			if req.File != w.openedFile || req.Session != w.openedSession {
				t.Fatalf("cleanup targeted another capability: %+v, opened=%s/%s", req, w.openedSession, w.openedFile)
			}
		}
	}
	if w.openedFile == "" || w.openedSession == "" || opens != 1 || closes != 1 || acks != wantACK {
		t.Fatalf("open/ACK/exact-Close requests=%d/%d/%d, want 1/%d/1 with a real opened capability", opens, acks, closes, wantACK)
	}
}

func openACKFixture(t *testing.T) (*openACKBackend, *httprest.Storage, *openACKTransport) {
	t.Helper()
	backend := &openACKBackend{Storage: volumeFixture(t)}
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	base := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(func() {
		backend.closeError.Store(0)
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Errorf("closing ACK fixture: %v", err)
		}
		base.CloseIdleConnections()
	})
	wire := &openACKTransport{base: base}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: wire, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return backend, client, wire
}

type reportedACKDeadline struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func (c *reportedACKDeadline) Done() <-chan struct{} { return c.done }

func (c *reportedACKDeadline) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *reportedACKDeadline) expire() { c.once.Do(func() { close(c.done) }) }
