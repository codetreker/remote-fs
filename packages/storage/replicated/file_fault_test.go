package replicated_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type fileReplyFault struct {
	underlying http.Handler
	mu         sync.Mutex
	op         authz.Operation
	corrupt    bool
	actions    []string
}

func (f *fileReplyFault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != httprest.Prefix+string(httprest.OpFile) {
		f.underlying.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var request struct {
		Op     authz.Operation
		Action string
	}
	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	intercept, corrupt := request.Op == f.op, f.corrupt
	if intercept {
		f.actions = append(f.actions, request.Action)
	}
	f.mu.Unlock()
	if !intercept {
		f.underlying.ServeHTTP(w, r)
		return
	}
	response := httptest.NewRecorder()
	f.underlying.ServeHTTP(response, r)
	for key, values := range response.Header() {
		w.Header()[key] = append([]string(nil), values...)
	}
	w.Header().Del("Content-Length")
	w.WriteHeader(response.Code)
	if corrupt {
		_, _ = w.Write([]byte("{"))
		return
	}
	var answer map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
		_, _ = w.Write(response.Body.Bytes())
		return
	}
	delete(answer, "barrier")
	_ = json.NewEncoder(w).Encode(answer)
}

func (f *fileReplyFault) arm(op authz.Operation, corrupt bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op, f.corrupt, f.actions = op, corrupt, nil
}

func TestRetainedOpenFailureReclaimsItsUnreturnedReference(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	mounted, _ := mount(t, s)
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, err := mounted.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	fault.arm(authz.FileOpen, false)
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}, Mode: 0600})
	if file != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("open accepted a missing barrier: %v, %v", file, err)
	}
	fault.arm("", false)
	file = retainedOpen(t, session, "file", false)
	if _, err := file.Stat(t.Context()); err != nil {
		t.Fatal("failed open retained the only native file slot:", err)
	}
}

func TestRetainedUnknownResponseDoesNotRecommitTheMutation(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	mounted, _ := mount(t, s)
	session := retainedSession(t, mounted)
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}, Mode: 0600})
	if err != nil {
		t.Fatal(err)
	}
	fault.arm(authz.FileWrite, true)
	if attr, err := file.WriteAt(t.Context(), 0, []byte("committed")); !errors.Is(err, syscall.EIO) || attr != (storage.Attr{}) {
		t.Fatalf("unknown retained mutation returned a confirmed answer: %+v, %v", attr, err)
	}
	fault.mu.Lock()
	actions := append([]string(nil), fault.actions...)
	fault.mu.Unlock()
	if len(actions) != 2 || actions[0] == "" || actions[0] != actions[1] {
		t.Fatalf("unknown mutation did not use exactly one action identity for transport reconciliation: %v", actions)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "committed" {
		t.Fatalf("unknown result hid the actual authoritative content: %q, %v", content, err)
	}
}

func TestRetainedSessionAdmissionAndStorageCleanup(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	options := replicated.DefaultOptions()
	options.MaxFileSessions = 1
	mounted, _ := mountWithOptions(t, s, options)
	first := retainedSession(t, mounted)
	before := s.calls.of(httprest.OpFile)
	if _, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("session ownership exceeded its local bound:", err)
	}
	if s.calls.of(httprest.OpFile) != before {
		t.Fatal("a rejected session reached the authority")
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := retainedSession(t, mounted)
	file := retainedOpen(t, second, "retained", true)
	if err := mounted.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal("storage close did not reclaim its file sessions:", err)
	}
	if _, err := second.Status(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatal("closed storage left a live session:", err)
	}
	if err := s.elsewhere.Write(t.Context(), "retained", []byte("authority still owns its lifetime")); err != nil {
		t.Fatal("closing the replica closed its authority:", err)
	}
}

type volumeWithoutFiles struct {
	storage.BoundedStorage
	locking.Service
}

func (s volumeWithoutFiles) LockService() locking.Service { return s.Service }

func TestRetainedSessionsRejectAnAuthorityWithoutFileCapabilities(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	plain := volumeWithoutFiles{BoundedStorage: s.storage, Service: s.storage.LockService()}
	handler, err := httprest.NewHandler(plain, s.meta)
	if err != nil {
		t.Fatal(err)
	}
	s.events.handler = handler
	mounted, _ := mount(t, s)
	if session, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); session != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing retained capability was emulated through names: %v, %v", session, err)
	}
	if err := mounted.Write(t.Context(), "ordinary", []byte("named operations remain supported")); err != nil {
		t.Fatal(err)
	}
}

type completedSessionResponse struct {
	once      sync.Once
	received  chan struct{}
	cancelled chan struct{}
	resume    chan struct{}
	closes    atomic.Int64
}

func (p *completedSessionResponse) RoundTrip(request *http.Request) (*http.Response, error) {
	var call struct{ Op authz.Operation }
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		_ = json.Unmarshal(body, &call)
	}
	if call.Op == authz.FileSessionClose {
		p.closes.Add(1)
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil || call.Op != authz.FileSessionOpen {
		return response, err
	}
	pause := false
	p.once.Do(func() { pause = true })
	if !pause {
		return response, nil
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	close(p.received)
	<-request.Context().Done()
	close(p.cancelled)
	<-p.resume
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func TestRetainedClosingStorageReclaimsAnAlreadyCreatedSession(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	paused := &completedSessionResponse{received: make(chan struct{}), cancelled: make(chan struct{}), resume: make(chan struct{})}
	remote, err := httprest.Dial(s.url, &http.Client{Transport: paused})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := replicated.New(t.Context(), local, remote)
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mounted.Close() })
	resume := sync.OnceFunc(func() { close(paused.resume) })
	t.Cleanup(resume)
	created := make(chan error, 1)
	go func() {
		session, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		if session != nil {
			err = errors.Join(err, errors.New("closing storage returned a new session"), session.Close(context.Background()))
		}
		created <- err
	}()
	<-paused.received
	closed := make(chan error, 1)
	go func() { closed <- mounted.Close() }()
	<-paused.cancelled
	select {
	case err := <-closed:
		t.Fatal("storage close returned before its session creation drained:", err)
	default:
	}
	resume()
	if err := <-created; !errors.Is(err, syscall.EIO) {
		t.Fatal("late session creation did not report its failed delivery:", err)
	}
	if err := <-closed; err != nil || paused.closes.Load() != 1 {
		t.Fatalf("late capability was not reclaimed: %v; closes %d", err, paused.closes.Load())
	}
	shared, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal("replica close consumed the shared remote client:", err)
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
