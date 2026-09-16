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

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type fileReplyFault struct {
	underlying http.Handler
	mu         sync.Mutex
	op         storage.Operation
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
		Op     storage.Operation
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

func (f *fileReplyFault) arm(op storage.Operation, corrupt bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op, f.corrupt, f.actions = op, corrupt, nil
}

func TestRetainedFailedConfirmationPreservesReferenceOwnership(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	mounted, _ := mount(t, s)
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 2
	session, status, err := mounted.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	epoch := status.ActionEpoch
	closeAction := retainedAction(t, epoch)
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), closeAction); err != nil {
			t.Error(err)
		}
	})
	target, closeParent := retainedTarget(t, mounted, session, epoch, "file")
	action := retainedAction(t, epoch)
	fault.arm(storage.OpFileCreateAndRetainAt, false)
	receipt, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, action)
	if !errors.Is(err, syscall.EIO) || receipt.Reference == 0 || receipt.Effects&storage.EffectRetained == 0 || receipt.Effects&storage.EffectCreated == 0 {
		t.Fatalf("missing barrier discarded confirmed reference ownership: %+v, %v", receipt, err)
	}
	fault.arm("", false)
	recorded, err := session.QueryAction(t.Context(), action)
	if err != nil || recorded.Reference != receipt.Reference || recorded.Effects != receipt.Effects {
		t.Fatalf("retained action cannot be reconciled: %+v, %v", recorded, err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(t.Context(), retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	closeParent()
	file = retainedOpen(t, mounted, session, epoch, "file", false)
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal("closing the retained receipt did not release its native slot:", err)
	}
}

func TestRetainedUnknownResponseDoesNotRecommitTheMutation(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	mounted, _ := mount(t, s)
	session, epoch := retainedSession(t, mounted)
	file := retainedOpen(t, mounted, session, epoch, "file", true)
	fault.arm(storage.OpFileWrite, true)
	action := retainedAction(t, epoch)
	receipt, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("committed")}, action)
	if !errors.Is(err, syscall.EIO) || receipt.State != storage.FileActionUnknown {
		t.Fatalf("unknown retained mutation returned a confirmed answer: %+v, %v", receipt, err)
	}
	fault.mu.Lock()
	actions := append([]string(nil), fault.actions...)
	fault.mu.Unlock()
	if len(actions) != 2 || actions[0] != string(action) || actions[1] != string(action) {
		t.Fatalf("unknown mutation did not retain one action identity during reconciliation: %v", actions)
	}
	fault.arm("", false)
	recorded, err := session.QueryAction(t.Context(), action)
	if err != nil || recorded.State != storage.FileActionCompleted || recorded.Effects&storage.EffectContentChanged == 0 || recorded.Observation.Attr.Size != 9 {
		t.Fatalf("committed action did not preserve its observed effects: %+v, %v", recorded, err)
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
	first, firstEpoch := retainedSession(t, mounted)
	before := s.calls.of(httprest.OpFile)
	if _, _, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("session ownership exceeded its local bound:", err)
	}
	if s.calls.of(httprest.OpFile) != before {
		t.Fatal("a rejected session reached the authority")
	}
	if _, err := first.Close(t.Context(), retainedAction(t, firstEpoch)); err != nil {
		t.Fatal(err)
	}
	second, secondEpoch := retainedSession(t, mounted)
	file := retainedOpen(t, mounted, second, secondEpoch, "retained", true)
	closeAction := retainedAction(t, secondEpoch)
	if err := mounted.Close(); err != nil {
		t.Fatal(err)
	}
	if receipt, err := file.Close(t.Context(), closeAction); err != nil || receipt.State != storage.FileActionRetired {
		t.Fatalf("storage close did not confirm the terminal reference: %+v, %v", receipt, err)
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
	if session, _, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); session != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
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
	var call struct{ Op storage.Operation }
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		_ = json.Unmarshal(body, &call)
	}
	if call.Op == storage.OpFileSessionClose {
		p.closes.Add(1)
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil || call.Op != storage.OpFileSessionOpen {
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
		session, status, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		if session != nil {
			action, actionErr := storage.NewFileActionID(status.ActionEpoch)
			var closeErr error
			if actionErr == nil {
				_, closeErr = session.Close(context.Background(), action)
			}
			err = errors.Join(err, errors.New("closing storage returned a new session"), actionErr, closeErr)
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
	shared, status, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal("replica close consumed the shared remote client:", err)
	}
	if _, err := shared.Close(t.Context(), retainedAction(t, status.ActionEpoch)); err != nil {
		t.Fatal(err)
	}
}
