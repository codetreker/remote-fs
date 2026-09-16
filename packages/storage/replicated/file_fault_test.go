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
	"time"

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
	actions    map[storage.Operation][]string
}

func (f *fileReplyFault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != httprest.Prefix+string(httprest.OpFile) && r.URL.Path != httprest.Prefix+string(httprest.OpFileControl) {
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
	if f.actions == nil {
		f.actions = make(map[storage.Operation][]string)
	}
	f.actions[request.Op] = append(f.actions[request.Op], request.Action)
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

func (f *fileReplyFault) actionsFor(op storage.Operation) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.actions[op]...)
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
	actions := fault.actionsFor(storage.OpFileWrite)
	if len(actions) != 1 || actions[0] != string(action) {
		t.Fatalf("unknown mutation was redispatched: %v", actions)
	}
	if queries := fault.actionsFor(storage.OpFileQueryAction); len(queries) != 0 {
		t.Fatalf("unknown mutation was reconciled implicitly: %v", queries)
	}
	fault.arm("", false)
	recorded, err := session.QueryAction(t.Context(), action)
	if err != nil || recorded.State != storage.FileActionCompleted || recorded.Effects&storage.EffectContentChanged == 0 || recorded.Observation.Attr.Size != 9 {
		t.Fatalf("committed action did not preserve its observed effects: %+v, %v", recorded, err)
	}
	if queries := fault.actionsFor(storage.OpFileQueryAction); len(queries) != 1 || queries[0] != string(action) {
		t.Fatalf("explicit reconciliation changed action identity: %v", queries)
	}
	if writes := fault.actionsFor(storage.OpFileWrite); len(writes) != 0 {
		t.Fatalf("explicit reconciliation redispatched the mutation: %v", writes)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "committed" {
		t.Fatalf("unknown result hid the actual authoritative content: %q, %v", content, err)
	}
}

func TestRetainedSessionAdmissionAndStorageCleanup(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	options := replicated.DefaultOptions()
	options.MaxFileSessions = 1
	mounted, _ := mountWithOptions(t, s, options)
	first, firstEpoch := retainedSession(t, mounted)
	if opened := fault.actionsFor(storage.OpFileSessionOpen); len(opened) != 1 {
		t.Fatalf("first session enrollment count = %d", len(opened))
	}
	if _, _, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("session ownership exceeded its local bound:", err)
	}
	if opened := fault.actionsFor(storage.OpFileSessionOpen); len(opened) != 1 {
		t.Fatalf("a rejected session reached authority enrollment: %d opens", len(opened))
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

func TestRetainedScopedVolumeStateUsesAuthoritativeFacts(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	fault := &fileReplyFault{underlying: s.server.Config.Handler}
	s.server.Config.Handler = fault
	mounted, _ := mount(t, s)
	view, err := mounted.Scope(locking.MutationScope{})
	if err != nil {
		t.Fatal(err)
	}
	files, ok := view.(storage.FileStorage)
	if !ok {
		t.Fatal("scoped view lost retained file capability")
	}
	want, err := mounted.FileState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := files.FileState(t.Context())
	if err != nil || got != want {
		t.Fatalf("scoped volume state = %+v, %v; want %+v", got, err, want)
	}
	fault.arm(storage.OpFileState, true)
	if state, err := files.FileState(t.Context()); !errors.Is(err, syscall.EIO) || state != (storage.FileVolumeState{}) {
		t.Fatalf("failed authority returned invented volume facts: %+v, %v", state, err)
	}
	if calls := fault.actionsFor(storage.OpFileState); len(calls) != 1 {
		t.Fatalf("scoped observation did not use authority: %v", calls)
	}
	fault.arm("", false)
	if state, err := files.FileState(t.Context()); err != nil || state != want {
		t.Fatalf("restored authority = %+v, %v", state, err)
	}
}

func TestRetainedExpiredEnrollmentPreservesCleanupOwnership(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	underlying := s.server.Config.Handler
	reported := make(chan storage.FileSessionStatus, 1)
	var delay atomic.Bool
	delay.Store(true)
	delayed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != httprest.Prefix+string(httprest.OpFile) {
			underlying.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var request struct{ Op storage.Operation }
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if request.Op != storage.OpFileSessionOpen || !delay.CompareAndSwap(true, false) {
			underlying.ServeHTTP(w, r)
			return
		}
		response := httptest.NewRecorder()
		underlying.ServeHTTP(response, r)
		var answer struct{ Status storage.FileSessionStatus }
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil || answer.Status.Remaining <= 0 {
			http.Error(w, "enrollment did not return a live session", http.StatusBadGateway)
			return
		}
		reported <- answer.Status
		timer := time.NewTimer(answer.Status.Remaining + time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		for name, values := range response.Header() {
			w.Header()[name] = append([]string(nil), values...)
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	})
	fault := &fileReplyFault{underlying: delayed}
	s.server.Config.Handler = fault
	limits := replicated.DefaultOptions()
	limits.MaxFileSessions = 1
	mounted, _ := mountWithOptions(t, s, limits)
	options := storage.DefaultFileSessionOptions()
	options.Lease = 100 * time.Millisecond
	session, status, err := mounted.NewFileSession(t.Context(), options)
	if !errors.Is(err, syscall.EIO) || session == nil || status.ActionEpoch == 0 || status.Remaining != 0 {
		t.Fatalf("elapsed enrollment lost cleanup ownership: %v, %+v, %v", session, status, err)
	}
	original := <-reported
	if status.ActionEpoch != original.ActionEpoch || status.Epoch != original.Epoch || status.HistoryRemaining <= 0 {
		t.Fatalf("elapsed enrollment changed ownership status: %+v; original %+v", status, original)
	}
	if _, err := session.StatNode(t.Context(), 1, storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) || len(fault.actionsFor(storage.OpFileStatNode)) != 0 {
		t.Fatalf("partial enrollment admitted data access: %v", err)
	}
	if _, _, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("partial session was not counted in local ownership: %v", err)
	}
	action := retainedAction(t, status.ActionEpoch)
	first, err := session.Close(t.Context(), action)
	if err != nil || first.State != storage.FileActionCompleted && first.State != storage.FileActionRetired {
		t.Fatalf("partial enrollment cleanup = %+v, %v", first, err)
	}
	if _, err := session.Close(t.Context(), action); err != nil {
		t.Fatalf("cleanup replay = %v", err)
	}
	if calls := fault.actionsFor(storage.OpFileSessionClose); len(calls) != 1 || calls[0] != string(action) {
		t.Fatalf("cleanup did not preserve exactly one action: %v", calls)
	}
	next, nextStatus, err := mounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil || next == nil {
		t.Fatalf("cleanup did not release local enrollment capacity: %v", err)
	}
	if _, err := next.Close(t.Context(), retainedAction(t, nextStatus.ActionEpoch)); err != nil {
		t.Fatal(err)
	}
}
