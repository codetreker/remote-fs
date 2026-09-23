package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type failedFirstCloseBackend struct {
	*objectstore.Storage
	failed atomic.Bool
}

type failedFirstCloseSession struct {
	storage.FileSession
	backend *failedFirstCloseBackend
}

type failedFirstCloseFile struct {
	storage.File
	backend *failedFirstCloseBackend
}

func (b *failedFirstCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &failedFirstCloseSession{FileSession: session, backend: b}, nil
}

func (s *failedFirstCloseSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &failedFirstCloseFile{File: file, backend: s.backend}, nil
}

func (f *failedFirstCloseFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	if f.backend.failed.CompareAndSwap(false, true) {
		return storage.ReferenceCloseResult{Released: false}, syscall.EIO
	}
	return f.File.CloseWithResult(ctx)
}

func (f *failedFirstCloseFile) Close(ctx context.Context) error {
	_, err := f.CloseWithResult(ctx)
	return err
}

func TestHTTPCloseRetainedErrorUsesFreshActionAndLostResponseReplaysSameAction(t *testing.T) {
	for _, test := range []struct {
		name      string
		failFirst bool
		loseFirst bool
	}{
		{name: "definite non-release", failFirst: true},
		{name: "unknown response", loseFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, native := memoryfixture.New(t, "close-recovery", 1<<20, locking.DefaultOptions())
			backend := &failedFirstCloseBackend{Storage: native}
			if !test.failFirst {
				backend.failed.Store(true)
			}
			if err := backend.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			handler, err := NewHandler(backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			original := client.http.Transport
			var mu sync.Mutex
			var actions []storage.LockRequestID
			var lost atomic.Bool
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				isClose := false
				if request.GetBody != nil {
					body, err := request.GetBody()
					if err != nil {
						return nil, err
					}
					var command struct {
						Op     storage.Operation     `json:"op"`
						Action storage.LockRequestID `json:"action"`
					}
					err = json.NewDecoder(body).Decode(&command)
					_ = body.Close()
					if err != nil {
						return nil, err
					}
					if command.Op == storage.OpFileClose {
						isClose = true
						mu.Lock()
						actions = append(actions, command.Action)
						mu.Unlock()
					}
				}
				response, err := original.RoundTrip(request)
				if test.loseFirst && isClose && err == nil && lost.CompareAndSwap(false, true) {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					return nil, errors.New("lost close response")
				}
				return response, err
			})
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			if test.failFirst {
				result, err := file.CloseWithResult(t.Context())
				if !errors.Is(err, syscall.EIO) || result.Released {
					t.Fatalf("first close=%+v err=%v", result, err)
				}
			}
			result, err := file.CloseWithResult(t.Context())
			if err != nil || !result.Released {
				t.Fatalf("final close=%+v err=%v", result, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(actions) < 2 || actions[0] == "" || actions[1] == "" || (actions[0] == actions[1]) != test.loseFirst {
				t.Fatalf("close action sequence=%v", actions)
			}
		})
	}
}

func TestCloseWireRequiresExplicitReleaseResult(t *testing.T) {
	req := fileRequest{Op: storage.OpFileClose}
	valid := fileResponse{Epoch: 1, Data: []byte{}, CloseResult: &referenceCloseResult{Released: true}}
	if err := validateFileResponse(req, valid); err != nil {
		t.Fatal(err)
	}
	if err := validatePartialFileResponse(req, valid); err != nil {
		t.Fatal(err)
	}
	for _, result := range []*referenceCloseResult{nil, {Released: false}} {
		invalid := valid
		invalid.CloseResult = result
		if err := validateFileResponse(req, invalid); err == nil {
			t.Fatal("accepted close without release proof")
		}
	}
	if err := validatePartialFileResponse(req, fileResponse{Epoch: 1, Data: []byte{}, CloseResult: &referenceCloseResult{Released: false}}); err != nil {
		t.Fatalf("rejected explicit retained ownership on error: %v", err)
	}
	pending := fileResponse{Epoch: 1, Data: []byte{}, CloseResult: &referenceCloseResult{Released: true, BarrierPending: true}}
	if err := validatePartialFileResponse(req, pending); err != nil {
		t.Fatalf("rejected recoverable close barrier state: %v", err)
	}
	if err := validateFileResponse(req, pending); err == nil {
		t.Fatal("accepted a pending barrier as a successful close result")
	}
	pending.CloseResult.Released = false
	if err := validatePartialFileResponse(req, pending); err == nil {
		t.Fatal("accepted a pending barrier without a released reference")
	}
	if err := validatePartialFileResponse(req, fileResponse{Epoch: 1, Data: []byte{}}); err == nil {
		t.Fatal("accepted an error response without close ownership result")
	}
}

func TestHTTPCloseReplaysReleasedActionUntilBarrierKnown(t *testing.T) {
	for _, kind := range []string{"file", "session"} {
		t.Run(kind, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "close-barrier-retry", 1<<20, locking.DefaultOptions())
			if err := backend.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			log := &retryBarrierLog{Log: meta}
			handler, err := NewHandler(backend, log)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "session" {
				if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released {
					t.Fatalf("preparation close=%+v err=%v", result, err)
				}
			}
			before := log.calls.Load()
			log.failures = before + 1
			var first storage.ReferenceCloseResult
			var barrier *MutationBarrier
			if kind == "file" {
				first, barrier, err = file.(*remoteFile).CloseWithBarrier(t.Context())
			} else {
				first, err = session.CloseWithResult(t.Context())
			}
			var pending *CloseBarrierPendingError
			if !first.Released || barrier != nil || !errors.As(err, &pending) || log.calls.Load() != before+1 {
				t.Fatalf("initial close=%+v barrier=%+v err=%v barrierCalls=%d", first, barrier, err, log.calls.Load())
			}
			if pending.Error() != pending.Cause.Error() || !errors.Is(pending, syscall.EIO) {
				t.Fatalf("pending close lost its original error chain: %v", pending)
			}
			var settled storage.ReferenceCloseResult
			retryCtx := locking.WithScope(t.Context(), locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}})
			if kind == "file" {
				settled, barrier, err = file.(*remoteFile).CloseWithBarrier(retryCtx)
			} else {
				settled, barrier, err = session.(*remoteFileSession).CloseWithBarrier(retryCtx)
			}
			if !settled.Released || barrier == nil || err != nil || log.calls.Load() != before+2 {
				t.Fatalf("settled close=%+v barrier=%+v err=%v barrierCalls=%d", settled, barrier, err, log.calls.Load())
			}
		})
	}
}

func TestHTTPBarrierReplayDoesNotChangeAnEarlierCloseResponse(t *testing.T) {
	first := fileResponse{CloseResult: &referenceCloseResult{Released: true, BarrierPending: true}}
	handler := &Handler{}
	replayed, err := handler.finishFileMutation(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if first.CloseResult == nil || !first.CloseResult.BarrierPending || replayed.CloseResult == nil || replayed.CloseResult.BarrierPending || first.CloseResult == replayed.CloseResult {
		t.Fatalf("close response aliasing: first=%+v replay=%+v", first.CloseResult, replayed.CloseResult)
	}
}

func TestHTTPActiveCloseReceiptTracksConcurrentReplay(t *testing.T) {
	identifier, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	action := &servedFileAction{
		op: storage.OpFileClose, done: make(chan struct{}),
		response: fileResponse{Epoch: 1, CloseResult: &referenceCloseResult{Released: true}},
	}
	close(action.done)
	session := &servedFileSession{
		actions: map[storage.LockRequestID]*servedFileAction{identifier: action},
		started: time.Now(), expires: time.Now().Add(time.Minute),
		options: storage.FileSessionOptions{History: time.Minute},
	}
	handler := &Handler{files: &fileRegistry{sessions: map[string]*servedFileSession{"session": session}}}
	var work sync.WaitGroup
	work.Add(1)
	go func() {
		defer work.Done()
		for i := 0; i < 1000; i++ {
			action.retryMu.Lock()
			action.response = fileResponse{Epoch: 1, CloseResult: &referenceCloseResult{Released: true, BarrierPending: i%2 == 0}}
			action.retryMu.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		response, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: "session", FileAction: storage.FileActionID(identifier)}, [32]byte{})
		if err != nil || response.ActionReceipt == nil || response.ActionReceipt.Outcome != storage.FileActionCompleted {
			t.Fatalf("concurrent receipt=%+v err=%v", response.ActionReceipt, err)
		}
	}
	work.Wait()
}

func TestHTTPCloseActionEpochRolloverKeepsTheEffectiveActionID(t *testing.T) {
	for _, kind := range []string{"file", "session"} {
		t.Run(kind, func(t *testing.T) {
			client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
			if err := backend.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			remote := session.(*remoteFileSession)
			file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "session" {
				if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released {
					t.Fatalf("preparation close=%+v err=%v", result, err)
				}
			}
			handler.files.mu.Lock()
			served := handler.files.sessions[remote.id]
			handler.files.mu.Unlock()
			served.mu.Lock()
			served.started = time.Now().Add(-served.options.History)
			served.mu.Unlock()
			var result storage.ReferenceCloseResult
			var action storage.LockRequestID
			if kind == "file" {
				result, err = file.CloseWithResult(t.Context())
				action = file.(*remoteFile).closeAction
			} else {
				result, err = session.CloseWithResult(t.Context())
				action = remote.closeAction
			}
			epoch, epochErr := action.Epoch()
			if err != nil || !result.Released || epochErr != nil || epoch != 2 {
				t.Fatalf("close after epoch rollover=%+v err=%v action=%q epoch=%d epochErr=%v", result, err, action, epoch, epochErr)
			}
		})
	}
}

func TestDeleteIntentListWireRejectsInvalidOwnerCursorAndLimit(t *testing.T) {
	request := fileRequest{
		Op: storage.OpFileListDeleteIntents, Session: strings.Repeat("a", 64),
		Path: []byte{}, Data: []byte{}, DeleteOwner: "owner", DeleteLimit: 1,
	}
	for _, invalid := range []fileRequest{
		func() fileRequest { value := request; value.DeleteOwner = ""; return value }(),
		func() fileRequest { value := request; value.DeleteOwner = "bad\x00owner"; return value }(),
		func() fileRequest { value := request; value.DeleteAfter = math.MaxUint64; return value }(),
		func() fileRequest { value := request; value.DeleteLimit = 0; return value }(),
		func() fileRequest {
			value := request
			value.DeleteLimit = storage.MaxDeleteIntentPageEntries + 1
			return value
		}(),
	} {
		if err := validateFileRequest(invalid); err != nil {
			t.Fatalf("shape validation failed before semantic validation: %v", err)
		}
		if err := validateCapabilityArguments(invalid); err == nil {
			t.Fatalf("accepted invalid delete intent listing: %+v", invalid)
		}
	}
	if err := validateFileRequest(request); err != nil {
		t.Fatal(err)
	}
	if err := validateCapabilityArguments(request); err != nil {
		t.Fatal(err)
	}
	valid := fileResponse{Epoch: 1, Data: []byte{}, DeletePage: &deleteIntentPage{Intents: []deleteIntentStatus{}, Next: 0}}
	if err := validateFileResponse(request, valid); err != nil {
		t.Fatal(err)
	}
	valid.DeletePage.Next = 1
	if err := validateFileResponse(request, valid); err == nil {
		t.Fatal("accepted an empty page with an advanced cursor")
	}
}

func TestHTTPFileAndSessionCloseReplayAfterCapabilityRetirement(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	ctx := t.Context()
	if err := backend.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	remoteFile := file.(*remoteFile)
	result, err := file.CloseWithResult(ctx)
	if err != nil || !result.Released {
		t.Fatalf("file close=%+v err=%v", result, err)
	}
	replayed, err := client.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: remote.id, File: remoteFile.id, Action: remoteFile.closeAction})
	if err != nil || replayed.CloseResult == nil || !replayed.CloseResult.Released {
		t.Fatalf("file replay=%+v err=%v", replayed.CloseResult, err)
	}
	fileAction := storage.FileActionID(remoteFile.closeAction)
	fileReceipt, err := session.(storage.FileActions).QueryFileAction(ctx, fileAction)
	if err != nil || fileReceipt.Operation != storage.OpFileClose || fileReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("file receipt=%+v err=%v", fileReceipt, err)
	}
	result, err = session.CloseWithResult(ctx)
	if err != nil || !result.Released {
		t.Fatalf("session close=%+v err=%v", result, err)
	}
	replayed, err = client.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: remote.closeAction})
	if err != nil || replayed.CloseResult == nil || !replayed.CloseResult.Released {
		t.Fatalf("session replay=%+v err=%v", replayed.CloseResult, err)
	}
	query, err := client.fileCall(ctx, fileRequest{Op: storage.OpFileQueryAction, Session: remote.id, FileAction: storage.FileActionID(remote.closeAction)})
	if err != nil || query.ActionReceipt == nil || query.ActionReceipt.Operation != storage.OpFileSessionClose || query.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("session close receipt=%+v err=%v", query.ActionReceipt, err)
	}
	handler.files.mu.Lock()
	terminal := handler.files.terminalCloses[remote.id]
	terminal.mu.Lock()
	terminal.expires = time.Now().Add(-time.Second)
	terminal.mu.Unlock()
	handler.files.mu.Unlock()
	_, err = client.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: remote.closeAction})
	if !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired receipt returned %v, want ESTALE", err)
	}
}

func TestHTTPRetiredCloseHistoryIsBoundedWithoutBlockingLiveSessionSlot(t *testing.T) {
	limits := DefaultFileLimits()
	limits.MaxSessions = 1
	client, handler, _ := retainedHTTPFixture(t, limits)
	closeSession := func() {
		t.Helper()
		session, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
		if err != nil {
			t.Fatal(err)
		}
		result, err := session.CloseWithResult(context.Background())
		if err != nil || !result.Released {
			t.Fatalf("close=%+v err=%v", result, err)
		}
	}
	closeSession()
	closeSession()
	if _, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("unbounded close history admission returned %v", err)
	}
	handler.files.mu.Lock()
	for id, terminal := range handler.files.terminalCloses {
		terminal.mu.Lock()
		terminal.expires = time.Now().Add(-time.Second)
		terminal.mu.Unlock()
		delete(handler.files.terminalCloses, id)
	}
	handler.files.mu.Unlock()
	session, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatalf("admission after expiry: %v", err)
	}
	if _, err := session.CloseWithResult(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPExplicitSessionCloseKeepsItsReceiptDuringBackgroundRetirement(t *testing.T) {
	_, native := memoryfixture.New(t, "close-retirement-race", 1<<20, locking.DefaultOptions())
	backend := &closeOrderBackend{Storage: native, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseNative := func() { releaseOnce.Do(func() { close(backend.release) }) }
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer func() {
		releaseNative()
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	resultCh := make(chan error, 1)
	go func() {
		result, err := session.CloseWithResult(context.Background())
		if err == nil && !result.Released {
			err = errors.New("session close returned no release proof")
		}
		resultCh <- err
	}()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit close did not reach native session")
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	if served == nil {
		t.Fatal("in-flight explicit close lost its session")
	}
	secondAction, err := storage.NewLockRequestID(remote.epoch)
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: secondAction}
	if _, err := client.fileCall(t.Context(), secondRequest); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("second close was admitted during the first: %v", err)
	}
	served.mu.Lock()
	_, reserved := served.actions[secondAction]
	served.mu.Unlock()
	if reserved {
		t.Fatal("unadmitted second close reserved action history")
	}
	handler.files.closeRetiringSession(remote.id, served, false)
	if calls := backend.calls.Load(); calls != 1 {
		t.Fatalf("background retirement entered native close %d times", calls)
	}
	handler.files.mu.Lock()
	stillActive := handler.files.sessions[remote.id] == served
	handler.files.mu.Unlock()
	if !stillActive {
		t.Fatal("background retirement removed an admitted close action")
	}
	releaseNative()
	if err := <-resultCh; err != nil {
		t.Fatal(err)
	}
	replay, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: remote.closeAction})
	if err != nil || replay.CloseResult == nil || !replay.CloseResult.Released {
		t.Fatalf("retired close replay=%+v err=%v", replay.CloseResult, err)
	}
	receipt, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: remote.id, FileAction: storage.FileActionID(remote.closeAction)})
	if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("retired close receipt=%+v err=%v", receipt.ActionReceipt, err)
	}
	secondResult, err := client.fileCall(t.Context(), secondRequest)
	if err != nil || secondResult.CloseResult == nil || !secondResult.CloseResult.Released {
		t.Fatalf("second close after retirement=%+v err=%v", secondResult.CloseResult, err)
	}
	secondReceipt, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: remote.id, FileAction: storage.FileActionID(secondAction)})
	if err != nil || secondReceipt.ActionReceipt == nil || secondReceipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("second close receipt=%+v err=%v", secondReceipt.ActionReceipt, err)
	}
}

type failedFirstSessionCloseBackend struct {
	*objectstore.Storage
	calls         atomic.Int32
	terminalError error
}

type failedFirstSessionCloseSession struct {
	storage.FileSession
	backend *failedFirstSessionCloseBackend
}

func (b *failedFirstSessionCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &failedFirstSessionCloseSession{FileSession: session, backend: b}, nil
}

func (s *failedFirstSessionCloseSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	if s.backend.calls.Add(1) == 1 {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	result, err := s.FileSession.CloseWithResult(ctx)
	if err != nil {
		return result, err
	}
	return result, s.backend.terminalError
}

func (s *failedFirstSessionCloseSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}

func TestHTTPSweeperReleaseAfterDefiniteCloseFailureRetainsRecovery(t *testing.T) {
	_, native := memoryfixture.New(t, "close-retirement-failure", 1<<20, locking.DefaultOptions())
	backend := &failedFirstSessionCloseBackend{Storage: native, terminalError: syscall.ENOTEMPTY}
	limits := DefaultFileLimits()
	limits.MaxCleanupActions = 1
	options := DefaultHandlerOptions()
	options.Files = limits
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	result, err := session.CloseWithResult(t.Context())
	if !errors.Is(err, syscall.EIO) || result.Released {
		t.Fatalf("first close=%+v err=%v", result, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	var failedAction storage.LockRequestID
	if served != nil {
		served.mu.Lock()
		for id, action := range served.actions {
			if action.op == storage.OpFileSessionClose {
				failedAction = id
			}
		}
		served.mu.Unlock()
		handler.files.closeRetiringSession(remote.id, served, false)
	}
	handler.files.mu.Lock()
	terminal := handler.files.terminalCloses[remote.id]
	handler.files.mu.Unlock()
	if terminal == nil || backend.calls.Load() != 2 {
		t.Fatalf("sweeper release was not retained: terminal=%v native calls=%d", terminal != nil, backend.calls.Load())
	}
	if failedAction == "" {
		terminal.mu.Lock()
		for id := range terminal.actions {
			failedAction = id
		}
		terminal.mu.Unlock()
	}
	failed, failedErr := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: failedAction})
	if failed.CloseResult == nil || failed.CloseResult.Released || !errors.Is(failedErr, syscall.EIO) {
		t.Fatalf("original failed action changed result=%+v err=%v", failed.CloseResult, failedErr)
	}
	result, err = session.CloseWithResult(t.Context())
	if !result.Released || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("fresh close after sweeper release=%+v err=%v", result, err)
	}
	if backend.calls.Load() != 2 {
		t.Fatalf("reconciliation reclosed native session: %d calls", backend.calls.Load())
	}
	replay, replayErr := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: remote.id, Action: remote.closeAction})
	if replay.CloseResult == nil || !replay.CloseResult.Released || !errors.Is(replayErr, syscall.ENOTEMPTY) {
		t.Fatalf("terminal action replay=%+v err=%v", replay.CloseResult, replayErr)
	}
	receipt, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: remote.id, FileAction: storage.FileActionID(remote.closeAction)})
	if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("terminal action receipt=%+v err=%v", receipt.ActionReceipt, err)
	}
}

func TestHTTPHandlerCloseRetriesRetainedOwnershipAndKeepsTerminalFailure(t *testing.T) {
	for _, test := range []struct {
		name          string
		terminalError error
	}{
		{name: "eventual success"},
		{name: "released with semantic error", terminalError: syscall.ENOTEMPTY},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, native := memoryfixture.New(t, "handler-close-retry", 1<<20, locking.DefaultOptions())
			backend := &failedFirstSessionCloseBackend{Storage: native, terminalError: test.terminalError}
			handler, err := NewHandler(backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := handler.files.enroll(t.Context(), storage.DefaultFileSessionOptions()); err != nil {
				t.Fatal(err)
			}
			if err := handler.Close(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("first close returned %v, want retained EIO", err)
			}
			handler.files.mu.Lock()
			retained := len(handler.files.sessions)
			handler.files.mu.Unlock()
			if retained != 1 {
				t.Fatalf("failed close lost %d retained sessions", retained)
			}
			if err := handler.Close(t.Context()); !errors.Is(err, test.terminalError) || (test.terminalError == nil && err != nil) {
				t.Fatalf("retry close returned %v, want %v", err, test.terminalError)
			}
			handler.files.mu.Lock()
			retained = len(handler.files.sessions)
			handler.files.mu.Unlock()
			if retained != 0 {
				t.Fatalf("successful retry retained %d live sessions", retained)
			}
			if err := handler.Close(t.Context()); !errors.Is(err, test.terminalError) || (test.terminalError == nil && err != nil) {
				t.Fatalf("repeated close returned %v, want %v", err, test.terminalError)
			}
			if calls := backend.calls.Load(); calls != 2 {
				t.Fatalf("native close calls=%d, want 2", calls)
			}
		})
	}
}
