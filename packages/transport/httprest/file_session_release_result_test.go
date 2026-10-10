package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

const testSessionReleaseResultOperation storage.Operation = "file.session-release-result"

type sessionReleaseResultBackend struct {
	*objectstore.Storage
	result    storage.ReferenceCloseResult
	closeErr  error
	inlineErr error
	closes    atomic.Int32
}

type sessionReleaseResultNativeSession struct {
	storage.FileSession
	backend *sessionReleaseResultBackend
}

func (b *sessionReleaseResultBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &sessionReleaseResultNativeSession{FileSession: session, backend: b}, nil
}
func (s *sessionReleaseResultNativeSession) CheckInlineCloseSettlement() error {
	if s.backend.inlineErr != nil {
		return s.backend.inlineErr
	}
	return s.FileSession.(storage.InlineCloseSettlement).CheckInlineCloseSettlement()
}
func (s *sessionReleaseResultNativeSession) CheckRecoverableReferenceClose() error {
	return s.FileSession.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose()
}
func (s *sessionReleaseResultNativeSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.backend.closes.Add(1)
	if _, err := s.FileSession.CloseWithResult(ctx); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	return s.backend.result, s.backend.closeErr
}
func (s *sessionReleaseResultNativeSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}

type sessionReleaseResultLog struct {
	metastore.Log
	calls atomic.Int32
	fail  atomic.Bool
}

func (l *sessionReleaseResultLog) Barrier(ctx context.Context, maximum int64) (metastore.LogBarrier, error) {
	l.calls.Add(1)
	if l.fail.Load() {
		return metastore.LogBarrier{}, syscall.EIO
	}
	return l.Log.Barrier(ctx, maximum)
}

type sessionReleaseResultFixture struct {
	client  *Storage
	handler *Handler
	server  *httptest.Server
	backend *sessionReleaseResultBackend
	log     *sessionReleaseResultLog
	session *remoteFileSession
}

func newSessionReleaseResultFixture(t *testing.T, result storage.ReferenceCloseResult, closeErr, inlineErr error, policy authz.Authorizer) *sessionReleaseResultFixture {
	t.Helper()
	meta, volume := memoryfixture.New(t, "session-release-result", 1<<20, locking.DefaultOptions())
	backend := &sessionReleaseResultBackend{Storage: volume, result: result, closeErr: closeErr, inlineErr: inlineErr}
	log := &sessionReleaseResultLog{Log: meta}
	options := DefaultHandlerOptions()
	if policy != nil {
		options.Authorizer, options.Volume = policy, "trusted-release-volume"
	}
	handler, err := NewHandlerWithOptions(backend, log, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		err := handler.Close(context.Background())
		if err != nil && storage.ErrnoOf(err) != storage.ErrnoOf(closeErr) && storage.ErrnoOf(err) != syscall.EIO {
			t.Errorf("handler cleanup: %v", err)
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
	return &sessionReleaseResultFixture{client: client, handler: handler, server: server, backend: backend, log: log, session: session.(*remoteFileSession)}
}

func (f *sessionReleaseResultFixture) retire(t *testing.T) *terminalFileClose {
	t.Helper()
	f.handler.files.mu.Lock()
	served := f.handler.files.sessions[f.session.id]
	f.handler.files.mu.Unlock()
	if served == nil {
		t.Fatal("session disappeared before retirement")
	}
	f.handler.files.closeRetiringSession(f.session.id, served, false)
	f.handler.files.mu.Lock()
	terminal := f.handler.files.terminalCloses[f.session.id]
	f.handler.files.mu.Unlock()
	return terminal
}

func (f *sessionReleaseResultFixture) query(t *testing.T) (fileResponse, error) {
	t.Helper()
	return f.client.fileCall(t.Context(), fileRequest{Op: testSessionReleaseResultOperation, Session: f.session.id})
}

func requireSessionReleaseFact(t *testing.T, response fileResponse, err, semantic error, pending bool) {
	t.Helper()
	if response.CloseResult == nil || !response.CloseResult.Released || !response.CloseResult.Determined || response.CloseResult.BarrierPending != pending || response.ActionReceipt != nil || response.Retry {
		t.Fatalf("release fact = %+v, %v", response, err)
	}
	if pending {
		if response.Barrier != nil || storage.ErrnoOf(err) != syscall.EIO {
			t.Fatalf("pending release fact = %+v, %v", response, err)
		}
	} else if response.Barrier == nil || !errors.Is(err, semantic) || semantic == nil && err != nil || semantic != nil && errors.Is(err, syscall.EIO) {
		t.Fatalf("settled release fact = %+v, %v; want semantic %v", response, err, semantic)
	}
	var operation *operationError
	if errors.As(err, &operation) && operation.recorded {
		t.Fatalf("read-only release fact marked as recorded action: %+v", operation)
	}
}

func TestHTTPSessionReleaseResultRecoversAutoCloseWithoutAParentCloseAction(t *testing.T) {
	for _, semantic := range []error{nil, syscall.ENOTEMPTY} {
		name := "success"
		if semantic != nil {
			name = "semantic error"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, semantic, nil, nil)
			terminal := fixture.retire(t)
			if terminal == nil {
				t.Fatal("auto close left no terminal release record")
			}
			terminal.mu.Lock()
			expires, actions := terminal.expires, len(terminal.actions)
			terminal.mu.Unlock()
			if actions != 0 || fixture.backend.closes.Load() != 1 || fixture.session.closeAction != "" {
				t.Fatalf("auto close = actions %d, closes %d, client action %q", actions, fixture.backend.closes.Load(), fixture.session.closeAction)
			}
			response, err := fixture.query(t)
			requireSessionReleaseFact(t, response, err, semantic, false)
			original := fixture.client.http.Transport
			var operations []storage.Operation
			fixture.client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := request.GetBody()
				if err != nil {
					return nil, err
				}
				var command struct {
					Op storage.Operation `json:"op"`
				}
				err = json.NewDecoder(body).Decode(&command)
				_ = body.Close()
				if err != nil {
					return nil, err
				}
				operations = append(operations, command.Op)
				return original.RoundTrip(request)
			})
			result, err := fixture.session.CloseWithResult(t.Context())
			fixture.client.http.Transport = original
			var unsettled *storage.CloseSettlementError
			if !result.Released || !result.Determined || !errors.Is(err, semantic) || semantic == nil && err != nil || errors.As(err, &unsettled) {
				t.Fatalf("implicit close recovery = %+v, %v", result, err)
			}
			if !reflect.DeepEqual(operations, []storage.Operation{storage.OpFileSessionClose, testSessionReleaseResultOperation}) {
				t.Fatalf("implicit close did not check original action before release fact: %v", operations)
			}
			terminal.mu.Lock()
			defer terminal.mu.Unlock()
			if fixture.backend.closes.Load() != 1 || !terminal.expires.Equal(expires) || len(terminal.actions) != actions {
				t.Fatalf("release query changed terminal ownership: closes=%d expires=%v actions=%d", fixture.backend.closes.Load(), terminal.expires, len(terminal.actions))
			}
		})
	}
}

func TestHTTPSessionReleaseResultReadsExplicitTerminalProofWithoutChangingItsReceipt(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ENOTEMPTY, nil, nil)
	result, err := fixture.session.CloseWithResult(t.Context())
	if !result.Released || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("explicit parent close = %+v, %v", result, err)
	}
	fixture.handler.files.mu.Lock()
	terminal := fixture.handler.files.terminalCloses[fixture.session.id]
	fixture.handler.files.mu.Unlock()
	if terminal == nil {
		t.Fatal("explicit close left no terminal proof")
	}
	terminal.mu.Lock()
	expires, count := terminal.expires, len(terminal.actions)
	original := terminal.actions[fixture.session.closeAction]
	terminal.mu.Unlock()
	if original == nil || count != 1 {
		t.Fatalf("explicit parent receipt missing: actions=%d", count)
	}
	original.retryMu.Lock()
	closeResponse, closeErr, actionExpiry := original.response, original.err, original.expires
	original.retryMu.Unlock()
	response, err := fixture.query(t)
	requireSessionReleaseFact(t, response, err, syscall.ENOTEMPTY, false)
	terminal.mu.Lock()
	if !terminal.expires.Equal(expires) || len(terminal.actions) != count || terminal.actions[fixture.session.closeAction] != original {
		t.Error("query changed explicit terminal receipt")
	}
	terminal.mu.Unlock()
	original.retryMu.Lock()
	if !reflect.DeepEqual(original.response, closeResponse) || original.err != closeErr || !original.expires.Equal(actionExpiry) {
		t.Error("query changed original action result or retention")
	}
	original.retryMu.Unlock()
	if fixture.backend.closes.Load() != 1 {
		t.Fatalf("query repeated native close %d times", fixture.backend.closes.Load())
	}
}

func TestHTTPSessionReleaseResultReadsCurrentBarrierWithoutRefreshingHistory(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ENOTEMPTY, nil, nil)
	terminal := fixture.retire(t)
	if terminal == nil {
		t.Fatal("auto close left no terminal record")
	}
	terminal.mu.Lock()
	expires, originalErr := terminal.expires, terminal.releaseErr
	actions := make(map[storage.LockRequestID]*servedFileAction, len(terminal.actions))
	for id, action := range terminal.actions {
		actions[id] = action
	}
	terminal.mu.Unlock()
	fixture.log.fail.Store(true)
	before := fixture.log.calls.Load()
	pending, err := fixture.query(t)
	requireSessionReleaseFact(t, pending, err, syscall.ENOTEMPTY, true)
	fixture.log.fail.Store(false)
	settled, err := fixture.query(t)
	requireSessionReleaseFact(t, settled, err, syscall.ENOTEMPTY, false)
	if err := fixture.backend.Create(t.Context(), "later"); err != nil {
		t.Fatal(err)
	}
	later, err := fixture.query(t)
	requireSessionReleaseFact(t, later, err, syscall.ENOTEMPTY, false)
	if later.Barrier.Incarnation != settled.Barrier.Incarnation || later.Barrier.Position <= settled.Barrier.Position {
		t.Fatalf("query reused an old barrier: previous=%+v current=%+v", settled.Barrier, later.Barrier)
	}
	terminal.mu.Lock()
	defer terminal.mu.Unlock()
	if fixture.log.calls.Load() != before+3 || fixture.backend.closes.Load() != 1 || !terminal.expires.Equal(expires) || !reflect.DeepEqual(terminal.actions, actions) || terminal.releaseErr != originalErr {
		t.Fatalf("read changed release history: barriers=%d native closes=%d expires=%v actions=%d", fixture.log.calls.Load(), fixture.backend.closes.Load(), terminal.expires, len(terminal.actions))
	}
}

func TestHTTPSessionReleaseResultExpiryDoesNotReplayNativeClose(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, nil, nil, nil)
	terminal := fixture.retire(t)
	if terminal == nil {
		t.Fatal("auto close left no terminal record")
	}
	terminal.mu.Lock()
	terminal.expires = time.Now().Add(-time.Second)
	terminal.mu.Unlock()
	before := fixture.log.calls.Load()
	response, err := fixture.query(t)
	if response.CloseResult != nil || !errors.Is(err, syscall.ESTALE) || fixture.backend.closes.Load() != 1 || fixture.log.calls.Load() != before {
		t.Fatalf("expired release = %+v, %v; closes=%d barriers=%d", response, err, fixture.backend.closes.Load(), fixture.log.calls.Load())
	}
}

func TestHTTPSessionReleaseResultRejectsAbsentInvalidAndUnsettledProofs(t *testing.T) {
	for _, test := range []struct {
		name           string
		result         storage.ReferenceCloseResult
		err, inlineErr error
		retire         bool
	}{
		{name: "live session", result: storage.ReferenceCloseResult{Released: true, Determined: true}},
		{name: "invalid settlement marker", result: storage.ReferenceCloseResult{Released: true, Determined: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementPending}, retire: true},
		{name: "no inline settlement", result: storage.ReferenceCloseResult{Released: true, Determined: true}, inlineErr: syscall.EOPNOTSUPP, retire: true},
		{name: "pending lower settlement", result: storage.ReferenceCloseResult{Released: true, Determined: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}, retire: true},
		{name: "unknown lower settlement", result: storage.ReferenceCloseResult{Released: true, Determined: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}, retire: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSessionReleaseResultFixture(t, test.result, test.err, test.inlineErr, nil)
			if test.retire {
				fixture.retire(t)
			}
			beforeCloses, beforeBarriers := fixture.backend.closes.Load(), fixture.log.calls.Load()
			response, err := fixture.query(t)
			if response.CloseResult != nil || err == nil || fixture.backend.closes.Load() != beforeCloses || fixture.log.calls.Load() != beforeBarriers {
				t.Fatalf("unproven release = %+v, %v; closes=%d barriers=%d", response, err, fixture.backend.closes.Load(), fixture.log.calls.Load())
			}
		})
	}
}

func TestHTTPSessionReleaseResultNormalizesConfirmedReleaseDetermination(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true}, nil, nil, nil)
	if fixture.retire(t) == nil {
		t.Fatal("confirmed release left no terminal record")
	}
	response, err := fixture.query(t)
	requireSessionReleaseFact(t, response, err, nil, false)
}

func TestHTTPSessionReleaseResultUsesCurrentCloseAuthorizationBeforeBarrier(t *testing.T) {
	var deny atomic.Bool
	requests := make(chan authz.AccessRequest, 10)
	policy := authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		requests <- request
		if deny.Load() {
			return authz.ErrDenied
		}
		return nil
	})
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, nil, nil, policy)
	fixture.retire(t)
	for len(requests) > 0 {
		<-requests
	}
	deny.Store(true)
	before := fixture.log.calls.Load()
	response, err := fixture.query(t)
	if response.CloseResult != nil || !errors.Is(err, syscall.EACCES) || fixture.log.calls.Load() != before || fixture.backend.closes.Load() != 1 {
		t.Fatalf("denied release read = %+v, %v; closes=%d barriers=%d", response, err, fixture.backend.closes.Load(), fixture.log.calls.Load())
	}
	request := <-requests
	if request.Operation != storage.OpFileSessionClose || request.Volume != "trusted-release-volume" || request.Open != (storage.OpenAccess{}) {
		t.Fatalf("release authorization = %+v", request)
	}
}

func TestHTTPSessionReleaseResultStrictWireRefusesActionGenerationAndAmbiguousJSON(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, nil, nil, nil)
	fixture.retire(t)
	valid, err := json.Marshal(fileRequest{Op: testSessionReleaseResultOperation, Session: fixture.session.id, Path: []byte{}, Data: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	if !fileReadOnly(testSessionReleaseResultOperation) || !fileControl(testSessionReleaseResultOperation) || fileMutation(testSessionReleaseResultOperation) || fileActionRequired(testSessionReleaseResultOperation) {
		t.Fatal("session release result has incorrect transport classification")
	}
	action, err := storage.NewLockRequestID(fixture.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	for name, changed := range map[string][]byte{
		"missing session":   bytes.Replace(valid, []byte(`"session":"`+fixture.session.id+`",`), nil, 1),
		"empty session":     bytes.Replace(valid, []byte(`"session":"`+fixture.session.id+`"`), []byte(`"session":""`), 1),
		"null session":      bytes.Replace(valid, []byte(`"session":"`+fixture.session.id+`"`), []byte(`"session":null`), 1),
		"duplicate session": bytes.Replace(valid, []byte(`"session":"`+fixture.session.id+`"`), []byte(`"session":"`+fixture.session.id+`","session":"`+fixture.session.id+`"`), 1),
		"missing operation": bytes.Replace(valid, []byte(`"op":"file.session-release-result",`), nil, 1),
		"action":            bytes.Replace(valid, []byte(`"action":""`), []byte(`"action":"`+string(action)+`"`), 1),
		"generation":        bytes.Replace(valid, []byte(`"path":`), []byte(`"closeGeneration":1,"path":`), 1),
		"file reference":    bytes.Replace(valid, []byte(`"file":""`), []byte(`"file":"`+strings.Repeat("b", 64)+`"`), 1),
		"unknown member":    append(append([]byte{}, valid[:len(valid)-1]...), []byte(`,"extra":true}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			before := fixture.log.calls.Load()
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+Prefix+string(OpFileControl), bytes.NewReader(changed))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", contentJSON)
			response, err := fixture.server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode == http.StatusOK || bytes.Contains(body, []byte(`"closeResult"`)) || fixture.log.calls.Load() != before || fixture.backend.closes.Load() != 1 {
				t.Fatalf("invalid release query = status %d body %s; barriers=%d closes=%d", response.StatusCode, body, fixture.log.calls.Load(), fixture.backend.closes.Load())
			}
		})
	}
}

func TestHTTPSessionReleaseResultDoesNotResolveAnExplicitChildClose(t *testing.T) {
	fixture := newSessionReleaseResultFixture(t, storage.ReferenceCloseResult{Released: true, Determined: true}, nil, nil, nil)
	if err := fixture.backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	file, err := fixture.session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	closer := file.(storage.ReferenceCloseActions)
	owner, err := closer.CloseOwnerStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(owner.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: action, Generation: owner.NextGeneration}
	terminal := fixture.retire(t)
	if terminal == nil {
		t.Fatal("auto close left no release proof")
	}
	result, err := closer.CloseWithAction(t.Context(), attempt)
	if result.Released || !errors.Is(err, syscall.ESTALE) || fixture.backend.closes.Load() != 1 {
		t.Fatalf("parent release resolved an unmatched explicit child attempt: %+v, %v", result, err)
	}
}
