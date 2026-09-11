package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

type authorizationHostKey struct{}

type authorizationBackend struct {
	locked.Backend
	calls []Op
	err   error
}

func (b *authorizationBackend) called(op Op) error { b.calls = append(b.calls, op); return b.err }
func (b *authorizationBackend) Stat(context.Context, string) (storage.Attr, error) {
	return storage.Attr{}, b.called(OpStat)
}
func (b *authorizationBackend) ListBounded(context.Context, string, *storage.ListResult) error {
	return b.called(OpList)
}
func (b *authorizationBackend) ReadBounded(context.Context, string, int64) ([]byte, error) {
	return nil, b.called(OpRead)
}
func (b *authorizationBackend) Space(context.Context) (storage.Space, error) {
	return storage.Space{}, b.called(OpSpace)
}
func (b *authorizationBackend) SetAttr(context.Context, string, storage.AttrChange) error {
	return b.called(OpSetAttr)
}
func (b *authorizationBackend) Write(context.Context, string, []byte) error { return b.called(OpWrite) }
func (b *authorizationBackend) Create(context.Context, string) error        { return b.called(OpCreate) }
func (b *authorizationBackend) Mkdir(context.Context, string) error         { return b.called(OpMkdir) }
func (b *authorizationBackend) Remove(context.Context, string) error        { return b.called(OpRemove) }
func (b *authorizationBackend) RemoveDir(context.Context, string) error     { return b.called(OpRemoveDir) }
func (b *authorizationBackend) Rename(context.Context, string, string) error {
	return b.called(OpRename)
}

type authorizationLog struct {
	metastore.Log
	barriers int
}

func (l *authorizationLog) Barrier(context.Context, int64) (metastore.LogBarrier, error) {
	l.barriers++
	return metastore.LogBarrier{Incarnation: "log", Position: 1}, nil
}

func authorizationHandler(t *testing.T, backend storage.Storage, log metastore.Log, policy authz.Authorizer) *Handler {
	t.Helper()
	options := DefaultHandlerOptions()
	if policy != nil {
		options.Authorizer = policy
		options.Volume = "trusted-volume"
	}
	h, err := NewHandlerWithOptions(backend, log, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}

func authorizationRequest(t *testing.T, ctx context.Context, op Op, body string) *http.Request {
	t.Helper()
	base, _ := url.Parse("http://host.invalid")
	u, err := (Request{Op: op, Path: "untrusted-path", To: "untrusted-destination"}).URL(base)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequestWithContext(ctx, (Request{Op: op}).Method(), u.String(), strings.NewReader(body))
	if content := (Request{Op: op}).ContentType(); content != "" {
		r.Header.Set("Content-Type", content)
	}
	return r
}

func authorizationAnswer(t *testing.T, response *httptest.ResponseRecorder, errno, message string) {
	t.Helper()
	if response.Code != StatusStorageError || response.Header().Get(HeaderProtocol) != Version || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("authorization response status/headers: %d %v", response.Code, response.Header())
	}
	var fields map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, map[string]any{"errno": errno, "message": message}) {
		t.Fatalf("unsafe authorization envelope: %s", response.Body.String())
	}
}

func TestVolumeAuthorizationMapsEveryOperationBeforeBackendAndBarrier(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t), err: syscall.EROFS}
	log := &authorizationLog{}
	operations := map[Op]storage.Operation{OpStat: storage.OpVolumeStat, OpList: storage.OpVolumeList, OpRead: storage.OpVolumeRead, OpSpace: storage.OpVolumeSpace, OpSetAttr: storage.OpVolumeSetAttr, OpWrite: storage.OpVolumeWrite, OpCreate: storage.OpVolumeCreate, OpMkdir: storage.OpVolumeMkdir, OpRemove: storage.OpVolumeRemove, OpRemoveDir: storage.OpVolumeRemoveDir, OpRename: storage.OpVolumeRename}
	for operation, semantic := range operations {
		t.Run(string(operation), func(t *testing.T) {
			var requests []authz.AccessRequest
			denied := true
			h := authorizationHandler(t, backend, log, authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				if ctx.Value(authorizationHostKey{}) != "host-identity" {
					t.Error("host context value was lost")
				}
				requests = append(requests, request)
				if denied {
					return authz.ErrDenied
				}
				return nil
			}))
			body := ""
			if operation == OpSetAttr {
				body = `{"change":{}}`
			}
			if operation == OpWrite {
				body = "contents"
			}
			ctx := context.WithValue(t.Context(), authorizationHostKey{}, "host-identity")
			backend.calls = nil
			log.barriers = 0
			response := httptest.NewRecorder()
			h.ServeHTTP(response, authorizationRequest(t, ctx, operation, body))
			authorizationAnswer(t, response, "EACCES", "access denied")
			if len(backend.calls) != 0 || log.barriers != 0 {
				t.Fatal("refusal touched backend or log barrier")
			}
			if len(requests) != 1 || requests[0] != (authz.AccessRequest{Volume: "trusted-volume", Operation: semantic}) {
				t.Fatalf("semantic request=%+v", requests)
			}
			denied = false
			response = httptest.NewRecorder()
			h.ServeHTTP(response, authorizationRequest(t, ctx, operation, body))
			if len(requests) != 2 || len(backend.calls) != 1 || backend.calls[0] != operation || response.Code != StatusStorageError {
				t.Fatalf("allowed operation did not preserve backend failure: requests=%d calls=%v response=%d", len(requests), backend.calls, response.Code)
			}
			var answer ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
				t.Fatal(err)
			}
			if answer.Errno != "EROFS" {
				t.Fatalf("policy allow replaced backend error: %+v", answer)
			}
		})
	}
}

func TestAllowedMutationReadsBarrierOnlyAfterAuthorization(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t)}
	log := &authorizationLog{}
	calls := 0
	h := authorizationHandler(t, backend, log, authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		calls++
		if len(backend.calls) != 0 || log.barriers != 0 {
			t.Fatal("mutation or barrier preceded authorization")
		}
		return nil
	}))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, authorizationRequest(t, t.Context(), OpWrite, "data"))
	if response.Code != http.StatusOK || calls != 1 || log.barriers != 1 || len(backend.calls) != 1 {
		t.Fatalf("allowed mutation=%d calls=%d barriers=%d backend=%v", response.Code, calls, log.barriers, backend.calls)
	}
}

type nilAuthorizationPolicy struct{}

func (*nilAuthorizationPolicy) Authorize(context.Context, authz.AccessRequest) error {
	panic("nil authorizer invoked")
}

func TestAuthorizationOptionsRequireAnExplicitUsablePair(t *testing.T) {
	backend := volumeFixture(t)
	var pointer *nilAuthorizationPolicy
	var function authz.AuthorizerFunc
	allow := authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	for name, change := range map[string]func(*HandlerOptions){
		"missing policy":           func(o *HandlerOptions) { o.Volume = "volume" },
		"missing volume":           func(o *HandlerOptions) { o.Authorizer = allow },
		"typed nil pointer":        func(o *HandlerOptions) { o.Volume = "volume"; o.Authorizer = pointer },
		"nil function":             func(o *HandlerOptions) { o.Volume = "volume"; o.Authorizer = function },
		"typed nil without volume": func(o *HandlerOptions) { o.Authorizer = pointer },
	} {
		t.Run(name, func(t *testing.T) {
			options := DefaultHandlerOptions()
			change(&options)
			if err := options.Check(); err == nil {
				t.Fatal("invalid authorization options accepted")
			}
			if h, err := NewHandlerWithOptions(backend, nil, options); err == nil || h != nil {
				t.Fatalf("constructor accepted invalid pair: %v", err)
			}
		})
	}
	options := DefaultHandlerOptions()
	options.Volume = " "
	var observed authz.AccessRequest
	options.Authorizer = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error { observed = r; r.Open.Read = false; return nil })
	h, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	request := authz.AccessRequest{Volume: "request-selected-volume", Operation: storage.OpFileOpen, Open: storage.OpenAccess{Read: true, Create: true}}
	if err := h.authorize(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if observed.Volume != " " || observed.Open != request.Open || request.Volume != "request-selected-volume" {
		t.Fatalf("trusted opaque volume or intent copy changed: %+v / %+v", observed, request)
	}
}

func TestAuthorizationErrorsExposeOnlyTrustedFixedFields(t *testing.T) {
	secret := errors.New("private host policy details")
	native := &locking.Error{Code: locking.Conflict, Recorded: true, Message: "private native grant details"}
	for name, test := range map[string]struct {
		cause   error
		errno   syscall.Errno
		message string
	}{
		"wrapped denial":                      {fmt.Errorf("private context: %w", authz.ErrDenied), syscall.EACCES, "access denied"},
		"joined denial dominates":             {errors.Join(native, secret, authz.ErrDenied), syscall.EACCES, "access denied"},
		"policy fault":                        {secret, syscall.EIO, "authorization failed"},
		"native classification cannot escape": {native, syscall.EIO, "authorization failed"},
		"policy own cancellation":             {context.Canceled, syscall.EIO, "authorization failed"},
		"policy own deadline":                 {context.DeadlineExceeded, syscall.EIO, "authorization failed"},
	} {
		t.Run(name, func(t *testing.T) {
			h := &Handler{stopping: make(chan struct{}), volume: "trusted", authorizer: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return test.cause })}
			err := h.authorize(t.Context(), authz.AccessRequest{Operation: storage.OpVolumeRead})
			if !errors.Is(err, test.cause) || storage.ErrnoOf(err) != test.errno || err.Error() != test.message {
				t.Fatalf("local authorization error lost classification/cause: %v", err)
			}
			response := httptest.NewRecorder()
			response.Header().Set(HeaderProtocol, Version)
			response.Header().Set("Cache-Control", "no-store")
			h.maxBodyBytes = DefaultMaxBodyBytes
			h.writeOperationError(response, fmt.Errorf("adapter context: %w", err))
			authorizationAnswer(t, response, storage.ErrnoNameOf(test.errno), test.message)
			if _, ok := authorizationResponse(errors.Join(err, syscall.EIO)); ok {
				t.Fatal("independent cleanup failure inherited a nested authorization result")
			}
		})
	}
}

func TestMalformedVolumeRequestsDoNotInvokeAuthorization(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t), err: syscall.EIO}
	calls := 0
	h := authorizationHandler(t, backend, nil, authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { calls++; return nil }))
	requests := []*http.Request{
		httptest.NewRequest(http.MethodGet, Prefix+"unknown", nil),
		httptest.NewRequest(http.MethodPost, Prefix+"stat?path=f", nil),
		httptest.NewRequest(http.MethodGet, Prefix+"stat?path=f&volume=other", nil),
		authorizationRequest(t, t.Context(), OpSetAttr, "{"),
		authorizationRequest(t, t.Context(), OpSetAttr, "{}"),
		authorizationRequest(t, t.Context(), OpRead, "unexpected"),
	}
	oversized := authorizationRequest(t, t.Context(), OpWrite, "small")
	oversized.ContentLength = h.maxWriteBytes + 1
	requests = append(requests, oversized)
	for _, request := range requests {
		answer := httptest.NewRecorder()
		h.ServeHTTP(answer, request)
		if answer.Code == http.StatusOK {
			t.Fatalf("malformed request accepted: %s", request.URL)
		}
	}
	if calls != 0 || len(backend.calls) != 0 {
		t.Fatalf("malformed requests called policy=%d backend=%v", calls, backend.calls)
	}
}

func TestAuthorizationCallerCancellationPrecedesPolicyClassification(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprint(before), func(t *testing.T) {
			cause := errors.New("host cancellation cause")
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			calls := 0
			h := &Handler{stopping: make(chan struct{}), volume: "trusted", authorizer: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
				calls++
				cancel(cause)
				return errors.Join(authz.ErrDenied, errors.New("private policy details"))
			})}
			if before {
				cancel(cause)
			}
			err := h.authorize(ctx, authz.AccessRequest{Operation: storage.OpVolumeRead})
			if storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatalf("caller cancellation lost classification/cause: %v", err)
			}
			if _, ok := authorizationResponse(err); ok {
				t.Fatal("caller cancellation became a policy error")
			}
			if before && calls != 0 || !before && calls != 1 {
				t.Fatalf("callback count=%d before=%t", calls, before)
			}
		})
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	h := &Handler{stopping: make(chan struct{}), volume: "trusted", authorizer: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { t.Fatal("expired caller invoked policy"); return nil })}
	if err := h.authorize(ctx, authz.AccessRequest{Operation: storage.OpVolumeRead}); storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline classification: %v", err)
	}
}

func awaitAuthorization[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("authorization did not reach its synchronization point")
		var zero T
		return zero
	}
}

func TestHandlerStopCancelsAuthorizationWithoutWaitingForCallbackDrain(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t), err: syscall.EROFS}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	h := authorizationHandler(t, backend, nil, authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		entered <- ctx
		<-ctx.Done()
		<-release
		return authz.ErrDenied
	}))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, authorizationRequest(t, t.Context(), OpRead, ""))
		done <- response
	}()
	callback := awaitAuthorization(t, entered)
	stopped := make(chan struct{})
	go func() { h.Stop(); close(stopped) }()
	awaitAuthorization(t, stopped)
	awaitAuthorization(t, callback.Done())
	select {
	case <-done:
		t.Fatal("request returned before synchronous callback drained")
	default:
	}
	if len(backend.calls) != 0 {
		t.Fatal("stopping callback reached backend")
	}
	unblock()
	response := awaitAuthorization(t, done)
	if response.Code != StatusStorageError || !strings.Contains(response.Body.String(), `"errno":"EIO"`) || strings.Contains(response.Body.String(), "access denied") {
		t.Fatalf("Stop became policy denial: %s", response.Body.String())
	}
	if len(backend.calls) != 0 {
		t.Fatal("callback cancellation allowed backend dispatch")
	}
}

func TestAuthorizationContextIsReleasedWithoutChangingNoHookBehavior(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t), err: syscall.EROFS}
	ctx := context.WithValue(t.Context(), authorizationHostKey{}, "identity")
	plain := authorizationHandler(t, backend, nil, nil)
	unchanged, finish := plain.authorizationContext(ctx)
	finish()
	if unchanged != ctx || ctx.Err() != nil {
		t.Fatal("no-hook setup changed the caller context")
	}
	plain.Stop()
	answer := httptest.NewRecorder()
	plain.ServeHTTP(answer, authorizationRequest(t, ctx, OpRead, ""))
	if len(backend.calls) != 1 || !strings.Contains(answer.Body.String(), `"errno":"EROFS"`) {
		t.Fatalf("no-hook ordinary Stop behavior changed: %s", answer.Body.String())
	}
	var captured context.Context
	active := authorizationHandler(t, backend, nil, authz.AuthorizerFunc(func(got context.Context, _ authz.AccessRequest) error { captured = got; return nil }))
	answer = httptest.NewRecorder()
	active.ServeHTTP(answer, authorizationRequest(t, ctx, OpRead, ""))
	if captured == nil || captured.Value(authorizationHostKey{}) != "identity" {
		t.Fatal("request context lost host values")
	}
	awaitAuthorization(t, captured.Done())
	if ctx.Err() != nil {
		t.Fatal("request cleanup canceled host-owned context")
	}
}

func TestAuthorizationCallbacksAreConcurrentAndKeepSeparateHostValues(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	h := authorizationHandler(t, volumeFixture(t), nil, authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		calls.Add(1)
		entered <- ctx.Value(authorizationHostKey{}).(string)
		<-release
		return authz.ErrDenied
	}))
	done := make(chan struct{}, 2)
	for _, identity := range []string{"first", "second"} {
		go func() {
			ctx := context.WithValue(t.Context(), authorizationHostKey{}, identity)
			h.ServeHTTP(httptest.NewRecorder(), authorizationRequest(t, ctx, OpRead, ""))
			done <- struct{}{}
		}()
	}
	identities := map[string]bool{awaitAuthorization(t, entered): true, awaitAuthorization(t, entered): true}
	if calls.Load() != 2 || !identities["first"] || !identities["second"] {
		t.Fatalf("callbacks serialized or mixed identities: %v", identities)
	}
	unblock()
	awaitAuthorization(t, done)
	awaitAuthorization(t, done)
}

func TestAuthorizationUsesExistingBoundedResponseAdmission(t *testing.T) {
	backend := &authorizationBackend{Backend: volumeFixture(t), err: syscall.EROFS}
	var calls atomic.Int32
	h := authorizationHandler(t, backend, nil, authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { calls.Add(1); return authz.ErrDenied }))
	h.responses = newBodyAdmission(1, retainedResponseMultiplier*h.maxBodyBytes, 1)
	release, err := h.responses.acquire(t.Context(), h.maxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { h.ServeHTTP(httptest.NewRecorder(), authorizationRequest(t, ctx, OpRead, "")); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.responses.mu.Lock()
		waiting := h.responses.waiters
		h.responses.mu.Unlock()
		if waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request did not wait for response admission")
		}
		runtime.Gosched()
	}
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, authorizationRequest(t, t.Context(), OpRead, ""))
	if answer.Code != StatusStorageError || !strings.Contains(answer.Body.String(), `"errno":"EAGAIN"`) || calls.Load() != 0 || len(backend.calls) != 0 {
		t.Fatalf("capacity refusal reached policy/backend: policy=%d backend=%v response=%s", calls.Load(), backend.calls, answer.Body.String())
	}
	cancel()
	awaitAuthorization(t, done)
	release()
	answer = httptest.NewRecorder()
	h.ServeHTTP(answer, authorizationRequest(t, t.Context(), OpRead, ""))
	authorizationAnswer(t, answer, "EACCES", "access denied")
	if calls.Load() != 1 {
		t.Fatalf("released admission was not reusable: calls=%d", calls.Load())
	}
}
