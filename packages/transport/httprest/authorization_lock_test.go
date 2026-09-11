package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type lockAuthorizationIdentityKey struct{}

type lockAuthorizationPolicy struct {
	mu       sync.Mutex
	result   error
	requests []authz.AccessRequest
	identity []any
}

func (p *lockAuthorizationPolicy) Authorize(ctx context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
	p.identity = append(p.identity, ctx.Value(lockAuthorizationIdentityKey{}))
	return p.result
}

func (p *lockAuthorizationPolicy) decision(result error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.result = result
	p.requests = nil
	p.identity = nil
}

func (p *lockAuthorizationPolicy) check(t *testing.T, operation string, count int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.requests) != count {
		t.Fatalf("authorization calls = %d; want %d", len(p.requests), count)
	}
	for i, request := range p.requests {
		want := authz.AccessRequest{Volume: "trusted-policy-volume", Operation: storage.Operation(operation)}
		if request != want || p.identity[i] != "host-identity" {
			t.Fatalf("authorization request = %+v, identity = %v; want %+v and host identity", request, p.identity[i], want)
		}
	}
}

func authorizationLockFixture(t *testing.T) (*Handler, *lockAuthorizationProbe, *lockAuthorizationPolicy) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "backend-volume", 1<<20, locking.DefaultOptions())
	if err := backend.Write(t.Context(), "file", []byte("protected content")); err != nil {
		t.Fatal(err)
	}
	policy := &lockAuthorizationPolicy{result: authz.ErrDenied}
	options := DefaultHandlerOptions()
	options.Volume = "trusted-policy-volume"
	options.Authorizer = policy
	handler, err := NewHandlerWithOptions(backend, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	probe := &lockAuthorizationProbe{Service: handler.locks}
	handler.locks = probe
	return handler, probe, policy
}

func authorizationLockRequest(t *testing.T, handler *Handler, op Op, value any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return authorizationLockBody(handler, op, body)
}

func authorizationLockBody(handler *Handler, op Op, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, Prefix+string(op), bytes.NewReader(body))
	request.Header.Set("Content-Type", contentJSON)
	request = request.WithContext(context.WithValue(request.Context(), lockAuthorizationIdentityKey{}, "host-identity"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertLockAuthorizationError(t *testing.T, response *httptest.ResponseRecorder, errno, message string) {
	t.Helper()
	if response.Code != StatusStorageError || response.Header().Get(HeaderProtocol) != Version {
		t.Fatalf("authorization response status/protocol = %d/%q", response.Code, response.Header().Get(HeaderProtocol))
	}
	var fields map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"errno": errno, "message": message}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("authorization response = %s; want only %v", response.Body.String(), want)
	}
}

type lockAuthorizationProbe struct {
	locking.Service
	calls atomic.Int64
}

func (p *lockAuthorizationProbe) BeginEnrollment(ctx context.Context) (locking.EnrollmentTicket, error) {
	p.calls.Add(1)
	return p.Service.BeginEnrollment(ctx)
}

func (p *lockAuthorizationProbe) OpenSession(ctx context.Context, ticket locking.EnrollmentTicket) (locking.Session, error) {
	p.calls.Add(1)
	return p.Service.OpenSession(ctx, ticket)
}

func (p *lockAuthorizationProbe) CloseSession(ctx context.Context, session locking.SessionID) error {
	p.calls.Add(1)
	return p.Service.CloseSession(ctx, session)
}

func (p *lockAuthorizationProbe) CreateOwner(ctx context.Context, session locking.SessionID, request locking.RequestID) (locking.Owner, error) {
	p.calls.Add(1)
	return p.Service.CreateOwner(ctx, session, request)
}

func (p *lockAuthorizationProbe) RetireOwner(ctx context.Context, owner locking.OwnerRef) error {
	p.calls.Add(1)
	return p.Service.RetireOwner(ctx, owner)
}

func (p *lockAuthorizationProbe) Resolve(ctx context.Context, owner locking.OwnerRef, path string) (locking.ResourceRef, error) {
	p.calls.Add(1)
	return p.Service.Resolve(ctx, owner, path)
}

func (p *lockAuthorizationProbe) Acquire(ctx context.Context, request locking.AcquireRequest) (locking.ActionResult, error) {
	p.calls.Add(1)
	return p.Service.Acquire(ctx, request)
}

func (p *lockAuthorizationProbe) Renew(ctx context.Context, request locking.RenewRequest) (locking.ActionResult, error) {
	p.calls.Add(1)
	return p.Service.Renew(ctx, request)
}

func (p *lockAuthorizationProbe) Release(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.ReleaseResult, error) {
	p.calls.Add(1)
	return p.Service.Release(ctx, owner, grant)
}

func (p *lockAuthorizationProbe) Cancel(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.CancelResult, error) {
	p.calls.Add(1)
	return p.Service.Cancel(ctx, owner, request)
}

func (p *lockAuthorizationProbe) QueryAction(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.ActionResult, error) {
	p.calls.Add(1)
	return p.Service.QueryAction(ctx, owner, request)
}

func (p *lockAuthorizationProbe) QueryGrant(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.GrantStatus, error) {
	p.calls.Add(1)
	return p.Service.QueryGrant(ctx, owner, grant)
}

func (p *lockAuthorizationProbe) Status(ctx context.Context) (locking.Status, error) {
	p.calls.Add(1)
	return p.Service.(locking.StatusService).Status(ctx)
}

type authorizationLockCase struct {
	op        Op
	operation string
	body      any
}

func authorizationLockCases() []authorizationLockCase {
	owner := locking.OwnerRef{Session: "session-capability", Owner: "owner-capability"}
	resource := locking.ResourceRef{ID: "resource-capability", NowMillis: 100, ExpiresMillis: 10000, HistoryExpiresMillis: 10000}
	grant := locking.GrantRef{ID: "grant-capability", Resource: resource.ID, Generation: 1}
	return []authorizationLockCase{
		{OpSessionEnrollment, "lock.session-enrollment", struct{}{}},
		{OpSessionOpen, "lock.session-open", lockTicketMessage{Ticket: "ticket-capability"}},
		{OpSessionClose, "lock.session-close", lockSessionRequest{Session: owner.Session}},
		{OpOwnerCreate, "lock.owner-create", lockCreateOwnerRequest{Session: owner.Session, Request: "owner-action"}},
		{OpOwnerRetire, "lock.owner-retire", lockOwnerRequest{Owner: owner}},
		{OpLockResolve, "lock.resolve", lockResolveRequest{Owner: owner, Path: []byte("file")}},
		{OpLockAcquire, "lock.acquire", lockAcquireRequest{Owner: owner, Request: "acquire-action", Resource: resource, Mode: locking.Exclusive, TTLMillis: 1000}},
		{OpLockRenew, "lock.renew", lockRenewRequest{Owner: owner, Request: "renew-action", Grant: grant, TTLMillis: 1000}},
		{OpLockRelease, "lock.release", lockGrantRequest{Owner: owner, Grant: grant}},
		{OpLockCancel, "lock.cancel", lockActionRequest{Owner: owner, Request: "acquire-action"}},
		{OpLockQueryAction, "lock.query-action", lockActionRequest{Owner: owner, Request: "acquire-action"}},
		{OpLockQueryGrant, "lock.query-grant", lockGrantRequest{Owner: owner, Grant: grant}},
		{OpLockStatus, "lock.status", struct{}{}},
	}
}

func TestStrongLockAuthorizationMapsEveryControlBeforeNativeAccess(t *testing.T) {
	handler, probe, policy := authorizationLockFixture(t)
	for _, test := range authorizationLockCases() {
		t.Run(string(test.op), func(t *testing.T) {
			policy.decision(authz.ErrDenied)
			response := authorizationLockRequest(t, handler, test.op, test.body)
			assertLockAuthorizationError(t, response, "EACCES", "access denied")
			policy.check(t, test.operation, 1)
			if calls := probe.calls.Load(); calls != 0 {
				t.Fatalf("denied control reached native service %d times", calls)
			}
		})
	}
}

func TestStrongLockAuthorizationErrorsCannotClaimNativeOutcomes(t *testing.T) {
	handler, probe, policy := authorizationLockFixture(t)
	for _, test := range []struct {
		name    string
		cause   error
		errno   string
		message string
	}{
		{"recorded native error", &locking.Error{Code: locking.Conflict, Recorded: true, Message: "callback-secret"}, "EIO", "authorization failed"},
		{"wire parser error", errInvalidLockRequest, "EIO", "authorization failed"},
		{"joined denial", errors.Join(authz.ErrDenied, &locking.Error{Code: locking.Unavailable, Recorded: true, Message: "callback-secret"}), "EACCES", "access denied"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy.decision(test.cause)
			response := authorizationLockRequest(t, handler, OpLockStatus, struct{}{})
			assertLockAuthorizationError(t, response, test.errno, test.message)
			policy.check(t, "lock.status", 1)
			if probe.calls.Load() != 0 {
				t.Fatal("authorization fault reached native state")
			}
		})
	}
}

func TestStrongLockAuthorizationPrecedesStatusCapabilityLookup(t *testing.T) {
	handler, probe, policy := authorizationLockFixture(t)
	handler.locks = struct{ locking.Service }{probe}
	response := authorizationLockRequest(t, handler, OpLockStatus, struct{}{})
	assertLockAuthorizationError(t, response, "EACCES", "access denied")
	policy.check(t, "lock.status", 1)
	policy.decision(nil)
	response = authorizationLockRequest(t, handler, OpLockStatus, struct{}{})
	if response.Code != StatusStorageError {
		t.Fatalf("unavailable status response = %d", response.Code)
	}
	var failure lockErrorResponse
	if err := decodeLockJSON(response.Body.Bytes(), &failure); err != nil || failure.Code != locking.Unavailable || failure.Recorded {
		t.Fatalf("allowed unavailable status = %+v, %v", failure, err)
	}
	policy.check(t, "lock.status", 1)
}

func TestStrongLockAuthorizationRejectsInvalidRequestsBeforePolicy(t *testing.T) {
	handler, probe, policy := authorizationLockFixture(t)
	for _, test := range authorizationLockCases() {
		t.Run(string(test.op)+"/unknown-field", func(t *testing.T) {
			policy.decision(nil)
			encoded, err := json.Marshal(test.body)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			fields["unknown"] = true
			response := authorizationLockRequest(t, handler, test.op, fields)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("unknown field status = %d", response.Code)
			}
			policy.check(t, "", 0)
		})
	}
	for _, test := range []struct {
		name string
		op   Op
		body string
		code int
	}{
		{"null", OpSessionEnrollment, `null`, http.StatusBadRequest},
		{"duplicate", OpSessionClose, `{"session":"s","session":"s"}`, http.StatusBadRequest},
		{"empty capability", OpSessionClose, `{"session":""}`, http.StatusBadRequest},
		{"missing capability", OpOwnerRetire, `{"owner":{"session":"s"}}`, http.StatusBadRequest},
		{"invalid path", OpLockResolve, `{"owner":{"session":"s","owner":"o"},"path":"Li4vZmlsZQ=="}`, http.StatusBadRequest},
		{"zero TTL", OpLockRenew, `{"owner":{"session":"s","owner":"o"},"request":"r","grant":{"id":"g","resource":"f","generation":1},"ttlMillis":0}`, http.StatusBadRequest},
		{"invalid mode", OpLockAcquire, `{"owner":{"session":"s","owner":"o"},"request":"r","resource":{"id":"f","expiresMillis":10,"nowMillis":0,"historyExpiresMillis":10},"mode":"invalid","ttlMillis":1,"waitMillis":0}`, http.StatusBadRequest},
		{"oversized", OpSessionEnrollment, strings.Repeat("x", int(DefaultMaxLockControlBytes)+1), http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy.decision(nil)
			response := authorizationLockBody(handler, test.op, []byte(test.body))
			if response.Code != test.code {
				t.Fatalf("invalid request status = %d; want %d", response.Code, test.code)
			}
			policy.check(t, "", 0)
		})
	}
	if calls := probe.calls.Load(); calls != 0 {
		t.Fatalf("invalid request reached native service %d times", calls)
	}
}

func TestStrongLockAuthorizationRechecksPreviouslySuccessfulControls(t *testing.T) {
	for _, test := range authorizationLockCases() {
		t.Run(string(test.op), func(t *testing.T) {
			handler, probe, policy := authorizationLockFixture(t)
			body := realAuthorizationLockBody(t, probe.Service, test.op)
			policy.decision(nil)
			response := authorizationLockRequest(t, handler, test.op, body)
			if response.Code != http.StatusOK {
				t.Fatalf("allowed native control status = %d, body = %s", response.Code, response.Body.String())
			}
			policy.check(t, test.operation, 1)
			if calls := probe.calls.Load(); calls != 1 {
				t.Fatalf("allowed native calls = %d; want 1", calls)
			}
			before := authorizationNativeStatus(t, probe.Service)
			policy.decision(authz.ErrDenied)
			for range 3 {
				response := authorizationLockRequest(t, handler, test.op, body)
				assertLockAuthorizationError(t, response, "EACCES", "access denied")
			}
			policy.check(t, test.operation, 3)
			if calls := probe.calls.Load(); calls != 1 {
				t.Fatalf("denied replay reached native service: %d calls; want original call only", calls)
			}
			if after := authorizationNativeStatus(t, probe.Service); after != before {
				t.Fatalf("denied replay changed native state: before %+v, after %+v", before, after)
			}
		})
	}
}

func TestStrongLockAuthorizationDenialPreservesLiveCapabilitiesAndActions(t *testing.T) {
	for _, test := range authorizationLockCases() {
		t.Run(string(test.op), func(t *testing.T) {
			handler, probe, policy := authorizationLockFixture(t)
			body := realAuthorizationLockBody(t, probe.Service, test.op)
			before := authorizationNativeStatus(t, probe.Service)
			response := authorizationLockRequest(t, handler, test.op, body)
			assertLockAuthorizationError(t, response, "EACCES", "access denied")
			policy.check(t, test.operation, 1)
			if calls := probe.calls.Load(); calls != 0 {
				t.Fatalf("denied live control reached native service %d times", calls)
			}
			if after := authorizationNativeStatus(t, probe.Service); after != before {
				t.Fatalf("denied live control changed native state: before %+v, after %+v", before, after)
			}
		})
	}
}

func authorizationNativeStatus(t *testing.T, service locking.Service) locking.Status {
	t.Helper()
	status, err := service.(locking.StatusService).Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status.NowMillis = 0
	return status
}

func realAuthorizationLockBody(t *testing.T, service locking.Service, op Op) any {
	t.Helper()
	if op == OpSessionEnrollment || op == OpLockStatus {
		return struct{}{}
	}
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	if op == OpSessionOpen {
		return lockTicketMessage{Ticket: ticket}
	}
	if op == OpSessionClose {
		return lockSessionRequest{Session: session.ID}
	}
	if op == OpOwnerCreate {
		return lockCreateOwnerRequest{Session: session.ID, Request: "new-owner"}
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "seed-owner")
	if err != nil {
		t.Fatal(err)
	}
	if op == OpOwnerRetire {
		return lockOwnerRequest{Owner: owner.Ref}
	}
	if op == OpLockResolve {
		return lockResolveRequest{Owner: owner.Ref, Path: []byte("file")}
	}
	resource, err := service.Resolve(t.Context(), owner.Ref, "file")
	if err != nil {
		t.Fatal(err)
	}
	acquire := locking.AcquireRequest{
		Owner: owner.Ref, Request: "seed-acquire", Resource: resource,
		Mode: locking.Exclusive, TTL: time.Minute,
	}
	if op == OpLockAcquire {
		wire, err := lockAcquireOf(acquire)
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	acquired, err := service.Acquire(t.Context(), acquire)
	if err != nil || acquired.Grant == nil {
		t.Fatalf("seed grant = %+v, %v", acquired, err)
	}
	switch op {
	case OpLockRenew:
		return lockRenewRequest{Owner: owner.Ref, Request: "renew-action", Grant: acquired.Grant.Ref, TTLMillis: 60000}
	case OpLockRelease, OpLockQueryGrant:
		return lockGrantRequest{Owner: owner.Ref, Grant: acquired.Grant.Ref}
	case OpLockCancel, OpLockQueryAction:
		return lockActionRequest{Owner: owner.Ref, Request: acquire.Request}
	default:
		t.Fatalf("missing real native fixture for %s", op)
		return nil
	}
}
