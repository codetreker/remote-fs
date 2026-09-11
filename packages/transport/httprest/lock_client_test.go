package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
)

type lockRoundTripFunc func(*http.Request) (*http.Response, error)

func (trip lockRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return trip(request)
}

var lockTestOwner = locking.OwnerRef{Session: "session-capability", Owner: "owner-capability"}
var lockTestResource = locking.ResourceRef{ID: "resource-capability", ExpiresMillis: 10000, NowMillis: 100, HistoryExpiresMillis: 20000}
var lockTestGrant = locking.GrantRef{ID: "grant-capability", Resource: "resource-capability", Generation: 1}

func lockTestStatus() locking.GrantStatus {
	return locking.GrantStatus{Ref: lockTestGrant, Mode: locking.Exclusive, State: locking.Active, Revision: 1,
		DeadlineMillis: 1100, RemainingMillis: 1000, NowMillis: 100, HistoryExpiresMillis: 10000}
}

func lockTestReleasedStatus() locking.GrantStatus {
	status := lockTestStatus()
	status.State = locking.Released
	status.RemainingMillis = 0
	return status
}

func lockTestAcquire() locking.AcquireRequest {
	return locking.AcquireRequest{Owner: lockTestOwner, Request: "acquisition", Resource: lockTestResource,
		Mode: locking.Exclusive, TTL: time.Second, Wait: 2 * time.Second}
}

func lockTestAction(outcome locking.ActionOutcome) locking.ActionResult {
	request := lockTestAcquire()
	return locking.ActionResult{Recorded: true, Receipt: locking.ActionReceipt{Kind: locking.AcquireAction, Request: request.Request,
		Acquire: &request, Outcome: outcome}, NowMillis: 100, HistoryExpiresMillis: 10000}
}

func lockTestResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{HeaderProtocol: []string{Version}},
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func lockTestClient(t *testing.T, trip lockRoundTripFunc) *Storage {
	t.Helper()
	client, err := Dial("http://example.test/prefix", &http.Client{Transport: trip})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestLockJSONRejectsDamagedRequests(t *testing.T) {
	valid := `{"owner":{"session":"s","owner":"o"},"request":"r","resource":{"id":"f","expiresMillis":100,"nowMillis":0,"historyExpiresMillis":200},"mode":"X","ttlMillis":1000,"waitMillis":0}`
	for name, body := range map[string]string{
		"duplicate":        strings.Replace(valid, `"request":"r"`, `"request":"r","request":"r"`, 1),
		"nested duplicate": strings.Replace(valid, `"session":"s"`, `"session":"s","session":"s"`, 1),
		"unknown":          strings.Replace(valid, `"request":"r"`, `"request":"r","extra":1`, 1),
		"missing":          strings.Replace(valid, `,"waitMillis":0`, ``, 1),
		"null":             strings.Replace(valid, `"waitMillis":0`, `"waitMillis":null`, 1),
		"null owner":       strings.Replace(valid, `{"session":"s","owner":"o"}`, `null`, 1),
		"bad duration":     strings.Replace(valid, `"ttlMillis":1000`, `"ttlMillis":-1`, 1),
		"overflow":         strings.Replace(valid, `"ttlMillis":1000`, `"ttlMillis":9223372036854775807`, 1),
		"case mismatch":    strings.Replace(valid, `"request"`, `"Request"`, 1),
		"trailing":         valid + `{}`,
		"bad utf8":         strings.Replace(valid, `"request":"r"`, "\"request\":\"\xff\"", 1),
	} {
		t.Run(name, func(t *testing.T) {
			var request lockAcquireRequest
			if err := decodeLockJSON([]byte(body), &request); err == nil {
				t.Fatal("damaged request accepted")
			}
		})
	}
	var request lockAcquireRequest
	if err := decodeLockJSON([]byte(valid), &request); err != nil {
		t.Fatal(err)
	}
}

func TestLockWireDurationsAndRetainedReceipts(t *testing.T) {
	core := lockTestAction(locking.Pending)
	wire, err := lockActionOf(core)
	if err != nil {
		t.Fatal(err)
	}
	data, err := marshalLockJSON(lockActionResponse{Action: wire})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"ttlMillis":1000`) || !strings.Contains(string(data), `"waitMillis":2000`) || strings.Contains(string(data), `"ttl":`) {
		t.Fatalf("wrong duration encoding: %s", data)
	}
	var decoded lockActionResponse
	if err := decodeLockJSON(data, &decoded); err != nil {
		t.Fatal(err)
	}
	actual, err := decoded.Action.locking()
	if err != nil || !reflect.DeepEqual(core, actual) {
		t.Fatalf("receipt changed: %#v, %v", actual, err)
	}
	for _, ttl := range []time.Duration{0, -time.Second, time.Nanosecond, time.Second + time.Nanosecond} {
		request := lockTestAcquire()
		request.TTL = ttl
		if _, err := lockAcquireOf(request); err == nil {
			t.Fatalf("accepted ttl %v", ttl)
		}
	}
	if _, err := lockMillisDuration(math.MaxInt64, true); err == nil {
		t.Fatal("duration overflow accepted")
	}
}

func TestLockClientAllControlsUseBodyCapabilities(t *testing.T) {
	acquire := lockTestAcquire()
	acquireWire, err := lockAcquireOf(acquire)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := lockActionOf(lockTestAction(locking.Pending))
	if err != nil {
		t.Fatal(err)
	}
	renew := locking.RenewRequest{Owner: lockTestOwner, Request: "renewal", Grant: lockTestGrant, TTL: 2 * time.Second}
	renewWire, err := lockRenewOf(renew)
	if err != nil {
		t.Fatal(err)
	}
	renewedStatus := lockTestStatus()
	renewedStatus.Revision = 2
	renewedStatus.DeadlineMillis = 2100
	renewedStatus.RemainingMillis = 2000
	renewed := lockActionResult{Recorded: true, Grant: &renewedStatus, Receipt: lockActionReceipt{Kind: locking.RenewAction, Request: renew.Request,
		Renew: &renewWire, Outcome: locking.Renewed, Grant: &lockTestGrant, DeadlineMillis: 2100, Revision: 2}, NowMillis: 100, HistoryExpiresMillis: 10000}
	cases := []struct {
		op                Op
		request, response any
		invoke            func(*Storage) error
	}{
		{OpSessionEnrollment, struct{}{}, lockTicketMessage{Ticket: "enrollment-capability"}, func(s *Storage) error { _, e := s.BeginEnrollment(context.Background()); return e }},
		{OpSessionOpen, lockTicketMessage{Ticket: "enrollment-capability"}, lockSessionResponse{Session: locking.Session{ID: lockTestOwner.Session, Authority: "authority", HistoryExpiresMillis: 10000, NowMillis: 100}}, func(s *Storage) error { _, e := s.OpenSession(context.Background(), "enrollment-capability"); return e }},
		{OpSessionClose, lockSessionRequest{Session: lockTestOwner.Session}, struct{}{}, func(s *Storage) error { return s.CloseSession(context.Background(), lockTestOwner.Session) }},
		{OpOwnerCreate, lockCreateOwnerRequest{Session: lockTestOwner.Session, Request: "owner-request"}, lockOwnerResponse{Owner: locking.Owner{Ref: lockTestOwner, ActionCapacity: 16, HistoryExpiresMillis: 10000, NowMillis: 100}}, func(s *Storage) error {
			_, e := s.CreateOwner(context.Background(), lockTestOwner.Session, "owner-request")
			return e
		}},
		{OpOwnerRetire, lockOwnerRequest{Owner: lockTestOwner}, struct{}{}, func(s *Storage) error { return s.RetireOwner(context.Background(), lockTestOwner) }},
		{OpLockResolve, lockResolveRequest{Owner: lockTestOwner, Path: []byte("bad\xffname")}, lockResourceResponse{Resource: lockTestResource}, func(s *Storage) error {
			_, e := s.Resolve(context.Background(), lockTestOwner, "bad\xffname")
			return e
		}},
		{OpLockAcquire, acquireWire, lockActionResponse{Action: pending}, func(s *Storage) error {
			r, e := s.Acquire(context.Background(), acquire)
			if e == nil && r.Receipt.Outcome != locking.Pending {
				t.Fatal("pending lost")
			}
			return e
		}},
		{OpLockRenew, renewWire, lockActionResponse{Action: renewed}, func(s *Storage) error {
			r, e := s.Renew(context.Background(), renew)
			if e == nil && r.Receipt.Renew.TTL != renew.TTL {
				t.Fatal("renew duration changed")
			}
			return e
		}},
		{OpLockRelease, lockGrantRequest{Owner: lockTestOwner, Grant: lockTestGrant}, lockReleaseResponse{Release: locking.ReleaseResult{State: locking.Released, Grant: lockTestReleasedStatus(), NowMillis: 100, HistoryExpiresMillis: 10000}}, func(s *Storage) error {
			_, e := s.Release(context.Background(), lockTestOwner, lockTestGrant)
			return e
		}},
		{OpLockCancel, lockActionRequest{Owner: lockTestOwner, Request: "acquisition"}, lockCancelResponse{Cancel: locking.CancelResult{Outcome: locking.Cancelled, NowMillis: 100, HistoryExpiresMillis: 10000}}, func(s *Storage) error { _, e := s.Cancel(context.Background(), lockTestOwner, "acquisition"); return e }},
		{OpLockQueryAction, lockActionRequest{Owner: lockTestOwner, Request: "acquisition"}, lockActionResponse{Action: pending}, func(s *Storage) error {
			_, e := s.QueryAction(context.Background(), lockTestOwner, "acquisition")
			return e
		}},
		{OpLockQueryGrant, lockGrantRequest{Owner: lockTestOwner, Grant: lockTestGrant}, lockGrantResponse{Grant: lockTestStatus()}, func(s *Storage) error {
			_, e := s.QueryGrant(context.Background(), lockTestOwner, lockTestGrant)
			return e
		}},
		{OpLockStatus, struct{}{}, lockStatusResponse{Status: locking.Status{Authority: "authority"}}, func(s *Storage) error { _, e := s.Status(context.Background()); return e }},
	}
	for _, test := range cases {
		t.Run(string(test.op), func(t *testing.T) {
			calls := 0
			client := lockTestClient(t, func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodPost || r.URL.Path != "/prefix"+Prefix+string(test.op) || r.URL.RawQuery != "" || r.Header.Get(HeaderMutationScope) != "" {
					t.Fatal("control leaked credentials or used wrong route")
				}
				body, e := io.ReadAll(r.Body)
				if e != nil {
					t.Fatal(e)
				}
				want, e := json.Marshal(test.request)
				if e != nil {
					t.Fatal(e)
				}
				if string(body) != string(want) {
					t.Fatal("control request changed")
				}
				response, e := marshalLockJSON(test.response)
				if e != nil {
					t.Fatal(e)
				}
				return lockTestResponse(string(response)), nil
			})
			if e := test.invoke(client); e != nil {
				t.Fatal(e)
			}
			if calls != 1 {
				t.Fatalf("made %d requests", calls)
			}
		})
	}
}

func TestLockClientPreservesRecordedFailure(t *testing.T) {
	action := lockTestAction(locking.Rejected)
	action.Receipt.Code = locking.Conflict
	wire, e := lockActionOf(action)
	if e != nil {
		t.Fatal(e)
	}
	failure := lockErrorOf(&locking.Error{Code: locking.Conflict, Recorded: true, Message: "file is protected"})
	failure.Action = &wire
	body, e := marshalLockJSON(failure)
	if e != nil {
		t.Fatal(e)
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		response := lockTestResponse(string(body))
		response.StatusCode = StatusStorageError
		return response, nil
	})
	result, e := client.Acquire(context.Background(), lockTestAcquire())
	var typed *locking.Error
	if !errors.As(e, &typed) || typed.Code != locking.Conflict || !typed.Recorded || !errors.Is(e, syscall.EBUSY) {
		t.Fatalf("failure lost: %v", e)
	}
	if !reflect.DeepEqual(result, action) {
		t.Fatalf("retained receipt lost: %#v", result)
	}
	for _, code := range []locking.Code{locking.Invalid, locking.UnsupportedTarget, locking.Conflict, locking.AlreadyHeld,
		locking.RequestMismatch, locking.Capacity, locking.Retired, locking.OutcomeUnknown, locking.StaleResource,
		locking.StaleGrant, locking.UnrelatedProof, locking.Recovering, locking.Unavailable} {
		data, e := marshalLockJSON(lockErrorOf(&locking.Error{Code: code, Message: "refused"}))
		if e != nil {
			t.Fatal(e)
		}
		var decoded lockErrorResponse
		if e := decodeLockJSON(data, &decoded); e != nil {
			t.Fatal(e)
		}
		if !errors.Is(decoded.locking(), locking.Errno(code)) {
			t.Fatalf("code %s changed classification", code)
		}
	}
}

func TestLockClientRejectsMalformedResponses(t *testing.T) {
	for name, body := range map[string]string{
		"missing": "{}", "null": `{"ticket":null}`, "duplicate": `{"ticket":"secret","ticket":"secret"}`,
		"extra": `{"ticket":"secret","extra":0}`, "empty": `{"ticket":""}`,
		"long capability": `{"ticket":"` + strings.Repeat("x", 513) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			_, e := client.BeginEnrollment(context.Background())
			if !errors.Is(e, syscall.EIO) || strings.Contains(e.Error(), "secret") {
				t.Fatalf("malformed response error: %v", e)
			}
		})
	}
	for name, body := range map[string]string{
		"unknown code":     `{"errno":"EIO","lockCode":"newCode","recorded":false,"message":"refused"}`,
		"missing recorded": `{"errno":"EBUSY","lockCode":"conflict","message":"refused"}`,
		"null recorded":    `{"errno":"EBUSY","lockCode":"conflict","recorded":null,"message":"refused"}`,
		"wrong errno":      `{"errno":"ENOENT","lockCode":"conflict","recorded":false,"message":"refused"}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := lockTestResponse(body)
				r.StatusCode = StatusStorageError
				return r, nil
			})
			_, e := client.BeginEnrollment(context.Background())
			if !errors.Is(e, syscall.EIO) || errors.Is(e, syscall.ENOENT) {
				t.Fatalf("malformed error response: %v", e)
			}
		})
	}
}

type lockFailingBody struct{ err error }

func (b lockFailingBody) Read([]byte) (int, error) { return 0, b.err }
func (lockFailingBody) Close() error               { return nil }

func TestLockClientCancellationPhases(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		for _, phase := range []string{"before", "dispatch", "body"} {
			t.Run(phase+map[bool]string{false: " mutation", true: " query"}[readOnly], func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
					calls++
					cancel()
					if phase == "body" {
						r := lockTestResponse("{}")
						r.Body = lockFailingBody{context.Canceled}
						return r, nil
					}
					return nil, context.Canceled
				})
				if phase == "before" {
					cancel()
				}
				var e error
				if readOnly {
					_, e = client.QueryAction(ctx, lockTestOwner, "acquisition")
				} else {
					_, e = client.Acquire(ctx, lockTestAcquire())
				}
				want := syscall.EIO
				if phase == "before" || readOnly {
					want = syscall.EINTR
				}
				if !errors.Is(e, want) || !errors.Is(e, context.Canceled) {
					t.Fatalf("cancellation phase lost: %v", e)
				}
				if phase == "before" && calls != 0 {
					t.Fatal("cancelled call entered HTTP")
				}
			})
		}
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return nil, syscall.ENOENT })
	_, e := client.QueryAction(context.Background(), lockTestOwner, "acquisition")
	if !errors.Is(e, syscall.EIO) || errors.Is(e, syscall.ENOENT) {
		t.Fatalf("network errno leaked: %v", e)
	}
}

func TestLockClientAdmissionIndependentFromBulk(t *testing.T) {
	calls := 0
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		calls++
		return lockTestResponse(`{"ticket":"capability"}`), nil
	})
	client.responses = newBodyAdmission(1, 1024, 0)
	release, e := client.responses.acquire(context.Background(), 1024)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := client.BeginEnrollment(ctx); e != nil {
		t.Fatal(e)
	}
	if calls != 1 {
		t.Fatal("lock control did not proceed under bulk saturation")
	}
	client.lockControls = newBodyAdmission(1, 4*DefaultMaxLockControlBytes, 0)
	unlock, e := client.lockControls.acquire(context.Background(), 4*DefaultMaxLockControlBytes)
	if e != nil {
		t.Fatal(e)
	}
	defer unlock()
	_, e = client.BeginEnrollment(context.Background())
	var typed *locking.Error
	if !errors.As(e, &typed) || typed.Code != locking.Capacity || typed.Recorded || calls != 1 {
		t.Fatalf("lock admission capacity lost: %v", e)
	}
}

func TestLockDeadlineUsesRequestStartAndRemaining(t *testing.T) {
	started := time.Now()
	grant := lockTestStatus()
	deadline, e := ConservativeLockDeadline(started, grant)
	if e != nil || !deadline.Equal(started.Add(time.Second)) {
		t.Fatalf("deadline estimate: %v, %v", deadline, e)
	}
	grant.RemainingMillis = 0
	grant.NowMillis = grant.DeadlineMillis
	deadline, e = ConservativeLockDeadline(started, grant)
	if e != nil || !deadline.Equal(started) {
		t.Fatalf("zero remaining interval extended: %v, %v", deadline, e)
	}
	grant.State = locking.Expired
	if _, e := ConservativeLockDeadline(started, grant); e == nil {
		t.Fatal("expired grant has a deadline")
	}
}

func TestLockClientRejectsUnrelatedReceipts(t *testing.T) {
	for _, op := range []Op{OpLockAcquire, OpLockRenew, OpLockQueryAction, OpLockQueryGrant, OpLockRelease, OpOwnerCreate} {
		t.Run(string(op), func(t *testing.T) {
			var response any
			action := lockTestAction(locking.Pending)
			action.Receipt.Request = "different-request"
			action.Receipt.Acquire.Request = "different-request"
			wire, e := lockActionOf(action)
			if e != nil {
				t.Fatal(e)
			}
			status := lockTestStatus()
			status.Ref.ID = "different-grant"
			switch op {
			case OpLockQueryGrant:
				response = lockGrantResponse{Grant: status}
			case OpLockRelease:
				status.State = locking.Released
				status.RemainingMillis = 0
				response = lockReleaseResponse{Release: locking.ReleaseResult{State: locking.Released, Grant: status}}
			case OpOwnerCreate:
				response = lockOwnerResponse{Owner: locking.Owner{Ref: locking.OwnerRef{Session: "different-session", Owner: "owner"}, ActionCapacity: 16}}
			default:
				response = lockActionResponse{Action: wire}
			}
			body, e := marshalLockJSON(response)
			if e != nil {
				t.Fatal(e)
			}
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(string(body)), nil })
			switch op {
			case OpLockAcquire:
				_, e = client.Acquire(context.Background(), lockTestAcquire())
			case OpLockRenew:
				_, e = client.Renew(context.Background(), locking.RenewRequest{Owner: lockTestOwner, Request: "renewal", Grant: lockTestGrant, TTL: time.Second})
			case OpLockQueryAction:
				_, e = client.QueryAction(context.Background(), lockTestOwner, "acquisition")
			case OpLockQueryGrant:
				_, e = client.QueryGrant(context.Background(), lockTestOwner, lockTestGrant)
			case OpLockRelease:
				_, e = client.Release(context.Background(), lockTestOwner, lockTestGrant)
			case OpOwnerCreate:
				_, e = client.CreateOwner(context.Background(), lockTestOwner.Session, "owner-request")
			}
			if !errors.Is(e, syscall.EIO) {
				t.Fatalf("unrelated response accepted: %v", e)
			}
		})
	}
}

func TestLockClientBoundsRequestsBeforeDispatch(t *testing.T) {
	calls := 0
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { calls++; return lockTestResponse(`{}`), nil })
	_, e := client.Resolve(context.Background(), lockTestOwner, strings.Repeat("x", int(DefaultMaxLockControlBytes)+1))
	if !errors.Is(e, syscall.EINVAL) || calls != 0 {
		t.Fatalf("oversized path dispatched: %v", e)
	}
	request := lockTestAcquire()
	request.Owner.Owner = locking.OwnerID(strings.Repeat("x", 513))
	_, e = client.Acquire(context.Background(), request)
	if !errors.Is(e, syscall.EINVAL) || calls != 0 {
		t.Fatalf("oversized capability dispatched: %v", e)
	}
	request = lockTestAcquire()
	request.TTL = time.Second + time.Nanosecond
	_, e = client.Acquire(context.Background(), request)
	if !errors.Is(e, syscall.EINVAL) || calls != 0 {
		t.Fatalf("inexact duration dispatched: %v", e)
	}
}

func lockTestGrantedResult(t *testing.T) lockActionResult {
	t.Helper()
	request := lockTestAcquire()
	wire, err := lockAcquireOf(request)
	if err != nil {
		t.Fatal(err)
	}
	grant := lockTestGrant
	status := lockTestStatus()
	return lockActionResult{Recorded: true, Receipt: lockActionReceipt{Kind: locking.AcquireAction, Request: request.Request,
		Acquire: &wire, Outcome: locking.Granted, Grant: &grant, DeadlineMillis: 1100, Revision: 1},
		Grant: &status, NowMillis: 100, HistoryExpiresMillis: 10000}
}

func lockTestUncheckedJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestLockClientRejectsIncoherentActionGrants(t *testing.T) {
	for name, damage := range map[string]func(*lockActionResult){
		"receipt resource":           func(r *lockActionResult) { r.Receipt.Grant.Resource = "other-resource" },
		"current id":                 func(r *lockActionResult) { r.Grant.Ref.ID = "other-grant" },
		"current generation":         func(r *lockActionResult) { r.Grant.Ref.Generation++ },
		"current resource":           func(r *lockActionResult) { r.Grant.Ref.Resource = "other-resource" },
		"current mode":               func(r *lockActionResult) { r.Grant.Mode = locking.Shared },
		"older revision":             func(r *lockActionResult) { r.Receipt.Revision = 2 },
		"shortened deadline":         func(r *lockActionResult) { r.Receipt.DeadlineMillis = 1200 },
		"unrevised deadline":         func(r *lockActionResult) { r.Grant.DeadlineMillis = 1200; r.Grant.RemainingMillis = 1100 },
		"missing current status":     func(r *lockActionResult) { r.Grant = nil },
		"ungranted receipt interval": func(r *lockActionResult) { r.Receipt.Outcome = locking.Pending },
		"ungranted current status": func(r *lockActionResult) {
			r.Receipt.Outcome = locking.Pending
			r.Receipt.Grant = nil
			r.Receipt.DeadlineMillis = 0
			r.Receipt.Revision = 0
		},
		"premature expiration": func(r *lockActionResult) { r.Grant.State = locking.Expired; r.Grant.RemainingMillis = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			action := lockTestGrantedResult(t)
			damage(&action)
			body := lockTestUncheckedJSON(t, lockActionResponse{Action: action})
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			result, err := client.Acquire(context.Background(), lockTestAcquire())
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(result, locking.ActionResult{}) {
				t.Fatalf("incoherent action returned result=%+v error=%v", result, err)
			}
		})
	}
}

func TestLockClientAcceptsHistoricalGrantAndCancellationReceipts(t *testing.T) {
	for _, state := range []locking.GrantState{locking.Active, locking.Released, locking.Expired, locking.TargetGone, locking.OwnerRetired} {
		t.Run(string(state), func(t *testing.T) {
			action := lockTestGrantedResult(t)
			action.Grant.Revision = 2
			action.Grant.DeadlineMillis = 2100
			action.Grant.RemainingMillis = 2000
			action.Grant.State = state
			if state != locking.Active {
				action.Grant.RemainingMillis = 0
			}
			if state == locking.Expired {
				action.Grant.NowMillis = 2100
			}
			body, err := marshalLockJSON(lockActionResponse{Action: action})
			if err != nil {
				t.Fatal(err)
			}
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(string(body)), nil })
			result, err := client.QueryAction(context.Background(), lockTestOwner, "acquisition")
			if err != nil || result.Receipt.Outcome != locking.Granted || result.Grant.State != state || result.Grant.Revision != 2 {
				t.Fatalf("historical receipt rejected: %+v %v", result, err)
			}
		})
	}
	tombstone := lockActionResult{Recorded: true, Receipt: lockActionReceipt{Kind: locking.AcquireAction, Request: "acquisition", Outcome: locking.Cancelled}, NowMillis: 100, HistoryExpiresMillis: 10000}
	body, err := marshalLockJSON(lockActionResponse{Action: tombstone})
	if err != nil {
		t.Fatal(err)
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(string(body)), nil })
	result, err := client.Acquire(context.Background(), lockTestAcquire())
	if err != nil || result.Receipt.Outcome != locking.Cancelled || result.Receipt.Acquire != nil || result.Grant != nil {
		t.Fatalf("immutable cancel tombstone rejected: %+v %v", result, err)
	}
}

func TestLockClientRejectsIncoherentRenewalReceipts(t *testing.T) {
	request := locking.RenewRequest{Owner: lockTestOwner, Request: "renewal", Grant: lockTestGrant, TTL: time.Second}
	wire, err := lockRenewOf(request)
	if err != nil {
		t.Fatal(err)
	}
	for name, damage := range map[string]func(*lockActionResult){
		"different receipt grant": func(r *lockActionResult) { r.Receipt.Grant.ID = "other-grant"; r.Grant.Ref.ID = "other-grant" },
		"different current grant": func(r *lockActionResult) { r.Grant.Ref.ID = "other-grant" },
		"missing current status":  func(r *lockActionResult) { r.Grant = nil },
		"acquisition timeout": func(r *lockActionResult) {
			r.Receipt.Outcome = locking.TimedOut
			r.Receipt.Grant = nil
			r.Receipt.Revision = 0
			r.Receipt.DeadlineMillis = 0
			r.Grant = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			action := lockTestGrantedResult(t)
			action.Receipt.Kind = locking.RenewAction
			action.Receipt.Request = request.Request
			action.Receipt.Acquire = nil
			action.Receipt.Renew = &wire
			action.Receipt.Outcome = locking.Renewed
			action.Receipt.Revision = 2
			action.Grant.Revision = 2
			damage(&action)
			body := lockTestUncheckedJSON(t, lockActionResponse{Action: action})
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			result, err := client.Renew(context.Background(), request)
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(result, locking.ActionResult{}) {
				t.Fatalf("incoherent renewal returned %+v %v", result, err)
			}
		})
	}
}

func TestLockClientRequiresRecordedFailureReceipt(t *testing.T) {
	failure := lockErrorOf(&locking.Error{Code: locking.Invalid, Recorded: true, Message: "rejected"})
	body, err := marshalLockJSON(failure)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []Op{OpLockAcquire, OpLockRenew, OpLockQueryAction} {
		t.Run(string(op), func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				response := lockTestResponse(string(body))
				response.StatusCode = StatusStorageError
				return response, nil
			})
			var result locking.ActionResult
			var err error
			switch op {
			case OpLockAcquire:
				result, err = client.Acquire(context.Background(), lockTestAcquire())
			case OpLockRenew:
				result, err = client.Renew(context.Background(), locking.RenewRequest{Owner: lockTestOwner, Request: "renewal", Grant: lockTestGrant, TTL: time.Second})
			case OpLockQueryAction:
				result, err = client.QueryAction(context.Background(), lockTestOwner, "acquisition")
			}
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(result, locking.ActionResult{}) {
				t.Fatalf("recorded failure lost its receipt: %+v %v", result, err)
			}
		})
	}
	action := lockTestAction(locking.Rejected)
	action.Receipt.Code = locking.Invalid
	wire, err := lockActionOf(action)
	if err != nil {
		t.Fatal(err)
	}
	failure.Action = &wire
	body, err = marshalLockJSON(failure)
	if err != nil {
		t.Fatal(err)
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		response := lockTestResponse(string(body))
		response.StatusCode = StatusStorageError
		return response, nil
	})
	result, err := client.Release(context.Background(), lockTestOwner, lockTestGrant)
	if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINVAL) || result != (locking.ReleaseResult{}) {
		t.Fatalf("cleanup accepted unrelated action error: %+v %v", result, err)
	}
	body, err = marshalLockJSON(lockActionResponse{Action: wire})
	if err != nil {
		t.Fatal(err)
	}
	client = lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(string(body)), nil })
	response, err := client.Acquire(context.Background(), lockTestAcquire())
	if !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(response, locking.ActionResult{}) {
		t.Fatalf("rejection returned as success: %+v %v", response, err)
	}
}

func TestLockClientRejectsIncoherentCleanupResults(t *testing.T) {
	for name, result := range map[string]locking.ReleaseResult{
		"active release":            {State: locking.Active, Grant: lockTestStatus()},
		"active current status":     {State: locking.Released, Grant: lockTestStatus()},
		"mismatched terminal state": {State: locking.Expired, Grant: lockTestReleasedStatus()},
	} {
		t.Run(name, func(t *testing.T) {
			body := lockTestUncheckedJSON(t, lockReleaseResponse{Release: result})
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			actual, err := client.Release(context.Background(), lockTestOwner, lockTestGrant)
			if !errors.Is(err, syscall.EIO) || actual != (locking.ReleaseResult{}) {
				t.Fatalf("incoherent release returned %+v %v", actual, err)
			}
		})
	}
	for name, result := range map[string]locking.CancelResult{
		"pending": {Outcome: locking.Pending}, "renewal": {Outcome: locking.Renewed},
		"release without grant": {Outcome: locking.Cancelled, Released: true},
	} {
		t.Run(name, func(t *testing.T) {
			body := lockTestUncheckedJSON(t, lockCancelResponse{Cancel: result})
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			actual, err := client.Cancel(context.Background(), lockTestOwner, "acquisition")
			if !errors.Is(err, syscall.EIO) || actual != (locking.CancelResult{}) {
				t.Fatalf("incoherent cancellation returned %+v %v", actual, err)
			}
		})
	}
}

func TestLockResourceHistoryTiming(t *testing.T) {
	valid := `{"resource":{"id":"resource","expiresMillis":200,"nowMillis":100,"historyExpiresMillis":300}}`
	for name, body := range map[string]string{
		"missing history":          strings.Replace(valid, `,"historyExpiresMillis":300`, "", 1),
		"null history":             strings.Replace(valid, `"historyExpiresMillis":300`, `"historyExpiresMillis":null`, 1),
		"negative history":         strings.Replace(valid, `"historyExpiresMillis":300`, `"historyExpiresMillis":-1`, 1),
		"history before capture":   strings.Replace(valid, `"historyExpiresMillis":300`, `"historyExpiresMillis":99`, 1),
		"reference before capture": strings.Replace(valid, `"expiresMillis":200`, `"expiresMillis":99`, 1),
		"negative capture":         strings.Replace(valid, `"nowMillis":100`, `"nowMillis":-1`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(body), nil })
			ref, err := client.Resolve(context.Background(), lockTestOwner, "f")
			if !errors.Is(err, syscall.EIO) || ref != (locking.ResourceRef{}) {
				t.Fatalf("invalid resource timing accepted: %+v %v", ref, err)
			}
		})
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(valid), nil })
	ref, err := client.Resolve(context.Background(), lockTestOwner, "f")
	if err != nil || ref.HistoryExpiresMillis != 300 {
		t.Fatalf("resolve history deadline lost: %+v %v", ref, err)
	}
	action := lockTestGrantedResult(t)
	action.NowMillis = lockTestResource.HistoryExpiresMillis + 1
	action.Grant.NowMillis = action.NowMillis
	action.Grant.State = locking.Expired
	action.Grant.RemainingMillis = 0
	action.HistoryExpiresMillis = action.NowMillis + 10000
	action.Grant.HistoryExpiresMillis = action.HistoryExpiresMillis
	body, err := marshalLockJSON(lockActionResponse{Action: action})
	if err != nil {
		t.Fatal(err)
	}
	client = lockTestClient(t, func(*http.Request) (*http.Response, error) { return lockTestResponse(string(body)), nil })
	result, err := client.QueryAction(context.Background(), lockTestOwner, "acquisition")
	if err != nil || result.Receipt.Acquire.Resource != lockTestResource {
		t.Fatalf("historical resolve reference changed: %+v %v", result, err)
	}
}

type lockControlFailureService struct {
	locking.Service
	result  locking.ActionResult
	failure error
}

func (s lockControlFailureService) Acquire(context.Context, locking.AcquireRequest) (locking.ActionResult, error) {
	return s.result, s.failure
}

type lockControlClassifiedFailure struct{ cause error }

func (e lockControlClassifiedFailure) Error() string {
	return "control completion uncertain: " + e.cause.Error()
}
func (e lockControlClassifiedFailure) Unwrap() error         { return e.cause }
func (e lockControlClassifiedFailure) Classification() error { return syscall.EIO }

func TestLockControlErrorDispatchPreservesWholeOutcome(t *testing.T) {
	refusal := &locking.Error{Code: locking.Conflict, Recorded: true, Message: "file occupied"}
	cleanup := fmt.Errorf("control cleanup failed: %w", syscall.EIO)
	action := lockTestAction(locking.Rejected)
	action.Receipt.Code = locking.Conflict
	for name, test := range map[string]struct {
		failure error
		exact   bool
	}{
		"wrapped refusal":          {fmt.Errorf("acquire failed: %w", refusal), true},
		"singleton join":           {errors.Join(fmt.Errorf("acquire failed: %w", refusal)), true},
		"refusal then cleanup":     {errors.Join(refusal, cleanup), false},
		"cleanup then refusal":     {errors.Join(cleanup, refusal), false},
		"enclosing classification": {lockControlClassifiedFailure{cause: refusal}, false},
	} {
		t.Run(name, func(t *testing.T) {
			handler := &Handler{locks: lockControlFailureService{result: action, failure: test.failure}, lockControls: configuredLockControlAdmission(DefaultMaxConcurrentLockControls, DefaultMaxWaitingLockControls)}
			client := lockTestClient(t, func(request *http.Request) (*http.Response, error) {
				incoming := request.Clone(request.Context())
				incoming.URL.Path = strings.TrimPrefix(incoming.URL.Path, "/prefix")
				answer := httptest.NewRecorder()
				handler.ServeHTTP(answer, incoming)
				if answer.Code != StatusStorageError {
					t.Fatalf("error dispatch returned status %d: %s", answer.Code, answer.Body.String())
				}
				return answer.Result(), nil
			})
			result, err := client.Acquire(context.Background(), lockTestAcquire())
			var failure *locking.Error
			if !errors.As(err, &failure) || failure.Message != test.failure.Error() {
				t.Fatalf("whole diagnostic lost: %v", err)
			}
			if test.exact {
				if failure.Code != locking.Conflict || !failure.Recorded || !reflect.DeepEqual(result, action) {
					t.Fatalf("exact refusal lost: %+v %v", result, err)
				}
			} else if failure.Code != locking.Unavailable || failure.Recorded || !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EBUSY) || !reflect.DeepEqual(result, locking.ActionResult{}) {
				t.Fatalf("uncertainty became a nested refusal: %+v %v", result, err)
			}
		})
	}
}

func TestLockControlErrorDiagnosticBoundPreservesClassification(t *testing.T) {
	original := fmt.Errorf("acquire failed: %w", &locking.Error{Code: locking.StaleGrant, Message: strings.Repeat("long detail ", 200)})
	failure := lockErrorOf(original)
	if failure.Code != locking.StaleGrant || len(failure.Message) > 1024 || !strings.Contains(failure.Message, "wire text bound") {
		t.Fatalf("bounded diagnostic lost classification: %+v", failure)
	}
	if _, err := marshalLockJSON(failure); err != nil {
		t.Fatal(err)
	}
}

func TestLockClientAuthorizationErrorsHaveNoNativeOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		errno      syscall.Errno
	}{
		{"denied", `{"errno":"EACCES","message":"access denied"}`, syscall.EACCES},
		{"failed", `{"errno":"EIO","message":"authorization failed"}`, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, call := range map[string]func(*Storage) error{
				"enroll": func(s *Storage) error { _, err := s.BeginEnrollment(t.Context()); return err },
				"acquire": func(s *Storage) error {
					got, err := s.Acquire(t.Context(), lockTestAcquire())
					if !reflect.DeepEqual(got, locking.ActionResult{}) {
						t.Fatalf("invented acquisition: %+v", got)
					}
					return err
				},
				"query": func(s *Storage) error {
					got, err := s.QueryAction(t.Context(), lockTestOwner, "acquisition")
					if !reflect.DeepEqual(got, locking.ActionResult{}) {
						t.Fatalf("invented query: %+v", got)
					}
					return err
				},
				"cancel": func(s *Storage) error {
					got, err := s.Cancel(t.Context(), lockTestOwner, "acquisition")
					if got != (locking.CancelResult{}) {
						t.Fatalf("invented cancellation: %+v", got)
					}
					return err
				},
			} {
				t.Run(name, func(t *testing.T) {
					client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
						r := lockTestResponse(tc.body)
						r.StatusCode = StatusStorageError
						return r, nil
					})
					err := call(client)
					var ordinary *operationError
					var native *locking.Error
					if !errors.Is(err, tc.errno) || !errors.As(err, &ordinary) || ordinary.unknown || errors.As(err, &native) {
						t.Fatalf("authorization outcome = %#v", err)
					}
				})
			}
		})
	}
}

func TestLockClientAuthorizationEnvelopeIsAnExactUnion(t *testing.T) {
	for name, body := range map[string]string{
		"recorded without code":  `{"errno":"EACCES","message":"access denied","recorded":false}`,
		"code without recorded":  `{"errno":"EACCES","message":"access denied","lockCode":"conflict"}`,
		"null code":              `{"errno":"EACCES","message":"access denied","lockCode":null}`,
		"null recorded":          `{"errno":"EACCES","message":"access denied","recorded":null}`,
		"inconsistent native":    `{"errno":"EACCES","message":"access denied","lockCode":"conflict","recorded":false}`,
		"native code wrong type": `{"errno":"EACCES","message":"access denied","lockCode":3,"recorded":false}`,
		"missing errno":          `{"message":"access denied"}`,
		"missing message":        `{"errno":"EACCES"}`,
		"null errno":             `{"errno":null,"message":"access denied"}`,
		"null message":           `{"errno":"EACCES","message":null}`,
		"nonstring errno":        `{"errno":13,"message":"access denied"}`,
		"nonstring message":      `{"errno":"EACCES","message":false}`,
		"unknown errno":          `{"errno":"NEW_ERRNO","message":"access denied"}`,
		"ordinary native errno":  `{"errno":"EBUSY","message":"access denied"}`,
		"extra":                  `{"errno":"EACCES","message":"access denied","action":{}}`,
		"duplicate":              `{"errno":"EIO","errno":"EACCES","message":"access denied"}`,
		"case mismatch":          `{"Errno":"EACCES","message":"access denied"}`,
		"trailing":               `{"errno":"EACCES","message":"access denied"}{}`,
		"array":                  `[]`, "null": `null`, "invalid": `not json`,
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := lockTestResponse(body)
				r.StatusCode = StatusStorageError
				return r, nil
			})
			result, err := client.Acquire(t.Context(), lockTestAcquire())
			var ordinary *operationError
			var native *locking.Error
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) || !errors.As(err, &ordinary) || !ordinary.unknown || errors.As(err, &native) || !reflect.DeepEqual(result, locking.ActionResult{}) {
				t.Fatalf("malformed authorization response = %+v, %#v", result, err)
			}
		})
	}
}

func TestLockClientAuthorizationEnvelopeRequiresProtocolAndCompleteBoundedBody(t *testing.T) {
	for name, damage := range map[string]func(*http.Response){
		"missing protocol": func(r *http.Response) { r.Header.Del(HeaderProtocol) },
		"wrong protocol":   func(r *http.Response) { r.Header.Set(HeaderProtocol, "0") },
		"wrong status":     func(r *http.Response) { r.StatusCode = http.StatusForbidden },
		"oversized":        func(r *http.Response) { r.ContentLength = DefaultMaxLockControlBytes + 1 },
		"truncated":        func(r *http.Response) { r.ContentLength++ },
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := lockTestResponse(`{"errno":"EACCES","message":"access denied"}`)
				r.StatusCode = StatusStorageError
				damage(r)
				return r, nil
			})
			_, err := client.BeginEnrollment(t.Context())
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) {
				t.Fatalf("invalid transport authorization = %v", err)
			}
		})
	}
}

func TestLockClientDeniedReconciliationPreservesUnknownAcquisition(t *testing.T) {
	first := true
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		if first {
			first = false
			return nil, errors.New("lost acquisition response")
		}
		r := lockTestResponse(`{"errno":"EACCES","message":"access denied"}`)
		r.StatusCode = StatusStorageError
		return r, nil
	})
	result, original := client.Acquire(t.Context(), lockTestAcquire())
	var unknown *operationError
	if !errors.As(original, &unknown) || !unknown.unknown || !reflect.DeepEqual(result, locking.ActionResult{}) {
		t.Fatalf("lost reply = %+v, %v", result, original)
	}
	queried, err := client.QueryAction(t.Context(), lockTestOwner, "acquisition")
	if !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(queried, locking.ActionResult{}) {
		t.Fatalf("denied query = %+v, %v", queried, err)
	}
	cancelled, err := client.Cancel(t.Context(), lockTestOwner, "acquisition")
	if !errors.Is(err, syscall.EACCES) || cancelled != (locking.CancelResult{}) {
		t.Fatalf("denied cancel = %+v, %v", cancelled, err)
	}
	if !unknown.unknown || !errors.Is(original, syscall.EIO) {
		t.Fatalf("reconciliation changed original unknown result: %v", original)
	}
}

func TestLockClientDecodesRealHandlerAuthorizationRefusals(t *testing.T) {
	backing := volumeFixture(t)
	for _, tc := range []struct {
		name    string
		cause   error
		errno   syscall.Errno
		message string
	}{
		{"denied", errors.Join(authz.ErrDenied, errors.New("private authorization detail"), syscall.ENOENT), syscall.EACCES, "access denied"},
		{"failed", fmt.Errorf("private policy detail: %w", syscall.EACCES), syscall.EIO, "authorization failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := DefaultHandlerOptions()
			options.Volume = "client-authorization"
			options.Authorizer = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return tc.cause })
			handler, err := NewHandlerWithOptions(backing, nil, options)
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
			for name, call := range map[string]func() error{
				"enroll":      func() error { _, err := client.BeginEnrollment(t.Context()); return err },
				"acquire":     func() error { _, err := client.Acquire(t.Context(), lockTestAcquire()); return err },
				"query":       func() error { _, err := client.QueryAction(t.Context(), lockTestOwner, "acquisition"); return err },
				"cancel":      func() error { _, err := client.Cancel(t.Context(), lockTestOwner, "acquisition"); return err },
				"subscribe":   func() error { _, err := client.Subscribe(t.Context()); return err },
				"resubscribe": func() error { _, err := client.Resubscribe(t.Context(), "log", 3); return err },
				"snapshot":    func() error { _, err := client.Snapshot(t.Context()); return err },
			} {
				t.Run(name, func(t *testing.T) {
					err := call()
					var native *locking.Error
					if !errors.Is(err, tc.errno) || errors.Is(err, syscall.ENOENT) || errors.As(err, &native) || !strings.Contains(err.Error(), tc.message) || strings.Contains(err.Error(), "private") {
						t.Fatalf("authorization response = %v", err)
					}
				})
			}
		})
	}
}
