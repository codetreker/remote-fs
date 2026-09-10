package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func lockServerFixture(t *testing.T) (*Handler, *Storage) {
	t.Helper()
	return lockServerFixtureWithOptions(t, locking.DefaultOptions())
}

func lockServerFixtureWithOptions(t *testing.T, options locking.Options) (*Handler, *Storage) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "lock-control", 1<<20, options)
	handler, err := NewHandler(backend, meta)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return handler, client
}

func lockServerOwner(t *testing.T, service locking.Service) locking.OwnerRef {
	t.Helper()
	ticket, err := service.BeginEnrollment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(context.Background(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	return owner.Ref
}

func lockServerGrant(t *testing.T, client *Storage, owner locking.OwnerRef, path string) locking.GrantRef {
	t.Helper()
	resource, err := client.Resolve(context.Background(), owner, path)
	if err != nil {
		t.Fatal(err)
	}
	action, err := client.Acquire(context.Background(), locking.AcquireRequest{
		Owner: owner, Request: "acquire", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if action.Receipt.Outcome != locking.Granted || action.Grant == nil {
		t.Fatal("missing granted receipt")
	}
	return action.Grant.Ref
}

func TestLockControlsRemainAvailableWithBulkAdmissionsFull(t *testing.T) {
	h, client := lockServerFixture(t)
	ctx := context.Background()
	bodyRelease, err := h.bodies.acquire(ctx, h.bodies.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer bodyRelease()
	responseRelease, err := h.responses.acquire(ctx, h.responses.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer responseRelease()
	clientRelease, err := client.responses.acquire(ctx, client.responses.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer clientRelease()
	timed, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ticket, err := client.BeginEnrollment(timed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenSession(timed, ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status(timed); err != nil {
		t.Fatal(err)
	}
}

func TestMutationScopeHeaderRejectsMalformedAndMisplacedProofs(t *testing.T) {
	h, _ := lockServerFixture(t)
	encode := func(body string) string { return base64.RawURLEncoding.EncodeToString([]byte(body)) }
	for _, value := range []string{"", "=", "%%%", encode(`null`),
		encode(`{"owner":{"session":"s","owner":"o"},"grants":null}`),
		encode(`{"owner":{"session":"s","owner":"o"},"grants":[],"grants":[]}`),
		encode(`{"owner":{"session":"s","owner":"o"},"grants":[],"extra":1}`),
		strings.Repeat("a", base64.RawURLEncoding.EncodedLen(MaxMutationScopeBytes)+1),
	} {
		request := httptest.NewRequest(http.MethodPost, Prefix+"create?path=f", nil)
		request.Header[HeaderMutationScope] = []string{value}
		answer := httptest.NewRecorder()
		h.ServeHTTP(answer, request)
		if answer.Code != http.StatusBadRequest {
			t.Fatalf("malformed scope status = %d", answer.Code)
		}
	}
	valid := encode(`{"owner":{"session":"s","owner":"o"},"grants":[]}`)
	for _, op := range []Op{OpRead, OpStat, OpList, OpSpace} {
		path := Prefix + string(op)
		if op != OpSpace {
			path += "?path=f"
		}
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set(HeaderMutationScope, valid)
		answer := httptest.NewRecorder()
		h.ServeHTTP(answer, request)
		if answer.Code != http.StatusBadRequest {
			t.Fatalf("read scope status = %d", answer.Code)
		}
	}
	request := httptest.NewRequest(http.MethodPost, Prefix+"create?path=f", nil)
	request.Header[HeaderMutationScope] = []string{valid, valid}
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, request)
	if answer.Code != http.StatusBadRequest {
		t.Fatalf("duplicate header status = %d", answer.Code)
	}
	if _, err := h.storage.Stat(context.Background(), "f"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("malformed scope changed volume: %v", err)
	}
}

func TestQueuedAcquisitionDoesNotOccupyControlAdmission(t *testing.T) {
	options := locking.DefaultOptions()
	options.MaxQueued = 1
	h, client := lockServerFixtureWithOptions(t, options)
	ctx := context.Background()
	if err := client.Write(ctx, "f", []byte("old")); err != nil {
		t.Fatal(err)
	}
	first := lockServerOwner(t, client)
	held := lockServerGrant(t, client, first, "f")
	second := lockServerOwner(t, client)
	resource, err := client.Resolve(ctx, second, "f")
	if err != nil {
		t.Fatal(err)
	}
	request := locking.AcquireRequest{Owner: second, Request: "waiting", Resource: resource,
		Mode: locking.Exclusive, TTL: time.Minute, Wait: time.Minute}
	action, err := client.Acquire(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !action.Recorded || action.Receipt.Outcome != locking.Pending {
		t.Fatal("conflicting acquisition was not queued")
	}
	third := lockServerOwner(t, client)
	thirdResource, err := client.Resolve(ctx, third, "f")
	if err != nil {
		t.Fatal(err)
	}
	full, err := client.Acquire(ctx, locking.AcquireRequest{Owner: third, Request: "full", Resource: thirdResource, Mode: locking.Exclusive, TTL: time.Minute, Wait: time.Minute})
	if !errors.Is(err, syscall.EAGAIN) || !full.Recorded || full.Receipt.Outcome != locking.Rejected {
		t.Fatalf("full acquisition queue result: %+v %v", full.Receipt, err)
	}
	bounded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	release, err := h.lockControls.acquire(bounded, h.lockControls.maxBytes)
	if err != nil {
		t.Fatalf("queued action retains HTTP admission: %v", err)
	}
	release()
	renewed, err := client.Renew(ctx, locking.RenewRequest{Owner: first, Request: "renew", Grant: held, TTL: time.Minute})
	if err != nil || renewed.Receipt.Outcome != locking.Renewed {
		t.Fatalf("renew: %v", err)
	}
	observed, err := client.QueryAction(ctx, second, "waiting")
	if err != nil || observed.Receipt.Outcome != locking.Pending {
		t.Fatalf("query pending: %v", err)
	}
	canceled, err := client.Cancel(ctx, second, "waiting")
	if err != nil || canceled.Outcome != locking.Cancelled {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := client.Release(ctx, first, held); err != nil {
		t.Fatal(err)
	}
	replayed, err := client.Acquire(ctx, request)
	if err != nil || replayed.Receipt.Outcome != locking.Cancelled {
		t.Fatalf("cancelled replay: %v", err)
	}
}

func TestAnonymousHandlerNeverInheritsServingClientScope(t *testing.T) {
	_, client := lockServerFixture(t)
	ctx := context.Background()
	if err := client.Write(ctx, "f", []byte("old")); err != nil {
		t.Fatal(err)
	}
	owner := lockServerOwner(t, client)
	grant := lockServerGrant(t, client, owner, "f")
	scoped, err := client.WithScope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := locked.New(scoped)
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.Write(ctx, "f", []byte("inherited")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("ordinary facade inherited backend proof: %v", err)
	}
	handler, err := NewHandler(scoped, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	anonymous, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := anonymous.Write(ctx, "f", []byte("bad")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("anonymous proxy mutation: %v", err)
	}
	content, err := client.Read(ctx, "f")
	if err != nil || string(content) != "old" {
		t.Fatalf("protected bytes changed: %q %v", content, err)
	}
}

func TestLockServerProtocolAndControlBodies(t *testing.T) {
	h, _ := lockServerFixture(t)
	request := httptest.NewRequest(http.MethodPost, "/v2/create?path=f", nil)
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, request)
	if answer.Code != http.StatusNotFound || answer.Header().Get(HeaderProtocol) != "3" {
		t.Fatalf("v2 status = %d", answer.Code)
	}
	for _, body := range []string{`null`, `{"extra":1}`, `{} {}`, strings.Repeat("x", int(DefaultMaxLockControlBytes)+1)} {
		request := httptest.NewRequest(http.MethodPost, Prefix+string(OpSessionEnrollment), strings.NewReader(body))
		request.Header.Set("Content-Type", contentJSON)
		answer := httptest.NewRecorder()
		h.ServeHTTP(answer, request)
		want := http.StatusBadRequest
		if int64(len(body)) > DefaultMaxLockControlBytes {
			want = http.StatusRequestEntityTooLarge
		}
		if answer.Code != want {
			t.Fatalf("invalid control status = %d, want %d", answer.Code, want)
		}
		var failure ErrorResponse
		if err := json.Unmarshal(answer.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Errno != "" || failure.Recorded != nil {
			t.Fatal("malformed exchange stated an authority outcome")
		}
	}
	body, _ := json.Marshal(map[string]any{"owner": map[string]any{"session": "secret-session", "owner": "secret-owner"}, "request": "id", "extra": "secret-extra"})
	request = httptest.NewRequest(http.MethodPost, Prefix+string(OpLockQueryAction), bytes.NewReader(body))
	request.Header.Set("Content-Type", contentJSON)
	answer = httptest.NewRecorder()
	h.ServeHTTP(answer, request)
	if strings.Contains(answer.Body.String(), "secret-") {
		t.Fatal("control diagnostic exposes capability")
	}
	if _, err := h.storage.Stat(context.Background(), "f"); storage.ErrnoOf(err) != syscall.ENOENT {
		t.Fatalf("v2 mutation executed: %v", err)
	}
}

func TestLockControlConfigurationBounds(t *testing.T) {
	for _, value := range []int{-1, maximumStreams + 1} {
		handler := DefaultHandlerOptions()
		handler.MaxConcurrentLockControls = value
		if handler.Check() == nil {
			t.Fatal("invalid active control bound accepted")
		}
		handler = DefaultHandlerOptions()
		handler.MaxWaitingLockControls = value
		if handler.Check() == nil {
			t.Fatal("invalid waiter control bound accepted")
		}
		dial := DefaultDialOptions()
		dial.MaxConcurrentLockControls = value
		if _, err := dial.settle(); err == nil {
			t.Fatal("invalid client active bound accepted")
		}
		dial = DefaultDialOptions()
		dial.MaxWaitingLockControls = value
		if _, err := dial.settle(); err == nil {
			t.Fatal("invalid client waiter bound accepted")
		}
	}
	options := DefaultHandlerOptions()
	options.MaxConcurrentLockControls = 1
	options.MaxWaitingLockControls = 2
	if err := options.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestLostAcquisitionReplyReconcilesTheOriginalIntent(t *testing.T) {
	_, client := lockServerFixture(t)
	ctx := context.Background()
	if err := client.Write(ctx, "f", []byte("old")); err != nil {
		t.Fatal(err)
	}
	owner := lockServerOwner(t, client)
	resource, err := client.Resolve(ctx, owner, "f")
	if err != nil {
		t.Fatal(err)
	}
	original := locking.AcquireRequest{Owner: owner, Request: "lost-reply", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute}
	lost := false
	underlying := client.http.Transport
	if underlying == nil {
		underlying = http.DefaultTransport
	}
	unreliable := *client
	unreliable.http = &http.Client{Transport: lockRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := underlying.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		if !lost && strings.HasSuffix(request.URL.Path, string(OpLockAcquire)) {
			lost = true
			_, readErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			return nil, io.ErrUnexpectedEOF
		}
		return response, nil
	})}
	if _, err := unreliable.Acquire(ctx, original); !errors.Is(err, syscall.EIO) {
		t.Fatalf("lost response = %v", err)
	}
	observed, err := client.QueryAction(ctx, owner, original.Request)
	if err != nil || observed.Receipt.Outcome != locking.Granted || observed.Grant == nil {
		t.Fatalf("query lost receipt: %v", err)
	}
	replayed, err := unreliable.Acquire(ctx, original)
	if err != nil || replayed.Grant == nil || replayed.Grant.Ref != observed.Grant.Ref {
		t.Fatalf("replay created another grant: %v", err)
	}
	canceled, err := client.Cancel(ctx, owner, original.Request)
	if err != nil || !canceled.Released {
		t.Fatalf("cancel granted acquisition: %v", err)
	}
	terminal, err := client.Acquire(ctx, original)
	if err != nil || terminal.Receipt.Outcome != locking.Granted || terminal.Grant == nil || terminal.Grant.State != locking.Released {
		t.Fatalf("original receipt changed after cleanup: %v", err)
	}
	if err := client.Write(ctx, "f", []byte("new")); err != nil {
		t.Fatalf("cleanup left file occupied: %v", err)
	}
}

func TestHTTPDelayedAcquisitionPreservesUnseenCancellation(t *testing.T) {
	_, client := lockServerFixture(t)
	ctx := context.Background()
	if err := client.Write(ctx, "f", nil); err != nil {
		t.Fatal(err)
	}
	owner := lockServerOwner(t, client)
	resource, err := client.Resolve(ctx, owner, "f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Cancel(ctx, owner, "cancel-before-delivery"); err != nil {
		t.Fatal(err)
	}
	original, err := client.QueryAction(ctx, owner, "cancel-before-delivery")
	if err != nil || original.Receipt.Outcome != locking.Cancelled || original.Receipt.Acquire != nil {
		t.Fatalf("unseen cancellation: %v", err)
	}
	for _, mode := range []locking.Mode{locking.Exclusive, locking.Shared} {
		result, err := client.Acquire(ctx, locking.AcquireRequest{Owner: owner, Request: "cancel-before-delivery", Resource: resource, Mode: mode, TTL: time.Minute})
		if err != nil || result.Receipt.Outcome != locking.Cancelled || result.Receipt.Acquire != nil || result.Grant != nil {
			t.Fatalf("delayed cancelled acquisition: %v", err)
		}
	}
	if err := client.Write(ctx, "f", []byte("anonymous")); err != nil {
		t.Fatalf("cancelled request acquired file: %v", err)
	}
}

func TestHTTPObjectVolumeLockContract(t *testing.T) {
	lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
		_, client := lockServerFixtureWithOptions(t, options)
		return lockcontract.Fixture{Storage: client, Locks: client, Scope: client.Scope}
	})
}
