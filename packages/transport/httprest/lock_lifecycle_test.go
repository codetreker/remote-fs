package httprest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type lockHistoryClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *lockHistoryClock) Now() time.Time                       { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *lockHistoryClock) After(time.Duration) <-chan time.Time { return nil }
func (c *lockHistoryClock) advance(d time.Duration)              { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

func TestHTTPResolveAdvertisesExtendedOwnerHistory(t *testing.T) {
	clock := &lockHistoryClock{now: time.Now().Add(time.Hour)}
	options := locking.DefaultOptions()
	options.Clock = clock
	options.SessionIdle = time.Second
	options.MaxLease = 100 * time.Millisecond
	options.MaxWait = 100 * time.Millisecond
	_, backend := memoryfixture.New(t, "resolve-history", 1<<20, options)
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Write(ctx, "f", nil); err != nil {
		t.Fatal(err)
	}
	ticket, err := client.BeginEnrollment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.OpenSession(ctx, ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := client.CreateOwner(ctx, session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(100 * time.Millisecond)
	resource, err := client.Resolve(ctx, owner.Ref, "f")
	if err != nil {
		t.Fatal(err)
	}
	if resource.HistoryExpiresMillis <= owner.HistoryExpiresMillis || resource.HistoryExpiresMillis-resource.NowMillis != options.SessionIdle.Milliseconds() {
		t.Fatalf("Resolve did not advertise its owner history extension: old=%d resource=%+v", owner.HistoryExpiresMillis, resource)
	}
	clock.advance(901 * time.Millisecond)
	next, err := client.Resolve(ctx, owner.Ref, "f")
	if err != nil {
		t.Fatalf("owner expired at its old history deadline: %v", err)
	}
	if next.NowMillis <= owner.HistoryExpiresMillis || next.NowMillis >= resource.HistoryExpiresMillis {
		t.Fatal("fixture did not cross only the old owner deadline")
	}
}

func TestLockControlAdmissionRefusesBeforeReadingAndRecovers(t *testing.T) {
	handler := &Handler{lockControls: configuredLockControlAdmission(1, 0)}
	release, err := handler.lockControls.acquire(context.Background(), retainedResponseMultiplier*DefaultMaxLockControlBytes)
	if err != nil {
		t.Fatal(err)
	}
	blockedBody := &unreadLockBody{t: t}
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpSessionEnrollment), blockedBody)
	request.Header.Set("Content-Type", contentJSON)
	answer := httptest.NewRecorder()
	handler.ServeHTTP(answer, request)
	release()
	if answer.Code != StatusStorageError {
		t.Fatalf("admission status=%d", answer.Code)
	}
	var failure lockErrorResponse
	if err := decodeLockJSON(answer.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != locking.Capacity || failure.Recorded || !errors.Is(failure.locking(), syscall.EAGAIN) {
		t.Fatal("admission fabricated a retained action")
	}
	request = httptest.NewRequest(http.MethodPost, Prefix+string(OpSessionEnrollment), strings.NewReader(`{"unknown":true}`))
	request.Header.Set("Content-Type", contentJSON)
	answer = httptest.NewRecorder()
	handler.ServeHTTP(answer, request)
	if answer.Code != http.StatusBadRequest {
		t.Fatalf("released admission did not recover: %d", answer.Code)
	}
}

type unreadLockBody struct{ t *testing.T }

func (b *unreadLockBody) Read([]byte) (int, error) {
	b.t.Fatal("admission refusal read the request body")
	return 0, errors.New("unexpected body read")
}
