package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHandlerRetirementPreservesLiveFileSession(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	enrolled, err := h.h.files.enroll(h.ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retirement=%v", err)
	}
	result, err := h.h.fileCall(h.ctx, fileRequest{Op: storage.OpFileStatus, Session: enrolled.Session}, [32]byte{})
	if err != nil || result.Status == nil {
		t.Fatalf("busy retirement disrupted session: %+v, %v", result, err)
	}
	if h.h.stopped() {
		t.Fatal("busy retirement stopped handler")
	}
}

func TestHandlerRetirementPreservesStrongSessionAndRollsBackFileReservation(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	ticket, err := h.h.locks.BeginEnrollment(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := h.h.locks.OpenSession(h.ctx, ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := h.h.locks.CreateOwner(h.ctx, session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retirement=%v", err)
	}
	if _, err := h.h.locks.CreateOwner(h.ctx, session.ID, "owner"); err != nil {
		t.Fatalf("strong owner invalidated: %v", err)
	}
	if _, err := h.h.files.enroll(h.ctx, storage.DefaultFileSessionOptions()); err != nil {
		t.Fatalf("failed strong reservation left file admission closed: %v", err)
	}
	if err := h.h.locks.RetireOwner(h.ctx, owner.Ref); err != nil {
		t.Fatal(err)
	}
	if err := h.h.locks.CloseSession(h.ctx, session.ID); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerRetirementReservationBlocksHTTPAndCanRollBack(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	if err := h.h.ReserveRetirement(t.Context()); err != nil {
		t.Fatal(err)
	}
	response := serverTransferRequest(h.h, h.ctx, []byte(`{"op":"check"}`))
	if response.Code != StatusStorageError {
		t.Fatalf("request crossed reservation: %d %s", response.Code, response.Body.String())
	}
	var refusal ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &refusal); err != nil || refusal.Errno != "EBUSY" {
		t.Fatalf("retirement refusal=%+v err=%v", refusal, err)
	}
	if _, err := h.h.locks.BeginEnrollment(h.ctx); locking.CodeOf(err) != locking.Retired {
		t.Fatalf("strong admission crossed reservation: %v", err)
	}
	h.h.CancelRetirement()
	h.h.CancelRetirement()
	response = serverTransferRequest(h.h, h.ctx, []byte(`{"op":"check"}`))
	if response.Code != http.StatusOK {
		t.Fatalf("rollback did not restore request admission: %d %s", response.Code, response.Body.String())
	}
	if err := h.h.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("second reservation=%v", err)
	}
	h.h.Stop()
	h.h.CancelRetirement()
	if !h.h.stopped() {
		t.Fatal("rollback revived a stopped handler")
	}
}

func TestHandlerRetirementCannotInterruptAdmittedHTTPRequest(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := newServerTransferHarness(t, authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		once.Do(func() { close(entered) })
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- serverTransferRequest(h.h, h.ctx, []byte(`{"op":"check"}`)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach authorization")
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("active request retirement=%v", err)
	}
	close(resume)
	select {
	case response := <-done:
		if response.Code != http.StatusOK {
			t.Fatalf("busy retirement disrupted accepted request: %d %s", response.Code, response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("accepted request remained blocked")
	}
	if err := h.h.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("finished request remained busy: %v", err)
	}
}

func TestHandlerRetirementPreservesActiveSubscription(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	server := httptest.NewServer(h.h)
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscription, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("active subscription retirement=%v", err)
	}
	if err := client.Create(t.Context(), "visible"); err != nil {
		t.Fatalf("busy retirement stopped volume: %v", err)
	}
	next := make(chan error, 1)
	go func() { _, err := subscription.Next(); next <- err }()
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("busy retirement lost subscribed change: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("busy retirement stopped subscription delivery")
	}
	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	serverTransferAwait(t, func() bool {
		h.h.requestMu.Lock()
		defer h.h.requestMu.Unlock()
		return h.h.activeRequests == 0
	})
	if err := h.h.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("closed subscription still busy: %v", err)
	}
}

func TestHandlerRetirementKeepsUnknownImportResponsibility(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	begin := serverTransferBegin(t, h, []byte("pending"))
	h.backend.importer.unknownCancel.Store(true)
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "cancel", Token: begin.Token})
	if err != nil || result.Error == nil {
		t.Fatalf("cancel=%+v err=%v", result, err)
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unknown transfer retirement=%v", err)
	}
	if serverTransfersSettled(h) || h.h.stopped() {
		t.Fatal("busy retirement discarded unknown transfer")
	}
	h.backend.importer.unknownCancel.Store(false)
	serverTransferAwait(t, func() bool { return h.h.transfers.idle() })
	if err := h.h.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("confirmed cleanup still busy: %v", err)
	}
}

func TestHandlerStopCannotSettleTransferBeforeNativeInitialization(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	command := transferClientCommand([]byte("initializing"))
	entry, err := h.h.transfers.admit(h.ctx, command.DeclaredBytes, command.Action)
	if err != nil {
		t.Fatal(err)
	}
	h.h.Stop()
	if _, err := h.h.transfers.settle(context.Background(), entry); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("uninitialized settlement=%v", err)
	}
	if h.h.transfers.idle() || serverTransfersSettled(h) || h.backend.importer.cancelCalls.Load() != 0 || h.backend.importer.detachCalls.Load() != 0 {
		t.Fatal("Stop retired admission before its native resource became known")
	}
	entry.mu.Lock()
	if entry.closing {
		entry.mu.Unlock()
		t.Fatal("initialization refusal froze admitted native work")
	}
	h.backend.importer.mu.Lock()
	h.backend.importer.command = command
	h.backend.importer.mu.Unlock()
	entry.importFile = h.backend.importer
	entry.initializing = false
	entry.mu.Unlock()
	if result, err := h.h.transfers.settle(context.Background(), entry); err != nil || !result.Released {
		t.Fatalf("initialized cleanup=%+v err=%v", result, err)
	}
	serverTransferAwait(t, func() bool { return h.h.transfers.idle() && serverTransfersSettled(h) })
	if h.backend.importer.cancelCalls.Load() != 1 {
		t.Fatal("initialized native staging was leaked or released twice")
	}
}
