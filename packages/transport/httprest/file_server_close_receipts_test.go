package httprest

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestHTTPExplicitSessionCloseRetainsEarlierActionResults(t *testing.T) {
	_, native := memoryfixture.New(t, "multiple-close-actions", 1<<20, locking.DefaultOptions())
	backend := &failedFirstSessionCloseBackend{Storage: native, terminalError: syscall.ENOTEMPTY}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	enrollment, err := handler.files.enroll(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := storage.NewLockRequestID(enrollment.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := storage.NewLockRequestID(enrollment.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := fileRequest{Op: storage.OpFileSessionClose, Session: enrollment.Session, Action: firstID}
	first, firstErr := handler.fileCall(t.Context(), firstRequest, [32]byte{1})
	if first.CloseResult == nil || first.CloseResult.Released || !errors.Is(firstErr, syscall.EIO) {
		t.Fatalf("first close=%+v err=%v", first.CloseResult, firstErr)
	}
	secondRequest := fileRequest{Op: storage.OpFileSessionClose, Session: enrollment.Session, Action: secondID}
	second, secondErr := handler.fileCall(t.Context(), secondRequest, [32]byte{2})
	if second.CloseResult == nil || !second.CloseResult.Released || !errors.Is(secondErr, syscall.ENOTEMPTY) {
		t.Fatalf("second close=%+v err=%v", second.CloseResult, secondErr)
	}
	for _, test := range []struct {
		request  fileRequest
		digest   [32]byte
		released bool
		errno    error
		outcome  storage.FileActionOutcome
	}{
		{firstRequest, [32]byte{1}, false, syscall.EIO, storage.FileActionUnknown},
		{secondRequest, [32]byte{2}, true, syscall.ENOTEMPTY, storage.FileActionCompleted},
	} {
		replayed, replayErr := handler.fileCall(t.Context(), test.request, test.digest)
		if replayed.CloseResult == nil || replayed.CloseResult.Released != test.released || !errors.Is(replayErr, test.errno) {
			t.Fatalf("replayed action %s: result=%+v err=%v", test.request.Action, replayed.CloseResult, replayErr)
		}
		receipt, receiptErr := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: enrollment.Session, FileAction: storage.FileActionID(test.request.Action)}, [32]byte{})
		if receiptErr != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Operation != storage.OpFileSessionClose || receipt.ActionReceipt.Outcome != test.outcome {
			t.Fatalf("receipt for %s: result=%+v err=%v", test.request.Action, receipt.ActionReceipt, receiptErr)
		}
	}
	handler.files.mu.Lock()
	terminal := handler.files.terminalCloses[enrollment.Session]
	handler.files.mu.Unlock()
	if terminal == nil || len(terminal.actions) != 2 || backend.calls.Load() != 2 {
		t.Fatalf("terminal history=%v native close calls=%d", terminal, backend.calls.Load())
	}
}

func TestHTTPExpiredTerminalReceiptsKeepBoundedCleanupFailureReport(t *testing.T) {
	registry := &fileRegistry{
		closed: true, wake: make(chan struct{}, 1),
		sessions: make(map[string]*servedFileSession),
		terminalCloses: map[string]*terminalFileClose{
			"expired": {expires: time.Now().Add(-time.Second)},
		},
	}
	registry.terminalErr.add(syscall.ENOTEMPTY)
	for i := 0; i < 1024; i++ {
		registry.terminalErr.add(fmt.Errorf("cleanup %d: %w", i, syscall.EIO))
	}
	registry.mu.Lock()
	registry.startLocked()
	registry.mu.Unlock()
	registry.wake <- struct{}{}
	<-registry.done
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.terminalCloses) != 0 || registry.terminalErr.count != 1025 || len(registry.terminalErr.samples) != maxRetainedCloseErrors {
		t.Fatalf("expired receipts=%d failures=%d samples=%d", len(registry.terminalCloses), registry.terminalErr.count, len(registry.terminalErr.samples))
	}
	if !errors.Is(registry.err, syscall.ENOTEMPTY) || !errors.Is(registry.err, syscall.EIO) {
		t.Fatalf("close failure classification lost: %v", registry.err)
	}
	if got := registry.err.Error(); len(got) > 4096 {
		t.Fatalf("close failure report is unbounded: %d bytes", len(got))
	}
}
