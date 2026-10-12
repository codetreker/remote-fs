package httprest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type serverTransferBackend struct {
	*objectstore.Storage
	mu                sync.Mutex
	checkErr          error
	beginCalls        int
	resumeCalls       int
	cancelImportCalls int
	cancelImportErr   error
	exportCalls       int
	importer          *serverTransferImport
	exporter          *serverTransferExport
	beginEntered      chan struct{}
	beginResume       chan struct{}
}

func (s *serverTransferBackend) CheckFileTransfer() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkErr
}

func (s *serverTransferBackend) BeginImport(ctx context.Context, command storage.ImportCommand) (storage.FileImport, error) {
	s.mu.Lock()
	s.beginCalls++
	s.importer.mu.Lock()
	s.importer.command = command
	s.importer.receipt = storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionPending}
	s.importer.mu.Unlock()
	entered, resume := s.beginEntered, s.beginResume
	s.mu.Unlock()
	if entered != nil {
		close(entered)
		select {
		case <-resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.importer, nil
}

func (s *serverTransferBackend) ResumeImport(_ context.Context, action storage.ActionKey) (storage.FileImport, error) {
	s.mu.Lock()
	s.resumeCalls++
	s.mu.Unlock()
	s.importer.mu.Lock()
	defer s.importer.mu.Unlock()
	if !s.importer.sealed {
		return nil, syscall.EBUSY
	}
	if action != s.importer.command.Action {
		return nil, syscall.EINVAL
	}
	return s.importer, nil
}

func (s *serverTransferBackend) QueryImport(context.Context, storage.ActionKey) (storage.ImportReceipt, error) {
	s.importer.mu.Lock()
	defer s.importer.mu.Unlock()
	return s.importer.receipt, nil
}

func (s *serverTransferBackend) CancelImport(ctx context.Context, action storage.ActionKey) (storage.ImportReceipt, error) {
	s.mu.Lock()
	s.cancelImportCalls++
	command := s.importer.command
	forcedErr := s.cancelImportErr
	s.mu.Unlock()
	if forcedErr != nil {
		return storage.ImportReceipt{}, forcedErr
	}
	pending := storage.ImportReceipt{Action: action, Fingerprint: command.Fingerprint, Outcome: storage.DurableActionPending}
	if err := ctx.Err(); err != nil {
		return pending, err
	}
	if s.importer.receiving.Load() {
		return storage.ImportReceipt{}, syscall.EBUSY
	}
	s.importer.mu.Lock()
	defer s.importer.mu.Unlock()
	if action != s.importer.command.Action {
		return storage.ImportReceipt{}, syscall.EINVAL
	}
	if s.importer.receipt.Outcome.Terminal() {
		return s.importer.receipt, nil
	}
	if !s.importer.sealed {
		return storage.ImportReceipt{}, syscall.EBUSY
	}
	if s.importer.unknownCancel.Load() {
		return storage.ImportReceipt{Action: action, Fingerprint: s.importer.command.Fingerprint, Outcome: storage.DurableActionUnknown}, syscall.EIO
	}
	s.importer.sealed = false
	s.importer.receipt = storage.ImportReceipt{Action: action, Fingerprint: s.importer.command.Fingerprint, Outcome: storage.DurableActionNotExecuted, Failure: syscall.ECANCELED}
	return s.importer.receipt, nil
}

func (s *serverTransferBackend) ExportFile(context.Context, storage.ExportCommand) (storage.FileExport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exportCalls++
	return s.exporter, nil
}

type serverTransferImport struct {
	mu                sync.Mutex
	command           storage.ImportCommand
	sealed            bool
	sealOverride      *storage.ImportSeal
	receiveErr        error
	receiveCalls      int
	publishCalls      int
	receipt           storage.ImportReceipt
	publish           func(context.Context) (storage.Attr, error)
	unknownCancel     atomic.Bool
	blockCancel       atomic.Bool
	cancelCalls       atomic.Int64
	detachCalls       atomic.Int64
	settlementUnknown atomic.Bool
	receiving         atomic.Bool
}

func (f *serverTransferImport) Receive(ctx context.Context, reader io.Reader) (storage.ImportSeal, error) {
	f.receiving.Store(true)
	defer f.receiving.Store(false)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receiveCalls++
	if f.receiveErr != nil {
		return storage.ImportSeal{}, f.receiveErr
	}
	seal, err := storage.CopyTransferContent(ctx, io.Discard, reader, f.command.DeclaredBytes, f.command.Digest)
	if err == nil {
		f.sealed = true
		f.receipt = storage.ImportReceipt{Action: f.command.Action, Fingerprint: f.command.Fingerprint, Outcome: storage.DurableActionPending}
		if f.sealOverride != nil {
			seal = *f.sealOverride
		}
	}
	return seal, err
}

func (f *serverTransferImport) Publish(ctx context.Context) (storage.ImportReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishCalls++
	if f.receipt.Outcome.Terminal() {
		return f.receipt, nil
	}
	if !f.sealed {
		return storage.ImportReceipt{}, syscall.EIO
	}
	attr := storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: f.command.DeclaredBytes}
	if f.publish != nil {
		var err error
		attr, err = f.publish(ctx)
		if err != nil {
			return storage.ImportReceipt{}, err
		}
	}
	f.receipt = storage.ImportReceipt{Action: f.command.Action, Fingerprint: f.command.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: attr}
	return f.receipt, nil
}

func (f *serverTransferImport) Cancel(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.cancelCalls.Add(1)
	if f.blockCancel.Load() {
		<-ctx.Done()
		return storage.ReferenceCloseResult{}, ctx.Err()
	}
	if f.unknownCancel.Load() {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	if f.settlementUnknown.Load() {
		return storage.ReferenceCloseResult{Determined: true, Released: true}, &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}
	}
	f.mu.Lock()
	if f.receipt.Outcome != storage.DurableActionCompleted {
		f.sealed = false
		f.receipt = storage.ImportReceipt{Action: f.command.Action, Fingerprint: f.command.Fingerprint, Outcome: storage.DurableActionNotExecuted}
	}
	f.mu.Unlock()
	return storage.ReferenceCloseResult{Determined: true, Released: true}, nil
}

func (f *serverTransferImport) Detach(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.detachCalls.Add(1)
	if f.blockCancel.Load() {
		<-ctx.Done()
		return storage.ReferenceCloseResult{}, ctx.Err()
	}
	if f.unknownCancel.Load() {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	if f.settlementUnknown.Load() {
		return storage.ReferenceCloseResult{Determined: true, Released: true}, &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}
	}
	return storage.ReferenceCloseResult{Determined: true, Released: true}, nil
}

type serverTransferExport struct {
	mu                sync.Mutex
	info              storage.ExportInfo
	content           []byte
	copyBytes         int
	corrupt           bool
	writeErr          error
	unknownClose      atomic.Bool
	closeCalls        atomic.Int64
	settlementUnknown atomic.Bool
}

func (f *serverTransferExport) Info() storage.ExportInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.info
}

func (f *serverTransferExport) WriteTo(ctx context.Context, writer io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	content := f.content
	if f.copyBytes >= 0 {
		content = content[:f.copyBytes]
	}
	for len(content) != 0 {
		chunk := content[:min(len(content), 64<<10)]
		if n, err := writer.Write(chunk); err != nil {
			return err
		} else if n != len(chunk) {
			return io.ErrShortWrite
		}
		content = content[len(chunk):]
	}
	f.info.Digest = sha256.Sum256(f.content)
	f.info.DigestAvailable = true
	if f.corrupt {
		f.info.Digest[0] ^= 1
	}
	return f.writeErr
}

func (f *serverTransferExport) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	f.closeCalls.Add(1)
	if f.unknownClose.Load() {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	if f.settlementUnknown.Load() {
		return storage.ReferenceCloseResult{Determined: true, Released: true}, &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}
	}
	return storage.ReferenceCloseResult{Determined: true, Released: true}, nil
}

type serverTransferHarness struct {
	h       *Handler
	backend *serverTransferBackend
	ctx     context.Context
}

func newServerTransferHarness(t *testing.T, authorize authz.Authorizer) *serverTransferHarness {
	t.Helper()
	return newServerTransferHarnessWithLimits(t, authorize, nil)
}

func newServerTransferHarnessWithLimits(t *testing.T, authorize authz.Authorizer, tune func(*TransferOptions)) *serverTransferHarness {
	t.Helper()
	meta, backend := memoryfixture.New(t, "transfer-server", 1<<20, locking.DefaultOptions())
	importer := &serverTransferImport{}
	exporter := &serverTransferExport{content: []byte("private captured bytes"), copyBytes: -1}
	exporter.info.Attr = storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: int64(len(exporter.content))}
	fake := &serverTransferBackend{Storage: backend, importer: importer, exporter: exporter}
	spool := t.TempDir()
	if err := os.Chmod(spool, 0700); err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.Authorizer = authorize
	if authorize != nil {
		options.Volume = "transfer-server"
	}
	options.Transfers = TransferOptions{Enabled: true, SpoolDir: spool, MaxBytes: 1 << 20, MaxConcurrent: 1, Lifetime: time.Minute,
		ActionNamespace: func(context.Context, storage.ActionKey) error { return nil },
	}
	if tune != nil {
		tune(&options.Transfers)
	}
	handler, err := NewHandlerWithOptions(fake, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		importer.unknownCancel.Store(false)
		importer.blockCancel.Store(false)
		importer.settlementUnknown.Store(false)
		exporter.unknownClose.Store(false)
		exporter.settlementUnknown.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return &serverTransferHarness{h: handler, backend: fake, ctx: ctx}
}

type serverTransferRecorder struct{ *httptest.ResponseRecorder }

func (*serverTransferRecorder) SetReadDeadline(time.Time) error  { return nil }
func (*serverTransferRecorder) SetWriteDeadline(time.Time) error { return nil }

func serverTransferRequest(h *Handler, ctx context.Context, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransfer), bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", contentJSON)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func serverTransferData(h *Handler, ctx context.Context, token string, content []byte, length int64) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferData)+"?path="+token, bytes.NewReader(content)).WithContext(ctx)
	r.Header.Set("Content-Type", contentOctets)
	r.ContentLength = length
	w := &serverTransferRecorder{httptest.NewRecorder()}
	h.ServeHTTP(w, r)
	return w.ResponseRecorder
}

func serverTransferBegin(t *testing.T, h *serverTransferHarness, content []byte) transferResponse {
	t.Helper()
	command := transferClientCommand(content)
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "begin", Command: &command})
	if err != nil || result.Token == "" {
		t.Fatalf("begin=%+v err=%v", result, err)
	}
	return result
}

func serverTransferAwait(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("transfer responsibility did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

func serverTransfersSettled(h *serverTransferHarness) bool {
	h.h.transfers.mu.Lock()
	entries := make([]*servedTransfer, 0, len(h.h.transfers.entries))
	for _, entry := range h.h.transfers.entries {
		entries = append(entries, entry)
	}
	h.h.transfers.mu.Unlock()
	for _, entry := range entries {
		entry.mu.Lock()
		settled := entry.settled && entry.importFile == nil && entry.exportFile == nil && entry.spool == nil && entry.spoolPath == ""
		entry.mu.Unlock()
		if !settled {
			return false
		}
	}
	spool, err := os.ReadDir(h.h.transfers.options.SpoolDir)
	return err == nil && len(spool) == 0
}

func TestTransferServerPreflightAndMalformedCommandsDoNotAdmitWork(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	h.backend.mu.Lock()
	h.backend.checkErr = syscall.EOPNOTSUPP
	h.backend.mu.Unlock()
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "check"}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("backing-chain failure hidden: %v", err)
	}
	command := transferClientCommand([]byte("abc"))
	valid, err := json.Marshal(transferRequest{Op: "begin", Command: &command})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"missing command":     []byte(`{"op":"begin"}`),
		"unrelated operand":   []byte(`{"op":"check","token":"` + strings.Repeat("a", 64) + `"}`),
		"duplicate operation": []byte(`{"op":"check","op":"begin"}`),
		"unknown operation":   []byte(`{"op":"unknown"}`),
		"unknown field":       []byte(`{"op":"check","trusted":true}`),
	} {
		t.Run(name, func(t *testing.T) {
			response := serverTransferRequest(h.h, h.ctx, body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	for _, field := range []string{"Fingerprint", "Digest"} {
		t.Run("short "+field, func(t *testing.T) {
			var shape map[string]any
			if err := json.Unmarshal(valid, &shape); err != nil {
				t.Fatal(err)
			}
			shape["command"].(map[string]any)[field] = []any{1, 2}
			body, err := json.Marshal(shape)
			if err != nil {
				t.Fatal(err)
			}
			response := serverTransferRequest(h.h, h.ctx, body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("short fixed array admitted: status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	h.backend.mu.Lock()
	defer h.backend.mu.Unlock()
	if h.backend.beginCalls != 0 || !serverTransfersSettled(h) {
		t.Fatal("malformed request reached native admission")
	}
}

func TestTransferServerRejectsStolenAccessWithoutDamagingOriginalTransfer(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("owned bytes")
	begin := serverTransferBegin(t, h, content)
	other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"publish", "cancel"} {
		if _, err := h.h.transferControl(other, transferRequest{Op: op, Token: begin.Token}); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("stolen %s=%v", op, err)
		}
	}
	response := serverTransferData(h.h, other, begin.Token, content, int64(len(content)))
	if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EACCES")) {
		t.Fatalf("stolen stream status=%d body=%s", response.Code, response.Body.String())
	}
	h.backend.importer.mu.Lock()
	if h.backend.importer.receiveCalls != 0 || h.backend.importer.publishCalls != 0 {
		t.Fatal("stolen capability reached native transfer")
	}
	h.backend.importer.mu.Unlock()
	response = serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content)))
	if response.Code != http.StatusOK {
		t.Fatalf("original receive=%d %s", response.Code, response.Body.String())
	}
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "publish", Token: begin.Token})
	if err != nil || result.Receipt == nil || result.Receipt.Outcome != storage.DurableActionCompleted || result.Barrier == nil {
		t.Fatalf("original publish=%+v err=%v", result, err)
	}
	if !serverTransfersSettled(h) {
		t.Fatal("completed import retained its native handle")
	}
}

func TestTransferServerFailedReceiveCannotPublish(t *testing.T) {
	for _, failure := range []string{"body short", "native failure", "bad native seal", "header length"} {
		t.Run(failure, func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			content := []byte("three")
			begin := serverTransferBegin(t, h, content)
			body, length := content, int64(-1)
			switch failure {
			case "body short":
				body = content[:2]
			case "native failure":
				h.backend.importer.receiveErr = syscall.EIO
			case "bad native seal":
				h.backend.importer.sealOverride = &storage.ImportSeal{Bytes: 1}
			case "header length":
				length = 2
			}
			response := serverTransferData(h.h, h.ctx, begin.Token, body, length)
			if response.Code == http.StatusOK {
				t.Fatalf("failed receive acknowledged: %s", response.Body.String())
			}
			if failure == "header length" {
				h.backend.importer.mu.Lock()
				calls := h.backend.importer.receiveCalls
				h.backend.importer.mu.Unlock()
				if calls != 0 {
					t.Fatal("known length mismatch reached native receive")
				}
				if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "cancel", Token: begin.Token}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "publish", Token: begin.Token}); err == nil {
					t.Fatal("failed receive permitted publication")
				}
				h.backend.importer.mu.Lock()
				calls := h.backend.importer.publishCalls
				h.backend.importer.mu.Unlock()
				if calls != 0 {
					t.Fatal("failed receive reached native publication")
				}
			}
		})
	}
}

type revokingTransferReader struct {
	reader  io.Reader
	revoked *atomic.Bool
}

func (r revokingTransferReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.revoked.Store(true)
	}
	return n, err
}

func TestTransferServerReauthorizesEachUploadChunk(t *testing.T) {
	var revoked atomic.Bool
	h := newServerTransferHarness(t, authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		if revoked.Load() {
			return authz.ErrDenied
		}
		return nil
	}))
	content := bytes.Repeat([]byte("x"), 128<<10)
	begin := serverTransferBegin(t, h, content)
	r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferData)+"?path="+begin.Token,
		io.NopCloser(revokingTransferReader{reader: bytes.NewReader(content), revoked: &revoked})).WithContext(h.ctx)
	r.Header.Set("Content-Type", contentOctets)
	w := &serverTransferRecorder{httptest.NewRecorder()}
	h.h.ServeHTTP(w, r)
	if w.Code != StatusStorageError || !bytes.Contains(w.Body.Bytes(), []byte("EACCES")) {
		t.Fatalf("revoked upload status=%d body=%s", w.Code, w.Body.String())
	}
	h.backend.importer.mu.Lock()
	defer h.backend.importer.mu.Unlock()
	if h.backend.importer.sealed || h.backend.importer.publishCalls != 0 {
		t.Fatal("revoked upload became publishable")
	}
}

func TestTransferServerExportVerifiesCompleteContentBeforeReturningCapability(t *testing.T) {
	for _, failure := range []string{"digest", "short copy", "copy error"} {
		t.Run(failure, func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			switch failure {
			case "digest":
				h.backend.exporter.corrupt = true
			case "short copy":
				h.backend.exporter.copyBytes = 3
			case "copy error":
				h.backend.exporter.writeErr = syscall.EIO
			}
			command := storage.ExportCommand{NodeID: 2}
			body, err := json.Marshal(transferRequest{Op: "export", Export: &command})
			if err != nil {
				t.Fatal(err)
			}
			response := serverTransferRequest(h.h, h.ctx, body)
			if response.Code == http.StatusOK || bytes.Contains(response.Body.Bytes(), h.backend.exporter.content) || bytes.Contains(response.Body.Bytes(), []byte(`"token"`)) {
				t.Fatalf("unverified bytes exposed: status=%d body=%s", response.Code, response.Body.String())
			}
			if !serverTransfersSettled(h) || h.backend.exporter.closeCalls.Load() != 1 {
				t.Fatal("failed export leaked its staging responsibility")
			}
			entries, err := os.ReadDir(h.h.transfers.options.SpoolDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("spool=%v err=%v", entries, err)
			}
		})
	}
}

func TestTransferServerUnknownExportCleanupRetainsResponsibilityUntilConfirmed(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	h.backend.exporter.unknownClose.Store(true)
	exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
	if err != nil || exported.Token == "" {
		t.Fatalf("export=%+v err=%v", exported, err)
	}
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "close-export", Token: exported.Token})
	if err != nil || result.Error == nil || result.Close == nil || result.Close.Determined || result.Close.Released {
		t.Fatalf("unknown cleanup=%+v err=%v", result, err)
	}
	entry, err := h.h.transfers.lookup(h.ctx, exported.Token)
	if err != nil {
		t.Fatal(err)
	}
	entry.mu.Lock()
	retained := !entry.settled && entry.exportFile == h.backend.exporter && entry.spool != nil && entry.spoolPath != ""
	entry.mu.Unlock()
	if !retained || h.h.transfers.idle() {
		t.Fatal("unknown cleanup discarded its native handle or spool")
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retirement=%v", err)
	}
	closeCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := h.h.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded close=%v", err)
	}
	if serverTransfersSettled(h) {
		t.Fatal("close timeout lost cleanup ownership")
	}
	h.backend.exporter.unknownClose.Store(false)
	serverTransferAwait(t, func() bool { return h.h.transfers.idle() })
	if err := h.h.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !serverTransfersSettled(h) {
		t.Fatal("confirmed cleanup retained native handles or spool")
	}
}

func TestTransferServerCompletedImportPublishesBarrierAndWakesObservers(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	woken, release, err := h.h.publisher.attach()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	content := []byte("committed bytes")
	h.backend.importer.publish = func(ctx context.Context) (storage.Attr, error) {
		if err := h.backend.Storage.Create(ctx, "file"); err != nil {
			return storage.Attr{}, err
		}
		if err := h.backend.Storage.Write(ctx, "file", content); err != nil {
			return storage.Attr{}, err
		}
		return h.backend.Storage.Stat(ctx, "file")
	}
	begin := serverTransferBegin(t, h, content)
	response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content)))
	if response.Code != http.StatusOK {
		t.Fatalf("receive=%d %s", response.Code, response.Body.String())
	}
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "publish", Token: begin.Token})
	if err != nil || result.Barrier == nil || result.Barrier.Position == 0 {
		t.Fatalf("publication=%+v err=%v", result, err)
	}
	select {
	case <-woken:
	case <-time.After(time.Second):
		t.Fatal("completed import did not wake existing observers")
	}
	barrier, err := h.h.mutationBarrier(h.ctx)
	if err != nil || *barrier != *result.Barrier {
		t.Fatalf("receipt barrier=%+v current=%+v err=%v", result.Barrier, barrier, err)
	}
	actual, err := h.backend.Storage.Read(h.ctx, "file")
	if err != nil || !bytes.Equal(actual, content) {
		t.Fatalf("published bytes=%q err=%v", actual, err)
	}
}

type revokingTransferRecorder struct {
	*serverTransferRecorder
	revoked *atomic.Bool
}

func (w *revokingTransferRecorder) Write(content []byte) (int, error) {
	n, err := w.serverTransferRecorder.Write(content)
	if n > 0 {
		w.revoked.Store(true)
	}
	return n, err
}

func TestTransferServerExportRejectsStolenTokenAndStopsOnRevocation(t *testing.T) {
	var revoked atomic.Bool
	h := newServerTransferHarness(t, authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		if revoked.Load() {
			return authz.ErrDenied
		}
		return nil
	}))
	h.backend.exporter.content = bytes.Repeat([]byte("x"), 128<<10)
	h.backend.exporter.info.Attr.Size = int64(len(h.backend.exporter.content))
	exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	request := func(ctx context.Context) *http.Request {
		return httptest.NewRequest(http.MethodGet, Prefix+string(OpFileTransferExport)+"?path="+exported.Token, nil).WithContext(ctx)
	}
	stolen := &serverTransferRecorder{httptest.NewRecorder()}
	h.h.ServeHTTP(stolen, request(other))
	if stolen.Code != StatusStorageError || !bytes.Contains(stolen.Body.Bytes(), []byte("EACCES")) {
		t.Fatalf("stolen export=%d %s", stolen.Code, stolen.Body.String())
	}
	if _, err := h.h.transferControl(other, transferRequest{Op: "close-export", Token: exported.Token}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("stolen export cleanup=%v", err)
	}
	out := &revokingTransferRecorder{serverTransferRecorder: &serverTransferRecorder{httptest.NewRecorder()}, revoked: &revoked}
	func() {
		defer func() {
			if failure := recover(); failure != http.ErrAbortHandler {
				t.Fatalf("revoked output must abort framing, got %v", failure)
			}
		}()
		h.h.ServeHTTP(out, request(h.ctx))
	}()
	if out.Body.Len() != 64<<10 || out.Header().Get("Content-Length") != "131072" {
		t.Fatalf("revoked output silently completed: bytes=%d length=%s", out.Body.Len(), out.Header().Get("Content-Length"))
	}
	if !serverTransfersSettled(h) {
		t.Fatal("aborted output retained confirmed released export resources")
	}
	result, err := h.h.transferControl(h.ctx, transferRequest{Op: "close-export", Token: exported.Token})
	if err != nil || result.Error != nil || !result.Close.Released {
		t.Fatalf("owned export cleanup=%+v err=%v", result, err)
	}
}

func TestTransferServerUnknownReceiptsAlwaysExposeFailure(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	command := transferClientCommand([]byte("pending"))
	woken, release, err := h.h.publisher.attach()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, outcome := range []storage.DurableActionOutcome{storage.DurableActionUnknown, storage.DurableActionHistoryExpired} {
		h.backend.importer.mu.Lock()
		h.backend.importer.receipt = storage.ImportReceipt{Action: command.Action, Fingerprint: command.Fingerprint, Outcome: outcome}
		h.backend.importer.mu.Unlock()
		response, err := h.h.transferControl(h.ctx, transferRequest{Op: "query", Action: &command.Action})
		if err != nil || response.Receipt == nil || response.Error == nil || response.Error.Errno != "EIO" || response.Barrier != nil {
			t.Fatalf("unknown receipt=%+v err=%v", response, err)
		}
		select {
		case <-woken:
			t.Fatal("unknown receipt announced a committed mutation")
		default:
		}
	}
}

func TestTransferServerConcurrentBeginRetainsOneBoundAdmission(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	command := transferClientCommand([]byte("same command"))
	start := make(chan struct{})
	results := make(chan transferResponse, 16)
	failures := make(chan error, 16)
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			result, err := h.h.transferControl(h.ctx, transferRequest{Op: "begin", Command: &command})
			results <- result
			failures <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var token string
	for result := range results {
		if token == "" {
			token = result.Token
		}
		if result.Token == "" || result.Token != token {
			t.Fatal("same begin produced independent transfer admissions")
		}
	}
	other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.h.transferControl(other, transferRequest{Op: "begin", Command: &command}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("another access inherited begin result: %v", err)
	}
	changed := command
	changed.DeclaredBytes++
	changed.Fingerprint = changed.EffectFingerprint()
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "begin", Command: &changed}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("same action changed intent: %v", err)
	}
	h.backend.mu.Lock()
	defer h.backend.mu.Unlock()
	if h.backend.beginCalls != 1 || serverTransfersSettled(h) {
		t.Fatal("begin replay duplicated native staging or lost its registry entry")
	}
}

func TestTransferServerReauthorizesWhileBuildingVerifiedExport(t *testing.T) {
	var decisions atomic.Int64
	h := newServerTransferHarness(t, authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == opExport && decisions.Add(1) > 2 {
			return authz.ErrDenied
		}
		return nil
	}))
	h.backend.exporter.content = bytes.Repeat([]byte("x"), 128<<10)
	h.backend.exporter.info.Attr.Size = int64(len(h.backend.exporter.content))
	command := storage.ExportCommand{NodeID: 2}
	body, err := json.Marshal(transferRequest{Op: "export", Export: &command})
	if err != nil {
		t.Fatal(err)
	}
	response := serverTransferRequest(h.h, h.ctx, body)
	if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EACCES")) || bytes.Contains(response.Body.Bytes(), []byte(`"token"`)) {
		t.Fatalf("revoked export staging=%d %s", response.Code, response.Body.String())
	}
	if decisions.Load() != 3 || !serverTransfersSettled(h) {
		t.Fatal("staging revocation was ignored or leaked retained resources")
	}
}

func TestTransferServerRetirementDetachesSealedImportForDurableResume(t *testing.T) {
	for _, cause := range []string{"expiry", "handler stop"} {
		t.Run(cause, func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			content := []byte("sealed durable bytes")
			command := transferClientCommand(content)
			begin := serverTransferBegin(t, h, content)
			response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content)))
			if response.Code != http.StatusOK {
				t.Fatalf("seal=%d %s", response.Code, response.Body.String())
			}
			switch cause {
			case "expiry":
				entry, err := h.h.transfers.lookup(h.ctx, begin.Token)
				if err != nil {
					t.Fatal(err)
				}
				entry.mu.Lock()
				entry.expires = time.Now().Add(-time.Second)
				entry.mu.Unlock()
			case "handler stop":
				if err := h.h.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			serverTransferAwait(t, func() bool { return h.h.transfers.idle() && serverTransfersSettled(h) })
			if h.backend.importer.detachCalls.Load() != 1 || h.backend.importer.cancelCalls.Load() != 0 {
				t.Fatal("automatic retirement canceled durable staged work")
			}
			h.backend.importer.mu.Lock()
			sealed := h.backend.importer.sealed
			h.backend.importer.mu.Unlock()
			if !sealed {
				t.Fatal("detach discarded sealed durable content")
			}
			other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
			if err != nil {
				t.Fatal(err)
			}
			active := h.h
			if cause == "handler stop" {
				options := DefaultHandlerOptions()
				options.Transfers = h.h.transfers.options
				active, err = NewHandlerWithOptions(h.backend, h.h.log, options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := active.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			resumed, err := active.transferControl(other, transferRequest{Op: "resume", Action: &command.Action})
			if err != nil || resumed.Token == "" || resumed.Token == begin.Token {
				t.Fatalf("resume=%+v err=%v", resumed, err)
			}
			if cause == "expiry" {
				for range 8 {
					replayed, err := active.transferControl(other, transferRequest{Op: "begin", Command: &command})
					if err != nil || replayed.Token != resumed.Token || replayed.DeclaredBytes == nil || *replayed.DeclaredBytes != command.DeclaredBytes {
						t.Fatalf("retained old history interfered with live resumed action: %+v, %v", replayed, err)
					}
				}
			}
			if _, err := active.transferControl(h.ctx, transferRequest{Op: "publish", Token: resumed.Token}); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("old access inherited resumed capability: %v", err)
			}
			published, err := active.transferControl(other, transferRequest{Op: "publish", Token: resumed.Token})
			if err != nil || published.Receipt == nil || published.Receipt.Outcome != storage.DurableActionCompleted || published.Barrier == nil {
				t.Fatalf("resumed publication=%+v err=%v", published, err)
			}
		})
	}
}

func TestTransferServerExplicitCancelAbandonsSealedWork(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("explicit cancellation")
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	response, err := h.h.transferControl(h.ctx, transferRequest{Op: "cancel", Token: begin.Token})
	if err != nil || response.Error != nil || response.Close == nil || !response.Close.Released {
		t.Fatalf("cancel=%+v err=%v", response, err)
	}
	if h.backend.importer.cancelCalls.Load() != 1 || h.backend.importer.detachCalls.Load() != 0 {
		t.Fatal("explicit cancellation used detach")
	}
	h.backend.importer.mu.Lock()
	defer h.backend.importer.mu.Unlock()
	if h.backend.importer.sealed || h.backend.importer.receipt.Outcome != storage.DurableActionNotExecuted {
		t.Fatal("explicit cancel preserved publishable work")
	}
}

func TestTransferServerReleasedFactCannotBypassUnknownNativeSettlement(t *testing.T) {
	for _, kind := range []string{"import", "export"} {
		t.Run(kind, func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			var token, op string
			var unknown *atomic.Bool
			switch kind {
			case "import":
				token, op, unknown = serverTransferBegin(t, h, []byte("pending")).Token, "cancel", &h.backend.importer.settlementUnknown
			case "export":
				response, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
				if err != nil {
					t.Fatal(err)
				}
				token, op, unknown = response.Token, "close-export", &h.backend.exporter.settlementUnknown
			}
			unknown.Store(true)
			response, err := h.h.transferControl(h.ctx, transferRequest{Op: op, Token: token})
			if err != nil || response.Error == nil || response.Close == nil {
				t.Fatalf("unknown native settlement=%+v err=%v", response, err)
			}
			if h.h.transfers.idle() || serverTransfersSettled(h) {
				t.Fatal("released native fact returned unsettled responsibility")
			}
			if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("unknown settlement retirement=%v", err)
			}
			unknown.Store(false)
			serverTransferAwait(t, func() bool {
				return serverTransfersSettled(h)
			})
		})
	}
}

func TestTransferServerCloseReplyIsReplayableWithinItsOriginalAccess(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	request := transferRequest{Op: "close-export", Token: exported.Token}
	first, err := h.h.transferControl(h.ctx, request)
	if err != nil || first.Close == nil || first.Error != nil || !first.Close.Released {
		t.Fatalf("first close=%+v err=%v", first, err)
	}
	second, err := h.h.transferControl(h.ctx, request)
	if err != nil || !reflect.DeepEqual(first, second) || h.backend.exporter.closeCalls.Load() != 1 {
		t.Fatalf("lost-reply replay changed result: first=%+v second=%+v err=%v", first, second, err)
	}
	other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.h.transferControl(other, request); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("another access obtained terminal receipt: %v", err)
	}
	if !serverTransfersSettled(h) {
		t.Fatal("terminal replay retained data resources")
	}
	if err := h.h.ReserveRetirement(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("volatile export history lost its authority: %v", err)
	}
}

func TestTransferServerTerminalCapacityRejectsBeforeNativeWorkAndReusesExpiredSlots(t *testing.T) {
	h := newServerTransferHarnessWithLimits(t, nil, func(options *TransferOptions) { options.TerminalResults = 1 })
	export := transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}}
	first, err := h.h.transferControl(h.ctx, export)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "close-export", Token: first.Token}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.h.transferControl(h.ctx, export); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("full terminal history admission=%v", err)
	}
	h.backend.mu.Lock()
	calls := h.backend.exportCalls
	h.backend.mu.Unlock()
	if calls != 1 || !serverTransfersSettled(h) {
		t.Fatal("terminal exhaustion reached native work or leaked new admission")
	}
	entry, err := h.h.transfers.lookup(h.ctx, first.Token)
	if err != nil {
		t.Fatal(err)
	}
	entry.mu.Lock()
	entry.expires = time.Now().Add(-time.Second)
	entry.mu.Unlock()
	serverTransferAwait(t, func() bool {
		h.h.transfers.mu.Lock()
		defer h.h.transfers.mu.Unlock()
		return len(h.h.transfers.entries) == 0
	})
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "close-export", Token: first.Token}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired history fabricated result: %v", err)
	}
	second, err := h.h.transferControl(h.ctx, export)
	if err != nil || second.Token == first.Token {
		t.Fatalf("reused history slot=%+v err=%v", second, err)
	}
}

type failingTransferRecorder struct{ *serverTransferRecorder }

func (*failingTransferRecorder) Write([]byte) (int, error) { return 0, syscall.EIO }

func TestTransferServerGETConsumesExportAndSettlesAllKnownResources(t *testing.T) {
	for _, size := range []int{0, 128 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			content := bytes.Repeat([]byte("x"), size)
			h.backend.exporter.content, h.backend.exporter.info.Attr.Size = content, int64(size)
			exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
			if err != nil {
				t.Fatal(err)
			}
			request := func() *http.Request {
				return httptest.NewRequest(http.MethodGet, Prefix+string(OpFileTransferExport)+"?path="+exported.Token, nil).WithContext(h.ctx)
			}
			first := &serverTransferRecorder{httptest.NewRecorder()}
			h.h.ServeHTTP(first, request())
			if first.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), content) {
				t.Fatalf("GET=%d bytes=%d", first.Code, first.Body.Len())
			}
			if !serverTransfersSettled(h) || h.backend.exporter.closeCalls.Load() != 1 {
				t.Fatal("completed GET retained native handle or spool")
			}
			second := &serverTransferRecorder{httptest.NewRecorder()}
			h.h.ServeHTTP(second, request())
			if second.Code != StatusStorageError || !bytes.Contains(second.Body.Bytes(), []byte("ESTALE")) {
				t.Fatalf("consumed GET reused bytes: %d %s", second.Code, second.Body.String())
			}
			closed, err := h.h.transferControl(h.ctx, transferRequest{Op: "close-export", Token: exported.Token})
			if err != nil || closed.Close == nil || !closed.Close.Released || h.backend.exporter.closeCalls.Load() != 1 {
				t.Fatalf("GET cleanup replay=%+v err=%v", closed, err)
			}
		})
	}
}

func TestTransferServerFailedGETKeepsUnconfirmedCleanupResponsibility(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			h.backend.exporter.unknownClose.Store(unknown)
			exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, Prefix+string(OpFileTransferExport)+"?path="+exported.Token, nil).WithContext(h.ctx)
			func() {
				defer func() {
					if recovered := recover(); recovered != http.ErrAbortHandler {
						t.Fatalf("failed GET framing did not abort: %v", recovered)
					}
				}()
				h.h.ServeHTTP(&failingTransferRecorder{&serverTransferRecorder{httptest.NewRecorder()}}, request)
			}()
			if unknown {
				if serverTransfersSettled(h) {
					t.Fatal("unknown failed-GET cleanup discarded responsibility")
				}
			} else if !serverTransfersSettled(h) {
				t.Fatal("known failed-GET cleanup retained staging")
			}
			h.backend.exporter.unknownClose.Store(false)
			serverTransferAwait(t, func() bool { return serverTransfersSettled(h) })
		})
	}
}

func TestTransferServerProtectedCleanupSurvivesSaturatedDataAdmission(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	exported, err := h.h.transferControl(h.ctx, transferRequest{Op: "export", Export: &storage.ExportCommand{NodeID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	releaseResponse, err := h.h.responses.acquire(t.Context(), h.h.responses.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseResponse()
	releaseBody, err := h.h.bodies.acquire(t.Context(), h.h.bodies.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBody()
	body, err := json.Marshal(transferRequest{Op: "close-export", Token: exported.Token})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferControl), bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", contentJSON)
	response := httptest.NewRecorder()
	h.h.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("protected cleanup=%d %s", response.Code, response.Body.String())
	}
	var result transferResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Close == nil || !result.Close.Released || result.Error != nil {
		t.Fatalf("protected result=%+v err=%v", result, err)
	}
	if !serverTransfersSettled(h) {
		t.Fatal("protected cleanup did not release data responsibility")
	}
}

func TestTransferServerResumeRecoversSameAccessCleanupWithoutAnotherAdmission(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("sealed cleanup responsibility")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	h.backend.importer.unknownCancel.Store(true)
	closed, err := h.h.transferControl(h.ctx, transferRequest{Op: "detach", Token: begin.Token})
	if err != nil || closed.Error == nil {
		t.Fatalf("unknown detach=%+v err=%v", closed, err)
	}
	resumed, err := h.h.transferControl(h.ctx, transferRequest{Op: "resume", Action: &command.Action})
	if err != nil || resumed.Token != begin.Token || serverTransfersSettled(h) {
		t.Fatalf("cleanup resume=%+v err=%v", resumed, err)
	}
	h.backend.mu.Lock()
	calls := h.backend.resumeCalls
	h.backend.mu.Unlock()
	if calls != 0 {
		t.Fatal("same access resume acquired another native handle")
	}
	h.backend.importer.unknownCancel.Store(false)
	settled, err := h.h.transferControl(h.ctx, transferRequest{Op: "detach", Token: resumed.Token})
	if err != nil || settled.Error != nil || !settled.Close.Released || !serverTransfersSettled(h) {
		t.Fatalf("retried cleanup=%+v err=%v", settled, err)
	}
}

func TestTransferServerResumeCannotReplaceAnUnsealedSameAccessAdmission(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("unfinished receive")
	command := transferClientCommand(content)
	serverTransferBegin(t, h, content)
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "resume", Action: &command.Action}); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unsealed resume=%v", err)
	}
	h.backend.mu.Lock()
	calls := h.backend.resumeCalls
	h.backend.mu.Unlock()
	if calls != 0 || serverTransfersSettled(h) {
		t.Fatal("unsealed resume duplicated native ownership or lost its registry entry")
	}
}

func TestTransferServerBeginReplayAfterResumePreservesDeclaredCommand(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("restart sealed command")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if err := h.h.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.Transfers = h.h.transfers.options
	restarted, err := NewHandlerWithOptions(h.backend, h.h.log, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restarted.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	resumed, err := restarted.transferControl(h.ctx, transferRequest{Op: "resume", Action: &command.Action})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.transferControl(h.ctx, transferRequest{Op: "begin", Command: &command})
	if err != nil || replayed.Token != resumed.Token || replayed.DeclaredBytes == nil || *replayed.DeclaredBytes != command.DeclaredBytes {
		t.Fatalf("resumed begin replay=%+v err=%v", replayed, err)
	}
	changed := command
	changed.DeclaredBytes++
	changed.Fingerprint = changed.EffectFingerprint()
	if _, err := restarted.transferControl(h.ctx, transferRequest{Op: "begin", Command: &changed}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("resumed action changed intent: %v", err)
	}
	h.backend.mu.Lock()
	defer h.backend.mu.Unlock()
	if h.backend.beginCalls != 1 || h.backend.resumeCalls != 1 {
		t.Fatal("resumed begin replay created another native handle")
	}
}

func serverTransferProtectedControl(t *testing.T, h *Handler, ctx context.Context, command transferRequest) (*httptest.ResponseRecorder, transferResponse) {
	t.Helper()
	body, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferControl), bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", contentJSON)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var result transferResponse
	if response.Code == http.StatusOK {
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
	}
	return response, result
}

func TestTransferServerStableCancellationChecksSubjectBeforeNativeAccess(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	command := transferClientCommand([]byte("private action"))
	serverTransferBegin(t, h, []byte("private action"))
	other, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "other-subject", AccessID: "other-access"})
	if err != nil {
		t.Fatal(err)
	}
	response, _ := serverTransferProtectedControl(t, h.h, other, transferRequest{Op: "cancel-action", Action: &command.Action})
	if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EACCES")) {
		t.Fatalf("cross-subject cancel=%d %s", response.Code, response.Body.String())
	}
	h.backend.mu.Lock()
	defer h.backend.mu.Unlock()
	if h.backend.cancelImportCalls != 0 {
		t.Fatal("cross-subject cancellation queried or changed native action")
	}
}

func TestTransferServerStableCancellationUsesCurrentAccessWithoutInheritingToken(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("sealed action")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	current, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.h.transferControl(current, transferRequest{Op: "cancel", Token: begin.Token}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("new access inherited old token: %v", err)
	}
	response, canceled := serverTransferProtectedControl(t, h.h, current, transferRequest{Op: "cancel-action", Action: &command.Action})
	if response.Code != http.StatusOK || canceled.Receipt == nil || canceled.Error != nil || canceled.Token != "" || canceled.Receipt.Action != command.Action || canceled.Receipt.Fingerprint != command.Fingerprint || canceled.Receipt.Outcome != storage.DurableActionNotExecuted || canceled.Receipt.Failure != "ECANCELED" || canceled.Barrier != nil {
		t.Fatalf("stable cancellation=%d %+v %s", response.Code, canceled, response.Body.String())
	}
	if _, err := h.h.transferControl(current, transferRequest{Op: "publish", Token: begin.Token}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("stable cancellation exposed old volatile capability: %v", err)
	}
	if serverTransfersSettled(h) {
		t.Fatal("stable receipt falsely acknowledged old handle settlement")
	}
	if _, err := h.h.transferControl(h.ctx, transferRequest{Op: "detach", Token: begin.Token}); err != nil {
		t.Fatal(err)
	}
}

func TestTransferServerStableCancellationPreservesUnknownResponsibility(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("uncertain cancellation")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	h.backend.importer.unknownCancel.Store(true)
	response, unknown := serverTransferProtectedControl(t, h.h, h.ctx, transferRequest{Op: "cancel-action", Action: &command.Action})
	if response.Code != http.StatusOK || unknown.Receipt == nil || unknown.Receipt.Outcome != storage.DurableActionUnknown || unknown.Error == nil || unknown.Error.Errno != "EIO" || serverTransfersSettled(h) {
		t.Fatalf("unknown cancellation=%d %+v", response.Code, unknown)
	}
}

func TestTransferServerStableCancellationCannotAbandonLiveUnsealedWork(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("still receiving")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	response, blocked := serverTransferProtectedControl(t, h.h, h.ctx, transferRequest{Op: "cancel-action", Action: &command.Action})
	if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EBUSY")) || blocked.Receipt != nil || serverTransfersSettled(h) {
		t.Fatalf("live cancellation=%d %+v", response.Code, blocked)
	}
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatalf("refused cancellation damaged receiver: %d %s", response.Code, response.Body.String())
	}
}

func TestTransferServerStableCancellationPreservesCompletedPublicationAndBarrier(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("committed before cancellation")
	command := transferClientCommand(content)
	h.backend.importer.publish = func(ctx context.Context) (storage.Attr, error) {
		if err := h.backend.Storage.Create(ctx, "file"); err != nil {
			return storage.Attr{}, err
		}
		if err := h.backend.Storage.Write(ctx, "file", content); err != nil {
			return storage.Attr{}, err
		}
		return h.backend.Storage.Stat(ctx, "file")
	}
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	published, err := h.h.transferControl(h.ctx, transferRequest{Op: "publish", Token: begin.Token})
	if err != nil {
		t.Fatal(err)
	}
	current, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "access-b"})
	if err != nil {
		t.Fatal(err)
	}
	response, preserved := serverTransferProtectedControl(t, h.h, current, transferRequest{Op: "cancel-action", Action: &command.Action})
	if response.Code != http.StatusOK || preserved.Error != nil || preserved.Receipt == nil || !reflect.DeepEqual(preserved.Receipt, published.Receipt) || preserved.Barrier == nil || *preserved.Barrier != *published.Barrier {
		t.Fatalf("completed cancellation changed receipt/barrier: %d %+v", response.Code, preserved)
	}
	actual, err := h.backend.Storage.Read(h.ctx, "file")
	if err != nil || !bytes.Equal(actual, content) {
		t.Fatalf("cancellation rolled back committed bytes: %q, %v", actual, err)
	}
}

func TestTransferServerStableCancellationRejectsAStalledReceivePromptly(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("live stream")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	upload := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferData)+"?path="+begin.Token, reader).WithContext(h.ctx)
	upload.Header.Set("Content-Type", contentOctets)
	upload.ContentLength = int64(len(content))
	uploadResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := &serverTransferRecorder{httptest.NewRecorder()}
		h.h.ServeHTTP(response, upload)
		uploadResult <- response.ResponseRecorder
	}()
	serverTransferAwait(t, h.backend.importer.receiving.Load)
	body, err := json.Marshal(transferRequest{Op: "cancel-action", Action: &command.Action})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferControl), bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", contentJSON)
	controlResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		h.h.ServeHTTP(response, request)
		controlResult <- response
	}()
	var response *httptest.ResponseRecorder
	select {
	case response = <-controlResult:
	case <-time.After(250 * time.Millisecond):
		reader.CloseWithError(io.ErrClosedPipe)
		select {
		case <-controlResult:
		case <-time.After(time.Second):
			t.Fatal("cancellation remained blocked after receive ended")
		}
		t.Fatal("protected stable cancellation waited for a stalled receive")
	}
	var result transferResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EBUSY")) || result.Receipt != nil {
		t.Fatalf("receiving-owner cancellation=%d %+v", response.Code, result)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case upload := <-uploadResult:
		if upload.Code != http.StatusOK {
			t.Fatalf("refused cancellation disrupted receiver: %d %s", upload.Code, upload.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("live receive did not finish after refused cancellation")
	}
}

func TestTransferServerStableCancellationWaitHonorsDeadlineDuringPublication(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	content := []byte("publication in progress")
	command := transferClientCommand(content)
	begin := serverTransferBegin(t, h, content)
	if response := serverTransferData(h.h, h.ctx, begin.Token, content, int64(len(content))); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resumePublication := func() { resumeOnce.Do(func() { close(resume) }) }
	defer resumePublication()
	h.backend.importer.publish = func(context.Context) (storage.Attr, error) {
		close(entered)
		<-resume
		return storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: int64(len(content))}, nil
	}
	type publicationResult struct {
		response transferResponse
		err      error
	}
	published := make(chan publicationResult, 1)
	go func() {
		response, err := h.h.transferControl(h.ctx, transferRequest{Op: "publish", Token: begin.Token})
		published <- publicationResult{response: response, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("publication did not enter its native transition")
	}
	body, err := json.Marshal(transferRequest{Op: "cancel-action", Action: &command.Action})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileTransferControl), bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", contentJSON)
	canceled := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		h.h.ServeHTTP(response, request)
		canceled <- response
	}()
	select {
	case response := <-canceled:
		if response.Code != StatusStorageError || !bytes.Contains(response.Body.Bytes(), []byte("EIO")) {
			t.Fatalf("publication wait fabricated cancellation outcome: %d %s", response.Code, response.Body.String())
		}
	case <-time.After(250 * time.Millisecond):
		resumePublication()
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("cancel remained blocked after publication was released")
		}
		t.Fatal("stable cancellation ignored its deadline while publication held the action gate")
	}
	h.backend.mu.Lock()
	nativeCancels := h.backend.cancelImportCalls
	h.backend.mu.Unlock()
	if nativeCancels != 0 {
		t.Fatal("expired cancellation reached native mutation")
	}
	resumePublication()
	select {
	case result := <-published:
		if result.err != nil || result.response.Receipt == nil || result.response.Receipt.Outcome != storage.DurableActionCompleted || result.response.Barrier == nil {
			t.Fatalf("cancel deadline interrupted admitted publication: %+v, %v", result.response, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted publication did not finish")
	}
}

func TestTransferServerStableCancellationPreservesEarlyNativeErrorsWithoutReceipts(t *testing.T) {
	for _, failure := range []syscall.Errno{syscall.EBUSY, syscall.EACCES, syscall.EAGAIN} {
		t.Run(failure.Error(), func(t *testing.T) {
			h := newServerTransferHarness(t, nil)
			content := []byte("native refused cancellation")
			command := transferClientCommand(content)
			serverTransferBegin(t, h, content)
			h.backend.mu.Lock()
			h.backend.cancelImportErr = failure
			h.backend.mu.Unlock()
			response, result := serverTransferProtectedControl(t, h.h, h.ctx, transferRequest{Op: "cancel-action", Action: &command.Action})
			var reported ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &reported); err != nil {
				t.Fatal(err)
			}
			if response.Code != StatusStorageError || reported.Errno != storage.ErrnoNameOf(failure) || result.Receipt != nil || serverTransfersSettled(h) {
				t.Fatalf("early native failure was flattened or inferred: %d %+v %+v", response.Code, reported, result)
			}
		})
	}
}

func TestTransferServerBeginWaitHonorsDeadlineWithoutCancelingAdmittedProducer(t *testing.T) {
	h := newServerTransferHarness(t, nil)
	command := transferClientCommand([]byte("original producer"))
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	releaseProducer := func() { resumeOnce.Do(func() { close(resume) }) }
	defer releaseProducer()
	h.backend.mu.Lock()
	h.backend.beginEntered, h.backend.beginResume = entered, resume
	h.backend.mu.Unlock()
	type beginResult struct {
		response transferResponse
		err      error
	}
	first := make(chan beginResult, 1)
	go func() {
		response, err := h.h.transferControl(h.ctx, transferRequest{Op: "begin", Command: &command})
		first <- beginResult{response: response, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("original producer did not enter native admission")
	}
	ctx, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, err := h.h.transferControl(ctx, transferRequest{Op: "begin", Command: &command})
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("begin wait lost deadline: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		releaseProducer()
		select {
		case <-second:
		case <-time.After(time.Second):
			t.Fatal("begin remained blocked after producer was released")
		}
		t.Fatal("duplicate begin ignored its deadline while another producer was admitted")
	}
	h.backend.mu.Lock()
	calls := h.backend.beginCalls
	h.backend.mu.Unlock()
	h.h.transfers.mu.Lock()
	entries := len(h.h.transfers.entries)
	h.h.transfers.mu.Unlock()
	if calls != 1 || entries != 1 {
		t.Fatal("expired begin duplicated or released native admission")
	}
	releaseProducer()
	var admitted beginResult
	select {
	case admitted = <-first:
	case <-time.After(time.Second):
		t.Fatal("original producer did not finish admission")
	}
	if admitted.err != nil || admitted.response.Token == "" || admitted.response.DeclaredBytes == nil || *admitted.response.DeclaredBytes != command.DeclaredBytes {
		t.Fatalf("deadline damaged original producer: %+v, %v", admitted.response, admitted.err)
	}
	replayed, err := h.h.transferControl(h.ctx, transferRequest{Op: "begin", Command: &command})
	if err != nil || !reflect.DeepEqual(replayed, admitted.response) {
		t.Fatalf("original begin receipt changed after waiting cancellation: %+v, %v", replayed, err)
	}
}
