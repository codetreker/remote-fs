package httprest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	opImport storage.Operation = storage.OpFileImport
	opExport storage.Operation = storage.OpFileExport
)

func validateTransferRequest(req transferRequest) error {
	expected := transferRequest{Op: req.Op}
	switch req.Op {
	case "check":
	case "begin":
		if req.Command == nil {
			return syscall.EINVAL
		}
		expected.Command = req.Command
		if err := req.Command.Check(); err != nil {
			return err
		}
	case "resume", "query", "cancel-action":
		if req.Action == nil {
			return syscall.EINVAL
		}
		expected.Action = req.Action
		if err := req.Action.Check(); err != nil {
			return err
		}
	case "publish", "cancel", "detach", "close-export":
		expected.Token = req.Token
		if !validFileCapability(req.Token) {
			return syscall.EINVAL
		}
	case "export":
		if req.Export == nil {
			return syscall.EINVAL
		}
		expected.Export = req.Export
		if err := req.Export.Check(); err != nil {
			return err
		}
	default:
		return syscall.EINVAL
	}
	if !reflect.DeepEqual(req, expected) {
		return syscall.EINVAL
	}
	return nil
}

func (h *Handler) transferAction(ctx context.Context, action storage.ActionKey) error {
	if err := action.Check(); err != nil {
		return err
	}
	if binding, present := storage.AccessBindingFrom(ctx); present && binding.SubjectID != action.SubjectID {
		return syscall.EACCES
	}
	if err := h.transfers.options.ActionNamespace(ctx, action); err != nil {
		errno := syscall.EIO
		if errors.Is(err, authz.ErrDenied) || errors.Is(err, syscall.EACCES) {
			errno = syscall.EACCES
		}
		return &authzFailure{errno: errno, cause: err}
	}
	return nil
}

func (h *Handler) serveTransfer(w http.ResponseWriter, r *http.Request, req Request) {
	if h.transfers == nil {
		h.writeOperationError(w, syscall.ENOSYS)
		return
	}
	if req.Op == OpFileTransferData {
		h.receiveTransfer(w, r, req.Path)
		return
	}
	if req.Op == OpFileTransferExport {
		h.sendTransfer(w, r, req.Path)
		return
	}
	if r.Header.Get("Content-Type") != contentJSON {
		h.writeFault(w, http.StatusBadRequest, errors.New("transfer control requires application/json"))
		return
	}
	var body []byte
	var release func()
	var err error
	if req.Op == OpFileTransferControl {
		release, err = h.lockControls.acquire(r.Context(), retainedResponseMultiplier*MaxFileControlBytes)
		if err == nil {
			defer release()
			body, err = readAtMost(r.Body, min(h.maxBodyBytes, MaxFileControlBytes))
			if err == nil && r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
				err = syscall.EIO
			}
		}
	} else {
		releaseResponse, admitErr := h.responses.acquire(r.Context(), retainedResponseMultiplier*h.maxBodyBytes)
		if admitErr != nil {
			h.writeOperationError(w, admitErr)
			return
		}
		defer releaseResponse()
		body, release, err = h.readBody(r, h.maxBodyBytes)
		if err == nil {
			defer release()
		}
	}
	if err != nil {
		h.writeRequestBodyFault(w, err)
		return
	}
	var command transferRequest
	if err := decodeFileJSON(body, &command); err != nil {
		h.writeFault(w, http.StatusBadRequest, err)
		return
	}
	if err := validateTransferRequest(command); err != nil {
		h.writeFault(w, http.StatusBadRequest, err)
		return
	}
	if req.Op == OpFileTransferControl && (command.Op == "begin" || command.Op == "export") {
		h.writeFault(w, http.StatusBadRequest, errors.New("new transfer uses the data admission endpoint"))
		return
	}
	scope, present, err := requestMutationScope(r, OpWrite)
	if err != nil {
		h.writeFault(w, http.StatusBadRequest, err)
		return
	}
	if present {
		r = r.WithContext(locking.WithScope(r.Context(), scope))
	}
	operation := opImport
	if command.Op == "export" || command.Op == "close-export" {
		operation = opExport
	}
	if err := h.authorize(r.Context(), authz.AccessRequest{Operation: operation}); err != nil {
		h.writeOperationError(w, err)
		return
	}
	response, err := h.transferControl(r.Context(), command)
	if err != nil {
		if req.Op == OpFileTransferControl {
			h.writeBoundedFileResponse(w, StatusStorageError, transferErrorOf(err), min(h.maxBodyBytes, MaxFileControlBytes))
			return
		}
		h.writeOperationError(w, err)
		return
	}
	if req.Op == OpFileTransferControl {
		h.writeBoundedFileResponse(w, http.StatusOK, response, min(h.maxBodyBytes, MaxFileControlBytes))
		return
	}
	h.writeJSON(w, http.StatusOK, response)
}

func (h *Handler) transferControl(ctx context.Context, req transferRequest) (transferResponse, error) {
	r := h.transfers
	switch req.Op {
	case "check":
		if err := r.backend.CheckFileTransfer(); err != nil {
			return transferResponse{}, err
		}
		return transferResponse{Capabilities: &transferCapabilities{MaxBytes: r.options.MaxBytes, Replication: h.log != nil}}, nil
	case "begin", "resume":
		releaseBegin, err := r.begins.acquireOperation(ctx)
		if err != nil {
			return transferResponse{}, err
		}
		defer releaseBegin()
		action := req.Action
		bytes := int64(0)
		if req.Command != nil {
			action = &req.Command.Action
			bytes = req.Command.DeclaredBytes
		}
		if err := h.transferAction(ctx, *action); err != nil {
			return transferResponse{}, err
		}
		if req.Command == nil {
			r.mu.Lock()
			entries := make([]*servedTransfer, 0, len(r.entries))
			for _, current := range r.entries {
				entries = append(entries, current)
			}
			r.mu.Unlock()
			for _, current := range entries {
				if current.action != *action {
					continue
				}
				if !current.mu.TryLock() {
					if err := storage.RequireAccessBinding(ctx, current.binding); err != nil {
						return transferResponse{}, err
					}
					return transferResponse{}, syscall.EBUSY
				}
				if current.action == *action && !current.settled && storage.RequireAccessBinding(ctx, current.binding) == nil {
					sealed := current.sealed
					response := transferResponse{Token: current.token}
					current.mu.Unlock()
					if !sealed {
						return transferResponse{}, syscall.EBUSY
					}
					return response, nil
				}
				current.mu.Unlock()
			}
		}
		if req.Command != nil {
			r.mu.Lock()
			entries := make([]*servedTransfer, 0, len(r.entries))
			for _, current := range r.entries {
				entries = append(entries, current)
			}
			r.mu.Unlock()
			for _, current := range entries {
				if current.action != *action {
					continue
				}
				if !current.mu.TryLock() {
					if err := storage.RequireAccessBinding(ctx, current.binding); err != nil {
						return transferResponse{}, err
					}
					return transferResponse{}, syscall.EBUSY
				}
				if current.action == *action && !current.settled {
					err := storage.RequireAccessBinding(ctx, current.binding)
					if err == nil && current.fingerprint != req.Command.Fingerprint {
						err = syscall.EINVAL
					}
					if err == nil && (current.closing || !time.Now().Before(current.expires)) {
						err = syscall.EBUSY
					}
					if err == nil && current.declared == nil {
						declared := req.Command.DeclaredBytes
						current.declared = &declared
						current.digest = req.Command.Digest
					}
					response := transferResponse{Token: current.token, DeclaredBytes: current.declared}
					current.mu.Unlock()
					return response, err
				}
				current.mu.Unlock()
			}
		}
		t, err := r.admit(ctx, bytes, *action)
		if err != nil {
			return transferResponse{}, err
		}
		t.mu.Lock()
		if req.Command != nil {
			t.fingerprint = req.Command.Fingerprint
			t.digest = req.Command.Digest
			t.declared = &bytes
			t.importFile, err = r.backend.BeginImport(ctx, *req.Command)
		} else {
			t.importFile, err = r.backend.ResumeImport(ctx, *action)
			t.sealed = err == nil
			if err == nil {
				receipt, queryErr := r.backend.QueryImport(ctx, *action)
				if queryErr != nil || receipt.Check() != nil || receipt.Action != *action {
					err = errors.Join(queryErr, syscall.EIO)
				} else {
					t.fingerprint = receipt.Fingerprint
				}
			}
		}
		if err == nil && t.importFile == nil {
			err = syscall.EIO
		}
		t.initializing = false
		declared := t.declared
		t.mu.Unlock()
		if err != nil {
			_, cleanupErr := r.settle(t.cleanupContext(ctx), t)
			r.forgetUnpublished(t)
			return transferResponse{}, errors.Join(err, cleanupErr)
		}
		return transferResponse{Token: t.token, DeclaredBytes: declared}, nil
	case "query":
		if err := h.transferAction(ctx, *req.Action); err != nil {
			return transferResponse{}, err
		}
		receipt, err := r.backend.QueryImport(ctx, *req.Action)
		if receipt.Action != *req.Action {
			return transferResponse{}, errors.Join(err, syscall.EIO)
		}
		return h.transferReceiptResponse(ctx, receipt, err)
	case "cancel-action":
		if err := h.transferAction(ctx, *req.Action); err != nil {
			return transferResponse{}, err
		}
		gate := r.sharedActionGate(*req.Action)
		releaseAction, err := gate.acquireOperation(ctx)
		if err != nil {
			return transferResponse{}, err
		}
		defer releaseAction()
		receipt, err := r.backend.CancelImport(ctx, *req.Action)
		if err != nil && reflect.DeepEqual(receipt, storage.ImportReceipt{}) {
			return transferResponse{}, err
		}
		if receipt.Action != *req.Action {
			return transferResponse{}, errors.Join(err, syscall.EIO)
		}
		return h.transferReceiptResponse(ctx, receipt, err)
	case "publish":
		t, err := r.lookup(ctx, req.Token)
		if err != nil {
			return transferResponse{}, err
		}
		if err := h.transferAction(ctx, t.action); err != nil {
			return transferResponse{}, err
		}
		releaseAction, err := t.actionGate.acquireOperation(ctx)
		if err != nil {
			return transferResponse{}, err
		}
		defer releaseAction()
		if !t.mu.TryLock() {
			return transferResponse{}, syscall.EBUSY
		}
		if t.closing || t.importFile == nil || !time.Now().Before(t.expires) {
			t.mu.Unlock()
			return transferResponse{}, syscall.ESTALE
		}
		receipt, publishErr := t.importFile.Publish(ctx)
		if receipt.Action != t.action || t.fingerprint != (storage.ActionFingerprint{}) && receipt.Fingerprint != t.fingerprint {
			t.mu.Unlock()
			return transferResponse{}, errors.Join(publishErr, syscall.EIO)
		}
		t.mu.Unlock()
		response, err := h.transferReceiptResponse(ctx, receipt, publishErr)
		if receipt.Outcome.Terminal() {
			closeResult, cleanupErr := r.settle(t.cleanupContext(ctx), t)
			response.Close = referenceCloseResultOf(closeResult)
			response.Settlement = transferSettlementOf(cleanupErr)
			if cleanupErr != nil && err == nil {
				response.Error = transferErrorOf(cleanupErr)
			}
		}
		return response, err
	case "cancel", "detach", "close-export":
		t, err := r.lookup(ctx, req.Token)
		if err != nil {
			return transferResponse{}, err
		}
		result, err := r.settle(ctx, t, req.Op)
		return transferResponse{Close: referenceCloseResultOf(result), Error: transferErrorOf(err), Settlement: transferSettlementOf(err)}, nil
	case "export":
		return h.prepareExport(ctx, *req.Export)
	}
	return transferResponse{}, syscall.EINVAL
}

func transferErrorOf(err error) *ErrorResponse {
	if err == nil {
		return nil
	}
	if response, ok := authorizationResponse(err); ok {
		return &response
	}
	return &ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: "transfer operation failed", CapabilityCode: capabilityErrorCode(err)}
}

func transferSettlementOf(err error) *transferCloseSettlement {
	var marker *storage.CloseSettlementError
	if !errors.As(err, &marker) {
		return nil
	}
	if marker.Check() != nil {
		return &transferCloseSettlement{State: storage.CloseSettlementUnknown, Cause: transferErrorOf(syscall.EIO)}
	}
	return &transferCloseSettlement{State: marker.State, Semantic: transferErrorOf(marker.SemanticErr), Cause: transferErrorOf(marker.Cause)}
}

func (h *Handler) transferReceiptResponse(ctx context.Context, receipt storage.ImportReceipt, err error) (transferResponse, error) {
	if checkErr := receipt.Check(); checkErr != nil {
		return transferResponse{}, errors.Join(err, syscall.EIO)
	}
	if err == nil && (receipt.Outcome == storage.DurableActionUnknown || receipt.Outcome == storage.DurableActionHistoryExpired) {
		err = syscall.EIO
	}
	response := transferResponse{Receipt: transferReceiptOf(receipt), Error: transferErrorOf(err)}
	if receipt.Outcome == storage.DurableActionCompleted {
		if h.publisher != nil {
			h.publisher.wake()
		}
		if h.log != nil {
			barrier, barrierErr := h.mutationBarrier(ctx)
			if barrierErr != nil {
				response.Error = transferErrorOf(errors.Join(barrierErr, syscall.EIO))
			} else {
				response.Barrier = barrier
			}
		}
	}
	return response, nil
}

func (h *Handler) receiveTransfer(w http.ResponseWriter, r *http.Request, token string) {
	if r.Header.Get("Content-Type") != contentOctets {
		h.writeFault(w, http.StatusBadRequest, errors.New("transfer data requires application/octet-stream"))
		return
	}
	if _, _, err := requestMutationScope(r, OpWrite); err != nil {
		h.writeFault(w, http.StatusBadRequest, err)
		return
	}
	t, err := h.transfers.lookup(r.Context(), token)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	if err := h.authorize(r.Context(), authz.AccessRequest{Operation: opImport}); err != nil {
		h.writeOperationError(w, err)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing || t.importFile == nil || t.declared == nil || !time.Now().Before(t.expires) {
		h.writeOperationError(w, syscall.ESTALE)
		return
	}
	if r.ContentLength >= 0 && r.ContentLength != *t.declared {
		h.writeOperationError(w, syscall.EINVAL)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), t.expires)
	defer cancel()
	stopBody := context.AfterFunc(ctx, func() { _ = r.Body.Close() })
	defer stopBody()
	if err := http.NewResponseController(w).SetReadDeadline(t.expires); err != nil {
		h.writeOperationError(w, errors.Join(err, syscall.EIO))
		return
	}
	defer http.NewResponseController(w).SetReadDeadline(time.Time{})
	seal, err := t.importFile.Receive(ctx, &authorizedTransferReader{h: h, ctx: ctx, operation: opImport, reader: r.Body})
	if err != nil {
		t.closing = true
		h.writeOperationError(w, err)
		return
	}
	if seal.Bytes != *t.declared || seal.Digest != t.digest {
		t.closing = true
		h.writeOperationError(w, syscall.EIO)
		return
	}
	t.sealed = true
	h.writeJSON(w, http.StatusOK, transferResponse{Seal: &seal, DeclaredBytes: t.declared})
}

type authorizedTransferReader struct {
	h         *Handler
	ctx       context.Context
	operation storage.Operation
	reader    io.Reader
}

func (r *authorizedTransferReader) Read(p []byte) (int, error) {
	if err := r.h.authorize(r.ctx, authz.AccessRequest{Operation: r.operation}); err != nil {
		return 0, err
	}
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	return r.reader.Read(p)
}

type exactTransferWriter struct {
	writer    io.Writer
	remaining int64
}

type authorizedTransferWriter struct {
	h      *Handler
	ctx    context.Context
	writer io.Writer
}

func (w *authorizedTransferWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.h.authorize(w.ctx, authz.AccessRequest{Operation: opExport}); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

func (w *exactTransferWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, syscall.EFBIG
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func (h *Handler) prepareExport(ctx context.Context, command storage.ExportCommand) (transferResponse, error) {
	r := h.transfers
	t, err := r.admit(ctx, r.options.MaxBytes, storage.ActionKey{})
	if err != nil {
		return transferResponse{}, err
	}
	t.mu.Lock()
	ctx, cancel := context.WithDeadline(ctx, t.expires)
	defer cancel()
	t.exportFile, err = r.backend.ExportFile(ctx, command)
	if err == nil && t.exportFile == nil {
		err = syscall.EIO
	}
	if err == nil {
		t.info = t.exportFile.Info()
		if t.info.Attr.Kind != storage.NodeRegular || t.info.Attr.ID != command.NodeID || t.info.Attr.Size < 0 || t.info.Attr.Size > r.options.MaxBytes {
			err = syscall.EFBIG
		}
	}
	if err == nil {
		t.spool, err = os.CreateTemp(r.options.SpoolDir, "export-"+t.token+"-")
		if err == nil {
			t.spoolPath = t.spool.Name()
		}
	}
	if err == nil {
		hash := sha256.New()
		writer := &exactTransferWriter{writer: &authorizedTransferWriter{h: h, ctx: ctx, writer: io.MultiWriter(t.spool, hash)}, remaining: t.info.Attr.Size}
		err = t.exportFile.WriteTo(ctx, writer)
		if err == nil && writer.remaining != 0 {
			err = syscall.EIO
		}
		if err == nil {
			info := t.exportFile.Info()
			if !info.DigestAvailable || !reflect.DeepEqual(info.Attr, t.info.Attr) || !reflect.DeepEqual(hash.Sum(nil), info.Digest[:]) {
				err = syscall.EIO
			} else {
				t.info = info
			}
		}
		if err == nil {
			err = t.spool.Sync()
		}
		if err == nil {
			_, err = t.spool.Seek(0, io.SeekStart)
		}
		if err == nil {
			t.ready = true
		}
	}
	var response transferResponse
	if err == nil {
		response = transferResponse{Token: t.token, Info: &transferExportInfo{Attr: AttrOf(t.info.Attr), Digest: append([]byte(nil), t.info.Digest[:]...)}}
	}
	t.initializing = false
	t.mu.Unlock()
	if err != nil {
		_, cleanupErr := r.settle(t.cleanupContext(context.WithoutCancel(ctx)), t)
		r.forgetUnpublished(t)
		return transferResponse{}, errors.Join(err, cleanupErr)
	}
	return response, nil
}

func (h *Handler) sendTransfer(w http.ResponseWriter, r *http.Request, token string) {
	if err := h.requireEmptyBody(r); err != nil {
		h.writeRequestBodyFault(w, err)
		return
	}
	t, err := h.transfers.lookup(r.Context(), token)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	if err := h.authorize(r.Context(), authz.AccessRequest{Operation: opExport}); err != nil {
		h.writeOperationError(w, err)
		return
	}
	t.mu.Lock()
	if !t.ready || t.closing || t.consumed || t.spool == nil || !time.Now().Before(t.expires) {
		t.mu.Unlock()
		h.writeOperationError(w, syscall.ESTALE)
		return
	}
	t.consumed = true
	defer func() {
		t.closing = true
		t.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.cleanupContext(context.Background()), 5*time.Second)
		defer cancel()
		_, _ = h.transfers.settle(ctx, t)
	}()
	ctx, cancel := context.WithDeadline(r.Context(), t.expires)
	defer cancel()
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(t.expires); err != nil {
		h.writeOperationError(w, errors.Join(err, syscall.EIO))
		return
	}
	stopWrite := context.AfterFunc(ctx, func() { _ = controller.SetWriteDeadline(time.Now()) })
	defer func() { stopWrite(); _ = controller.SetWriteDeadline(time.Time{}) }()
	if _, err := t.spool.Seek(0, io.SeekStart); err != nil {
		h.writeOperationError(w, err)
		return
	}
	w.Header().Set("Content-Type", contentOctets)
	w.Header().Set("Content-Length", fmt.Sprint(t.info.Attr.Size))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"file-%d\"", t.info.Attr.ID))
	buffer := make([]byte, 64<<10)
	remaining := t.info.Attr.Size
	for remaining > 0 {
		if err := h.authorize(ctx, authz.AccessRequest{Operation: opExport}); err != nil {
			panic(http.ErrAbortHandler)
		}
		n, err := t.spool.Read(buffer[:min(int64(len(buffer)), remaining)])
		if n > 0 {
			written, writeErr := w.Write(buffer[:n])
			if writeErr != nil || written != n {
				panic(http.ErrAbortHandler)
			}
			remaining -= int64(n)
		}
		if err != nil && (remaining != 0 || !errors.Is(err, io.EOF)) || n == 0 && err == nil {
			panic(http.ErrAbortHandler)
		}
	}
}
