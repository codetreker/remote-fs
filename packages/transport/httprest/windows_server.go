package httprest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func emptyWindowsResponse() windowsResponse {
	return windowsResponse{Data: []byte{}, Entries: []windowsEntry{}}
}

func (h *Handler) serveWindows(w http.ResponseWriter, r *http.Request) {
	control := r.URL.Path == Prefix+string(OpWindowsControl)
	fault := func(status int, err error) {
		h.writeWindowsResponse(w, status, windowsErrorResponse{Errno: "EIO", Message: err.Error()}, control)
	}
	failure := func(response windowsResponse, err error) {
		body := windowsErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error(), Failure: storage.WindowsFailureOf(err), Action: response.Action, Activation: response.Activation}
		if policy, ok := authorizationResponse(err); ok {
			body = windowsErrorResponse{Errno: policy.Errno, Message: policy.Message}
		} else {
			var link *storage.WindowsSymlinkError
			if storage.ErrnoOf(err) == syscall.ELOOP && errors.As(err, &link) {
				body.Symlink = &windowsSymlink{Target: link.Target, Location: link.Location, Unparsed: link.Unparsed}
				if validation := validateWindowsSymlink(body.Symlink); validation != nil {
					body = windowsErrorResponse{Errno: "EIO", Message: "invalid native Windows symbolic-link result"}
				}
			}
		}
		h.writeWindowsResponse(w, StatusStorageError, body, control)
	}
	if h.stopped() {
		failure(windowsResponse{}, syscall.EIO)
		return
	}
	if r.Header.Get("Content-Type") != contentJSON {
		fault(http.StatusUnsupportedMediaType, errors.New("Windows calls require application/json"))
		return
	}
	var body []byte
	var err error
	if control {
		release, e := h.windowsControls.acquire(r.Context(), retainedResponseMultiplier*windowsControlResponseLimit())
		if e != nil {
			failure(windowsResponse{}, e)
			return
		}
		defer release()
		body, err = readAtMost(r.Body, DefaultMaxLockControlBytes)
		if err == nil && r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
			err = errors.New("Windows control body did not arrive whole")
		}
	} else {
		release, e := h.responses.acquire(r.Context(), retainedResponseMultiplier*h.maxBodyBytes)
		if e != nil {
			failure(windowsResponse{}, e)
			return
		}
		defer release()
		var releaseBody func()
		body, releaseBody, err = h.readBody(r, h.maxBodyBytes)
		if err == nil {
			defer releaseBody()
		}
	}
	if err != nil {
		if storage.ErrnoOf(err) == syscall.EAGAIN {
			failure(windowsResponse{}, err)
			return
		}
		fault(http.StatusBadRequest, err)
		return
	}
	var req windowsRequest
	if err := decodeWindowsJSON(body, &req); err != nil {
		fault(http.StatusBadRequest, err)
		return
	}
	if err := validateWindowsRequest(req, h.windows.limits.Session); err != nil {
		switch storage.ErrnoOf(err) {
		case syscall.EINVAL, syscall.EFBIG, syscall.ENAMETOOLONG:
			failure(windowsResponse{}, err)
			return
		}
		fault(http.StatusBadRequest, err)
		return
	}
	if windowsControl(req.Op) != control {
		fault(http.StatusBadRequest, errors.New("Windows operation uses the wrong admission endpoint"))
		return
	}
	if req.Op == storage.OpWindowsRead && int64(req.Length) > windowsReadLimit(h.maxBodyBytes) || req.Op == storage.OpWindowsWrite && int64(len(req.Data)) > h.maxWriteBytes {
		failure(windowsResponse{}, syscall.EFBIG)
		return
	}
	scopeOp := OpWindows
	if windowsMutation(req.Op) {
		scopeOp = OpWrite
	}
	scope, present, err := requestMutationScope(r, scopeOp)
	if err != nil {
		fault(http.StatusBadRequest, err)
		return
	}
	if present || windowsMutation(req.Op) {
		r = r.WithContext(locking.WithScope(r.Context(), scope))
	}
	access := authz.AccessRequest{Operation: req.Op}
	if req.Op == storage.OpWindowsOpen {
		access.WindowsOpen = req.Open.Intent
	}
	if err := h.authorize(r.Context(), access); err != nil {
		failure(windowsResponse{}, err)
		return
	}
	canonical, err := json.Marshal(req)
	if err != nil {
		failure(windowsResponse{}, err)
		return
	}
	canonicalScope := ""
	if present {
		canonicalScope, err = encodeMutationScope(scope)
		if err != nil {
			failure(windowsResponse{}, err)
			return
		}
	}
	digest := sha256.Sum256(append(canonical, []byte(canonicalScope)...))
	response, err := h.windowsCall(r.Context(), req, digest)
	if err != nil {
		failure(response, err)
		return
	}
	if err := validateWindowsResponse(req, response); err != nil {
		failure(windowsResponse{}, fmt.Errorf("invalid native Windows result: %w", errors.Join(syscall.EIO, err)))
		return
	}
	h.writeWindowsResponse(w, http.StatusOK, response, control)
}

func (h *Handler) writeWindowsResponse(w http.ResponseWriter, status int, body any, control bool) {
	limit := h.maxBodyBytes
	if control {
		limit = windowsControlResponseLimit()
	}
	if failure, ok := body.(windowsErrorResponse); ok {
		if len(failure.Message) > windowsDiagnosticBytes || !utf8.ValidString(failure.Message) {
			failure.Message = "Windows failure detail exceeds its response bound"
		}
		body = failure
	}
	encoded, err := json.Marshal(body)
	if err != nil || int64(len(encoded)) > limit {
		status = http.StatusInternalServerError
		encoded = []byte(`{"errno":"EIO","message":"Windows response exceeds its protocol bound"}`)
	}
	w.Header().Set("Content-Type", contentJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func windowsReadLimit(limit int64) int64 {
	worstTime := Time{UnixSec: math.MinInt64, Nanos: 999999999}
	response := emptyWindowsResponse()
	response.ReadAttr = &Attr{ID: math.MaxUint64, Mode: math.MaxUint32, Size: math.MaxInt64, AccessTime: worstTime, ModTime: worstTime}
	encoded, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	if limit < int64(len(encoded)) {
		return -1
	}
	return (limit - int64(len(encoded))) / 4 * 3
}

func newWindowsListResult(limit int64) (*storage.WindowsListResult, error) {
	empty, err := json.Marshal(emptyWindowsResponse())
	if err != nil {
		return nil, err
	}
	return storage.NewWindowsListResult(limit, int64(len(empty)), func(index int, nameBytes int64, attr storage.WindowsBasicAttr) (int64, error) {
		encoded, err := json.Marshal(windowsBasicAttrOf(attr))
		if err != nil {
			return 0, err
		}
		if nameBytes > limit/6 {
			return limit + 1, nil
		}
		charge := int64(len(`{"name":"","attr":}`)) + 6*nameBytes + int64(len(encoded))
		if index != 0 {
			charge++
		}
		return charge, nil
	})
}

func (h *Handler) windowsCall(parent context.Context, req windowsRequest, digest [32]byte) (windowsResponse, error) {
	response := emptyWindowsResponse()
	done, err := h.windows.begin()
	if err != nil {
		return response, err
	}
	defer done()
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(h.lifetime, func() { cancel(errHandlerStopped) })
	defer func() { stop(); cancel(context.Canceled) }()
	if h.windows.backend == nil || h.log == nil {
		return response, syscall.EOPNOTSUPP
	}
	if err := h.windows.backend.CheckWindowsStorage(); err != nil {
		return response, err
	}
	if req.Op == storage.OpWindowsState || req.Op == storage.OpWindowsEnable || req.Op == storage.OpWindowsSessionOpen {
		state, err := h.checkedWindowsState(ctx)
		if err != nil {
			return response, err
		}
		if req.Op == storage.OpWindowsState {
			response.State = &state
			return response, nil
		}
	}
	switch req.Op {
	case storage.OpWindowsEnable, storage.OpWindowsQueryActivation:
		var result storage.WindowsActivation
		var err error
		if req.Op == storage.OpWindowsEnable {
			result, err = h.windows.backend.EnableWindows(ctx, req.Action)
		} else {
			result, err = h.windows.backend.QueryWindowsActivation(ctx, req.Action)
		}
		if result.State != 0 {
			name, namingErr := windowsErrnoName(result.Errno)
			response.Activation = &windowsActivation{Action: result.Action, State: result.State, Enabled: result.Enabled, Errno: name, HistoryRemaining: result.HistoryRemaining}
			if validation := validateWindowsActivation(req.Action, response.Activation); validation != nil || namingErr != nil {
				return emptyWindowsResponse(), errors.Join(syscall.EIO, err, namingErr, validation)
			}
		}
		return response, err
	case storage.OpWindowsSessionOpen:
		return h.windows.enroll(ctx, *req.Options)
	}
	h.windows.mu.Lock()
	session := h.windows.sessions[req.Session]
	h.windows.mu.Unlock()
	if session == nil {
		return response, syscall.ESTALE
	}
	session.mu.Lock()
	retired := session.retired || !time.Now().Before(session.expires)
	session.mu.Unlock()
	if retired && req.Op != storage.OpWindowsSessionClose {
		return response, syscall.ESTALE
	}
	if req.Op == storage.OpWindowsSessionClose {
		session.retire(syscall.ESTALE)
		return response, session.native.Close(ctx)
	}
	stopSession := context.AfterFunc(session.lifetime, func() { cancel(context.Cause(session.lifetime)) })
	defer stopSession()
	var action *servedWindowsAction
	if windowsActionRequired(req.Op) && req.Op != storage.OpWindowsQueryAction && req.Op != storage.OpWindowsCancelAction {
		action, err = session.beginAction(ctx, req, digest, h.windows.limits)
		if err != nil {
			return response, err
		}
		defer session.finishAction(action)
	}
	response, err = h.performWindows(ctx, session, req, action)
	if invalid := validateWindowsBatchAction(req, response.Action); invalid != nil {
		session.retire(syscall.EIO)
		return emptyWindowsResponse(), errors.Join(syscall.EIO, err, invalid)
	}
	if windowsMutation(req.Op) && h.publisher != nil {
		h.publisher.wake()
	}
	return response, err
}

func (h *Handler) checkedWindowsState(ctx context.Context) (storage.WindowsState, error) {
	if h.maxBodyBytes < windowsControlResponseLimit() {
		return storage.WindowsState{}, fmt.Errorf("Windows metadata needs a body bound of at least %d bytes, above the configured %d-byte bound: %w", windowsControlResponseLimit(), h.maxBodyBytes, syscall.EFBIG)
	}
	state, err := h.windows.backend.WindowsState(ctx)
	if err != nil {
		return state, err
	}
	response := emptyWindowsResponse()
	response.State = &state
	if err := validateWindowsResponse(windowsRequest{Op: storage.OpWindowsState}, response); err != nil {
		return storage.WindowsState{}, errors.Join(syscall.EIO, err)
	}
	required, err := maxChangeFrameBytes(state.MaxEventBytes)
	if err != nil {
		return storage.WindowsState{}, err
	}
	if required > h.maxFrameBytes {
		return storage.WindowsState{}, fmt.Errorf("Windows change observation needs %d bytes per frame, above the configured %d-byte bound: %w", required, h.maxFrameBytes, syscall.EFBIG)
	}
	return state, nil
}

func (h *Handler) performWindows(ctx context.Context, s *servedWindowsSession, req windowsRequest, action *servedWindowsAction) (windowsResponse, error) {
	response := emptyWindowsResponse()
	if action != nil {
		s.mu.Lock()
		known := action.known
		s.mu.Unlock()
		if known {
			result, err := s.native.QueryAction(ctx, req.Action)
			receipt, err := windowsResultResponse(s, response, result, err)
			if req.Op == storage.OpWindowsOpen && err == nil && receipt.Action.File != "" {
				response.File, response.Attr, response.CreateAction = receipt.Action.File, receipt.Action.Attr, receipt.Action.CreateAction
				return response, nil
			}
			return receipt, err
		}
	}
	switch req.Op {
	case storage.OpWindowsStatus, storage.OpWindowsRenew:
		started := time.Now()
		var status storage.FileSessionStatus
		var err error
		if req.Op == storage.OpWindowsRenew {
			status, err = s.native.Renew(ctx)
		} else {
			status, err = s.native.Status(ctx)
		}
		if err != nil {
			return response, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.retired || status.Epoch != s.authority || len(status.Epoch) > MaxLockCapabilityBytes || !utf8.ValidString(status.Epoch) || status.ActionEpoch == 0 || status.Revision == 0 || status.Remaining < 0 || status.Remaining > s.options.Lease || status.HistoryRemaining < 0 || status.HistoryRemaining > s.options.History || status.Retired {
			s.retired = true
			s.cancel(syscall.ESTALE)
			return response, syscall.ESTALE
		}
		if req.Op == storage.OpWindowsRenew && status.Revision > s.revision {
			s.revision = status.Revision
			candidate := started.Add(status.Remaining)
			if candidate.After(s.expires) {
				s.expires = candidate
			}
		}
		status.Remaining = min(maxDuration(time.Until(s.expires)), maxDuration(status.Remaining-time.Since(started)))
		status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(started))
		response.Status = &status
		return response, nil
	case storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
		var result storage.WindowsActionResult
		var err error
		if req.Op == storage.OpWindowsQueryAction {
			result, err = s.native.QueryAction(ctx, req.Action)
		} else {
			result, err = s.native.CancelAction(ctx, req.Action)
		}
		return windowsResultResponse(s, response, result, err)
	case storage.OpWindowsOpen:
		request := req.Open.storage()
		lookup, err := s.lookup(request.Lookup)
		if err != nil {
			s.mu.Lock()
			delete(s.files, action.file)
			action.expires = time.Now().Add(s.options.History)
			s.mu.Unlock()
			return response, err
		}
		request.Lookup = lookup
		opened, err := s.native.Open(ctx, request, req.Action)
		if opened.File != nil {
			capability, bindErr := s.bind(opened.File, opened.Attr, action)
			if bindErr != nil {
				return response, errors.Join(err, bindErr)
			}
			response.File, response.Attr, response.CreateAction = capability, windowsAttrOf(opened.Attr), opened.CreateAction
		}
		if err != nil {
			if ctx.Err() == nil {
				result, queryErr := s.native.QueryAction(ctx, req.Action)
				if result.State != 0 {
					reconciled, receiptErr := windowsResultResponse(s, emptyWindowsResponse(), result, queryErr)
					response.Action = reconciled.Action
					if receiptErr != nil && result.Errno == 0 {
						err = errors.Join(err, receiptErr)
					}
				}
			}
			return response, err
		}
		if response.File == "" {
			s.retire(syscall.EIO)
			return response, syscall.EIO
		}
		s.mu.Lock()
		action.known = true
		action.expires = time.Now().Add(s.options.History)
		s.mu.Unlock()
		return response, nil
	}
	file, err := s.file(req.File)
	if err != nil {
		return response, err
	}
	var result storage.WindowsActionResult
	switch req.Op {
	case storage.OpWindowsStat:
		attr, err := file.Stat(ctx)
		response.Attr = windowsAttrOf(attr)
		return response, err
	case storage.OpWindowsRead:
		read, err := file.ReadAt(ctx, req.Offset, req.Length)
		if err != nil {
			return response, err
		}
		response.ReadAttr = AttrOf(read.Attr)
		response.Data = append(response.Data, read.Data...)
		return response, nil
	case storage.OpWindowsList:
		listing, err := newWindowsListResult(h.maxBodyBytes)
		if err != nil {
			return response, err
		}
		if err := file.ListBounded(ctx, listing); err != nil {
			return response, err
		}
		entries, err := listing.Entries()
		if err != nil {
			return response, err
		}
		for _, entry := range entries {
			response.Entries = append(response.Entries, windowsEntry{Name: entry.Name, Attr: windowsBasicAttrOf(entry.Attr)})
		}
		return response, nil
	case storage.OpWindowsSync:
		return response, file.Sync(ctx)
	case storage.OpWindowsReadLink:
		link, readErr := file.ReadLink(ctx)
		err = readErr
		if err == nil {
			response.Symlink = &windowsSymlink{Target: link.Target, Location: link.Location, Unparsed: link.Unparsed}
		}
		return response, err
	case storage.OpWindowsSetLink:
		result, err = file.SetLink(ctx, req.Target, req.Action)
	case storage.OpWindowsWrite:
		result, err = file.WriteAt(ctx, req.Offset, req.Data, req.Action)
	case storage.OpWindowsTruncate:
		result, err = file.Truncate(ctx, req.Offset, req.Action)
	case storage.OpWindowsSetAttr:
		result, err = file.SetAttr(ctx, req.Change.storage(), req.Action)
	case storage.OpWindowsSetDeletePending:
		result, err = file.SetDeletePending(ctx, req.DeletePending, req.Action)
	case storage.OpWindowsLockBatch:
		result, err = file.LockBatch(ctx, storage.WindowsLockBatch{Ranges: req.Ranges}, req.Action)
	case storage.OpWindowsClose:
		result, err = file.Close(ctx, req.Action)
	case storage.OpWindowsRename:
		request := *req.Rename
		request.Source, err = s.lookup(request.Source)
		if err != nil {
			return response, err
		}
		request.Destination, err = s.lookup(request.Destination)
		if err != nil {
			return response, err
		}
		result, err = file.Rename(ctx, request, req.Action)
	default:
		return response, syscall.EINVAL
	}
	return windowsResultResponse(s, response, result, err)
}

func windowsResultResponse(s *servedWindowsSession, response windowsResponse, result storage.WindowsActionResult, err error) (windowsResponse, error) {
	if err == nil && result.Errno != 0 {
		s.retire(syscall.EIO)
		return emptyWindowsResponse(), fmt.Errorf("native Windows action omitted its recorded failure: %w", syscall.EIO)
	}
	if result.State != 0 {
		action, receiptErr := s.actionResponse(result)
		response.Action = action
		if receiptErr != nil {
			return response, errors.Join(syscall.EIO, err, receiptErr)
		}
	}
	if err == nil && response.Action == nil {
		return response, syscall.EIO
	}
	return response, err
}
