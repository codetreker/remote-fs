package httprest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileProtocolFault struct{ cause error }

func (e *fileProtocolFault) Error() string         { return "invalid native file outcome: " + e.cause.Error() }
func (e *fileProtocolFault) Unwrap() error         { return e.cause }
func (e *fileProtocolFault) Classification() error { return syscall.EIO }
func nativeFileFault(err error) error {
	return &fileProtocolFault{cause: errors.Join(syscall.EIO, err)}
}

func (h *Handler) serveFile(w http.ResponseWriter, r *http.Request) {
	control := r.URL.Path == Prefix+string(OpFileControl)
	fault := func(status int, err error) {
		h.writeFileResponse(w, status, fileErrorResponse{Errno: "EIO", Message: err.Error()}, control)
	}
	failure := func(response fileResponse, err error) {
		if policy, ok := authorizationResponse(err); ok {
			h.writeFileResponse(w, StatusStorageError, fileErrorResponse{Errno: policy.Errno, Message: policy.Message}, control)
			return
		}
		var invalid *fileProtocolFault
		if errors.As(err, &invalid) {
			fault(http.StatusInternalServerError, err)
			return
		}
		body := fileErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error(), NotAdmitted: storage.IsFileCallNotAdmitted(err), Receipt: response.Receipt, Barrier: response.Barrier}
		var native *storage.FileError
		if errors.As(err, &native) {
			body.Conflict = fileConflictOf(native.Conflict)
		}
		if locked := volumeLockFailure(err); locked != nil {
			code, recorded := locked.Code, locked.Recorded
			body.LockCode, body.Recorded = &code, &recorded
		}
		h.writeFileResponse(w, StatusStorageError, body, control)
	}
	if h.stopped() {
		failure(fileResponse{}, syscall.EIO)
		return
	}
	if r.Header.Get("Content-Type") != contentJSON {
		fault(http.StatusUnsupportedMediaType, errors.New("file calls require application/json"))
		return
	}
	var body []byte
	var err error
	var releaseBody, releaseResponse func()
	if control {
		releaseResponse, err = h.fileControls.acquire(r.Context(), retainedResponseMultiplier*max(fileControlRequestLimit(), fileControlResponseLimit()))
		if err != nil {
			failure(fileResponse{}, err)
			return
		}
		body, err = readAtMost(r.Body, fileControlRequestLimit())
		if err == nil && r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
			err = errors.New("file control body did not arrive whole")
		}
	} else {
		releaseResponse, err = h.responses.acquire(r.Context(), retainedResponseMultiplier*h.maxBodyBytes)
		if err != nil {
			failure(fileResponse{}, err)
			return
		}
		body, releaseBody, err = h.readBody(r, h.maxBodyBytes)
	}
	defer func() {
		if releaseResponse != nil {
			releaseResponse()
		}
		if releaseBody != nil {
			releaseBody()
		}
	}()
	if err != nil {
		fault(http.StatusBadRequest, err)
		return
	}
	var req fileRequest
	if err := decodeFileJSON(body, &req); err != nil {
		fault(http.StatusBadRequest, err)
		return
	}
	if err := validateFileRequest(req); err != nil {
		if storage.ErrnoOf(err) != syscall.EIO {
			failure(fileResponse{}, err)
		} else {
			fault(http.StatusBadRequest, err)
		}
		return
	}
	if fileControl(req.Op) != control {
		fault(http.StatusBadRequest, errors.New("file operation uses the wrong admission endpoint"))
		return
	}
	if req.Options != nil {
		if err := checkFileSessionOptions(*req.Options, h.files.limits.Session); err != nil {
			failure(fileResponse{}, err)
			return
		}
	}
	if req.Read != nil && int64(req.Read.Length) > fileReadLimit(h.maxBodyBytes) || req.Write != nil && int64(len(req.Write.Data)) > h.maxWriteBytes {
		failure(fileResponse{}, syscall.EFBIG)
		return
	}
	if req.List != nil && fileDirectoryResponseLimit(*req.List) > h.maxBodyBytes || req.Op == storage.OpFileRangeSnapshot && fileRangeSnapshotResponseLimit() > h.maxBodyBytes {
		failure(fileResponse{}, syscall.EFBIG)
		return
	}
	if fileWait(req.Op) {
		if h.stopped() {
			failure(fileResponse{}, syscall.EIO)
			return
		}
		releaseWait, err := h.fileWaits.tryAcquire(r.Context(), retainedResponseMultiplier*h.maxBodyBytes)
		if err != nil {
			failure(fileResponse{}, err)
			return
		}
		defer releaseWait()
		releaseResponse()
		releaseResponse = nil
		if releaseBody != nil {
			releaseBody()
			releaseBody = nil
		}
	}
	scopeOp := OpFile
	if fileMutation(req.Op) {
		scopeOp = OpWrite
	}
	scope, present, err := requestMutationScope(r, scopeOp)
	if err != nil {
		fault(http.StatusBadRequest, err)
		return
	}
	if present || fileMutation(req.Op) {
		r = r.WithContext(locking.WithScope(r.Context(), scope))
	}
	if err := h.authorize(r.Context(), fileAccess(req)); err != nil {
		failure(fileResponse{}, err)
		return
	}
	response, err := h.fileCall(r.Context(), req)
	if err != nil {
		failure(response, err)
		return
	}
	if err := validateFileResponse(req, response); err != nil {
		fault(http.StatusInternalServerError, nativeFileFault(err))
		return
	}
	h.writeFileResponse(w, http.StatusOK, response, control)
}

func (h *Handler) writeFileResponse(w http.ResponseWriter, status int, body any, control bool) {
	limit := h.maxBodyBytes
	if control {
		limit = fileControlResponseLimit()
	}
	if failure, ok := body.(fileErrorResponse); ok {
		if len(failure.Message) > fileDiagnosticBytes || !utf8.ValidString(failure.Message) {
			failure.Message = "file operation failed"
		}
		body = failure
	}
	data, err := marshalFileJSON(body)
	if err != nil || int64(len(data)) > limit {
		status = http.StatusInternalServerError
		data = []byte(`{"errno":"EIO","message":"file response exceeds its encoded bound"}`)
	}
	w.Header().Set("Content-Type", contentJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func fileAccess(r fileRequest) authz.AccessRequest {
	a := authz.AccessRequest{Operation: r.Op, Reference: r.Reference, Node: r.Node}
	const cleanup = storage.EffectReferenceRetired | storage.EffectClaimChanged | storage.EffectRangesChanged | storage.EffectPreparedChanged | storage.EffectDrainChanged | storage.EffectEntryDetached
	switch r.Op {
	case storage.OpFileRetain:
		a.Node = r.Retain.NodeID
		a.Claim = r.Retain.Claim
		a.Effects = storage.EffectRetained | storage.EffectClaimChanged
		if r.Retain.Prepared != nil {
			a.Effects |= storage.EffectPreparedChanged
		}
	case storage.OpFileRetainAt:
		a.Parent = r.RetainAt.Target.Parent
		a.Claim = r.RetainAt.Claim
		a.Effects = storage.EffectRetained | storage.EffectClaimChanged
		if r.RetainAt.Prepared != nil {
			a.Effects |= storage.EffectPreparedChanged
		}
	case storage.OpFileCreateAndRetainAt, storage.OpFileReplaceAndRetainAt:
		a.Parent = r.Create.Target.Parent
		a.Claim = r.Create.Claim
		a.Effects = storage.EffectRetained | storage.EffectCreated | storage.EffectClaimChanged
		if r.Op == storage.OpFileReplaceAndRetainAt {
			a.Effects |= storage.EffectEntryDetached
		}
		if r.Create.Prepared != nil {
			a.Effects |= storage.EffectPreparedChanged
		}
	case storage.OpFileResetAndRetainAt:
		a.Parent = r.Reset.Target.Parent
		a.Claim = r.Reset.Claim
		a.Effects = storage.EffectRetained | storage.EffectContentChanged | storage.EffectMetadataChanged | storage.EffectClaimChanged
		if r.Reset.Prepared != nil {
			a.Effects |= storage.EffectPreparedChanged
		}
	case storage.OpFileWrite, storage.OpFileTruncate:
		a.Effects = storage.EffectContentChanged | storage.EffectMetadataChanged
	case storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
		a.Effects = storage.EffectMetadataChanged
	case storage.OpFileSetKind:
		a.Effects = storage.EffectContentChanged | storage.EffectMetadataChanged
	case storage.OpFileRename:
		a.Parent = r.Rename.Source.Parent
		a.Destination = r.Rename.Destination.Parent
		a.Effects = storage.EffectEntryMoved | storage.EffectEntryDetached
	case storage.OpFileReplaceClaim:
		a.Claim = *r.Claim
		a.Effects = storage.EffectClaimChanged
	case storage.OpFilePrepareRemoval, storage.OpFileCancelPrepared:
		a.Effects = storage.EffectPreparedChanged
	case storage.OpFileDrainEntry, storage.OpFileCancelDrain:
		a.Effects = storage.EffectDrainChanged
	case storage.OpFileRetireRangeOwner, storage.OpFileRetireRanges, storage.OpFileReplaceRanges:
		a.Effects = storage.EffectRangesChanged
	case storage.OpFileClose, storage.OpFileSessionClose:
		a.Effects = cleanup
	}
	return a
}

func (h *Handler) fileCall(parent context.Context, req fileRequest) (fileResponse, error) {
	done, err := h.files.begin()
	if err != nil {
		return fileResponse{}, err
	}
	defer done()
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	stop := context.AfterFunc(h.lifetime, func() { cancel(errHandlerStopped) })
	defer stop()
	if err := ctx.Err(); err != nil {
		return fileResponse{}, err
	}
	if h.files.backend == nil {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := h.files.backend.CheckFileStorage(); err != nil {
		return fileResponse{}, err
	}
	switch req.Op {
	case storage.OpFileState:
		state, err := h.checkedFileState(ctx)
		if err != nil {
			return fileResponse{}, err
		}
		return fileResponse{State: &state}, nil
	case storage.OpFileSessionOpen:
		if _, err := h.checkedFileState(ctx); err != nil {
			return fileResponse{}, err
		}
		return h.files.enroll(ctx, *req.Options)
	}
	h.files.mu.Lock()
	s := h.files.sessions[req.Session]
	h.files.mu.Unlock()
	if s == nil {
		return fileResponse{}, syscall.ESTALE
	}
	reconcile := req.Op == storage.OpFileQueryAction || req.Op == storage.OpFileCancelAction || req.Op == storage.OpFileStatus || req.Op == storage.OpFileSessionClose || req.Op == storage.OpFileClose
	ctx, release, err := s.begin(ctx, reconcile)
	if err != nil {
		return fileResponse{}, err
	}
	defer release()
	response, err := h.performFile(ctx, s, req)
	if response.Receipt != nil {
		if invalid := validateFileReceipt(req, *response.Receipt); invalid != nil {
			return fileResponse{}, nativeFileFault(errors.Join(err, invalid))
		}
		if response.Receipt.Effects != 0 {
			if h.publisher != nil {
				h.publisher.wake()
			}
			if h.log != nil {
				barrier, barrierErr := h.mutationBarrier(ctx)
				response.Barrier = barrier
				if barrierErr != nil {
					err = errors.Join(err, &storage.FileError{Code: syscall.EIO, Cause: barrierErr})
				}
			}
		}
	}
	return response, err
}

func (h *Handler) checkedFileState(ctx context.Context) (storage.FileVolumeState, error) {
	if h.maxBodyBytes < max(fileControlRequestLimit(), fileControlResponseLimit()) {
		return storage.FileVolumeState{}, fmt.Errorf("file metadata exceeds the configured body bound: %w", syscall.EFBIG)
	}
	state, err := h.files.backend.FileState(ctx)
	if err != nil {
		return state, err
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileState}, fileResponse{State: &state}); err != nil {
		return storage.FileVolumeState{}, errors.Join(err, syscall.EIO)
	}

	return state, nil
}

func fileReceiptResponse(req fileRequest, result storage.FileActionReceipt, err error) (fileResponse, error) {
	if result.State != 0 && storage.IsFileCallNotAdmitted(err) {
		return fileResponse{}, nativeFileFault(errors.New("non-admission proof accompanied a historical receipt"))
	}
	if result.Errno != 0 && err == nil {
		return fileResponse{}, nativeFileFault(errors.New("receipt omitted its action error"))
	}
	if result.State == 0 {
		if err != nil && (req.Op == storage.OpFileQueryAction || storage.IsFileCallNotAdmitted(err)) {
			return fileResponse{}, err
		}
		return fileResponse{}, nativeFileFault(err)
	}
	receipt, conversion := fileReceiptOf(result)
	if conversion != nil {
		return fileResponse{}, nativeFileFault(errors.Join(err, conversion))
	}
	return fileResponse{Receipt: receipt}, err
}
func fileObservationResponse(observation storage.FileObservation, err error) (fileResponse, error) {
	if err != nil {
		return fileResponse{}, err
	}
	value, err := fileObservationOf(observation)
	return fileResponse{Observation: value}, err
}

func (h *Handler) performFile(ctx context.Context, s *servedFileSession, req fileRequest) (fileResponse, error) {
	var result storage.FileActionReceipt
	var err error
	switch req.Op {
	case storage.OpFileStatus, storage.OpFileRenew:
		started := time.Now()
		var status storage.FileSessionStatus
		if req.Op == storage.OpFileStatus {
			status, err = s.native.Status(ctx)
		} else {
			status, err = s.native.Renew(ctx)
		}
		if err != nil {
			return fileResponse{}, err
		}
		status, err = s.observe(status, started)
		if err != nil {
			return fileResponse{}, err
		}
		if status.Retired || status.Fenced {
			s.retire(syscall.ESTALE)
		}
		return fileResponse{Status: &status}, nil
	case storage.OpFileSessionClose:
		if err := ctx.Err(); err != nil {
			return fileResponse{}, err
		}
		s.cleanupMu.Lock()
		defer s.cleanupMu.Unlock()
		result, err = s.native.Close(ctx, req.Action)
		if err == nil {
			receipt, validation := fileReceiptOf(result)
			if validation == nil {
				validation = validateFileReceipt(req, *receipt)
			}
			if validation != nil {
				return fileResponse{}, nativeFileFault(validation)
			}
			if result.Errno == 0 && (result.State == storage.FileActionCompleted || result.State == storage.FileActionRetired) {
				s.retire(syscall.ESTALE)
				s.mu.Lock()
				s.cleanupComplete = true
				s.forgetAfter = laterFileTime(s.forgetAfter, time.Now().Add(result.HistoryRemaining))
				s.mu.Unlock()
			}
		}
	case storage.OpFileQueryAction:
		result, err = s.native.QueryAction(ctx, req.Action)
	case storage.OpFileCancelAction:
		result, err = s.native.CancelAction(ctx, req.Action)
	case storage.OpFileRetireRangeOwner:
		result, err = s.native.RetireRangeOwner(ctx, *req.Owner, req.Action)
	case storage.OpFileStatNode:
		return fileObservationResponse(s.native.StatNode(ctx, req.Node, *req.Observation))
	case storage.OpFileSetNodeAttr:
		change, e := req.Change.Storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = s.native.SetNodeAttr(ctx, req.Node, change, req.Action)
	case storage.OpFileRetain:
		operand, e := req.Retain.storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = s.native.Retain(ctx, operand, req.Action)
	case storage.OpFileRetainAt:
		operand, e := req.RetainAt.storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = s.native.RetainAt(ctx, operand, req.Action)
	case storage.OpFileCreateAndRetainAt, storage.OpFileReplaceAndRetainAt:
		operand, e := req.Create.storage()
		if e != nil {
			return fileResponse{}, e
		}
		if req.Op == storage.OpFileCreateAndRetainAt {
			result, err = s.native.CreateAndRetainAt(ctx, operand, req.Action)
		} else {
			result, err = s.native.ReplaceAndRetainAt(ctx, operand, req.Action)
		}
	case storage.OpFileResetAndRetainAt:
		operand, e := req.Reset.storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = s.native.ResetAndRetainAt(ctx, operand, req.Action)
	default:
		return performFileReference(ctx, s, req)
	}
	return fileReceiptResponse(req, result, err)
}

func performFileReference(ctx context.Context, s *servedFileSession, req fileRequest) (fileResponse, error) {
	file, err := s.native.Reference(ctx, req.Reference)
	if err != nil {
		return fileResponse{}, err
	}
	if file == nil || file.Reference() != req.Reference || file.NodeID() == 0 {
		return fileResponse{}, syscall.EIO
	}
	var result storage.FileActionReceipt
	switch req.Op {
	case storage.OpFileReference:
		return fileResponse{Reference: req.Reference, Node: file.NodeID()}, nil
	case storage.OpFileStat:
		return fileObservationResponse(file.Stat(ctx, *req.Observation))
	case storage.OpFileCheckObservation:
		return fileObservationResponse(file.CheckObservation(ctx, *req.Check))
	case storage.OpFileRead:
		read, err := file.ReadAt(ctx, req.Read.storage())
		if err != nil {
			return fileResponse{}, err
		}
		response, err := fileObservationResponse(storage.FileObservation{Attr: read.Attr}, nil)
		if err != nil {
			return fileResponse{}, err
		}
		response.Data = wireBytes(read.Data)
		return response, nil
	case storage.OpFileLookupAt:
		lookup, err := file.LookupAt(ctx, req.Name)
		if err != nil {
			return fileResponse{}, err
		}
		if lookup.ParentID != file.NodeID() {
			return fileResponse{}, nativeFileFault(errors.New("lookup changed parent identity"))
		}
		value, err := fileEntryLookupOf(lookup)
		if err != nil {
			return fileResponse{}, nativeFileFault(err)
		}
		return fileResponse{Lookup: value}, nil
	case storage.OpFileListAt:
		page, err := file.ListAt(ctx, *req.List)
		if err != nil {
			return fileResponse{}, err
		}
		value, err := fileDirectoryPageOf(page)
		if err != nil {
			return fileResponse{}, err
		}
		return fileResponse{Page: value}, nil
	case storage.OpFileRangeSnapshot:
		ranges, err := file.RangeSnapshot(ctx, *req.Owner, *req.Scope)
		if err != nil {
			return fileResponse{}, err
		}
		return fileResponse{Ranges: &ranges}, nil
	case storage.OpFileSync:
		return fileResponse{}, file.Sync(ctx)
	case storage.OpFileWrite:
		result, err = file.WriteAt(ctx, req.Write.storage(), req.Action)
	case storage.OpFileTruncate:
		result, err = file.Truncate(ctx, req.Truncate.storage(), req.Action)
	case storage.OpFileSetAttr:
		change, e := req.Change.Storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = file.SetAttr(ctx, change, req.Action)
	case storage.OpFileSetKind:
		change, e := req.Kind.storage()
		if e != nil {
			return fileResponse{}, e
		}
		result, err = file.SetKind(ctx, change, req.Action)
	case storage.OpFileRename:
		rename, conversion := req.Rename.storage()
		if conversion != nil {
			return fileResponse{}, conversion
		}
		result, err = file.Rename(ctx, rename, req.Action)
	case storage.OpFileReplaceClaim:
		result, err = file.ReplaceClaim(ctx, *req.Claim, req.Action)
	case storage.OpFilePrepareRemoval:
		result, err = file.PrepareRemoval(ctx, *req.Prepare, req.Action)
	case storage.OpFileCancelPrepared:
		result, err = file.CancelPrepared(ctx, *req.Intent, req.Action)
	case storage.OpFileDrainEntry:
		result, err = file.DrainEntry(ctx, *req.Drain, req.Action)
	case storage.OpFileCancelDrain:
		result, err = file.CancelDrain(ctx, *req.CancelDrain, req.Action)
	case storage.OpFileReplaceRanges:
		result, err = file.ReplaceRanges(ctx, *req.Ranges, req.Action)
	case storage.OpFileWaitRanges:
		result, err = file.WaitRanges(ctx, *req.Wait, req.Action)
	case storage.OpFileRetireRanges:
		result, err = file.RetireRangeOwner(ctx, *req.Owner, *req.Scope, req.Action)
	case storage.OpFileClose:
		result, err = file.Close(ctx, req.Action)
	default:
		return fileResponse{}, syscall.ENOSYS
	}
	return fileReceiptResponse(req, result, err)
}
