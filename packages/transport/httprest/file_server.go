package httprest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileRegistry struct {
	enrolling      int
	enrollments    sync.WaitGroup
	closeMu        sync.Mutex
	mu             sync.Mutex
	limits         FileLimits
	backend        storage.FileStorage
	sessions       map[string]*servedFileSession
	terminalCloses map[string]*terminalFileClose
	closed         bool
	running        bool
	wake           chan struct{}
	done           chan struct{}
	err            error
	terminalErr    closeErrorSummary
}

const maxRetainedCloseErrors = 16

// Close errors can precede Handler.Close by arbitrarily many retired sessions.
// Keep the first failure and a bounded sample of later failures while counting
// every released reference whose cleanup failed.
type closeErrorSummary struct {
	count   uint64
	samples []error
}

func (s *closeErrorSummary) add(err error) {
	if err == nil {
		return
	}
	s.count++
	retained := retainFileError(err)
	if len(s.samples) < maxRetainedCloseErrors {
		s.samples = append(s.samples, retained)
		return
	}
	copy(s.samples[1:], s.samples[2:])
	s.samples[len(s.samples)-1] = retained
}

func (s closeErrorSummary) result() error {
	if s.count == 0 {
		return nil
	}
	return &closeErrors{count: s.count, samples: append([]error(nil), s.samples...)}
}

type closeErrors struct {
	count   uint64
	samples []error
}

func (e *closeErrors) Error() string {
	return fmt.Sprintf("%d file reference cleanup failures: %v", e.count, errors.Join(e.samples...))
}

func (e *closeErrors) Unwrap() []error { return e.samples }

type servedFileSession struct {
	recoveryAdmission   *bodyAdmission
	dataActions         int
	cleanupActions      int
	cleanupReserved     int
	closeAction         storage.LockRequestID
	closeGeneration     uint64
	closeDetermined     bool
	closeReconciling    bool
	closeReleaseErr     error
	sessionCloseReserve int
	recoverable         bool
	stableIdentity      bool
	inlineSettlement    bool
	releaseFact         bool
	closeIDs            map[storage.LockRequestID]closeIDUse
	authority           string
	revision            uint64
	mu                  sync.Mutex
	native              storage.FileSession
	files               map[string]*servedFile
	actions             map[storage.LockRequestID]*servedFileAction
	options             storage.FileSessionOptions
	started             time.Time
	expires             time.Time
	retired             bool
	explicitClose       bool
	autoClose           bool
}

type servedFile struct {
	native          retainedReference
	contentEffects  []storage.ContentMetadataEffect
	pending         time.Time
	closing         bool
	replaying       int
	closeReserve    int
	closeAction     storage.LockRequestID
	closeGeneration uint64
	closeDetermined bool
	closeImplicit   bool
}

type closeIDUse struct {
	owner       string
	generation  uint64
	implicit    bool
	notExecuted bool
	proofEpoch  uint64
}

type retainedReference interface {
	Stat(context.Context) (storage.Attr, error)
	SetAttr(context.Context, storage.AttrChange) (storage.Attr, error)
	Close(context.Context) error
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}

type servedFileAction struct {
	retryMu          sync.Mutex
	id               storage.LockRequestID
	op               storage.Operation
	cleanup          bool
	digest           [32]byte
	done             chan struct{}
	expires          time.Time
	response         fileResponse
	file             string
	closeOwner       string
	closeGeneration  uint64
	closeImplicit    bool
	priorAction      storage.LockRequestID
	priorGeneration  uint64
	priorDetermined  bool
	priorImplicit    bool
	priorClosing     bool
	err              error
	closeSemanticErr error
	barrierPending   bool
	uncertain        bool
}

type terminalFileClose struct {
	mu          sync.Mutex
	epoch       uint64
	history     time.Duration
	expires     time.Time
	releaseErr  error
	releaseFact bool
	actions     map[storage.LockRequestID]*servedFileAction
}

type fileBarrierError struct {
	cause error
	prior error
}

func (e *fileBarrierError) Error() string { return errors.Join(e.prior, e.cause).Error() }
func (e *fileBarrierError) Unwrap() error { return errors.Join(e.prior, e.cause) }

type recordedFileError struct{ cause error }

func (e *recordedFileError) Error() string { return e.cause.Error() }
func (e *recordedFileError) Unwrap() error { return e.cause }

func newFileRegistry(s storage.Storage, limits FileLimits) *fileRegistry {
	backend, _ := s.(storage.FileStorage)
	return &fileRegistry{limits: limits, backend: backend, sessions: make(map[string]*servedFileSession), terminalCloses: make(map[string]*terminalFileClose), wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func fileCapability() string {
	var value [32]byte
	rand.Read(value[:])
	return hex.EncodeToString(value[:])
}

func (s *servedFileSession) epoch(now time.Time) uint64 {
	return uint64(now.Sub(s.started)/s.options.History) + 1
}

func (s *servedFileSession) pruneCloseIDsLocked(httpEpoch, nativeEpoch uint64) {
	for id, use := range s.closeIDs {
		idEpoch, err := id.Epoch()
		if err != nil {
			continue
		}
		if use.implicit && idEpoch < httpEpoch && httpEpoch-idEpoch >= 2 || !use.implicit && nativeEpoch != 0 && idEpoch < nativeEpoch {
			delete(s.closeIDs, id)
		}
	}
}

func (r *fileRegistry) startLocked() {
	if !r.running {
		r.done = make(chan struct{})
		if r.closed {
			r.err = r.terminalErr.result()
		}
		r.running = true
		go r.run()
	}
}

func (r *fileRegistry) stop() <-chan struct{} {
	r.mu.Lock()
	r.closed = true
	r.startLocked()
	done := r.done
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return done
}

// Close retires and drains sessions created by this handler. The volume
// backend remains owned by the caller. Drain the HTTP server before closing
// that backend; a failed Close can retain native references and cleanup work.
func (h *Handler) Close(ctx context.Context) error {
	h.files.closeMu.Lock()
	defer h.files.closeMu.Unlock()
	h.Stop()
	done := h.files.stop()
	select {
	case <-done:
		h.files.mu.Lock()
		defer h.files.mu.Unlock()
		return h.files.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *fileRegistry) run() {
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	done := r.done
	defer func() {
		r.mu.Lock()
		r.running = false
		close(done)
		r.mu.Unlock()
	}()
	for {
		select {
		case <-timer.C:
		case <-r.wake:
		}
		r.mu.Lock()
		closing := r.closed
		r.mu.Unlock()
		if closing {
			r.enrollments.Wait()
		}
		r.mu.Lock()
		for id, close := range r.terminalCloses {
			close.mu.Lock()
			expired := !time.Now().Before(close.expires)
			close.mu.Unlock()
			if expired {
				delete(r.terminalCloses, id)
			}
		}
		sessions := make(map[string]*servedFileSession, len(r.sessions))
		for id, s := range r.sessions {
			sessions[id] = s
		}
		r.mu.Unlock()
		now := time.Now()
		for id, s := range sessions {
			r.mu.Lock()
			current := r.sessions[id] == s
			r.mu.Unlock()
			if !current {
				continue
			}
			s.mu.Lock()
			expired := !now.Before(s.expires)
			retire := closing || expired || s.retired
			if retire {
				s.retired = true
			}
			pending := make(map[string]*servedFile)
			if !retire {
				for cap, f := range s.files {
					if !f.closing && f.replaying == 0 && !f.pending.IsZero() && !now.Before(f.pending) {
						f.closing = true
						pending[cap] = f
					}
				}
			}
			for key, a := range s.actions {
				select {
				case <-a.done:
					if !now.Before(a.expires) && !a.uncertain {
						delete(s.actions, key)
						if a.cleanup {
							s.cleanupActions--
							if a.op == storage.OpFileClose {
								if file := s.files[a.closeOwner]; file != nil {
									file.closeReserve++
									s.cleanupReserved++
								}
							} else if a.op == storage.OpFileSessionClose && s.sessionCloseReserve < 2 {
								s.sessionCloseReserve++
								s.cleanupReserved++
							}
						} else {
							s.dataActions--
						}
					}
				default:
				}
			}
			s.mu.Unlock()
			for cap, f := range pending {
				result, closeErr := f.native.CloseWithResult(context.Background())
				if err := errors.Join(closeErr, result.Check(closeErr)); err != nil {
					s.mu.Lock()
					s.retired = true
					s.mu.Unlock()
					retire = true
					if closing {
						r.mu.Lock()
						r.recordCloseErrorLocked(err, result.Released)
						r.mu.Unlock()
					} else if result.Released {
						r.mu.Lock()
						r.terminalErr.add(err)
						r.mu.Unlock()
					}
				}
				if result.Released {
					reconciled := r.reconcileReleasedClose(context.Background(), s, cap, f, "")
					s.mu.Lock()
					if reconciled {
						s.cleanupReserved -= f.closeReserve
						f.closeReserve = 0
						delete(s.files, cap)
					} else {
						s.retired = true
						retire = true
					}
					s.mu.Unlock()
				}
			}
			if retire {
				r.closeRetiringSession(id, s, closing)
			}
		}
		if closing {
			r.mu.Lock()
			pending := false
			for _, s := range r.sessions {
				s.mu.Lock()
				pending = pending || s.explicitClose || s.closeReconciling
				s.mu.Unlock()
			}
			r.mu.Unlock()
			if !pending {
				return
			}
		}
	}
}

func (r *fileRegistry) closeRetiringSession(id string, session *servedFileSession, closing bool) {
	session.mu.Lock()
	if session.explicitClose || session.autoClose {
		session.mu.Unlock()
		return
	}
	session.autoClose = true
	reconciling, releaseErr := session.closeReconciling, session.closeReleaseErr
	session.mu.Unlock()

	result := storage.ReferenceCloseResult{Released: true, Determined: true}
	err := releaseErr
	if !reconciling {
		var closeErr error
		result, closeErr = session.native.CloseWithResult(context.Background())
		session.observeSessionRelease(result, closeErr)
		err = errors.Join(closeErr, result.Check(closeErr))
	}
	reconciled := false
	if result.Released {
		reconciled = r.reconcileSessionFileCloses(context.Background(), session)
	}
	r.mu.Lock()
	if result.Released && reconciled && r.sessions[id] == session {
		delete(r.sessions, id)
		now := time.Now()
		terminal := &terminalFileClose{
			epoch: session.epoch(now), history: session.options.History,
			expires: now.Add(2 * session.options.History), releaseErr: retainFileError(err),
			actions: make(map[storage.LockRequestID]*servedFileAction),
		}
		session.mu.Lock()
		terminal.releaseFact = session.releaseFact
		for id, action := range session.actions {
			if (action.op == storage.OpFileClose || action.op == storage.OpFileSessionClose) && (now.Before(action.expires) || action.uncertain) {
				if action.uncertain && !now.Before(action.expires) {
					action.expires = now.Add(session.options.History)
				}
				terminal.actions[id] = action
			}
		}
		session.mu.Unlock()
		r.terminalCloses[id] = terminal
	}
	if err != nil && closing && !reconciling {
		r.recordCloseErrorLocked(err, result.Released)
	} else if err != nil && result.Released && !reconciling {
		r.terminalErr.add(err)
	}
	r.mu.Unlock()
	session.mu.Lock()
	session.closeReconciling = result.Released && !reconciled
	if session.closeReconciling {
		session.closeReleaseErr = retainFileError(err)
	}
	if !result.Released || !reconciled {
		session.autoClose = false
	}
	session.mu.Unlock()
}

func (r *fileRegistry) recordCloseErrorLocked(err error, released bool) {
	r.err = errors.Join(r.err, err)
	if released {
		r.terminalErr.add(err)
	}
}

func (r *fileRegistry) reconcileReleasedClose(ctx context.Context, session *servedFileSession, capability string, file *servedFile, active storage.LockRequestID) bool {
	closer, ok := file.native.(storage.ReferenceCloseActions)
	if !ok {
		return true
	}
	session.mu.Lock()
	actions := make([]*servedFileAction, 0, 2)
	for _, action := range session.actions {
		if action.op == storage.OpFileClose && action.closeOwner == capability && action.id != active {
			actions = append(actions, action)
		}
	}
	session.mu.Unlock()
	settled := true
	for _, action := range actions {
		// Initial publication owns the result until done. Reconciliation never
		// waits for a sibling retry, whose native close may still be in flight.
		select {
		case <-action.done:
		default:
			settled = false
			continue
		}
		if !action.retryMu.TryLock() {
			settled = false
			continue
		}
		if action.response.CloseResult != nil && action.response.CloseResult.Determined {
			action.retryMu.Unlock()
			continue
		}
		if ctx.Err() != nil {
			settled = false
			action.retryMu.Unlock()
			continue
		}
		actionID := storage.FileActionID(action.id)
		if actionID == "" {
			action.retryMu.Unlock()
			continue
		}
		attempt := storage.CloseAttempt{Action: actionID, Generation: action.closeGeneration}
		var result storage.ReferenceCloseResult
		var closeErr error
		boundCompleted := false
		if action.closeImplicit {
			result, closeErr = file.native.CloseWithResult(ctx)
		} else {
			receipt, queryErr := closer.QueryCloseAttempt(ctx, attempt)
			if queryErr != nil {
				settled = false
			} else if receipt.Outcome == storage.FileActionCompleted && receipt.Action == actionID {
				boundCompleted = true
				result, closeErr = closer.CloseWithAction(ctx, attempt)
				if !result.Released && !result.Determined {
					settled = false
				}
			}
		}
		if result.Released || boundCompleted && result.Determined {
			action.response.CloseResult = referenceCloseResultOf(result)
			action.response.CloseResult.BarrierPending = result.Released
			action.response.Barrier = nil
			action.closeSemanticErr = retainFileError(closeErr)
			action.err = action.closeSemanticErr
			action.barrierPending = result.Released
			session.mu.Lock()
			action.uncertain = false
			action.expires = time.Now().Add(session.options.History)
			session.mu.Unlock()
		} else if ctx.Err() != nil {
			settled = false
		}
		action.retryMu.Unlock()
	}
	return settled
}

func (r *fileRegistry) reconcileSessionFileCloses(ctx context.Context, session *servedFileSession) bool {
	session.mu.Lock()
	files := make(map[string]*servedFile, len(session.files))
	for cap, file := range session.files {
		files[cap] = file
	}
	session.mu.Unlock()
	settled := true
	for cap, file := range files {
		if !r.reconcileReleasedClose(ctx, session, cap, file, "") {
			settled = false
		}
	}
	return settled
}

func (r *fileRegistry) terminalizeSession(id string, session *servedFileSession, epoch uint64, expires time.Time, releaseErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[id] != session {
		return
	}
	delete(r.sessions, id)
	terminal := &terminalFileClose{
		epoch: epoch, history: session.options.History, expires: expires,
		releaseErr: releaseErr, actions: make(map[storage.LockRequestID]*servedFileAction),
	}
	session.mu.Lock()
	terminal.releaseFact = session.releaseFact
	now := time.Now()
	for actionID, recorded := range session.actions {
		if (recorded.op == storage.OpFileClose || recorded.op == storage.OpFileSessionClose) && (now.Before(recorded.expires) || recorded.uncertain) {
			if recorded.uncertain && !now.Before(recorded.expires) {
				recorded.expires = now.Add(session.options.History)
			}
			terminal.actions[actionID] = recorded
			if recorded.expires.After(terminal.expires) {
				terminal.expires = recorded.expires
			}
		}
	}
	session.mu.Unlock()
	r.terminalCloses[id] = terminal
}

func (h *Handler) serveFile(w http.ResponseWriter, r *http.Request) {
	control := r.URL.Path == Prefix+string(OpFileControl)
	recovery := r.URL.Path == Prefix+string(OpFileRecovery)
	writeResponse := func(status int, body any) {
		if recovery {
			h.writeBoundedFileResponse(w, status, body, min(h.maxBodyBytes, MaxFileRecoveryBytes))
			return
		}
		h.writeFileResponse(w, status, body, control)
	}
	writeFault := func(status int, err error) {
		writeResponse(status, ErrorResponse{Message: err.Error()})
	}
	writeError := func(err error) {
		var barrierFailure *fileBarrierError
		if errors.As(err, &barrierFailure) {
			writeFault(http.StatusInternalServerError, err)
			return
		}
		var recorded *recordedFileError
		if errors.As(err, &recorded) {
			writeResponse(StatusStorageError, fileErrorResponse(err, true))
			return
		}
		if response, ok := authorizationResponse(err); ok {
			writeResponse(StatusStorageError, response)
			return
		}
		writeResponse(StatusStorageError, fileErrorResponse(err, false))
	}
	if h.stopped() {
		writeError(syscall.EIO)
		return
	}
	if r.Header.Get("Content-Type") != contentJSON {
		writeFault(http.StatusUnsupportedMediaType, errors.New("file calls require application/json"))
		return
	}
	var body []byte
	var err error
	if recovery {
		release, e := h.fileRecovery.acquire(r.Context(), retainedResponseMultiplier*min(h.maxBodyBytes, MaxFileRecoveryBytes))
		if e != nil {
			writeError(e)
			return
		}
		defer release()
		body, err = readAtMost(r.Body, min(h.maxBodyBytes, MaxFileRecoveryBytes))
		if err == nil && r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
			err = errors.New("file recovery body did not arrive whole")
		}
	} else if control {
		release, e := h.lockControls.acquire(r.Context(), retainedResponseMultiplier*MaxFileControlBytes)
		if e != nil {
			writeError(e)
			return
		}
		defer release()
		body, err = readAtMost(r.Body, min(h.maxBodyBytes, MaxFileControlBytes))
		if err == nil && r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
			err = errors.New("file control body did not arrive whole")
		}
	} else {
		releaseResponse, e := h.responses.acquire(r.Context(), retainedResponseMultiplier*h.maxBodyBytes)
		if e != nil {
			writeError(e)
			return
		}
		defer releaseResponse()
		var release func()
		body, release, err = h.readBody(r, h.maxBodyBytes)
		if err == nil {
			defer release()
		}
	}
	if err != nil {
		writeFault(http.StatusBadRequest, err)
		return
	}
	var req fileRequest
	if err := decodeFileJSON(body, &req); err != nil {
		writeFault(http.StatusBadRequest, err)
		return
	}
	if err := validateFileRequest(req); err != nil {
		writeFault(http.StatusBadRequest, err)
		return
	}
	if recovery && (!isExplicitFileWrite(req) || len(req.Mutation.Data) > MaxFileRecoveryDataBytes) || !recovery && fileControl(req.Op) != control {
		writeFault(http.StatusBadRequest, errors.New("file operation uses the wrong admission endpoint"))
		return
	}
	if err := validateFileArguments(req, h.files.limits.Session); err != nil {
		writeError(err)
		return
	}
	if req.Op == storage.OpFileRangeApply && rangeResponseBound(req.Commands) > min(h.maxBodyBytes, MaxFileControlBytes) {
		writeError(syscall.EFBIG)
		return
	}
	if req.Op == storage.OpFileRead && int64(req.Length) > fileReadLimit(h.maxBodyBytes) {
		writeError(syscall.EFBIG)
		return
	}
	if req.Mutation != nil {
		if err := req.Mutation.storage().CheckDataLimit(h.maxWriteBytes); err != nil {
			writeError(err)
			return
		}
	}
	if req.Op == storage.OpFileWrite && int64(len(req.Data)) > h.maxWriteBytes {
		writeError(syscall.EFBIG)
		return
	}
	if req.Op == storage.OpFileSetNodeMetadata || req.Op == storage.OpFileSetMetadata {
		bound, e := metadataResponseBound(len(req.Payload), h.log != nil, h.maxIncarnationBytes)
		if e != nil || bound > min(req.ResultBytes, h.maxBodyBytes) {
			if e == nil {
				e = syscall.EFBIG
			}
			writeError(e)
			return
		}
	}
	scopeOp := OpFile
	if fileMutation(req.Op) {
		scopeOp = OpWrite
	}
	scope, present, err := requestMutationScope(r, scopeOp)
	if err != nil {
		writeFault(http.StatusBadRequest, err)
		return
	}
	if present || fileMutation(req.Op) {
		r = r.WithContext(locking.WithScope(r.Context(), scope))
	}
	if err := h.authorizeFile(r.Context(), req); err != nil {
		writeError(err)
		return
	}
	if recovery {
		h.files.mu.Lock()
		session := h.files.sessions[req.Session]
		h.files.mu.Unlock()
		if session == nil {
			writeError(syscall.ESTALE)
			return
		}
		session.mu.Lock()
		if session.recoveryAdmission == nil {
			session.recoveryAdmission = newBodyAdmission(1, retainedResponseMultiplier*min(h.maxBodyBytes, MaxFileRecoveryBytes), 0)
		}
		lane := session.recoveryAdmission
		session.mu.Unlock()
		release, e := lane.acquire(r.Context(), retainedResponseMultiplier*min(h.maxBodyBytes, MaxFileRecoveryBytes))
		if e != nil {
			writeError(e)
			return
		}
		defer release()
	}
	if fileAttrResult(req.Op) {
		r = r.WithContext(storage.WithBoundedAttrResult(r.Context(), req.ResultBytes, h.attrResultBudget(req)))
	}
	r = r.WithContext(h.observationResultContext(r.Context(), req))
	journalBody, err := canonicalFileRequestBody(body, req)
	if err != nil {
		writeFault(http.StatusInternalServerError, err)
		return
	}
	digest := sha256.Sum256(append(journalBody, []byte(r.Header.Get(HeaderMutationScope))...))
	response, err := h.fileCall(r.Context(), req, digest)
	if err != nil {
		var closeProof *storage.CloseActionNotExecutedError
		if errors.As(err, &closeProof) {
			writeResponse(StatusStorageError, ErrorResponse{Errno: "ESTALE", Message: closeProof.Error(), CloseNotExecutedEpoch: closeProof.CurrentEpoch})
			return
		}
		if response.Attempt != nil {
			writeResponse(StatusStorageError, ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error(), CapabilityCode: capabilityErrorCode(err), Attempt: response.Attempt})
			return
		}
		var recorded *recordedFileError
		if errors.As(err, &recorded) {
			body := fileErrorResponse(err, true)
			body.FileResult = partialFileResult(req, response)
			writeResponse(StatusStorageError, body)
			return
		}
		if result := partialFileResult(req, response); result != nil {
			body := fileErrorResponse(err, false)
			body.FileResult = result
			writeResponse(StatusStorageError, body)
			return
		}
		writeError(err)
		return
	}
	writeResponse(http.StatusOK, response)
}

func partialFileResult(request fileRequest, response fileResponse) *fileResponse {
	include := false
	switch request.Op {
	case storage.OpFileQueryAction:
		include = response.ActionReceipt != nil
	case storage.OpFileQueryDeleteIntent:
		include = response.DeleteStatus != nil
	case storage.OpFileListDeleteIntents:
		include = response.DeletePage != nil
	case storage.OpFileClose, storage.OpFileSessionClose, opFileSessionReleaseResult:
		include = response.CloseResult != nil
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		include = response.File != "" || response.Node != 0 || response.Attr != nil || response.Outcome != 0 || response.Barrier != nil
	case storage.OpFileMutateName, storage.OpFileMutate:
		include = response.Attr != nil || response.Barrier != nil
	case storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		include = response.State != nil || response.Barrier != nil
	}
	if !include {
		return nil
	}
	copy := response
	if copy.Data == nil {
		copy.Data = []byte{}
	}
	return &copy
}

func fileErrorResponse(err error, retained bool) ErrorResponse {
	response := ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error(), CapabilityCode: capabilityErrorCode(err)}
	if retained {
		value := true
		response.FileRecorded = &value
	}
	if failure := volumeLockFailure(err); failure != nil {
		response.CapabilityCode = ""
		response.Message = failure.Message
		response.LockCode = failure.Code
		recorded := failure.Recorded
		response.Recorded = &recorded
	}
	return response
}

func validateFileArguments(req fileRequest, maximum storage.FileSessionOptions) error {
	if fileBoundedResult(req.Op) && req.ResultBytes <= 0 {
		return syscall.EINVAL
	}
	switch req.Op {
	case storage.OpFileSessionOpen:
		return checkFileSessionOptions(req.Options, maximum)
	case storage.OpFileOpen:
		if err := req.Open.Check(); err != nil {
			return err
		}
		_, err := storage.CleanPath(string(req.Path))
		return err
	case storage.OpFileOpenNode:
		return req.Open.CheckNode(req.Node)
	case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileObserveDirectoryMetadata, storage.OpFileObserveName, storage.OpFileMutateName:
		return validateCapabilityArguments(req)
	case storage.OpFileRead:
		if req.Offset < 0 || req.Length < 0 {
			return syscall.EINVAL
		}
	case storage.OpFileWrite:
		if req.Offset < 0 {
			return syscall.EINVAL
		}
		if int64(len(req.Data)) > math.MaxInt64-req.Offset {
			return syscall.EFBIG
		}
	case storage.OpFileTruncate:
		if req.Offset < 0 {
			return syscall.EINVAL
		}
	case storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
		return req.Change.Storage().Check()
	default:
		return validateCapabilityArguments(req)
	}
	return nil
}

func (h *Handler) fileCall(ctx context.Context, req fileRequest, digest [32]byte) (fileResponse, error) {
	registry := h.files
	if req.Op == opFileSessionReleaseResult {
		return h.sessionReleaseResult(ctx, req.Session)
	}
	if req.Op == storage.OpFileBackendIdentity {
		return h.backendIdentity(ctx)
	}
	if req.Op == storage.OpFileSessionOpen {
		return registry.enroll(ctx, req.Options)
	}

	registry.mu.Lock()
	session := registry.sessions[req.Session]
	terminal, terminalFound := registry.terminalCloses[req.Session]
	closed := registry.closed
	registry.mu.Unlock()
	if closed {
		return fileResponse{}, syscall.EIO
	}
	if session == nil {
		if terminalFound {
			return h.replayTerminalFileClose(ctx, req, digest, terminal)
		}
		return fileResponse{}, syscall.ESTALE
	}
	session.mu.Lock()
	now := time.Now()
	epoch := session.epoch(now)
	if req.Op == storage.OpFileQueryAction {
		if action := session.actions[storage.LockRequestID(req.FileAction)]; action != nil && (action.op == storage.OpFileClose || action.op == storage.OpFileSessionClose) {
			if req.File != "" && (action.op != storage.OpFileClose || action.closeOwner != req.File || action.closeGeneration != req.CloseGeneration) {
				session.mu.Unlock()
				return fileResponse{}, syscall.EINVAL
			}
			session.mu.Unlock()
			select {
			case <-action.done:
			case <-ctx.Done():
				return fileResponse{}, ctx.Err()
			}
			action.retryMu.Lock()
			outcome := storage.FileActionUnknown
			if action.response.CloseResult != nil && action.response.CloseResult.Determined {
				outcome = storage.FileActionCompleted
			} else if req.File != "" && action.op == storage.OpFileClose && !action.closeImplicit {
				session.mu.Lock()
				file := session.files[req.File]
				session.mu.Unlock()
				if file != nil {
					if closer, ok := file.native.(storage.ReferenceCloseActions); ok {
						bound, queryErr := closer.QueryCloseAttempt(ctx, storage.CloseAttempt{Action: req.FileAction, Generation: req.CloseGeneration})
						if queryErr != nil {
							action.retryMu.Unlock()
							return fileResponse{}, queryErr
						}
						if bound.Action == req.FileAction && bound.Operation == storage.OpFileClose && bound.Outcome == storage.FileActionCompleted {
							outcome = storage.FileActionCompleted
						}
						if bound.Outcome == storage.FileActionNotExecuted {
							owner, ownerErr := closer.CloseOwnerStatus(ctx)
							if ownerErr != nil {
								action.retryMu.Unlock()
								return fileResponse{}, ownerErr
							}
							if err := owner.Check(); err != nil {
								action.retryMu.Unlock()
								return fileResponse{}, err
							}
							session.mu.Lock()
							if session.actions[action.id] == action {
								if _, retained := session.closeIDs[action.id]; !retained && len(session.closeIDs) >= session.options.MaxCloseActions {
									session.mu.Unlock()
									action.retryMu.Unlock()
									return fileResponse{}, syscall.EAGAIN
								}
								delete(session.actions, action.id)
								if session.closeIDs == nil {
									session.closeIDs = make(map[storage.LockRequestID]closeIDUse)
								}
								session.closeIDs[action.id] = closeIDUse{owner: req.File, generation: req.CloseGeneration, notExecuted: true, proofEpoch: owner.CurrentEpoch}
								session.cleanupActions--
								session.cleanupReserved++
								file.closeReserve++
								file.closeAction, file.closeGeneration = action.priorAction, action.priorGeneration
								file.closeDetermined, file.closeImplicit, file.closing = action.priorDetermined, action.priorImplicit, action.priorClosing
							}
							session.mu.Unlock()
							outcome = storage.FileActionNotExecuted
						}
					}
				}
			}
			receipt := storage.FileActionReceipt{Action: req.FileAction, Operation: action.op, Outcome: outcome}
			action.retryMu.Unlock()
			return fileResponse{Epoch: epoch, ActionReceipt: &receipt}, nil
		}
		if use, found := session.closeIDs[storage.LockRequestID(req.FileAction)]; found && use.notExecuted {
			if req.File != "" && (use.owner != req.File || use.generation != req.CloseGeneration) {
				session.mu.Unlock()
				return fileResponse{}, syscall.EINVAL
			}
			session.mu.Unlock()
			receipt := storage.FileActionReceipt{Action: req.FileAction, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}
			return fileResponse{Epoch: epoch, ActionReceipt: &receipt}, nil
		}
		if req.File != "" {
			file := session.files[req.File]
			session.mu.Unlock()
			if file == nil || file.native == nil {
				return fileResponse{}, syscall.ESTALE
			}
			closer, ok := file.native.(storage.ReferenceCloseActions)
			if !ok {
				return fileResponse{}, syscall.EOPNOTSUPP
			}
			receipt, queryErr := closer.QueryCloseAttempt(ctx, storage.CloseAttempt{Action: req.FileAction, Generation: req.CloseGeneration})
			if queryErr != nil {
				return fileResponse{}, queryErr
			}
			return fileResponse{Epoch: epoch, ActionReceipt: &receipt}, nil
		}
	}
	if req.Op == storage.OpFileClose {
		if use, found := session.closeIDs[req.Action]; found && use.notExecuted {
			if use.owner != req.File || use.generation != req.CloseGeneration {
				session.mu.Unlock()
				return fileResponse{}, syscall.EINVAL
			}
			session.mu.Unlock()
			return fileResponse{}, &storage.CloseActionNotExecutedError{CurrentEpoch: use.proofEpoch}
		}
	}
	if fileActionRequired(req.Op) {
		actionEpoch, err := req.Action.Epoch()
		if err != nil {
			session.mu.Unlock()
			return fileResponse{}, err
		}
		if previous := session.actions[req.Action]; previous != nil {
			if previous.digest != digest {
				session.mu.Unlock()
				return fileResponse{}, syscall.EINVAL
			}
			replayedFile := ""
			select {
			case <-previous.done:
				replayedFile = beginSemanticOpenReplay(session, req, previous)
				session.mu.Unlock()
			default:
				session.mu.Unlock()
				select {
				case <-previous.done:
					session.mu.Lock()
					replayedFile = beginSemanticOpenReplay(session, req, previous)
					session.mu.Unlock()
				case <-ctx.Done():
					return fileResponse{}, operationFailure(Request{Op: OpFile}, ctx.Err(), false)
				}
			}
			previous.retryMu.Lock()
			if replayedFile != "" {
				defer h.finishSemanticOpenReplay(session, replayedFile)
			}
			retryClose := !previous.barrierPending && (previous.response.CloseResult == nil || !previous.response.CloseResult.Determined) &&
				(req.Op == storage.OpFileClose && session.recoverable || req.Op == storage.OpFileSessionClose)
			if retryClose {
				response, retryErr := h.performFile(ctx, session, req)
				response.Epoch = epoch
				previous.response = response
				previous.err = retainFileActionError(retryErr)
				var barrierFailure *fileBarrierError
				previous.barrierPending = errors.As(retryErr, &barrierFailure)
				if previous.barrierPending {
					previous.closeSemanticErr = retainFileError(barrierFailure.prior)
				}
				session.mu.Lock()
				wasUncertain := previous.uncertain
				previous.uncertain = response.CloseResult == nil || !response.CloseResult.Determined
				if wasUncertain && !previous.uncertain {
					previous.expires = time.Now().Add(session.options.History)
				}
				if file := session.files[req.File]; file != nil {
					file.closeDetermined = response.CloseResult != nil && response.CloseResult.Determined && !response.CloseResult.Released
				}
				if req.Op == storage.OpFileSessionClose {
					session.closeDetermined = response.CloseResult != nil && response.CloseResult.Determined && !response.CloseResult.Released
				}
				session.mu.Unlock()
			}
			if previous.barrierPending {
				response, retryErr := h.finishFileMutation(ctx, previous.response)
				previous.response = response
				previous.err = errors.Join(previous.closeSemanticErr, retainFileActionError(retryErr))
				previous.barrierPending = retryErr != nil
			}
			response, resultErr := previous.response, previous.err
			barrierPending, expires := previous.barrierPending, previous.expires
			releaseErr := resultErr
			if barrierPending {
				releaseErr = previous.closeSemanticErr
			}
			previous.retryMu.Unlock()
			h.finishPublishedClose(ctx, session, req, response, expires, releaseErr)
			if resultErr != nil && !barrierPending {
				return response, &recordedFileError{cause: resultErr}
			}
			return response, resultErr
		}
		nativeExplicitClose := req.Op == storage.OpFileClose && !req.CloseImplicit && session.recoverable
		if req.Op == storage.OpFileClose && !req.CloseImplicit && !session.recoverable {
			session.mu.Unlock()
			return fileResponse{}, syscall.EOPNOTSUPP
		}
		var nativeEpoch uint64
		var nativeOwner storage.CloseOwnerStatus
		adoptingNative := false
		var priorCloseAction storage.LockRequestID
		var priorCloseGeneration uint64
		var priorCloseDetermined, priorFileClosing, priorCloseImplicit bool
		if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
			if nativeExplicitClose {
				file := session.files[req.File]
				if file == nil || file.native == nil {
					session.mu.Unlock()
					return fileResponse{}, syscall.ESTALE
				}
				closer, ok := file.native.(storage.ReferenceCloseActions)
				if !ok {
					session.mu.Unlock()
					return fileResponse{}, syscall.EOPNOTSUPP
				}
				status, statusErr := closer.CloseOwnerStatus(ctx)
				if statusErr != nil {
					session.mu.Unlock()
					return fileResponse{}, statusErr
				}
				if err := status.Check(); err != nil {
					session.mu.Unlock()
					return fileResponse{}, err
				}
				if status.Released {
					session.mu.Unlock()
					return fileResponse{}, syscall.ESTALE
				}
				nativeOwner = status
				nativeEpoch = status.CurrentEpoch
				if nativeEpoch == 0 {
					session.mu.Unlock()
					return fileResponse{}, syscall.EIO
				}
				if status.Current != nil {
					adoptingNative = status.Current.Action == storage.FileActionID(req.Action) && status.Current.Generation == req.CloseGeneration
					if !adoptingNative {
						session.mu.Unlock()
						return fileResponse{}, syscall.EBUSY
					}
				} else if !status.Ready || session.autoClose || session.explicitClose {
					session.mu.Unlock()
					return fileResponse{}, syscall.EAGAIN
				}
			}
			session.pruneCloseIDsLocked(epoch, nativeEpoch)
			if use, reused := session.closeIDs[req.Action]; reused {
				if adoptingNative && !use.notExecuted && use.owner == req.File && use.generation == req.CloseGeneration {
					// The native owner still identifies this exact attempt after HTTP history expiry.
				} else {
					session.mu.Unlock()
					if use.notExecuted && nativeExplicitClose && use.owner == req.File && use.generation == req.CloseGeneration {
						return fileResponse{}, &storage.CloseActionNotExecutedError{CurrentEpoch: nativeEpoch}
					}
					return fileResponse{}, syscall.ESTALE
				}
			}
		}
		if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
			var previous storage.LockRequestID
			var generation uint64
			var determined bool
			if req.Op == storage.OpFileClose {
				file := session.files[req.File]
				if file == nil || file.native == nil {
					session.mu.Unlock()
					return fileResponse{}, syscall.ESTALE
				}
				previous, generation, determined = file.closeAction, file.closeGeneration, file.closeDetermined
				priorCloseAction, priorCloseGeneration, priorCloseDetermined, priorFileClosing = file.closeAction, file.closeGeneration, file.closeDetermined, file.closing
				priorCloseImplicit = file.closeImplicit
			} else {
				previous, generation, determined = session.closeAction, session.closeGeneration, session.closeDetermined
			}
			if nativeExplicitClose {
				priorImplicit := req.Op == storage.OpFileClose && session.files[req.File].closeImplicit
				if previous != "" && !determined && !(adoptingNative && (previous == req.Action && generation == req.CloseGeneration || priorImplicit)) {
					session.mu.Unlock()
					return fileResponse{}, syscall.EAGAIN
				}
				if !adoptingNative && req.CloseGeneration != nativeOwner.NextGeneration {
					session.mu.Unlock()
					return fileResponse{}, syscall.EAGAIN
				}
			} else if previous == "" && req.CloseGeneration != 1 || !nativeExplicitClose && previous != "" && (!determined || req.CloseGeneration != generation+1) {
				session.mu.Unlock()
				return fileResponse{}, syscall.EAGAIN
			}
			if nativeExplicitClose && !adoptingNative && actionEpoch != nativeEpoch {
				if actionEpoch > nativeEpoch {
					session.mu.Unlock()
					return fileResponse{}, syscall.EINVAL
				}
				session.mu.Unlock()
				return fileResponse{}, &storage.CloseActionNotExecutedError{CurrentEpoch: nativeEpoch}
			}
			if len(session.closeIDs) >= session.options.MaxCloseActions {
				session.mu.Unlock()
				return fileResponse{}, syscall.EAGAIN
			}
		}
		if (session.autoClose || session.explicitClose) && req.Op != storage.OpFileClose {
			session.mu.Unlock()
			registry.mu.Lock()
			terminal := registry.terminalCloses[req.Session]
			registry.mu.Unlock()
			if terminal != nil {
				return h.replayTerminalFileClose(ctx, req, digest, terminal)
			}
			return fileResponse{}, syscall.EAGAIN
		}
		if actionEpoch != epoch && !nativeExplicitClose {
			session.mu.Unlock()
			// The immediately preceding window still has every admitted result.
			// Absence here proves this action never ran; older windows cannot prove it.
			if actionEpoch+1 == epoch && semanticFileAction(req) == "" {
				return fileResponse{Epoch: epoch, Retry: true}, nil
			}
			return fileResponse{}, syscall.ESTALE
		}
		if (session.retired || !now.Before(session.expires)) && req.Op != storage.OpFileClose && req.Op != storage.OpFileSessionClose {
			session.mu.Unlock()
			return fileResponse{}, syscall.ESTALE
		}
		if req.Op == storage.OpFileSessionClose && session.autoClose {
			session.mu.Unlock()
			return fileResponse{}, syscall.ESTALE
		}
		cleanup := req.Op == storage.OpFileRangeDrop || req.Op == storage.OpFileRetireUseOwner || req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose
		reservedClose := req.Op == storage.OpFileSessionClose || req.Op == storage.OpFileClose && session.recoverable
		if cleanup && !reservedClose && session.cleanupActions+session.cleanupReserved >= registry.limits.MaxCleanupActions {
			session.mu.Unlock()
			return fileResponse{}, syscall.EAGAIN
		}
		if !cleanup && session.dataActions >= registry.limits.MaxActions {
			session.mu.Unlock()
			return fileResponse{}, syscall.EAGAIN
		}
		if cleanup {
			if reservedClose {
				if req.Op == storage.OpFileClose {
					file := session.files[req.File]
					if file.closeReserve == 0 {
						session.mu.Unlock()
						return fileResponse{}, syscall.EAGAIN
					}
					file.closeReserve--
					file.closeAction = req.Action
					file.closeGeneration = req.CloseGeneration
					file.closeDetermined = false
					file.closeImplicit = req.CloseImplicit
				} else {
					if session.sessionCloseReserve == 0 {
						session.mu.Unlock()
						return fileResponse{}, syscall.EAGAIN
					}
					session.sessionCloseReserve--
					session.closeAction = req.Action
					session.closeGeneration = req.CloseGeneration
					session.closeDetermined = false
				}
				session.cleanupReserved--
			} else if session.cleanupActions+session.cleanupReserved >= registry.limits.MaxCleanupActions {
				session.mu.Unlock()
				return fileResponse{}, syscall.EAGAIN
			}
			if req.Op == storage.OpFileClose && !reservedClose {
				file := session.files[req.File]
				file.closeAction = req.Action
				file.closeGeneration = req.CloseGeneration
				file.closeDetermined = false
				file.closeImplicit = req.CloseImplicit
			}
			session.cleanupActions++
		} else {
			session.dataActions++
		}
		if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
			if session.closeIDs == nil {
				session.closeIDs = make(map[storage.LockRequestID]closeIDUse)
			}
			session.closeIDs[req.Action] = closeIDUse{owner: req.File, generation: req.CloseGeneration, implicit: !nativeExplicitClose}
		}
		action := &servedFileAction{id: req.Action, op: req.Op, cleanup: cleanup, digest: digest, done: make(chan struct{}), expires: now.Add(2*session.options.History - now.Sub(session.started)%session.options.History), closeOwner: req.File, closeGeneration: req.CloseGeneration, closeImplicit: req.CloseImplicit, priorAction: priorCloseAction, priorGeneration: priorCloseGeneration, priorDetermined: priorCloseDetermined, priorImplicit: priorCloseImplicit, priorClosing: priorFileClosing}
		session.actions[req.Action] = action
		if req.Op == storage.OpFileSessionClose {
			session.explicitClose = true
		}
		session.mu.Unlock()
		response, err := h.performFile(ctx, session, req)
		response.Epoch = epoch
		var barrierFailure *fileBarrierError
		session.mu.Lock()
		var closeProof *storage.CloseActionNotExecutedError
		if nativeExplicitClose && errors.As(err, &closeProof) {
			action.response = fileResponse{Epoch: epoch, Data: []byte{}}
			action.err = err
			close(action.done)
			delete(session.actions, req.Action)
			use := session.closeIDs[req.Action]
			use.notExecuted = true
			use.proofEpoch = closeProof.CurrentEpoch
			session.closeIDs[req.Action] = use
			session.cleanupActions--
			if file := session.files[req.File]; file != nil {
				session.cleanupReserved++
				file.closeReserve++
				file.closeAction = priorCloseAction
				file.closeGeneration = priorCloseGeneration
				file.closeDetermined = priorCloseDetermined
				file.closeImplicit = priorCloseImplicit
				file.closing = priorFileClosing
			}
			session.mu.Unlock()
			return fileResponse{}, err
		}
		action.response = response
		action.file = response.File
		action.err = retainFileActionError(err)
		if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
			action.uncertain = response.CloseResult == nil || !response.CloseResult.Determined
		}
		action.barrierPending = errors.As(err, &barrierFailure)
		if action.barrierPending && (req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose) {
			action.closeSemanticErr = retainFileError(barrierFailure.prior)
		}
		releaseErr := action.err
		if action.barrierPending {
			releaseErr = action.closeSemanticErr
		}
		retainSemanticOpenCapability(session, req, response, action.expires)
		close(action.done)
		if req.Op == storage.OpFileSessionClose && (response.CloseResult == nil || !response.CloseResult.Released) {
			session.explicitClose = false
		}
		if req.Op == storage.OpFileSessionClose {
			session.closeDetermined = response.CloseResult != nil && response.CloseResult.Determined && !response.CloseResult.Released
		} else if req.Op == storage.OpFileClose {
			if file := session.files[req.File]; file != nil {
				file.closeDetermined = response.CloseResult != nil && response.CloseResult.Determined && !response.CloseResult.Released
			}
		}
		expires := action.expires
		session.mu.Unlock()
		h.finishPublishedClose(ctx, session, req, response, expires, releaseErr)
		return response, err
	}
	boundCleanupRead := req.Op == storage.OpFileCloseOwnerStatus || req.Op == storage.OpFileQueryAction && req.File != ""
	if (session.autoClose || session.explicitClose) && req.Op != storage.OpFileClose && !boundCleanupRead {
		session.mu.Unlock()
		registry.mu.Lock()
		terminal := registry.terminalCloses[req.Session]
		registry.mu.Unlock()
		if terminal != nil {
			return h.replayTerminalFileClose(ctx, req, digest, terminal)
		}
		return fileResponse{}, syscall.EAGAIN
	}
	if req.Op != storage.OpFileSessionClose && req.Op != storage.OpFileClose && !boundCleanupRead && (session.retired || !now.Before(session.expires)) {
		session.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	session.mu.Unlock()
	response, err := h.performFile(ctx, session, req)
	response.Epoch = epoch
	return response, err
}

func (h *Handler) finishPublishedClose(ctx context.Context, session *servedFileSession, req fileRequest, response fileResponse, expires time.Time, releaseErr error) {
	if response.CloseResult == nil || !response.CloseResult.Released {
		return
	}
	registry := h.files
	if req.Op == storage.OpFileSessionClose {
		if registry.reconcileSessionFileCloses(ctx, session) {
			registry.terminalizeSession(req.Session, session, response.Epoch, expires, releaseErr)
		} else {
			session.mu.Lock()
			session.explicitClose = false
			session.closeReconciling = true
			session.closeReleaseErr = retainFileError(releaseErr)
			session.mu.Unlock()
			registry.wakeCleanup()
		}
		return
	}
	if req.Op != storage.OpFileClose {
		return
	}
	session.mu.Lock()
	file := session.files[req.File]
	session.mu.Unlock()
	if file == nil {
		return
	}
	settled := registry.reconcileReleasedClose(ctx, session, req.File, file, req.Action)
	session.mu.Lock()
	for _, action := range session.actions {
		if action.op == storage.OpFileClose && action.closeOwner == req.File && action.closeImplicit && action.uncertain {
			settled = false
			break
		}
	}
	if settled {
		session.cleanupReserved -= file.closeReserve
		file.closeReserve = 0
		delete(session.files, req.File)
	} else {
		session.retired = true
	}
	session.mu.Unlock()
	if !settled {
		registry.wakeCleanup()
	}
}

func (r *fileRegistry) wakeCleanup() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (h *Handler) replayTerminalFileClose(ctx context.Context, req fileRequest, digest [32]byte, terminal *terminalFileClose) (fileResponse, error) {
	terminal.mu.Lock()
	now := time.Now()
	if !now.Before(terminal.expires) {
		terminal.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	if req.Op == storage.OpFileCloseOwnerStatus {
		matches := make([]*servedFileAction, 0, 2)
		for _, action := range terminal.actions {
			if action.op == storage.OpFileClose && action.closeOwner == req.File && now.Before(action.expires) {
				matches = append(matches, action)
			}
		}
		epoch := terminal.epoch
		terminal.mu.Unlock()
		for _, action := range matches {
			select {
			case <-action.done:
			case <-ctx.Done():
				return fileResponse{}, ctx.Err()
			}
			action.retryMu.Lock()
			released := action.response.CloseResult != nil && action.response.CloseResult.Released && action.response.CloseResult.Determined
			generation := action.closeGeneration
			action.retryMu.Unlock()
			if released {
				status := storage.CloseOwnerStatus{Released: true, NextGeneration: generation, CurrentEpoch: epoch}
				return fileResponse{Epoch: epoch, CloseOwnerStatus: closeOwnerStatusResultOf(status)}, nil
			}
		}
		return fileResponse{}, syscall.ESTALE
	}
	if req.Op == storage.OpFileQueryAction {
		action := terminal.actions[storage.LockRequestID(req.FileAction)]
		if req.File != "" && action != nil && (action.op != storage.OpFileClose || action.closeOwner != req.File || action.closeGeneration != req.CloseGeneration) {
			terminal.mu.Unlock()
			return fileResponse{}, syscall.EINVAL
		}
		epoch := terminal.epoch
		terminal.mu.Unlock()
		outcome := storage.FileActionRetired
		operation := storage.Operation("")
		if action != nil && now.Before(action.expires) {
			select {
			case <-action.done:
			case <-ctx.Done():
				return fileResponse{}, ctx.Err()
			}
			action.retryMu.Lock()
			operation = action.op
			outcome = storage.FileActionUnknown
			if action.response.CloseResult != nil && action.response.CloseResult.Determined {
				outcome = storage.FileActionCompleted
			}
			action.retryMu.Unlock()
		}
		receipt := storage.FileActionReceipt{Action: req.FileAction, Operation: operation, Outcome: outcome}
		return fileResponse{Epoch: epoch, ActionReceipt: &receipt}, nil
	}
	if req.Op != storage.OpFileSessionClose && req.Op != storage.OpFileClose {
		terminal.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	action := terminal.actions[req.Action]
	if action == nil {
		terminal.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	if action.op != req.Op || action.closeOwner != req.File || action.closeGeneration != req.CloseGeneration {
		terminal.mu.Unlock()
		return fileResponse{}, syscall.EINVAL
	}
	if action.digest != digest {
		terminal.mu.Unlock()
		return fileResponse{}, syscall.EINVAL
	}
	terminal.mu.Unlock()
	select {
	case <-action.done:
	case <-ctx.Done():
		return fileResponse{}, ctx.Err()
	}
	action.retryMu.Lock()
	defer action.retryMu.Unlock()
	if action.barrierPending {
		response, barrierErr := h.finishFileMutation(ctx, action.response)
		action.response = response
		action.err = errors.Join(action.closeSemanticErr, retainFileActionError(barrierErr))
		action.barrierPending = barrierErr != nil
	}
	if action.err != nil && !action.barrierPending {
		return action.response, &recordedFileError{cause: action.err}
	}
	return action.response, action.err
}

func (h *Handler) semanticOpenReplayExpiry(session *servedFileSession, actionExpiry time.Time) time.Time {
	handoffExpiry := time.Now().Add(h.files.limits.PendingAck)
	if handoffExpiry.After(session.expires) {
		handoffExpiry = session.expires
	}
	if handoffExpiry.After(actionExpiry) {
		return handoffExpiry
	}
	return actionExpiry
}

func beginSemanticOpenReplay(session *servedFileSession, request fileRequest, action *servedFileAction) string {
	if semanticFileAction(request) == "" || action.file == "" {
		return ""
	}
	file := session.files[action.file]
	if file == nil || file.closing {
		return ""
	}
	file.replaying++
	return action.file
}

func (h *Handler) finishSemanticOpenReplay(session *servedFileSession, capability string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	file := session.files[capability]
	if file == nil || file.replaying == 0 {
		return
	}
	file.replaying--
	if !file.closing && !file.pending.IsZero() {
		file.pending = h.semanticOpenReplayExpiry(session, file.pending)
	}
}

func retainSemanticOpenCapability(session *servedFileSession, request fileRequest, response fileResponse, expires time.Time) {
	if semanticFileAction(request) == "" || response.File == "" {
		return
	}
	if file := session.files[response.File]; file != nil && !file.closing && !file.pending.IsZero() && file.pending.Before(expires) {
		file.pending = expires
	}
}

func maxDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

func (h *Handler) performFile(ctx context.Context, s *servedFileSession, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	var err error
	switch req.Op {
	case storage.OpFileStatus, storage.OpFileRenew:
		start := time.Now()
		var status storage.FileSessionStatus
		if req.Op == storage.OpFileRenew {
			status, err = s.native.Renew(ctx)
		} else {
			status, err = s.native.Status(ctx)
		}
		if err != nil {
			return response, err
		}
		s.mu.Lock()
		if s.retired || status.Epoch != s.authority {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if req.Op == storage.OpFileRenew && status.Revision > s.revision {
			s.revision = status.Revision
			candidate := start.Add(status.Remaining)
			if candidate.After(s.expires) {
				s.expires = candidate
			}
		}
		remaining := maxDuration(time.Until(s.expires))
		s.mu.Unlock()
		status.Remaining = min(remaining, maxDuration(status.Remaining-time.Since(start)))
		status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(start))
		response.Status = &status
		return response, nil
	case storage.OpFileSessionClose:
		s.mu.Lock()
		s.retired = true
		s.mu.Unlock()
		result, closeErr := s.native.CloseWithResult(ctx)
		s.observeSessionRelease(result, closeErr)
		response.CloseResult = referenceCloseResultOf(result)
		err = errors.Join(closeErr, result.Check(closeErr))
	case storage.OpFileStatNode:
		attr, err := s.native.StatNode(ctx, req.Node)
		wire := AttrOf(attr)
		response.Attr = wire
		return response, err
	case storage.OpFileSetNodeAttr:
		if req.Change == nil {
			return response, syscall.EINVAL
		}
		attr, e := s.native.SetNodeAttr(ctx, req.Node, req.Change.Storage())
		err = e
		wire := AttrOf(attr)
		response.Attr = wire
	case storage.OpFileQueryAction, storage.OpFileQueryDeleteIntent, storage.OpFileListDeleteIntents, storage.OpFileAcknowledgeDeleteIntent, storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileObserveDirectoryMetadata, storage.OpFileMutateName, storage.OpFileSetNodeMetadata, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
		response, err = h.performSessionCapability(ctx, s.native, req)
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		return h.openReference(ctx, s, req)
	case storage.OpFileAck:
		s.mu.Lock()
		defer s.mu.Unlock()
		file := s.files[req.File]
		if file == nil || file.closing {
			return response, syscall.ESTALE
		}
		if !file.pending.IsZero() && !time.Now().Before(file.pending) {
			return response, syscall.ESTALE
		}
		file.pending = time.Time{}
		return response, nil
	default:
		s.mu.Lock()
		file := s.files[req.File]
		if file == nil && req.Op == storage.OpFileClose {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if file == nil || file.native == nil || file.closing && req.Op != storage.OpFileClose && req.Op != storage.OpFileCloseOwnerStatus {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if !file.pending.IsZero() && req.Op != storage.OpFileClose && req.Op != storage.OpFileCloseOwnerStatus {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if req.Op == storage.OpFileClose {
			file.closing = true
		}
		s.mu.Unlock()
		var attr storage.Attr
		switch req.Op {
		case storage.OpFileStat:
			attr, err = file.native.Stat(ctx)
		case storage.OpFileCloseOwnerStatus:
			closer, ok := file.native.(storage.ReferenceCloseActions)
			if !ok {
				return response, syscall.EOPNOTSUPP
			}
			status, statusErr := closer.CloseOwnerStatus(ctx)
			if statusErr != nil {
				return response, statusErr
			}
			if err := status.Check(); err != nil {
				return response, err
			}
			s.mu.Lock()
			s.pruneCloseIDsLocked(s.epoch(time.Now()), status.CurrentEpoch)
			status.Ready = status.Ready && file.closeReserve > 0 && len(s.closeIDs) < s.options.MaxCloseActions && (file.closeAction == "" || file.closeDetermined) && !s.autoClose && !s.explicitClose
			s.mu.Unlock()
			response.CloseOwnerStatus = closeOwnerStatusResultOf(status)
		case storage.OpFileRead:
			data, ok := file.native.(storage.File)
			if !ok {
				return response, syscall.EBADF
			}
			value, e := data.ReadAt(ctx, req.Offset, req.Length)
			err = e
			attr = value.Attr
			response.Data = value.Data
		case storage.OpFileWrite:
			data, ok := file.native.(storage.File)
			if !ok {
				return response, syscall.EBADF
			}
			attr, err = data.WriteAt(ctx, req.Offset, req.Data)
		case storage.OpFileTruncate:
			data, ok := file.native.(storage.File)
			if !ok {
				return response, syscall.EBADF
			}
			attr, err = data.Truncate(ctx, req.Offset)
		case storage.OpFileSetAttr:
			if req.Change == nil {
				return response, syscall.EINVAL
			}
			attr, err = file.native.SetAttr(ctx, req.Change.Storage())
		case storage.OpFileSync:
			data, ok := file.native.(storage.File)
			if !ok {
				return response, syscall.EBADF
			}
			err = data.Sync(ctx)
		case storage.OpFileObserveName, storage.OpFileObserveContentMetadata, storage.OpFileState, storage.OpFileScope, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
			response, err = performReferenceCapability(ctx, file.native, req)
		case storage.OpFileClose:
			if !req.CloseImplicit && !s.recoverable {
				return response, syscall.EOPNOTSUPP
			}
			var result storage.ReferenceCloseResult
			var closeErr error
			if s.recoverable && !req.CloseImplicit {
				closer, ok := file.native.(storage.ReferenceCloseActions)
				if !ok {
					return response, syscall.EOPNOTSUPP
				}
				result, closeErr = closer.CloseWithAction(ctx, storage.CloseAttempt{Action: storage.FileActionID(req.Action), Generation: req.CloseGeneration})
			} else {
				result, closeErr = file.native.CloseWithResult(ctx)
			}
			response.CloseResult = referenceCloseResultOf(result)
			err = errors.Join(closeErr, result.Check(closeErr))
		default:
			return response, syscall.EINVAL
		}
		switch req.Op {
		case storage.OpFileStat, storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr:
			wire := AttrOf(attr)
			response.Attr = wire
		}
	}
	if err != nil {
		if fileMutation(req.Op) && (response.Attr != nil || response.State != nil || response.CloseResult != nil && response.CloseResult.Released) {
			updated, barrierErr := h.finishFileMutation(ctx, response)
			if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
				var barrierFailure *fileBarrierError
				if errors.As(barrierErr, &barrierFailure) {
					barrierFailure.prior = err
					return updated, barrierFailure
				}
			}
			return updated, errors.Join(err, barrierErr)
		}
		return response, err
	}
	if fileMutation(req.Op) {
		return h.finishFileMutation(ctx, response)
	}
	return response, nil
}

func (h *Handler) finishFileMutation(ctx context.Context, response fileResponse) (fileResponse, error) {
	if response.CloseResult != nil {
		closeResult := *response.CloseResult
		response.CloseResult = &closeResult
	}
	if h.publisher != nil {
		h.publisher.wake()
	}
	if h.log == nil {
		if response.CloseResult != nil {
			response.CloseResult.BarrierPending = false
		}
		return response, nil
	}
	barrier, err := h.mutationBarrier(ctx)
	if err != nil {
		if response.CloseResult != nil && response.CloseResult.Released {
			response.CloseResult.BarrierPending = true
		}
		return response, &fileBarrierError{cause: fmt.Errorf("file mutation completed but replication barrier is unknown: %v: %w", err, syscall.EIO)}
	}
	response.Barrier = barrier
	if response.CloseResult != nil {
		response.CloseResult.BarrierPending = false
	}
	return response, nil
}

func (h *Handler) writeFileResponse(w http.ResponseWriter, status int, body any, control bool) {
	if response, ok := body.(fileResponse); ok && response.Data == nil {
		response.Data = []byte{}
		body = response
	}
	if !control {
		h.writeJSON(w, status, body)
		return
	}
	h.writeBoundedFileResponse(w, status, body, min(h.maxBodyBytes, MaxFileControlBytes))
}
func (h *Handler) writeBoundedFileResponse(w http.ResponseWriter, status int, body any, limit int64) {
	if response, ok := body.(fileResponse); ok && response.Data == nil {
		response.Data = []byte{}
		body = response
	}
	var encoded []byte
	var err error
	if failure, ok := body.(ErrorResponse); ok {
		encoded, err = marshalBoundedErrorResponse(failure, limit)
	} else {
		encoded, err = json.Marshal(body)
	}
	if err != nil || int64(len(encoded)) > limit {
		status = http.StatusInternalServerError
		encoded = []byte(`{"message":"file response exceeds its protocol bound"}`)
	}
	w.Header().Set("Content-Type", contentJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func (r *fileRegistry) enroll(ctx context.Context, options storage.FileSessionOptions) (fileResponse, error) {
	if err := checkFileSessionOptions(options, r.limits.Session); err != nil {
		return fileResponse{}, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fileResponse{}, syscall.EIO
	}
	if r.backend == nil {
		r.mu.Unlock()
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if r.limits.MaxCleanupActions < 2 {
		r.mu.Unlock()
		return fileResponse{}, syscall.EAGAIN
	}
	if len(r.sessions)+r.enrolling >= r.limits.MaxSessions || len(r.sessions)+len(r.terminalCloses)+r.enrolling >= 2*r.limits.MaxSessions {
		r.mu.Unlock()
		return fileResponse{}, syscall.EAGAIN
	}
	r.enrolling++
	r.enrollments.Add(1)
	r.startLocked()
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.enrolling--; r.mu.Unlock(); r.enrollments.Done() }()
	if err := r.backend.CheckFileStorage(); err != nil {
		return fileResponse{}, err
	}
	started := time.Now()
	native, err := r.backend.NewFileSession(ctx, options)
	if err != nil {
		return fileResponse{}, err
	}
	status, err := native.Status(ctx)
	capabilities, capabilityErr := sessionCapabilitiesOf(native)
	err = errors.Join(err, capabilityErr)
	identity, identityErr := sessionIdentityOf(ctx, native, capabilities, status)
	err = errors.Join(err, identityErr)
	session := &servedFileSession{authority: status.Epoch, revision: status.Revision, native: native, files: make(map[string]*servedFile), actions: make(map[storage.LockRequestID]*servedFileAction), options: options, started: started, expires: started.Add(status.Remaining), sessionCloseReserve: 2, cleanupReserved: 2, recoverable: capabilities.CloseRecovery, stableIdentity: capabilities.StableIdentity}
	if inline, ok := native.(storage.InlineCloseSettlement); ok {
		session.inlineSettlement = inline.CheckInlineCloseSettlement() == nil
	}
	r.mu.Lock()
	closed := r.closed
	session.retired = closed || err != nil
	id := fileCapability()
	r.sessions[id] = session
	r.mu.Unlock()
	if err != nil {
		return fileResponse{}, err
	}
	if closed {
		return fileResponse{}, syscall.EIO
	}
	status.Remaining = maxDuration(time.Until(session.expires))
	status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(started))
	return fileResponse{Session: id, Epoch: session.epoch(time.Now()), Status: &status, Capabilities: capabilities, SessionIdentity: identity}, nil
}

func checkFileSessionOptions(options, maximum storage.FileSessionOptions) error {
	if err := options.Check(); err != nil {
		return err
	}
	if options.MaxFileSize > maximum.MaxFileSize || options.Lease > maximum.Lease || options.History > maximum.History || options.MaxFiles > maximum.MaxFiles || options.MaxOperations > maximum.MaxOperations || options.MaxWaiters > maximum.MaxWaiters || options.MaxLockOwners > maximum.MaxLockOwners || options.MaxLockRanges > maximum.MaxLockRanges || options.MaxPendingLocks > maximum.MaxPendingLocks || options.MaxLockActions > maximum.MaxLockActions || options.MaxCloseActions > maximum.MaxCloseActions {
		return fmt.Errorf("file session exceeds server resource limits: %w", syscall.EINVAL)
	}
	return nil
}

func retainFileError(err error) error {
	if err == nil {
		return nil
	}
	detail := boundedRetainedFileDetail(err.Error())
	if failure := volumeLockFailure(err); failure != nil {
		return &locking.Error{Code: failure.Code, Recorded: failure.Recorded, Message: boundedRetainedFileDetail(failure.Message)}
	}
	return &operationError{req: Request{Op: OpFile}, errno: storage.ErrnoOf(err), detail: detail, capability: capabilityErrors[capabilityErrorCode(err)], canceled: errors.Is(err, context.Canceled), deadline: errors.Is(err, context.DeadlineExceeded)}
}

func boundedRetainedFileDetail(detail string) string {
	if len(detail) > 4096 || !utf8.ValidString(detail) {
		return "file error detail cannot be retained within its text bound"
	}
	return strings.Clone(detail)
}

func retainFileActionError(err error) error {
	var barrierFailure *fileBarrierError
	if errors.As(err, &barrierFailure) {
		return &fileBarrierError{cause: retainFileError(barrierFailure.cause), prior: retainFileError(barrierFailure.prior)}
	}
	return retainFileError(err)
}
