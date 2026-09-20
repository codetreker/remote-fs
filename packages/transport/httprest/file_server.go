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
	enrolling   int
	enrollments sync.WaitGroup
	mu          sync.Mutex
	limits      FileLimits
	backend     storage.FileStorage
	sessions    map[string]*servedFileSession
	closed      bool
	running     bool
	wake        chan struct{}
	done        chan struct{}
	err         error
}

type servedFileSession struct {
	dataActions    int
	cleanupActions int
	authority      string
	revision       uint64
	mu             sync.Mutex
	native         storage.FileSession
	files          map[string]*servedFile
	actions        map[storage.LockRequestID]*servedFileAction
	options        storage.FileSessionOptions
	started        time.Time
	expires        time.Time
	retired        bool
}

type servedFile struct {
	native  retainedReference
	pending time.Time
	closing bool
}

type retainedReference interface {
	Stat(context.Context) (storage.Attr, error)
	SetAttr(context.Context, storage.AttrChange) (storage.Attr, error)
	Close(context.Context) error
}

type servedFileAction struct {
	retryMu        sync.Mutex
	cleanup        bool
	digest         [32]byte
	done           chan struct{}
	expires        time.Time
	response       fileResponse
	err            error
	barrierPending bool
}

type fileBarrierError struct{ cause error }

func (e *fileBarrierError) Error() string { return e.cause.Error() }
func (e *fileBarrierError) Unwrap() error { return e.cause }

type recordedFileError struct{ cause error }

func (e *recordedFileError) Error() string { return e.cause.Error() }
func (e *recordedFileError) Unwrap() error { return e.cause }

func newFileRegistry(s storage.Storage, limits FileLimits) *fileRegistry {
	backend, _ := s.(storage.FileStorage)
	return &fileRegistry{limits: limits, backend: backend, sessions: make(map[string]*servedFileSession), wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func fileCapability() string {
	var value [32]byte
	rand.Read(value[:])
	return hex.EncodeToString(value[:])
}

func (s *servedFileSession) epoch(now time.Time) uint64 {
	return uint64(now.Sub(s.started)/s.options.History) + 1
}

func (r *fileRegistry) startLocked() {
	if !r.running {
		r.running = true
		go r.run()
	}
}

func (r *fileRegistry) stop() {
	r.mu.Lock()
	r.closed = true
	r.startLocked()
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Close retires and drains sessions created by this handler. The volume
// backend remains owned by the caller. Drain the HTTP server before closing
// that backend; a failed Close can retain native references and cleanup work.
func (h *Handler) Close(ctx context.Context) error {
	h.Stop()
	h.files.stop()
	select {
	case <-h.files.done:
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
	defer close(r.done)
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
		sessions := make(map[string]*servedFileSession, len(r.sessions))
		for id, s := range r.sessions {
			sessions[id] = s
		}
		r.mu.Unlock()
		now := time.Now()
		for id, s := range sessions {
			s.mu.Lock()
			expired := !now.Before(s.expires)
			retire := closing || expired || s.retired
			if retire {
				s.retired = true
			}
			pending := make(map[string]*servedFile)
			if !retire {
				for cap, f := range s.files {
					if !f.closing && !f.pending.IsZero() && !now.Before(f.pending) {
						f.closing = true
						pending[cap] = f
					}
				}
			}
			for key, a := range s.actions {
				select {
				case <-a.done:
					if !now.Before(a.expires) {
						delete(s.actions, key)
						if a.cleanup {
							s.cleanupActions--
						} else {
							s.dataActions--
						}
					}
				default:
				}
			}
			s.mu.Unlock()
			for cap, f := range pending {
				if err := f.native.Close(context.Background()); err != nil {
					s.mu.Lock()
					s.retired = true
					s.mu.Unlock()
					retire = true
				} else {
					s.mu.Lock()
					delete(s.files, cap)
					s.mu.Unlock()
				}
			}
			if retire {
				err := s.native.Close(context.Background())
				r.mu.Lock()
				if err == nil {
					delete(r.sessions, id)
				} else if closing {
					r.err = errors.Join(r.err, err)
				}
				r.mu.Unlock()
			}
		}
		if closing {
			return
		}
	}
}

func (h *Handler) serveFile(w http.ResponseWriter, r *http.Request) {
	control := r.URL.Path == Prefix+string(OpFileControl)
	writeFault := func(status int, err error) {
		h.writeFileResponse(w, status, ErrorResponse{Message: err.Error()}, control)
	}
	writeError := func(err error) {
		var barrierFailure *fileBarrierError
		if errors.As(err, &barrierFailure) {
			writeFault(http.StatusInternalServerError, err)
			return
		}
		var recorded *recordedFileError
		if errors.As(err, &recorded) {
			h.writeFileResponse(w, StatusStorageError, fileErrorResponse(err, true), control)
			return
		}
		if response, ok := authorizationResponse(err); ok {
			h.writeFileResponse(w, StatusStorageError, response, control)
			return
		}
		h.writeFileResponse(w, StatusStorageError, fileErrorResponse(err, false), control)
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
	if control {
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
	if fileControl(req.Op) != control {
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
	if fileAttrResult(req.Op) {
		r = r.WithContext(storage.WithBoundedAttrResult(r.Context(), req.ResultBytes, h.attrResultBudget(req)))
	}
	digest := sha256.Sum256(append(body, []byte(r.Header.Get(HeaderMutationScope))...))
	response, err := h.fileCall(r.Context(), req, digest)
	if err != nil {
		if response.Attempt != nil {
			h.writeFileResponse(w, StatusStorageError, ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error(), CapabilityCode: capabilityErrorCode(err), Attempt: response.Attempt}, control)
			return
		}
		var recorded *recordedFileError
		if errors.As(err, &recorded) {
			body := fileErrorResponse(err, true)
			body.FileResult = partialFileResult(req, response)
			h.writeFileResponse(w, StatusStorageError, body, control)
			return
		}
		if result := partialFileResult(req, response); result != nil {
			body := fileErrorResponse(err, false)
			body.FileResult = result
			h.writeFileResponse(w, StatusStorageError, body, control)
			return
		}
		writeError(err)
		return
	}
	h.writeFileResponse(w, http.StatusOK, response, control)
}

func partialFileResult(request fileRequest, response fileResponse) *fileResponse {
	include := false
	switch request.Op {
	case storage.OpFileQueryAction:
		include = response.ActionReceipt != nil
	case storage.OpFileQueryDeleteIntent:
		include = response.DeleteStatus != nil
	case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		include = response.File != "" || response.Attr != nil || response.Outcome != 0 || response.Barrier != nil
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
	case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileLookupAt, storage.OpFileMutateName:
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
	if req.Op == storage.OpFileSessionOpen {
		return registry.enroll(ctx, req.Options)
	}

	registry.mu.Lock()
	session := registry.sessions[req.Session]
	closed := registry.closed
	registry.mu.Unlock()
	if closed {
		return fileResponse{}, syscall.EIO
	}
	if session == nil {
		return fileResponse{}, syscall.ESTALE
	}
	session.mu.Lock()
	now := time.Now()
	epoch := session.epoch(now)
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
			session.mu.Unlock()
			select {
			case <-previous.done:
				previous.retryMu.Lock()
				defer previous.retryMu.Unlock()
				if previous.barrierPending {
					response, retryErr := h.finishFileMutation(ctx, previous.response)
					previous.response = response
					previous.err = retainFileActionError(retryErr)
					previous.barrierPending = retryErr != nil
				}
				if previous.err != nil && !previous.barrierPending {
					return previous.response, &recordedFileError{cause: previous.err}
				}
				return previous.response, previous.err
			case <-ctx.Done():
				return fileResponse{}, operationFailure(Request{Op: OpFile}, ctx.Err(), false)
			}
		}
		if actionEpoch != epoch {
			session.mu.Unlock()
			// The immediately preceding window still has every admitted result.
			// Absence here proves this action never ran; older windows cannot prove it.
			if actionEpoch+1 == epoch {
				return fileResponse{Epoch: epoch, Retry: true}, nil
			}
			return fileResponse{}, syscall.ESTALE
		}
		if session.retired || !now.Before(session.expires) {
			session.mu.Unlock()
			return fileResponse{}, syscall.ESTALE
		}
		cleanup := req.Op == storage.OpFileRangeDrop || req.Op == storage.OpFileRetireUseOwner
		if cleanup && session.cleanupActions >= registry.limits.MaxCleanupActions {
			session.retired = true
			session.mu.Unlock()
			err := session.native.Close(ctx)
			return fileResponse{}, errors.Join(syscall.EIO, err)
		}
		if !cleanup && session.dataActions >= registry.limits.MaxActions {
			session.mu.Unlock()
			return fileResponse{}, syscall.EAGAIN
		}
		if cleanup {
			session.cleanupActions++
		} else {
			session.dataActions++
		}
		action := &servedFileAction{cleanup: cleanup, digest: digest, done: make(chan struct{}), expires: now.Add(2*session.options.History - now.Sub(session.started)%session.options.History)}
		session.actions[req.Action] = action
		session.mu.Unlock()
		response, err := h.performFile(ctx, session, req)
		response.Epoch = epoch
		var barrierFailure *fileBarrierError
		session.mu.Lock()
		action.response = response
		action.err = retainFileActionError(err)
		action.barrierPending = errors.As(err, &barrierFailure)
		close(action.done)
		session.mu.Unlock()
		return response, err
	}
	if req.Op != storage.OpFileSessionClose && req.Op != storage.OpFileClose && (session.retired || !now.Before(session.expires)) {
		session.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	session.mu.Unlock()
	response, err := h.performFile(ctx, session, req)
	response.Epoch = epoch
	return response, err
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
		err = s.native.Close(ctx)
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
	case storage.OpFileQueryAction, storage.OpFileQueryDeleteIntent, storage.OpFileAcknowledgeDeleteIntent, storage.OpFileLookupAt, storage.OpFileMutateName, storage.OpFileSetNodeMetadata, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
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
			return response, nil
		}
		if file == nil || file.native == nil || file.closing && req.Op != storage.OpFileClose {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if !file.pending.IsZero() && req.Op != storage.OpFileClose {
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
		case storage.OpFileState, storage.OpFileScope, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
			response, err = performReferenceCapability(ctx, file.native, req)
		case storage.OpFileClose:
			err = file.native.Close(ctx)
			if err == nil {
				s.mu.Lock()
				delete(s.files, req.File)
				s.mu.Unlock()
			}
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
		if fileMutation(req.Op) && (response.Attr != nil || response.State != nil) {
			updated, barrierErr := h.finishFileMutation(ctx, response)
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
	if h.publisher != nil {
		h.publisher.wake()
	}
	if h.log == nil {
		return response, nil
	}
	barrier, err := h.mutationBarrier(ctx)
	if err != nil {
		return response, &fileBarrierError{cause: fmt.Errorf("file mutation completed but replication barrier is unknown: %v: %w", err, syscall.EIO)}
	}
	response.Barrier = barrier
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
	limit := min(h.maxBodyBytes, MaxFileControlBytes)
	var encoded []byte
	var err error
	if failure, ok := body.(ErrorResponse); ok {
		encoded, err = marshalBoundedErrorResponse(failure, limit)
	} else {
		encoded, err = json.Marshal(body)
	}
	if err != nil || int64(len(encoded)) > limit {
		status = http.StatusInternalServerError
		encoded = []byte(`{"message":"file control response exceeds its protocol bound"}`)
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
	if len(r.sessions)+r.enrolling >= r.limits.MaxSessions {
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
	session := &servedFileSession{authority: status.Epoch, revision: status.Revision, native: native, files: make(map[string]*servedFile), actions: make(map[storage.LockRequestID]*servedFileAction), options: options, started: started, expires: started.Add(status.Remaining)}
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
	return fileResponse{Session: id, Epoch: session.epoch(time.Now()), Status: &status, Capabilities: capabilities}, nil
}

func checkFileSessionOptions(options, maximum storage.FileSessionOptions) error {
	if err := options.Check(); err != nil {
		return err
	}
	if options.MaxFileSize > maximum.MaxFileSize || options.Lease > maximum.Lease || options.History > maximum.History || options.MaxFiles > maximum.MaxFiles || options.MaxOperations > maximum.MaxOperations || options.MaxWaiters > maximum.MaxWaiters || options.MaxLockOwners > maximum.MaxLockOwners || options.MaxLockRanges > maximum.MaxLockRanges || options.MaxPendingLocks > maximum.MaxPendingLocks || options.MaxLockActions > maximum.MaxLockActions {
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
		return &fileBarrierError{cause: retainFileError(err)}
	}
	return retainFileError(err)
}
