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

	"github.com/codetreker/remote-fs/packages/authz"
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
	native  storage.File
	pending time.Time
	closing bool
}

type servedFileAction struct {
	cleanup  bool
	digest   [32]byte
	done     chan struct{}
	expires  time.Time
	response fileResponse
	err      error
}

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
		if response, ok := authorizationResponse(err); ok {
			h.writeFileResponse(w, StatusStorageError, response, control)
			return
		}
		response := ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error()}
		if failure := volumeLockFailure(err); failure != nil {
			response.LockCode = failure.Code
			recorded := failure.Recorded
			response.Recorded = &recorded
		}
		h.writeFileResponse(w, StatusStorageError, response, control)
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
		release, e := h.lockControls.acquire(r.Context(), retainedResponseMultiplier*DefaultMaxLockControlBytes)
		if e != nil {
			writeError(e)
			return
		}
		defer release()
		body, err = readAtMost(r.Body, DefaultMaxLockControlBytes)
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
	if req.Op == authz.FileRead && int64(req.Length) > fileReadLimit(h.maxBodyBytes) {
		writeError(syscall.EFBIG)
		return
	}
	if req.Op == authz.FileWrite && int64(len(req.Data)) > h.maxWriteBytes {
		writeError(syscall.EFBIG)
		return
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
	access := authz.AccessRequest{Operation: req.Op}
	if req.Op == authz.FileOpen || req.Op == authz.FileOpenNode {
		access.Open = req.Open.OpenAccess
	}
	if err := h.authorize(r.Context(), access); err != nil {
		writeError(err)
		return
	}
	digest := sha256.Sum256(append(body, []byte(r.Header.Get(HeaderMutationScope))...))
	response, err := h.fileCall(r.Context(), req, digest)
	if err != nil {
		writeError(err)
		return
	}
	h.writeFileResponse(w, http.StatusOK, response, control)
}

func validateFileArguments(req fileRequest, maximum storage.FileSessionOptions) error {
	switch req.Op {
	case authz.FileSessionOpen:
		return checkFileSessionOptions(req.Options, maximum)
	case authz.FileOpen:
		if err := req.Open.Check(); err != nil {
			return err
		}
		_, err := storage.CleanPath(string(req.Path))
		return err
	case authz.FileOpenNode:
		return req.Open.CheckNode(req.Node)
	case authz.FileRead:
		if req.Offset < 0 || req.Length < 0 {
			return syscall.EINVAL
		}
	case authz.FileWrite:
		if req.Offset < 0 {
			return syscall.EINVAL
		}
		if int64(len(req.Data)) > math.MaxInt64-req.Offset {
			return syscall.EFBIG
		}
	case authz.FileTruncate:
		if req.Offset < 0 {
			return syscall.EINVAL
		}
	case authz.FileSetAttr, authz.FileSetNodeAttr:
		return req.Change.Storage().Check()
	case authz.FileGetLock, authz.FileSetLock, authz.FileUnlock:
		if err := req.Lock.Check(); err != nil {
			return err
		}
		if req.Op == authz.FileGetLock {
			if req.Lock.Type == storage.Unlock {
				return syscall.EINVAL
			}
			return nil
		}
		if (req.Op == authz.FileUnlock) != (req.Lock.Type == storage.Unlock) {
			return syscall.EINVAL
		}
		_, err := req.LockID.Epoch()
		return err
	case authz.FileQueryLock, authz.FileCancelLock:
		_, err := req.LockID.Epoch()
		return err
	case authz.FileDropLocks:
		if req.Family != storage.Flock && req.Family != storage.POSIX {
			return syscall.EINVAL
		}
	}
	return nil
}

func (h *Handler) fileCall(ctx context.Context, req fileRequest, digest [32]byte) (fileResponse, error) {
	registry := h.files
	if req.Op == authz.FileSessionOpen {
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
		cleanup := req.Op == authz.FileDropLocks
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
		session.mu.Lock()
		action.response = response
		action.err = retainFileError(err)
		close(action.done)
		session.mu.Unlock()
		return response, err
	}
	if req.Op != authz.FileSessionClose && req.Op != authz.FileClose && (session.retired || !now.Before(session.expires)) {
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
	case authz.FileStatus, authz.FileRenew:
		start := time.Now()
		var status storage.FileSessionStatus
		if req.Op == authz.FileRenew {
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
		if req.Op == authz.FileRenew && status.Revision > s.revision {
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
	case authz.FileSessionClose:
		s.mu.Lock()
		s.retired = true
		s.mu.Unlock()
		return response, s.native.Close(ctx)
	case authz.FileStatNode:
		attr, err := s.native.StatNode(ctx, req.Node)
		wire := AttrOf(attr)
		response.Attr = wire
		return response, err
	case authz.FileSetNodeAttr:
		if req.Change == nil {
			return response, syscall.EINVAL
		}
		attr, e := s.native.SetNodeAttr(ctx, req.Node, req.Change.Storage())
		err = e
		wire := AttrOf(attr)
		response.Attr = wire
	case authz.FileOpen, authz.FileOpenNode:
		s.mu.Lock()
		if len(s.files) >= s.options.MaxFiles {
			s.mu.Unlock()
			return response, syscall.EAGAIN
		}
		cap := fileCapability()
		entry := &servedFile{closing: true}
		s.files[cap] = entry
		s.mu.Unlock()
		var file storage.File
		if req.Op == authz.FileOpen {
			file, err = s.native.OpenFile(ctx, string(req.Path), req.Open)
		} else {
			file, err = s.native.OpenNode(ctx, req.Node, req.Open)
		}
		s.mu.Lock()
		if err != nil {
			delete(s.files, cap)
		} else {
			entry.native = file
			entry.closing = false
			entry.pending = time.Now().Add(min(h.files.limits.PendingAck, s.options.Lease))
		}
		s.mu.Unlock()
		if err == nil {
			response.File = cap
		}
	case authz.FileAck:
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
		if file == nil && req.Op == authz.FileClose {
			s.mu.Unlock()
			return response, nil
		}
		if file == nil || file.native == nil || file.closing && req.Op != authz.FileClose {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if !file.pending.IsZero() && req.Op != authz.FileClose {
			s.mu.Unlock()
			return response, syscall.ESTALE
		}
		if req.Op == authz.FileClose {
			file.closing = true
		}
		s.mu.Unlock()
		var attr storage.Attr
		switch req.Op {
		case authz.FileStat:
			attr, err = file.native.Stat(ctx)
		case authz.FileRead:
			value, e := file.native.ReadAt(ctx, req.Offset, req.Length)
			err = e
			attr = value.Attr
			response.Data = value.Data
		case authz.FileWrite:
			attr, err = file.native.WriteAt(ctx, req.Offset, req.Data)
		case authz.FileTruncate:
			attr, err = file.native.Truncate(ctx, req.Offset)
		case authz.FileSetAttr:
			if req.Change == nil {
				return response, syscall.EINVAL
			}
			attr, err = file.native.SetAttr(ctx, req.Change.Storage())
		case authz.FileSync:
			err = file.native.Sync(ctx)
		case authz.FileGetLock:
			value, e := file.native.GetLock(ctx, req.Owner, req.Lock)
			response.Conflict = &value
			err = e
		case authz.FileSetLock, authz.FileUnlock:
			value, e := file.native.SetLock(ctx, req.Owner, req.Lock, req.LockID)
			err = e
			if err == nil {
				response.Attempt, err = fileAttemptOf(value)
			}
		case authz.FileQueryLock:
			value, e := file.native.QueryLock(ctx, req.Owner, req.LockID)
			err = e
			if err == nil {
				response.Attempt, err = fileAttemptOf(value)
			}
		case authz.FileCancelLock:
			value, e := file.native.CancelLock(ctx, req.Owner, req.LockID)
			err = e
			if err == nil {
				response.Attempt, err = fileAttemptOf(value)
			}
		case authz.FileDropLocks:
			err = file.native.DropLocks(ctx, req.Owner, req.Family)
		case authz.FileClose:
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
		case authz.FileStat, authz.FileRead, authz.FileWrite, authz.FileTruncate, authz.FileSetAttr:
			wire := AttrOf(attr)
			response.Attr = wire
		}
	}
	if err != nil {
		return response, err
	}
	if fileMutation(req.Op) {
		if h.publisher != nil {
			h.publisher.wake()
		}
		if h.log != nil {
			response.Barrier, err = h.mutationBarrier(ctx)
			if err != nil {
				return response, fmt.Errorf("file mutation completed but replication barrier is unknown: %v: %w", err, syscall.EIO)
			}
		}
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
	limit := min(h.maxBodyBytes, DefaultMaxLockControlBytes)
	if failure, ok := body.(ErrorResponse); ok {
		body = boundedErrorResponse(failure, limit)
	}
	encoded, err := json.Marshal(body)
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
	return fileResponse{Session: id, Epoch: session.epoch(time.Now()), Status: &status}, nil
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
	detail := err.Error()
	if len(detail) > 4096 {
		detail = strings.Clone(detail[:4096])
	}
	if failure := volumeLockFailure(err); failure != nil {
		return &locking.Error{Code: failure.Code, Recorded: failure.Recorded, Message: detail}
	}
	return &operationError{req: Request{Op: OpFile}, errno: storage.ErrnoOf(err), detail: detail, canceled: errors.Is(err, context.Canceled), deadline: errors.Is(err, context.DeadlineExceeded)}
}
