package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"
)

type fileScopeKey struct{}
type fileReadOnlyKey struct{}
type fileRequestAdmissionKey struct{}
type fileRequestAdmission struct {
	storage *Storage
	control bool
}

func fileReadOnly(op storage.Operation) bool {
	switch op {
	case storage.OpFileRead, storage.OpFileStat, storage.OpFileStatNode, storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileObserveDirectoryMetadata, storage.OpFileObserveName, storage.OpFileQueryAction, storage.OpFileQueryDeleteIntent, storage.OpFileState, storage.OpFileScope, storage.OpFileRangeGetConflict, storage.OpFileRangeQuery, storage.OpFileStatus:
		return true
	}
	return false
}
func requestInterruptible(ctx context.Context, r Request) bool {
	if r.Method() == "GET" {
		return true
	}
	read, _ := ctx.Value(fileReadOnlyKey{}).(bool)
	return (r.Op == OpFile || r.Op == OpFileControl) && read
}

func fileScopeEnabled(ctx context.Context) bool { v, _ := ctx.Value(fileScopeKey{}).(bool); return v }

type remoteFileSession struct {
	storage      *Storage
	id           string
	mu           sync.Mutex
	reconcileMu  sync.Mutex
	epoch        uint64
	failed       error
	closed       bool
	closeAction  storage.LockRequestID
	closeBarrier *MutationBarrier
	pending      map[string]pendingFileAction
	inflight     int
	pendingLimit int
	capabilities fileCapabilities
}

type pendingFileAction struct {
	request   fileRequest
	scope     locking.MutationScope
	hasScope  bool
	unknown   error
	response  *fileResponse
	resultErr error
}

type remoteFile struct {
	session      *remoteFileSession
	id           string
	node         uint64
	mu           sync.Mutex
	closed       bool
	closeAction  storage.LockRequestID
	closeBarrier *MutationBarrier
	capabilities fileCapabilities
}

func (s *Storage) CheckFileStorage() error { return nil }

var _ storage.FileStorage = (*Storage)(nil)
var _ FileSessionWithBarrier = (*remoteFileSession)(nil)
var _ FileWithBarrier = (*remoteFile)(nil)
var _ storage.AtomicFileOpener = (*remoteFileSession)(nil)
var _ storage.NamespaceAccess = (*remoteFileSession)(nil)
var _ storage.NodeReferences = (*remoteFileSession)(nil)
var _ storage.FileActions = (*remoteFileSession)(nil)

func (s *Storage) fileCall(ctx context.Context, req fileRequest) (fileResponse, error) {
	if fileBoundedResult(req.Op) && req.ResultBytes == 0 {
		req.ResultBytes = fileOperationLimit(req.Op, s.maxBodyBytes)
		if outer, ok := storage.AttrResultByteLimit(ctx); ok {
			req.ResultBytes = min(req.ResultBytes, outer)
		}
	}
	if req.Op == storage.OpFileRangeApply && rangeResponseBound(req.Commands) > min(s.maxBodyBytes, MaxFileControlBytes) {
		return fileResponse{}, syscall.EFBIG
	}
	if int64(len(req.Path)) > s.maxBodyBytes || int64(len(req.Data)) > s.maxWriteBytes {
		return fileResponse{}, syscall.EFBIG
	}
	if req.Op == storage.OpFileRead && (req.Length < 0 || int64(req.Length) > fileReadLimit(s.maxBodyBytes)) {
		return fileResponse{}, syscall.EFBIG
	}
	op := OpFile
	if fileControl(req.Op) {
		op = OpFileControl
	} else if admission, _ := ctx.Value(fileRequestAdmissionKey{}).(fileRequestAdmission); admission.storage != s || admission.control {
		release, err := s.fileRequests.acquire(ctx, retainedResponseMultiplier*s.maxBodyBytes)
		if err != nil {
			return fileResponse{}, operationFailure(Request{Op: OpFile}, err, true)
		}
		defer release()
	}
	if req.Path == nil {
		req.Path = []byte{}
	}
	if req.Data == nil {
		req.Data = []byte{}
	}
	envelope := req
	envelope.Path = []byte{}
	envelope.Data = []byte{}
	fixed, err := json.Marshal(envelope)
	if err != nil {
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	limit := s.maxBodyBytes
	if op == OpFileControl {
		limit = min(limit, MaxFileControlBytes)
	}
	encodedBytes := int64(len(fixed)) + int64(base64.StdEncoding.EncodedLen(len(req.Path))) + int64(base64.StdEncoding.EncodedLen(len(req.Data)))
	if encodedBytes > limit {
		return fileResponse{}, syscall.EFBIG
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	if fileMutation(req.Op) {
		ctx = context.WithValue(ctx, fileScopeKey{}, true)
	}
	ctx = context.WithValue(ctx, fileReadOnlyKey{}, fileReadOnly(req.Op))
	started := time.Now()
	answer, err := s.callWithin(ctx, Request{Op: op}, body, fileResponseLimit(req, s.maxBodyBytes))
	if err != nil {
		var operation *operationError
		if errors.As(err, &operation) && operation.attempt != nil {
			attempt := operation.attempt.Clone()
			attempt.HistoryRemaining = maxDuration(attempt.HistoryRemaining - time.Since(started))
			operation.attempt = &attempt
		}
		if errors.As(err, &operation) && operation.fileResult != nil {
			response := *operation.fileResult
			if validationErr := validatePartialFileResponse(req, response); validationErr != nil {
				return fileResponse{}, unreachable(Request{Op: op}, validationErr)
			}
			return response, err
		}
		return fileResponse{}, err
	}
	defer answer.release()
	var response fileResponse
	if req.Op == storage.OpFileReadDirNode || req.Op == storage.OpFileObserveDirectoryMetadata || req.Op == storage.OpFileObserveName {
		response, err = decodeObservedFileResponse(ctx, req, answer.content)
	} else {
		err = decodeFileJSON(answer.content, &response)
	}
	if err != nil {
		var budgetFailure *observationBudgetError
		if errors.As(err, &budgetFailure) {
			return fileResponse{}, budgetFailure.cause
		}
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	if err := validateFileResponse(req, response); err != nil {
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	elapsed := time.Since(started)
	if response.Status != nil {
		response.Status.Remaining = maxDuration(response.Status.Remaining - elapsed)
		response.Status.HistoryRemaining = maxDuration(response.Status.HistoryRemaining - elapsed)
		if response.Status.Epoch == "" || response.Status.ActionEpoch == 0 || response.Status.Revision == 0 {
			return fileResponse{}, unreachable(Request{Op: OpFile}, errors.New("invalid file session status"))
		}
	}
	if response.Attempt != nil {
		response.Attempt.HistoryRemaining = maxDuration(response.Attempt.HistoryRemaining - elapsed)
	}
	return response, nil
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	response, err := s.fileCall(ctx, fileRequest{Op: storage.OpFileSessionOpen, Options: options})
	if err != nil {
		return nil, err
	}
	if response.Session == "" || response.Status == nil || response.Status.Retired || response.Status.Remaining <= 0 {
		return nil, unreachable(Request{Op: OpFile}, errors.New("file session creation returned no live capability"))
	}
	return &remoteFileSession{storage: s, id: response.Session, epoch: response.Epoch, pendingLimit: options.MaxLockActions, capabilities: *response.Capabilities}, nil
}

func (s *remoteFileSession) call(ctx context.Context, req fileRequest) (fileResponse, error) {
	if int64(len(req.Path)) > s.storage.maxBodyBytes || int64(len(req.Data)) > s.storage.maxWriteBytes {
		return fileResponse{}, syscall.EFBIG
	}
	if req.Op == storage.OpFileRead && (req.Length < 0 || int64(req.Length) > fileReadLimit(s.storage.maxBodyBytes)) {
		return fileResponse{}, syscall.EFBIG
	}
	if fileBoundedResult(req.Op) && req.ResultBytes == 0 {
		req.ResultBytes = fileOperationLimit(req.Op, s.storage.maxBodyBytes)
		if outer, ok := storage.AttrResultByteLimit(ctx); ok {
			req.ResultBytes = min(req.ResultBytes, outer)
		}
	}
	if err := s.resolvePending(ctx); err != nil {
		return fileResponse{}, err
	}
	control := fileControl(req.Op)
	admission := s.storage.fileRequests
	requestLimit := s.storage.maxBodyBytes
	if control {
		admission = s.storage.lockControls
		requestLimit = MaxFileControlBytes
	}
	release, err := admission.acquire(ctx, retainedResponseMultiplier*requestLimit)
	if err != nil {
		return fileResponse{}, err
	}
	defer release()
	ctx = context.WithValue(ctx, fileRequestAdmissionKey{}, fileRequestAdmission{storage: s.storage, control: control})
	if req.Op == storage.OpFileRangeApply && rangeResponseBound(req.Commands) > min(s.storage.maxBodyBytes, MaxFileControlBytes) {
		return fileResponse{}, syscall.EFBIG
	}
	frozen, err := freezeFileRequest(req)
	if err != nil {
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	req = frozen
	if semantic := semanticFileAction(req); semantic != "" {
		if req.Action != "" && req.Action != semantic {
			return fileResponse{}, unreachable(Request{Op: OpFile}, errors.New("file operation action identity differs from its semantic action"))
		}
		req.Action = semantic
	}
	scope, hasScope := s.outgoingMutationScope(ctx, req)
	if response, recovered, err := s.takePendingResult(req, scope, hasScope); recovered || err != nil {
		return response, err
	}
	s.mu.Lock()
	if s.failed != nil && req.Op != storage.OpFileSessionClose {
		err := s.failed
		s.mu.Unlock()
		return fileResponse{}, err
	}
	if s.closed {
		s.mu.Unlock()
		return fileResponse{}, syscall.ESTALE
	}
	reserved := false
	if fileActionRequired(req.Op) {
		limit := s.pendingLimit
		if limit <= 0 {
			limit = storage.DefaultFileSessionOptions().MaxLockActions
		}
		if len(s.pending)+s.inflight >= limit {
			s.mu.Unlock()
			return fileResponse{}, syscall.EAGAIN
		}
		if req.Action == "" {
			req.Action, err = storage.NewLockRequestID(s.epoch)
			if err != nil {
				s.mu.Unlock()
				return fileResponse{}, err
			}
		}
		s.inflight++
		reserved = true
	}
	s.mu.Unlock()
	if reserved {
		defer func() {
			s.mu.Lock()
			s.inflight--
			s.mu.Unlock()
		}()
	}
	req.Session = s.id
	response, err := s.storage.fileCall(ctx, req)
	if err == nil && response.Retry {
		if !fileActionRequired(req.Op) {
			return fileResponse{}, unreachable(Request{Op: OpFile}, errors.New("unexpected file action retry receipt"))
		}
		req.Action, err = storage.NewLockRequestID(response.Epoch)
		if err == nil {
			response, err = s.storage.fileCall(ctx, req)
		}
		if err == nil && response.Retry {
			return fileResponse{}, syscall.EAGAIN
		}
	}
	recoveryAction := req.Action
	if recoveryAction == "" {
		recoveryAction = semanticFileAction(req)
	}
	var failure *operationError
	if err != nil && errors.As(err, &failure) && failure.unknown && (recoveryAction != "" || req.Op == storage.OpFileAck) {
		recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		recovered, recoveryErr := s.storage.fileCall(recovery, req)
		cancel()
		if recoveryErr == nil && !recovered.Retry {
			response, err = recovered, nil
		} else if recordedFileOutcome(recoveryErr) {
			response, err = recovered, recoveryErr
		} else if recoveryAction != "" {
			s.mu.Lock()
			if s.pending == nil {
				s.pending = make(map[string]pendingFileAction)
			}
			s.pending[string(recoveryAction)] = pendingFileAction{request: req, scope: locking.CloneScope(scope), hasScope: hasScope, unknown: err}
			s.mu.Unlock()
		} else {
			s.mu.Lock()
			s.failed = err
			s.mu.Unlock()
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.Close(cleanup)
			cancel()
		}
	}
	if err == nil {
		s.mu.Lock()
		if response.Epoch > s.epoch {
			s.epoch = response.Epoch
		}
		s.mu.Unlock()
	}
	return response, err
}

func recordedFileOutcome(err error) bool {
	var operation *operationError
	return errors.As(err, &operation) && operation.recorded
}

func freezeFileRequest(req fileRequest) (fileRequest, error) {
	if req.Path == nil {
		req.Path = []byte{}
	}
	if req.Data == nil {
		req.Data = []byte{}
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return fileRequest{}, err
	}
	var frozen fileRequest
	if err := decodeFileJSON(encoded, &frozen); err != nil {
		return fileRequest{}, err
	}
	return frozen, nil
}

func (s *remoteFileSession) outgoingMutationScope(ctx context.Context, req fileRequest) (locking.MutationScope, bool) {
	if !fileMutation(req.Op) {
		return locking.MutationScope{}, false
	}
	scope := locking.ScopeFromContext(ctx)
	if !locking.HasScope(ctx) && s.storage.scope != nil {
		scope = locking.CloneScope(*s.storage.scope)
	}
	present := scope.Owner != (locking.OwnerRef{}) || len(scope.Grants) != 0
	return scope, present
}

func (s *remoteFileSession) reconcilePending(ctx context.Context, incoming fileRequest, incomingScope locking.MutationScope, incomingHasScope bool) (fileResponse, bool, error) {
	if err := s.resolvePending(ctx); err != nil {
		return fileResponse{}, false, err
	}
	return s.takePendingResult(incoming, incomingScope, incomingHasScope)
}

func (s *remoteFileSession) resolvePending(ctx context.Context) error {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return nil
	}
	keys := make([]string, 0, len(s.pending))
	for key := range s.pending {
		keys = append(keys, key)
	}
	s.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		s.mu.Lock()
		pending, ok := s.pending[key]
		s.mu.Unlock()
		if !ok {
			continue
		}
		if pending.response != nil || pending.resultErr != nil {
			continue
		}
		recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if pending.hasScope {
			recovery = locking.WithScope(recovery, pending.scope)
		}
		response, err := s.storage.fileCall(recovery, pending.request)
		cancel()
		if err != nil {
			if recordedFileOutcome(err) {
				s.mu.Lock()
				copy := response
				pending.response = &copy
				pending.resultErr = err
				s.pending[key] = pending
				s.mu.Unlock()
				continue
			}
			return pending.unknown
		}
		if response.Retry {
			s.mu.Lock()
			delete(s.pending, key)
			s.mu.Unlock()
			continue
		}
		s.mu.Lock()
		if response.Epoch > s.epoch {
			s.epoch = response.Epoch
		}
		copy := response
		pending.response = &copy
		s.pending[key] = pending
		s.mu.Unlock()
	}
	return nil
}

func (s *remoteFileSession) takePendingResult(incoming fileRequest, incomingScope locking.MutationScope, incomingHasScope bool) (fileResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, pending := range s.pending {
		previous := pending.request
		previous.Session, previous.Action = "", ""
		candidate := incoming
		candidate.Session, candidate.Action = "", ""
		matches := reflect.DeepEqual(previous, candidate) && pending.hasScope == incomingHasScope && (!pending.hasScope || reflect.DeepEqual(pending.scope, incomingScope))
		if !matches {
			continue
		}
		if pending.response != nil {
			delete(s.pending, key)
			return *pending.response, true, pending.resultErr
		}
		if pending.resultErr != nil {
			delete(s.pending, key)
			return fileResponse{}, true, pending.resultErr
		}
	}
	return fileResponse{}, false, nil
}

func (s *remoteFileSession) open(ctx context.Context, req fileRequest) (storage.File, *MutationBarrier, error) {
	response, err := s.call(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	if response.File == "" {
		return nil, nil, unreachable(Request{Op: OpFile}, errors.New("file open returned no capability"))
	}
	_, err = s.call(ctx, fileRequest{Op: storage.OpFileAck, File: response.File})
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, cleanupErr := s.storage.fileCall(cleanup, fileRequest{Op: storage.OpFileClose, Session: s.id, File: response.File})
		cancel()
		s.mu.Lock()
		sessionClosed := s.closed
		s.mu.Unlock()
		interruptible := !req.Open.Create && !req.Open.Truncate &&
			errors.Is(err, context.Canceled) && storage.ErrnoOf(err) == syscall.EINTR && cleanupErr == nil && !sessionClosed
		if cleanupErr != nil && !errors.Is(cleanupErr, syscall.ESTALE) {
			err = errors.Join(err, cleanupErr)
		}
		return nil, nil, operationFailure(Request{Op: OpFile}, err, interruptible)
	}
	return &remoteFile{session: s, id: response.File, node: response.Node, capabilities: *response.Capabilities}, response.Barrier, nil
}

func (s *remoteFileSession) OpenFile(ctx context.Context, path string, o storage.FileOpenOptions) (storage.File, error) {
	f, _, err := s.OpenFileWithBarrier(ctx, path, o)
	return f, err
}
func (s *remoteFileSession) OpenFileWithBarrier(ctx context.Context, path string, o storage.FileOpenOptions) (storage.File, *MutationBarrier, error) {
	if err := o.Check(); err != nil {
		return nil, nil, err
	}
	return s.open(ctx, fileRequest{Op: storage.OpFileOpen, Path: []byte(path), Open: o})
}
func (s *remoteFileSession) OpenNode(ctx context.Context, id uint64, o storage.FileOpenOptions) (storage.File, error) {
	f, _, err := s.OpenNodeWithBarrier(ctx, id, o)
	return f, err
}
func (s *remoteFileSession) OpenNodeWithBarrier(ctx context.Context, id uint64, o storage.FileOpenOptions) (storage.File, *MutationBarrier, error) {
	if err := o.CheckNode(id); err != nil {
		return nil, nil, err
	}
	return s.open(ctx, fileRequest{Op: storage.OpFileOpenNode, Node: id, Open: o})
}
func responseFileAttr(response fileResponse, err error) (storage.Attr, error) {
	if err != nil {
		return storage.Attr{}, err
	}
	if response.Attr == nil {
		return storage.Attr{}, unreachable(Request{Op: OpFile}, errors.New("file result carries no attributes"))
	}
	return response.Attr.Storage(), nil
}

func responseRegularFileAttr(response fileResponse, err error) (storage.Attr, error) {
	attr, err := responseFileAttr(response, err)
	if err == nil && attr.Kind != storage.NodeRegular {
		return storage.Attr{}, unreachable(Request{Op: OpFile}, errors.New("file operation returned nonregular attributes"))
	}
	return attr, err
}
func (s *remoteFileSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	r, e := s.call(ctx, fileRequest{Op: storage.OpFileStatNode, Node: id})
	return responseFileAttr(r, e)
}
func (s *remoteFileSession) SetNodeAttr(ctx context.Context, id uint64, c storage.AttrChange) (storage.Attr, error) {
	a, _, e := s.SetNodeAttrWithBarrier(ctx, id, c)
	return a, e
}
func (s *remoteFileSession) SetNodeAttrWithBarrier(ctx context.Context, id uint64, c storage.AttrChange) (storage.Attr, *MutationBarrier, error) {
	change := AttrChangeOf(c)
	r, e := s.call(ctx, fileRequest{Op: storage.OpFileSetNodeAttr, Node: id, Change: change})
	a, e := responseFileAttr(r, e)
	return a, r.Barrier, e
}
func (s *remoteFileSession) status(ctx context.Context, op storage.Operation) (storage.FileSessionStatus, error) {
	r, e := s.call(ctx, fileRequest{Op: op})
	if e != nil {
		return storage.FileSessionStatus{}, e
	}
	if r.Status == nil {
		return storage.FileSessionStatus{}, unreachable(Request{Op: OpFile}, errors.New("file result carries no session status"))
	}
	return *r.Status, nil
}
func (s *remoteFileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.status(ctx, storage.OpFileStatus)
}
func (s *remoteFileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.status(ctx, storage.OpFileRenew)
}
func (s *remoteFileSession) Close(ctx context.Context) error {
	_, err := s.CloseWithBarrier(ctx)
	return err
}

func (s *remoteFileSession) CloseWithBarrier(ctx context.Context) (*MutationBarrier, error) {
	s.mu.Lock()
	if s.closed {
		barrier := s.closeBarrier
		s.mu.Unlock()
		return barrier, nil
	}
	if s.closeAction == "" {
		var err error
		s.closeAction, err = storage.NewLockRequestID(s.epoch)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	action := s.closeAction
	s.mu.Unlock()
	r, e := s.storage.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: s.id, Action: action})
	if e == nil || errors.Is(e, syscall.ESTALE) {
		s.mu.Lock()
		s.closed = true
		s.closeBarrier = r.Barrier
		s.mu.Unlock()
		return r.Barrier, nil
	}
	return nil, e
}

func (f *remoteFile) call(ctx context.Context, r fileRequest) (fileResponse, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return fileResponse{}, syscall.EBADF
	}
	r.File = f.id
	return f.session.call(ctx, r)
}
func (f *remoteFile) Stat(ctx context.Context) (storage.Attr, error) {
	return f.stat(ctx, true)
}
func (f *remoteFile) stat(ctx context.Context, regular bool) (storage.Attr, error) {
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileStat})
	if regular {
		return responseRegularFileAttr(r, e)
	}
	return responseFileAttr(r, e)
}
func (f *remoteFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if offset < 0 || length < 0 {
		return storage.FileRead{}, syscall.EINVAL
	}
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileRead, Offset: offset, Length: length})
	a, e := responseRegularFileAttr(r, e)
	if e != nil {
		return storage.FileRead{}, e
	}
	expected := int64(length)
	if offset >= a.Size {
		expected = 0
	} else if expected > a.Size-offset {
		expected = a.Size - offset
	}
	if int64(len(r.Data)) > expected || expected > 0 && len(r.Data) == 0 {
		return storage.FileRead{}, unreachable(Request{Op: OpFile}, errors.New("file range and captured size disagree"))
	}
	return storage.FileRead{Attr: a, Data: r.Data}, nil
}
func (f *remoteFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	a, _, e := f.WriteAtWithBarrier(ctx, offset, data)
	return a, e
}
func (f *remoteFile) WriteAtWithBarrier(ctx context.Context, offset int64, data []byte) (storage.Attr, *MutationBarrier, error) {
	if offset < 0 {
		return storage.Attr{}, nil, syscall.EINVAL
	}
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileWrite, Offset: offset, Data: data})
	a, e := responseRegularFileAttr(r, e)
	return a, r.Barrier, e
}
func (f *remoteFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	a, _, e := f.TruncateWithBarrier(ctx, size)
	return a, e
}
func (f *remoteFile) TruncateWithBarrier(ctx context.Context, size int64) (storage.Attr, *MutationBarrier, error) {
	if size < 0 {
		return storage.Attr{}, nil, syscall.EINVAL
	}
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileTruncate, Offset: size})
	a, e := responseRegularFileAttr(r, e)
	return a, r.Barrier, e
}
func (f *remoteFile) SetAttr(ctx context.Context, c storage.AttrChange) (storage.Attr, error) {
	a, _, e := f.setAttrWithBarrier(ctx, c, true)
	return a, e
}
func (f *remoteFile) SetAttrWithBarrier(ctx context.Context, c storage.AttrChange) (storage.Attr, *MutationBarrier, error) {
	return f.setAttrWithBarrier(ctx, c, true)
}
func (f *remoteFile) setAttrWithBarrier(ctx context.Context, c storage.AttrChange, regular bool) (storage.Attr, *MutationBarrier, error) {
	change := AttrChangeOf(c)
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileSetAttr, Change: change})
	var a storage.Attr
	if regular {
		a, e = responseRegularFileAttr(r, e)
	} else {
		a, e = responseFileAttr(r, e)
	}
	return a, r.Barrier, e
}
func (f *remoteFile) Sync(ctx context.Context) error {
	_, e := f.call(ctx, fileRequest{Op: storage.OpFileSync})
	return e
}
func (f *remoteFile) Close(ctx context.Context) error {
	_, err := f.CloseWithBarrier(ctx)
	return err
}

func (f *remoteFile) CloseWithBarrier(ctx context.Context) (*MutationBarrier, error) {
	f.mu.Lock()
	if f.closed {
		barrier := f.closeBarrier
		f.mu.Unlock()
		return barrier, nil
	}
	if f.closeAction == "" {
		f.session.mu.Lock()
		epoch := f.session.epoch
		f.session.mu.Unlock()
		var err error
		f.closeAction, err = storage.NewLockRequestID(epoch)
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
	}
	action := f.closeAction
	f.mu.Unlock()
	r, e := f.session.call(ctx, fileRequest{Op: storage.OpFileClose, File: f.id, Action: action})
	if e == nil || errors.Is(e, syscall.ESTALE) {
		f.mu.Lock()
		f.closed = true
		f.closeBarrier = r.Barrier
		f.mu.Unlock()
		return r.Barrier, nil
	}
	return nil, e
}
