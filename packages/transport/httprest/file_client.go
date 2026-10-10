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
	recovery bool
	storage  *Storage
	control  bool
}

func fileReadOnly(op storage.Operation) bool {
	if op == opFileSessionReleaseResult {
		return true
	}
	switch op {
	case storage.OpFileBackendIdentity, storage.OpFileRead, storage.OpFileStat, storage.OpFileStatNode, storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileObserveDirectoryMetadata, storage.OpFileObserveName, storage.OpFileObserveContentMetadata, storage.OpFileQueryAction, storage.OpFileCloseOwnerStatus, storage.OpFileQueryDeleteIntent, storage.OpFileListDeleteIntents, storage.OpFileState, storage.OpFileScope, storage.OpFileRangeGetConflict, storage.OpFileRangeQuery, storage.OpFileStatus:
		return true
	}
	return false
}
func requestInterruptible(ctx context.Context, r Request) bool {
	if r.Method() == "GET" {
		return true
	}
	read, _ := ctx.Value(fileReadOnlyKey{}).(bool)
	return (r.Op == OpFile || r.Op == OpFileControl || r.Op == OpFileRecovery) && read
}

func fileScopeEnabled(ctx context.Context) bool { v, _ := ctx.Value(fileScopeKey{}).(bool); return v }

type remoteFileSession struct {
	storage              *Storage
	id                   string
	mu                   sync.Mutex
	closeCallMu          sync.Mutex
	reconcileMu          sync.Mutex
	explicitWrites       map[string]*explicitFileWrite
	recoveryAdmission    *bodyAdmission
	epoch                uint64
	failed               error
	closed               bool
	closeAction          storage.LockRequestID
	closeGeneration      uint64
	closeDeterminedFalse bool
	closeBarrier         *MutationBarrier
	closeErr             error
	closeBarrierPending  bool
	closeScope           locking.MutationScope
	pending              map[string]pendingFileAction
	unclaimed            map[string]*remoteFile
	unclaimedLimit       int
	closeActionLimit     int
	closeHistory         time.Duration
	inflight             int
	cleanupInflight      int
	pendingLimit         int
	capabilities         fileCapabilities
	identity             storage.FileSessionIdentityResult
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
	session              *remoteFileSession
	id                   string
	node                 uint64
	mu                   sync.Mutex
	closeCallMu          sync.Mutex
	closed               bool
	closeAction          storage.LockRequestID
	closeGeneration      uint64
	closeDeterminedFalse bool
	closeImplicit        bool
	closeNotExecuted     map[storage.CloseAttempt]remoteCloseProof
	closePendingProof    *remoteClosePendingProof
	closeBarrier         *MutationBarrier
	closeErr             error
	closeBarrierPending  bool
	closeScope           locking.MutationScope
	closeScopeSet        bool
	capabilities         fileCapabilities
	contentEffects       []storage.ContentMetadataEffect
}

type remoteClosePendingProof struct {
	attempt            storage.CloseAttempt
	previousAction     storage.LockRequestID
	previousGeneration uint64
	previousDetermined bool
	previousImplicit   bool
	previousScope      locking.MutationScope
	previousScopeSet   bool
}

type remoteCloseProof struct {
	epoch   uint64
	expires time.Time
}

// CloseBarrierPendingError preserves a confirmed release while its replication
// barrier remains unresolved. Repeating the same close action may settle it.
type CloseBarrierPendingError struct {
	State       storage.CloseSettlementState
	SemanticErr error
	Cause       error
}

func (e *CloseBarrierPendingError) Error() string   { return e.Cause.Error() }
func (e *CloseBarrierPendingError) Unwrap() []error { return []error{e.SemanticErr, e.Cause} }

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
	recovery, _ := ctx.Value(fileRecoveryKey{}).(bool)
	if recovery {
		if !isExplicitFileWrite(req) || len(req.Mutation.Data) > MaxFileRecoveryDataBytes {
			return fileResponse{}, syscall.EINVAL
		}
		release, err := s.fileRecovery.acquire(ctx, retainedResponseMultiplier*min(s.maxBodyBytes, MaxFileRecoveryBytes))
		if err != nil {
			return fileResponse{}, err
		}
		defer release()
		ctx = context.WithValue(ctx, fileRequestAdmissionKey{}, fileRequestAdmission{storage: s, recovery: true})
		op = OpFileRecovery
	} else if fileControl(req.Op) {
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
	if op == OpFileRecovery {
		limit = min(limit, MaxFileRecoveryBytes)
	}
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
	var identity storage.FileSessionIdentityResult
	if response.SessionIdentity != nil {
		identity = response.SessionIdentity.storage()
	}
	return &remoteFileSession{storage: s, id: response.Session, epoch: response.Epoch, pendingLimit: options.MaxLockActions, unclaimedLimit: options.MaxFiles, closeActionLimit: options.MaxCloseActions, closeHistory: options.History, capabilities: *response.Capabilities, identity: identity}, nil
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
	if isExplicitFileWrite(req) {
		return s.callExplicitWrite(ctx, req)
	}
	if req.Op != storage.OpFileSync {
		if err := s.resolvePending(ctx); err != nil {
			return fileResponse{}, err
		}
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
	if retainsFileReference(req.Op) && s.unclaimedLimit > 0 && len(s.unclaimed)+s.inflight >= s.unclaimedLimit {
		s.mu.Unlock()
		return fileResponse{}, syscall.EAGAIN
	}
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
		count := s.dataPendingLocked() + len(s.explicitWrites) + s.inflight
		cleanup := cleanupFileAction(req.Op)
		if cleanup {
			limit = s.closeActionLimit
			if limit <= 0 {
				limit = storage.DefaultFileSessionOptions().MaxCloseActions
			}
			count = s.cleanupPendingLocked() + s.cleanupInflight
		}
		if count >= limit {
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
		if cleanup {
			s.cleanupInflight++
		} else {
			s.inflight++
		}
		reserved = true
	}
	s.mu.Unlock()
	if reserved {
		defer func() {
			s.mu.Lock()
			if cleanupFileAction(req.Op) {
				s.cleanupInflight--
			} else {
				s.inflight--
			}
			s.mu.Unlock()
		}()
	}
	req.Session = s.id
	response, err := s.storage.fileCall(ctx, req)
	if err == nil && response.Retry {
		if !fileActionRequired(req.Op) {
			return fileResponse{}, unreachable(Request{Op: OpFile}, errors.New("unexpected file action retry receipt"))
		}
		if req.Op == storage.OpFileClose {
			s.mu.Lock()
			if response.Epoch > s.epoch {
				s.epoch = response.Epoch
			}
			s.mu.Unlock()
			return response, nil
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
		} else if recordedFileOutcome(recoveryErr) || releasedCloseBarrierPending(req, recovered) {
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
		if s.capabilities.Allocation {
			if validationErr := checkReportedAllocation(response); validationErr != nil {
				return response, unreachable(Request{Op: OpFile}, validationErr)
			}
		}
		s.mu.Lock()
		if response.Epoch > s.epoch {
			s.epoch = response.Epoch
		}
		s.mu.Unlock()
	}
	return response, err
}

func retainsFileReference(op storage.Operation) bool {
	switch op {
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		return true
	}
	return false
}

func checkReportedAllocation(response fileResponse) error {
	check := func(attr *Attr) error {
		if attr == nil {
			return nil
		}
		if !attr.AllocationKnown {
			return errors.New("allocation-reporting authority returned unknown allocation")
		}
		return (storage.Attr{AllocationSize: attr.AllocationSize, AllocationKnown: attr.AllocationKnown}).CheckAllocation()
	}
	if err := check(response.Attr); err != nil {
		return err
	}
	if response.State != nil {
		if err := check(response.State.Attr); err != nil {
			return err
		}
	}
	if response.Directory != nil {
		for _, entry := range response.Directory.Entries {
			if err := check(entry.Attr); err != nil {
				return err
			}
		}
	}
	return nil
}

func recordedFileOutcome(err error) bool {
	var operation *operationError
	return errors.As(err, &operation) && operation.recorded
}

func releasedCloseBarrierPending(req fileRequest, response fileResponse) bool {
	return (req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose) &&
		response.CloseResult != nil && response.CloseResult.Released && response.CloseResult.BarrierPending
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
		if !isExplicitFileWrite(s.pending[key].request) {
			keys = append(keys, key)
		}
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
			if recordedFileOutcome(err) || releasedCloseBarrierPending(pending.request, response) {
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
			if s.capabilities.Allocation && pending.resultErr == nil {
				if err := checkReportedAllocation(*pending.response); err != nil {
					return *pending.response, true, unreachable(Request{Op: OpFile}, err)
				}
			}
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
		if response.File != "" {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err = errors.Join(err, s.closeUnclaimedFile(cleanup, response.File, response.Epoch))
			cancel()
		}
		return nil, nil, err
	}
	if response.File == "" {
		return nil, nil, unreachable(Request{Op: OpFile}, errors.New("file open returned no capability"))
	}
	_, err = s.call(ctx, fileRequest{Op: storage.OpFileAck, File: response.File})
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cleanupErr := s.closeUnclaimedFile(cleanup, response.File, response.Epoch)
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
	_, err := s.CloseWithResult(ctx)
	return err
}

func (s *remoteFileSession) closeUnclaimedFile(ctx context.Context, file string, epoch uint64) error {
	s.mu.Lock()
	if s.unclaimed == nil {
		s.unclaimed = make(map[string]*remoteFile)
	}
	owner := s.unclaimed[file]
	if owner == nil {
		owner = &remoteFile{session: s, id: file}
		s.unclaimed[file] = owner
	}
	if epoch > s.epoch {
		s.epoch = epoch
	}
	s.mu.Unlock()
	owner.mu.Lock()
	action := owner.closeAction
	generation := owner.closeGeneration
	advance := owner.closeDeterminedFalse
	owner.mu.Unlock()
	if action == "" || advance {
		id, idErr := storage.NewFileActionID(epoch)
		if idErr != nil {
			return idErr
		}
		action = storage.LockRequestID(id)
		generation++
	}
	result, _, err := owner.closeWithActionAndBarrier(ctx, storage.CloseAttempt{Action: storage.FileActionID(action), Generation: generation}, true)
	if result.Released {
		s.mu.Lock()
		delete(s.unclaimed, file)
		s.mu.Unlock()
		return err
	}
	if err == nil {
		return unreachable(Request{Op: OpFileControl}, errors.New("unclaimed file close did not prove release"))
	}
	return err
}

func (s *remoteFileSession) CloseWithBarrier(ctx context.Context) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	return s.closeWithResultAndBarrier(ctx)
}

func (s *remoteFileSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	result, _, err := s.closeWithResultAndBarrier(ctx)
	return result, neutralCloseSettlement(err)
}

func (s *remoteFileSession) CheckRecoverableReferenceClose() error {
	if s.capabilities.CloseRecovery {
		return nil
	}
	return syscall.EOPNOTSUPP
}

func (s *remoteFileSession) closeWithResultAndBarrier(ctx context.Context) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	s.closeCallMu.Lock()
	defer s.closeCallMu.Unlock()
	s.mu.Lock()
	if s.closed && !s.closeBarrierPending {
		barrier := s.closeBarrier
		err := s.closeErr
		s.mu.Unlock()
		return storage.ReferenceCloseResult{Released: true, Determined: true}, barrier, err
	}
	wasReleased := s.closed
	previousBarrier := s.closeBarrier
	previousErr := s.closeErr
	if s.closeDeterminedFalse {
		s.closeGeneration++
		s.closeAction = ""
		s.closeDeterminedFalse = false
	}
	if s.closeAction == "" {
		if s.closeGeneration == 0 {
			s.closeGeneration = 1
		}
		var err error
		s.closeAction, err = storage.NewLockRequestID(s.epoch)
		if err != nil {
			s.mu.Unlock()
			return storage.ReferenceCloseResult{}, nil, err
		}
		s.closeScope, _ = s.outgoingMutationScope(ctx, fileRequest{Op: storage.OpFileSessionClose})
		s.closeScope = locking.CloneScope(s.closeScope)
	}
	action := s.closeAction
	generation := s.closeGeneration
	scope := locking.CloneScope(s.closeScope)
	s.mu.Unlock()
	ctx = locking.WithScope(ctx, scope)
	r, e := s.storage.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: s.id, Action: action, CloseGeneration: generation})
	if e == nil && r.Retry {
		if wasReleased {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, unreachable(Request{Op: OpFileControl}, errors.New("released close lost its action receipt")))
		}
		action, e = storage.NewLockRequestID(r.Epoch)
		if e != nil {
			return storage.ReferenceCloseResult{}, nil, e
		}
		s.mu.Lock()
		s.closeAction = action
		s.epoch = r.Epoch
		s.mu.Unlock()
		r, e = s.storage.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: s.id, Action: action, CloseGeneration: generation})
		if e == nil && r.Retry {
			return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
		}
	}
	if r.CloseResult == nil && errors.Is(e, syscall.ESTALE) {
		fact, factErr := s.sessionReleaseResult(ctx)
		if fact.CloseResult != nil {
			r, e = fact, factErr
		} else {
			e = errors.Join(e, factErr)
		}
	}
	if r.CloseResult != nil && r.CloseResult.Released {

		e = closeReplayResultError(r.CloseResult, wasReleased, previousErr, e)
		s.mu.Lock()
		s.closed = true
		s.unclaimed = nil
		s.closeBarrier = r.Barrier
		s.closeErr = e
		s.closeBarrierPending = r.CloseResult.BarrierPending
		s.mu.Unlock()
		if !r.CloseResult.BarrierPending {
			s.detachExplicitWrites("")
		}
		return r.CloseResult.storage(), r.Barrier, e
	}
	if r.CloseResult != nil {
		if wasReleased {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, unreachable(Request{Op: OpFileControl}, errors.New("close replay revoked confirmed release")))
		}
		if r.CloseResult.Determined {
			s.mu.Lock()
			s.closeDeterminedFalse = true
			s.mu.Unlock()
		}
		return r.CloseResult.storage(), r.Barrier, e
	}
	if wasReleased {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, e)
	}
	return storage.ReferenceCloseResult{}, nil, e
}

func closeBarrierResultError(result *referenceCloseResult, err error) error {
	if !result.BarrierPending {
		return err
	}
	cause := err
	if cause == nil {
		cause = syscall.EIO
	}
	semantic := err
	// The wire combines barrier failures with the native outcome as EIO. Its
	// exact native error is recovered from the same receipt after settlement.
	if storage.ErrnoOf(err) == syscall.EIO {
		semantic = nil
	}
	return &CloseBarrierPendingError{State: storage.CloseSettlementPending, SemanticErr: semantic, Cause: cause}
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
	_, err := f.SyncWithBarrier(ctx)
	return err
}
func (f *remoteFile) SyncWithBarrier(ctx context.Context) (*MutationBarrier, error) {
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileSync})
	return response.Barrier, err
}
func (f *remoteFile) Close(ctx context.Context) error {
	_, err := f.CloseWithResult(ctx)
	return err
}

func (f *remoteFile) CloseWithBarrier(ctx context.Context) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	return f.closeWithResultAndBarrier(ctx)
}

func (f *remoteFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	result, _, err := f.closeWithResultAndBarrier(ctx)
	return result, neutralCloseSettlement(err)
}

func (f *remoteFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	result, _, err := f.CloseWithActionAndBarrier(ctx, attempt)
	return result, neutralCloseSettlement(err)
}

func (f *remoteFile) CloseWithActionAndBarrier(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	return f.closeWithActionAndBarrier(ctx, attempt, false)
}

func (f *remoteFile) closeWithActionAndBarrier(ctx context.Context, attempt storage.CloseAttempt, implicit bool) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	if !implicit && !f.session.capabilities.CloseRecovery {
		return storage.ReferenceCloseResult{}, nil, syscall.EOPNOTSUPP
	}
	if err := attempt.Check(); err != nil {
		return storage.ReferenceCloseResult{}, nil, err
	}
	f.closeCallMu.Lock()
	defer f.closeCallMu.Unlock()
	f.mu.Lock()
	f.pruneCloseProofsLocked(time.Now())
	if f.closePendingProof != nil && f.closePendingProof.attempt == attempt {
		f.mu.Unlock()
		resolved, proofErr := f.resolvePendingCloseProof(ctx)
		if proofErr != nil {
			return storage.ReferenceCloseResult{}, nil, proofErr
		}
		if resolved {
			return storage.ReferenceCloseResult{}, nil, unreachable(Request{Op: OpFileControl}, errors.New("resolved close proof has no result"))
		}
		f.mu.Lock()
	}
	adoptImplicit := false
	if !implicit && f.closeImplicit && f.closeAction != "" && f.closeAction != storage.LockRequestID(attempt.Action) && !f.closeDeterminedFalse {
		f.mu.Unlock()
		owner, statusErr := f.CloseOwnerStatus(ctx)
		if statusErr != nil {
			return storage.ReferenceCloseResult{}, nil, statusErr
		}
		if owner.Current == nil || *owner.Current != attempt {
			return storage.ReferenceCloseResult{}, nil, syscall.EBUSY
		}
		adoptImplicit = true
		f.mu.Lock()
	}
	if rejected, found := f.closeNotExecuted[attempt]; found {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: rejected.epoch}
	}
	if f.closeAction == storage.LockRequestID(attempt.Action) && f.closeGeneration != 0 && f.closeGeneration != attempt.Generation {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
	}
	historical := attempt.Generation < f.closeGeneration
	newExplicit := !implicit && !historical && (f.closeAction != storage.LockRequestID(attempt.Action) || f.closeGeneration != attempt.Generation)
	if newExplicit && !adoptImplicit {
		f.mu.Unlock()
		owner, statusErr := f.CloseOwnerStatus(ctx)
		if statusErr != nil {
			return storage.ReferenceCloseResult{}, nil, statusErr
		}
		if owner.Current != nil {
			if *owner.Current != attempt {
				return storage.ReferenceCloseResult{}, nil, syscall.EBUSY
			}
		} else {
			if !owner.Ready {
				return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
			}
			if attempt.Generation != owner.NextGeneration {
				return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
			}
			requestedEpoch, epochErr := attempt.Action.Epoch()
			if epochErr != nil {
				return storage.ReferenceCloseResult{}, nil, epochErr
			}
			if requestedEpoch > owner.CurrentEpoch {
				return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
			}
			if requestedEpoch == owner.CurrentEpoch {
				receipt, queryErr := f.QueryCloseAttempt(ctx, attempt)
				if queryErr != nil {
					return storage.ReferenceCloseResult{}, nil, queryErr
				}
				if receipt.Outcome != storage.FileActionNotExecuted {
					return storage.ReferenceCloseResult{}, nil, syscall.EBUSY
				}
			}
		}
		f.mu.Lock()
	}
	_, previousNotExecuted := f.closeNotExecuted[storage.CloseAttempt{Action: storage.FileActionID(f.closeAction), Generation: f.closeGeneration}]
	if !historical && f.closeAction != "" && f.closeAction != storage.LockRequestID(attempt.Action) && !f.closeDeterminedFalse && !previousNotExecuted && !adoptImplicit {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
	}
	if !historical && f.closeAction != "" && f.closeAction != storage.LockRequestID(attempt.Action) && attempt.Generation != f.closeGeneration+1 && !(previousNotExecuted && attempt.Generation == f.closeGeneration) && !adoptImplicit {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
	}
	previousAction, previousGeneration := f.closeAction, f.closeGeneration
	previousDetermined, previousImplicit := f.closeDeterminedFalse, f.closeImplicit
	previousScope, previousScopeSet := locking.CloneScope(f.closeScope), f.closeScopeSet
	wasReleased := f.closed && !historical && f.closeAction == storage.LockRequestID(attempt.Action) && f.closeGeneration == attempt.Generation
	previousBarrier, previousErr := f.closeBarrier, f.closeErr
	if !historical {
		f.closeAction = storage.LockRequestID(attempt.Action)
		f.closeGeneration = attempt.Generation
		f.closeDeterminedFalse = false
		f.closeImplicit = implicit
	}
	if !f.closeScopeSet {
		f.closeScope, _ = f.session.outgoingMutationScope(ctx, fileRequest{Op: storage.OpFileClose})
		f.closeScope = locking.CloneScope(f.closeScope)
		f.closeScopeSet = true
	}
	scope := locking.CloneScope(f.closeScope)
	f.mu.Unlock()
	ctx = locking.WithScope(ctx, scope)
	response, err := f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: f.session.id, File: f.id, Action: storage.LockRequestID(attempt.Action), CloseGeneration: attempt.Generation, CloseImplicit: implicit})
	if wasReleased && response.CloseResult == nil {
		if err == nil {
			err = unreachable(Request{Op: OpFileControl}, errors.New("released close replay returned no result"))
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, err)
	}
	if response.Retry {
		if !implicit {
			return storage.ReferenceCloseResult{}, nil, unreachable(Request{Op: OpFileControl}, errors.New("explicit close action received an HTTP epoch retry"))
		}
		newID, idErr := storage.NewFileActionID(response.Epoch)
		if idErr != nil {
			return storage.ReferenceCloseResult{}, nil, idErr
		}
		f.mu.Lock()
		f.closeAction = storage.LockRequestID(newID)
		f.mu.Unlock()
		attempt.Action = newID
		response, err = f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: f.session.id, File: f.id, Action: storage.LockRequestID(newID), CloseGeneration: attempt.Generation, CloseImplicit: true})
		if response.Retry {
			return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
		}
	}
	var closeProof *storage.CloseActionNotExecutedError
	if errors.As(err, &closeProof) {
		f.mu.Lock()
		f.pruneCloseProofsLocked(time.Now())
		if f.closeNotExecuted == nil {
			f.closeNotExecuted = make(map[storage.CloseAttempt]remoteCloseProof)
		}
		if len(f.closeNotExecuted) >= f.session.closeActionLimit {
			f.mu.Unlock()
			return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
		}
		f.closeNotExecuted[attempt] = remoteCloseProof{epoch: closeProof.CurrentEpoch, expires: time.Now().Add(f.session.closeHistory)}
		if newExplicit {
			f.closeAction, f.closeGeneration = previousAction, previousGeneration
			f.closeDeterminedFalse, f.closeImplicit = previousDetermined, previousImplicit
			f.closeScope, f.closeScopeSet = previousScope, previousScopeSet
		}
		f.closePendingProof = nil
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, nil, err
	}
	if newExplicit && (response.CloseResult == nil || !response.CloseResult.Determined) && err != nil {
		var serverError *operationError
		if errors.As(err, &serverError) && !serverError.unknown && !serverError.recorded {
			f.mu.Lock()
			f.closePendingProof = &remoteClosePendingProof{
				attempt: attempt, previousAction: previousAction, previousGeneration: previousGeneration,
				previousDetermined: previousDetermined, previousImplicit: previousImplicit,
				previousScope: previousScope, previousScopeSet: previousScopeSet,
			}
			f.mu.Unlock()
			queryContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			resolved, proofErr := f.resolvePendingCloseProof(queryContext)
			cancel()
			if resolved {
				return storage.ReferenceCloseResult{}, nil, proofErr
			}
		}
	}
	if response.CloseResult == nil {
		return storage.ReferenceCloseResult{}, nil, err
	}
	if wasReleased && !response.CloseResult.Released {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, unreachable(Request{Op: OpFileControl}, errors.New("close replay revoked confirmed release")))
	}
	if historical {
		return response.CloseResult.storage(), response.Barrier, err
	}
	if response.CloseResult.Released {

		err = closeReplayResultError(response.CloseResult, wasReleased, previousErr, err)
		f.mu.Lock()
		f.closed = true
		f.closeBarrier = response.Barrier
		f.closeErr = err
		f.closeBarrierPending = response.CloseResult.BarrierPending
		f.mu.Unlock()
		if !response.CloseResult.BarrierPending {
			f.session.detachExplicitWrites(f.id)
		}
	} else if response.CloseResult.Determined {
		f.mu.Lock()
		f.closeDeterminedFalse = true
		f.mu.Unlock()
	}
	return response.CloseResult.storage(), response.Barrier, err
}

func (f *remoteFile) resolvePendingCloseProof(ctx context.Context) (bool, error) {
	f.mu.Lock()
	pending := f.closePendingProof
	f.mu.Unlock()
	if pending == nil {
		return false, nil
	}
	receipt, err := f.QueryCloseAttempt(ctx, pending.attempt)
	if err != nil {
		return false, err
	}
	if receipt.Outcome != storage.FileActionNotExecuted {
		f.mu.Lock()
		f.closePendingProof = nil
		f.mu.Unlock()
		return false, nil
	}
	owner, err := f.CloseOwnerStatus(ctx)
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	f.pruneCloseProofsLocked(time.Now())
	if len(f.closeNotExecuted) >= f.session.closeActionLimit {
		f.mu.Unlock()
		return false, syscall.EAGAIN
	}
	if f.closeNotExecuted == nil {
		f.closeNotExecuted = make(map[storage.CloseAttempt]remoteCloseProof)
	}
	f.closeNotExecuted[pending.attempt] = remoteCloseProof{epoch: owner.CurrentEpoch, expires: time.Now().Add(f.session.closeHistory)}
	f.closeAction, f.closeGeneration = pending.previousAction, pending.previousGeneration
	f.closeDeterminedFalse, f.closeImplicit = pending.previousDetermined, pending.previousImplicit
	f.closeScope, f.closeScopeSet = pending.previousScope, pending.previousScopeSet
	f.closePendingProof = nil
	f.mu.Unlock()
	return true, &storage.CloseActionNotExecutedError{CurrentEpoch: owner.CurrentEpoch}
}

func (f *remoteFile) pruneCloseProofsLocked(now time.Time) {
	for attempt, proof := range f.closeNotExecuted {
		if !now.Before(proof.expires) && (f.closeAction != storage.LockRequestID(attempt.Action) || f.closeGeneration != attempt.Generation) {
			delete(f.closeNotExecuted, attempt)
		}
	}
}

func (f *remoteFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if err := attempt.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	response, err := f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileQueryAction, Session: f.session.id, File: f.id, FileAction: attempt.Action, CloseGeneration: attempt.Generation})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if response.ActionReceipt == nil || response.ActionReceipt.Action != attempt.Action || response.ActionReceipt.Operation != storage.OpFileClose {
		return storage.FileActionReceipt{}, unreachable(Request{Op: OpFileControl}, errors.New("close query returned an unrelated action"))
	}
	return *response.ActionReceipt, nil
}

func (f *remoteFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	if !f.session.capabilities.CloseRecovery || !f.capabilities.CloseRecovery {
		return storage.CloseOwnerStatus{}, syscall.EOPNOTSUPP
	}
	response, err := f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileCloseOwnerStatus, Session: f.session.id, File: f.id})
	if err != nil {
		return storage.CloseOwnerStatus{}, err
	}
	if response.CloseOwnerStatus == nil {
		return storage.CloseOwnerStatus{}, unreachable(Request{Op: OpFileControl}, errors.New("close owner status is absent"))
	}
	return response.CloseOwnerStatus.storage()
}

func (f *remoteFile) closeWithResultAndBarrier(ctx context.Context) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	f.closeCallMu.Lock()
	defer f.closeCallMu.Unlock()
	f.mu.Lock()
	if f.closed && !f.closeBarrierPending {
		barrier := f.closeBarrier
		err := f.closeErr
		f.mu.Unlock()
		return storage.ReferenceCloseResult{Released: true, Determined: true}, barrier, err
	}
	wasReleased := f.closed
	previousBarrier := f.closeBarrier
	previousErr := f.closeErr
	if f.closeDeterminedFalse {
		f.closeGeneration++
		f.closeAction = ""
		f.closeDeterminedFalse = false
		f.closeImplicit = true
	}
	if f.closeAction == "" {
		if f.closeGeneration == 0 {
			f.closeGeneration = 1
		}
		f.session.mu.Lock()
		epoch := f.session.epoch
		f.session.mu.Unlock()
		var err error
		f.closeAction, err = storage.NewLockRequestID(epoch)
		if err != nil {
			f.mu.Unlock()
			return storage.ReferenceCloseResult{}, nil, err
		}
		f.closeScope, _ = f.session.outgoingMutationScope(ctx, fileRequest{Op: storage.OpFileClose})
		f.closeScope = locking.CloneScope(f.closeScope)
		f.closeScopeSet = true
		f.closeImplicit = true
	}
	action := f.closeAction
	generation := f.closeGeneration
	implicit := f.closeImplicit
	scope := locking.CloneScope(f.closeScope)
	f.mu.Unlock()
	ctx = locking.WithScope(ctx, scope)
	var r fileResponse
	var e error
	f.session.mu.Lock()
	sessionClosed := f.session.closed
	f.session.mu.Unlock()
	if wasReleased || sessionClosed {
		r, e = f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: f.session.id, File: f.id, Action: action, CloseGeneration: generation, CloseImplicit: implicit})
	} else {
		r, e = f.session.call(ctx, fileRequest{Op: storage.OpFileClose, File: f.id, Action: action, CloseGeneration: generation, CloseImplicit: implicit})
	}
	if e == nil && r.Retry && !implicit {
		return storage.ReferenceCloseResult{}, nil, unreachable(Request{Op: OpFileControl}, errors.New("explicit close action received an HTTP epoch retry"))
	}
	if e == nil && r.Retry && implicit {
		if wasReleased {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, unreachable(Request{Op: OpFileControl}, errors.New("released close lost its action receipt")))
		}
		action, e = storage.NewLockRequestID(r.Epoch)
		if e != nil {
			return storage.ReferenceCloseResult{}, nil, e
		}
		f.mu.Lock()
		f.closeAction = action
		f.mu.Unlock()
		if sessionClosed {
			r, e = f.session.storage.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: f.session.id, File: f.id, Action: action, CloseGeneration: generation, CloseImplicit: implicit})
		} else {
			r, e = f.session.call(ctx, fileRequest{Op: storage.OpFileClose, File: f.id, Action: action, CloseGeneration: generation, CloseImplicit: implicit})
		}
		if e == nil && r.Retry {
			return storage.ReferenceCloseResult{}, nil, syscall.EAGAIN
		}
	}
	if r.CloseResult != nil && r.CloseResult.Released {
		e = closeReplayResultError(r.CloseResult, wasReleased, previousErr, e)
		f.mu.Lock()
		f.closed = true
		f.closeBarrier = r.Barrier
		f.closeErr = e
		f.closeBarrierPending = r.CloseResult.BarrierPending
		f.mu.Unlock()
		if !r.CloseResult.BarrierPending {
			f.session.detachExplicitWrites(f.id)
		}
		return r.CloseResult.storage(), r.Barrier, e
	}
	if r.CloseResult != nil {
		if wasReleased {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, unreachable(Request{Op: OpFileControl}, errors.New("close replay revoked confirmed release")))
		}
		if r.CloseResult.Determined {
			f.mu.Lock()
			f.closeDeterminedFalse = true
			f.mu.Unlock()
		}
		return r.CloseResult.storage(), r.Barrier, e
	}
	if wasReleased {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, previousBarrier, closeReplayError(previousErr, e)
	}
	return storage.ReferenceCloseResult{}, nil, e
}
