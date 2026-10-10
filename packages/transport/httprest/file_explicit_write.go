package httprest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type explicitFileWrite struct {
	mu       sync.Mutex
	request  fileRequest
	scope    locking.MutationScope
	hasScope bool
	pending  bool
	unknown  error
	detached bool
}

func isExplicitFileWrite(req fileRequest) bool {
	return req.Op == storage.OpFileMutate && req.Mutation != nil && req.Mutation.Action != "" && len(req.Mutation.Data) <= MaxFileRecoveryDataBytes && (req.Mutation.Kind == storage.MutateWriteAt || req.Mutation.Kind == storage.MutateAppend)
}
func (s *remoteFileSession) writeEntry(req fileRequest, scope locking.MutationScope, hasScope bool) (*explicitFileWrite, error) {
	key := req.File + ":" + string(req.Mutation.Action)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, syscall.ESTALE
	}
	if s.failed != nil {
		return nil, s.failed
	}
	if previous := s.explicitWrites[key]; previous != nil {
		if !reflect.DeepEqual(previous.request, req) || previous.hasScope != hasScope || hasScope && !reflect.DeepEqual(previous.scope, scope) {
			return nil, syscall.EINVAL
		}
		return previous, nil
	}
	limit := s.pendingLimit
	if limit <= 0 {
		limit = storage.DefaultFileSessionOptions().MaxLockActions
	}
	if len(s.explicitWrites)+s.dataPendingLocked()+s.inflight >= limit {
		return nil, syscall.EAGAIN
	}
	if s.explicitWrites == nil {
		s.explicitWrites = make(map[string]*explicitFileWrite)
	}
	entry := &explicitFileWrite{request: req, scope: locking.CloneScope(scope), hasScope: hasScope}
	s.explicitWrites[key] = entry
	return entry, nil
}
func (s *remoteFileSession) callExplicitWrite(ctx context.Context, req fileRequest) (fileResponse, error) {
	req.ResultBytes = min(req.ResultBytes, MaxFileRecoveryBytes)
	if req.ResultBytes == 0 {
		req.ResultBytes = min(s.storage.maxBodyBytes, MaxFileRecoveryBytes)
	}
	admitted, _ := ctx.Value(explicitWriteAdmissionKey{}).(explicitWriteAdmission)
	recovery := admitted.recovery
	if admitted.session != s || admitted.file != req.File || admitted.action != req.Mutation.Action {
		var release func()
		var err error
		ctx, release, recovery, err = s.admitExplicitWrite(ctx, req.File, req.Mutation.Action)
		if err != nil {
			return fileResponse{}, err
		}
		defer release()
	}
	frozen, err := freezeFileRequest(req)
	if err != nil {
		return fileResponse{}, unreachable(Request{Op: OpFile}, err)
	}
	req = frozen
	if req.Action != "" && req.Action != storage.LockRequestID(req.Mutation.Action) {
		return fileResponse{}, syscall.EINVAL
	}
	req.Action = storage.LockRequestID(req.Mutation.Action)
	req.Session = s.id
	scope, hasScope := s.outgoingMutationScope(ctx, req)
	entry, err := s.writeEntry(req, scope, hasScope)
	if err != nil {
		return fileResponse{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	s.mu.Lock()
	stale := s.closed || entry.detached
	s.mu.Unlock()
	if stale {
		return fileResponse{}, syscall.ESTALE
	}
	callCtx := ctx
	if entry.hasScope {
		callCtx = locking.WithScope(callCtx, entry.scope)
	}
	if entry.pending && recovery {
		callCtx = context.WithValue(callCtx, fileRecoveryKey{}, true)
	}
	response, err := s.storage.fileCall(callCtx, entry.request)
	var failure *operationError
	unknown := err != nil && errors.As(err, &failure) && failure.unknown && !recordedFileOutcome(err)
	if unknown {
		entry.pending = true
		if entry.unknown == nil {
			entry.unknown = err
		}
		return response, err
	}
	if entry.pending && err != nil && !recordedFileOutcome(err) {
		return response, err
	}
	if err == nil && response.Retry {
		return response, unreachable(Request{Op: OpFileRecovery}, errors.New("explicit mutation received an epoch retry"))
	}
	s.mu.Lock()
	delete(s.explicitWrites, req.File+":"+string(req.Mutation.Action))
	if response.Epoch > s.epoch {
		s.epoch = response.Epoch
	}
	s.mu.Unlock()
	if err == nil && s.capabilities.Allocation {
		if checkErr := checkReportedAllocation(response); checkErr != nil {
			return response, unreachable(Request{Op: OpFile}, checkErr)
		}
	}
	return response, err
}

// Retirement first fences the exact reference. Waiting on action gates happens
// outside the session map lock; a released backing cannot publish another write.
func (s *remoteFileSession) detachExplicitWrites(file string) {
	s.mu.Lock()
	entries := make(map[string]*explicitFileWrite)
	for key, entry := range s.explicitWrites {
		if file == "" || entry.request.File == file {
			entries[key] = entry
		}
	}
	s.mu.Unlock()
	for key, entry := range entries {
		entry.mu.Lock()
		entry.detached = true
		s.mu.Lock()
		if s.explicitWrites[key] == entry {
			delete(s.explicitWrites, key)
		}
		s.mu.Unlock()
		entry.mu.Unlock()
	}
}

type fileRecoveryKey struct{}

func cleanupFileAction(op storage.Operation) bool {
	return op == storage.OpFileRangeDrop || op == storage.OpFileRetireUseOwner || op == storage.OpFileClose || op == storage.OpFileSessionClose
}
func (s *remoteFileSession) dataPendingLocked() int {
	count := 0
	for _, pending := range s.pending {
		if !cleanupFileAction(pending.request.Op) {
			count++
		}
	}
	return count
}
func (s *remoteFileSession) cleanupPendingLocked() int { return len(s.pending) - s.dataPendingLocked() }

type explicitWriteAdmissionKey struct{}
type explicitWriteAdmission struct {
	session  *remoteFileSession
	file     string
	action   storage.FileActionID
	recovery bool
}

func (s *remoteFileSession) admitExplicitWrite(ctx context.Context, file string, action storage.FileActionID) (context.Context, func(), bool, error) {
	s.mu.Lock()
	recovery := s.explicitWrites[file+":"+string(action)] != nil
	if recovery && s.recoveryAdmission == nil {
		s.recoveryAdmission = newBodyAdmission(1, retainedResponseMultiplier*min(s.storage.maxBodyBytes, MaxFileRecoveryBytes), 0)
	}
	lane := s.storage.fileRequests
	reservation := retainedResponseMultiplier * s.storage.maxBodyBytes
	if recovery {
		lane = s.recoveryAdmission
		reservation = retainedResponseMultiplier * min(s.storage.maxBodyBytes, MaxFileRecoveryBytes)
	}
	s.mu.Unlock()
	release, err := lane.acquire(ctx, reservation)
	if err != nil {
		return ctx, nil, false, err
	}
	if !recovery {
		ctx = context.WithValue(ctx, fileRequestAdmissionKey{}, fileRequestAdmission{storage: s.storage})
	}
	ctx = context.WithValue(ctx, explicitWriteAdmissionKey{}, explicitWriteAdmission{session: s, file: file, action: action, recovery: recovery})
	return ctx, release, recovery, nil
}
