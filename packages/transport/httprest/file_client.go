package httprest

import (
	"bytes"
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"net/http"
	"syscall"
	"time"
)

func fileReadOnly(op storage.Operation) bool {
	switch op {
	case storage.OpFileState, storage.OpFileStatus, storage.OpFileStatNode, storage.OpFileReference,
		storage.OpFileStat, storage.OpFileCheckObservation, storage.OpFileRead, storage.OpFileListAt,
		storage.OpFileRangeSnapshot, storage.OpFileQueryAction, storage.OpFileLookupAt:
		return true
	}
	return false
}

type remoteFileSession struct {
	storage *Storage
	id      string
}
type remoteFile struct {
	session *remoteFileSession
	id      storage.FileReferenceID
	node    uint64
}

var _ storage.FileStorage = (*Storage)(nil)
var _ FileSessionWithBarrier = (*remoteFileSession)(nil)
var _ FileWithBarrier = (*remoteFile)(nil)

func (s *Storage) CheckFileStorage() error { _, err := s.FileState(context.Background()); return err }
func (s *Storage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	response, err := s.fileCall(ctx, fileRequest{Op: storage.OpFileState})
	if err != nil {
		return storage.FileVolumeState{}, err
	}
	return *response.State, nil
}
func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	if err := options.Check(); err != nil {
		return nil, storage.FileSessionStatus{}, fileLocalFailure(err)
	}
	response, err := s.fileCall(ctx, fileRequest{Op: storage.OpFileSessionOpen, Options: &options})
	if err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	session := &remoteFileSession{storage: s, id: response.Session}
	if response.Status.Remaining <= 0 || response.Status.HistoryRemaining <= 0 {
		return session, *response.Status, unreachable(Request{Op: OpFile}, errors.New("file enrollment lifetime elapsed before confirmation"))
	}
	return session, *response.Status, nil
}
func (s *Storage) fileCall(ctx context.Context, req fileRequest) (response fileResponse, returned error) {
	dispatched := false
	defer func() {
		if returned != nil && !dispatched {
			returned = fileLocalFailure(returned)
		}
	}()

	if err := validateFileRequest(req); err != nil {
		return fileResponse{}, err
	}
	if req.Op == storage.OpFileRangeSnapshot && s.maxBodyBytes < fileRangeSnapshotResponseLimit() || req.List != nil && s.maxBodyBytes < fileDirectoryResponseLimit(*req.List) {
		return fileResponse{}, syscall.EFBIG
	}
	if req.Write != nil && int64(len(req.Write.Data)) > s.maxWriteBytes {
		return fileResponse{}, syscall.EFBIG
	}
	if req.Read != nil && int64(req.Read.Length) > fileReadLimit(s.maxBodyBytes) {
		return fileResponse{}, syscall.EFBIG
	}
	op, requestLimit, responseLimit, admission := OpFile, s.maxBodyBytes, s.maxBodyBytes, s.fileRequests
	if fileControl(req.Op) {
		op, requestLimit, responseLimit, admission = OpFileControl, fileControlRequestLimit(), fileControlResponseLimit(), s.fileControls
	}
	if fileWait(req.Op) {
		admission = s.fileWaits
	}
	if s.maxBodyBytes < fileControlResponseLimit() {
		return fileResponse{}, syscall.EFBIG
	}
	request := Request{Op: op}
	release, err := admission.acquire(ctx, retainedResponseMultiplier*responseLimit)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EFBIG) {
			return fileResponse{}, &operationError{req: request, errno: storage.ErrnoOf(err), detail: err.Error()}
		}
		return fileResponse{}, operationFailure(request, err, true)
	}
	defer release()
	body, err := marshalFileJSON(req)
	if err != nil {
		return fileResponse{}, err
	}
	if int64(len(body)) > requestLimit {
		return fileResponse{}, syscall.EFBIG
	}
	endpoint, err := request.URL(s.base)
	if err != nil {
		return fileResponse{}, err
	}
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fileResponse{}, operationFailure(request, err, true)
	}
	outgoing.Header.Set("Content-Type", contentJSON)
	outgoing.Header.Set("Accept", contentJSON)
	if fileMutation(req.Op) {
		scope := locking.ScopeFromContext(ctx)
		if !locking.HasScope(ctx) && s.scope != nil {
			scope = *s.scope
		}
		if scope.Owner != (locking.OwnerRef{}) || len(scope.Grants) > 0 {
			encoded, err := encodeMutationScope(scope)
			if err != nil {
				return fileResponse{}, err
			}
			outgoing.Header.Set(HeaderMutationScope, encoded)
		}
	}
	if err := ctx.Err(); err != nil {
		return fileResponse{}, operationFailure(request, err, true)
	}
	started := time.Now()
	dispatched = true
	incoming, err := s.http.Do(outgoing)
	if err != nil {
		return fileResponse{}, operationFailure(request, err, fileReadOnly(req.Op))
	}
	defer incoming.Body.Close()
	if incoming.Header.Get(HeaderProtocol) != Version || incoming.Header.Get("Content-Type") != contentJSON {
		return fileResponse{}, unreachable(request, errors.New("invalid file response protocol or media type"))
	}
	if incoming.StatusCode != http.StatusOK && incoming.StatusCode != StatusStorageError {
		return fileResponse{}, unreachable(request, errors.New("invalid file response status"))
	}
	content, err := readWhole(incoming, responseLimit)
	if err != nil {
		return fileResponse{}, operationFailure(request, err, fileReadOnly(req.Op))
	}
	if incoming.StatusCode == StatusStorageError {
		var failure fileErrorResponse
		if err := decodeFileJSON(content, &failure); err != nil {
			return response, unreachable(request, err)
		}
		errno, ok := storage.ErrnoByName(failure.Errno)
		if !ok || errno == 0 || len(failure.Message) > fileDiagnosticBytes {
			return response, unreachable(request, errors.New("invalid file error classification"))
		}
		if failure.NotAdmitted && failure.Receipt != nil {
			return response, unreachable(request, errors.New("admission refusal carries an action receipt"))
		}
		if failure.Receipt != nil {
			if !fileActionRequired(req.Op) {
				return response, unreachable(request, errors.New("unsolicited action receipt"))
			}
			if err := validateFileReceipt(req, *failure.Receipt); err != nil {
				return response, unreachable(request, err)
			}
		}
		if err := validateFileConflict(failure.Conflict); err != nil {
			return fileResponse{}, unreachable(request, err)
		}
		response.Receipt = failure.Receipt
		response.Barrier = failure.Barrier
		var classified error = &operationError{req: request, errno: errno, detail: failure.Message}
		if failure.LockCode != nil || failure.Recorded != nil {
			if failure.LockCode == nil || failure.Recorded == nil || !validLockCode(*failure.LockCode) || locking.Errno(*failure.LockCode) != errno || failure.Conflict != nil {
				return fileResponse{}, unreachable(request, errors.New("invalid strong lock failure envelope"))
			}
			classified = &locking.Error{Code: *failure.LockCode, Recorded: *failure.Recorded, Message: failure.Message}
		}

		adjustFileLifetime(&response, time.Since(started))
		return response, &storage.FileError{Code: errno, Conflict: failure.Conflict.storage(), NotAdmitted: failure.NotAdmitted, Cause: classified}
	}
	if err := decodeFileJSON(content, &response); err != nil {
		return fileResponse{}, unreachable(request, err)
	}
	if err := validateFileResponse(req, response); err != nil {
		return fileResponse{}, unreachable(request, err)
	}
	if response.Receipt != nil && response.Receipt.Errno != "" {
		return fileResponse{}, unreachable(request, errors.New("failed receipt arrived as successful response"))
	}
	adjustFileLifetime(&response, time.Since(started))
	return response, nil
}
func adjustFileLifetime(r *fileResponse, elapsed time.Duration) {
	if r.Status != nil {
		r.Status.Remaining = maxDuration(r.Status.Remaining - elapsed)
		r.Status.HistoryRemaining = maxDuration(r.Status.HistoryRemaining - elapsed)
	}
	if r.Receipt != nil {
		r.Receipt.HistoryRemaining = int64(maxDuration(time.Duration(r.Receipt.HistoryRemaining) - elapsed))
	}
}
func (s *remoteFileSession) call(ctx context.Context, r fileRequest) (fileResponse, error) {
	r.Session = s.id
	return s.storage.fileCall(ctx, r)
}
func (f *remoteFile) call(ctx context.Context, r fileRequest) (fileResponse, error) {
	r.Reference = f.id
	response, err := f.session.call(ctx, r)
	if response.Observation != nil && response.Observation.Attr.ID != f.node || response.Page != nil && response.Page.ParentID != f.node || response.Lookup != nil && response.Lookup.ParentID != f.node {
		return fileResponse{}, unreachable(Request{Op: OpFile}, errors.New("file response changed the retained node identity"))
	}
	return response, err
}
func fileResult(r fileResponse, err error) (storage.FileActionReceipt, *MutationBarrier, error) {
	if r.Receipt == nil {
		return storage.FileActionReceipt{}, r.Barrier, err
	}
	receipt, decodeErr := r.Receipt.storage()
	if decodeErr != nil {
		return storage.FileActionReceipt{}, r.Barrier, decodeErr
	}
	return receipt, r.Barrier, err
}
func (s *remoteFileSession) action(ctx context.Context, r fileRequest) (storage.FileActionReceipt, *MutationBarrier, error) {
	return s.actionForNode(ctx, r, 0)
}
func (s *remoteFileSession) actionForNode(ctx context.Context, r fileRequest, node uint64) (storage.FileActionReceipt, *MutationBarrier, error) {
	response, err := s.call(ctx, r)
	if identityErr := fileReceiptNode(response.Receipt, node); identityErr != nil {
		response = fileResponse{}
		err = unreachable(Request{Op: OpFile}, identityErr)
	}
	var failed *operationError
	if err != nil && response.Receipt == nil && r.Action != "" && errors.As(err, &failed) && failed.unknown && !storage.IsFileCallNotAdmitted(err) {
		operation := r.Op
		if r.Op == storage.OpFileQueryAction || r.Op == storage.OpFileCancelAction {
			operation = ""
		}
		return storage.FileActionReceipt{Action: r.Action, Operation: operation, State: storage.FileActionUnknown, Errno: syscall.EIO}, nil, err
	}
	return fileResult(response, err)
}
func (f *remoteFile) action(ctx context.Context, r fileRequest) (storage.FileActionReceipt, *MutationBarrier, error) {
	r.Reference = f.id
	return f.session.actionForNode(ctx, r, f.node)
}
func fileReceiptNode(receipt *fileReceipt, node uint64) error {
	if node != 0 && receipt != nil && receipt.Observation != nil && receipt.Observation.Attr.ID != node {
		return errors.New("receipt changed the retained node identity")
	}
	return nil
}

func (s *remoteFileSession) Retain(ctx context.Context, value storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.RetainWithBarrier(ctx, value, id)
	return r, err
}
func (s *remoteFileSession) RetainWithBarrier(ctx context.Context, value storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := fileRetainOf(value)
	return s.action(ctx, fileRequest{Op: storage.OpFileRetain, Action: id, Retain: payload})
}

func (s *remoteFileSession) RetainAt(ctx context.Context, value storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.RetainAtWithBarrier(ctx, value, id)
	return r, err
}
func (s *remoteFileSession) RetainAtWithBarrier(ctx context.Context, value storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := fileRetainAtOf(value)
	return s.action(ctx, fileRequest{Op: storage.OpFileRetainAt, Action: id, RetainAt: payload})
}

func (s *remoteFileSession) CreateAndRetainAt(ctx context.Context, value storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.CreateAndRetainAtWithBarrier(ctx, value, id)
	return r, err
}
func (s *remoteFileSession) CreateAndRetainAtWithBarrier(ctx context.Context, value storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := fileCreateOf(value)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return s.action(ctx, fileRequest{Op: storage.OpFileCreateAndRetainAt, Action: id, Create: payload})
}

func (s *remoteFileSession) ReplaceAndRetainAt(ctx context.Context, value storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.ReplaceAndRetainAtWithBarrier(ctx, value, id)
	return r, err
}
func (s *remoteFileSession) ReplaceAndRetainAtWithBarrier(ctx context.Context, value storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := fileCreateOf(value)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return s.action(ctx, fileRequest{Op: storage.OpFileReplaceAndRetainAt, Action: id, Create: payload})
}

func (s *remoteFileSession) ResetAndRetainAt(ctx context.Context, value storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.ResetAndRetainAtWithBarrier(ctx, value, id)
	return r, err
}
func (s *remoteFileSession) ResetAndRetainAtWithBarrier(ctx context.Context, value storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := fileResetOf(value)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return s.action(ctx, fileRequest{Op: storage.OpFileResetAndRetainAt, Action: id, Reset: payload})
}

func (f *remoteFile) WriteAt(ctx context.Context, value storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.WriteAtWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) WriteAtWithBarrier(ctx context.Context, value storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &fileWriteRequest{Offset: value.Offset, Data: wireBytes(value.Data), Owner: value.Owner, ExpectedSize: value.ExpectedSize}
	return f.action(ctx, fileRequest{Op: storage.OpFileWrite, Action: id, Write: payload})
}

func (f *remoteFile) Truncate(ctx context.Context, value storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.TruncateWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) TruncateWithBarrier(ctx context.Context, value storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &fileTruncateRequest{Size: value.Size, Owner: value.Owner}
	return f.action(ctx, fileRequest{Op: storage.OpFileTruncate, Action: id, Truncate: payload})
}

func (f *remoteFile) SetAttr(ctx context.Context, value storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.SetAttrWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) SetAttrWithBarrier(ctx context.Context, value storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := AttrChangeOf(value)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return f.action(ctx, fileRequest{Op: storage.OpFileSetAttr, Action: id, Change: payload})
}

func (f *remoteFile) SetKind(ctx context.Context, value storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.SetKindWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) SetKindWithBarrier(ctx context.Context, value storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := fileKindOf(value)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return f.action(ctx, fileRequest{Op: storage.OpFileSetKind, Action: id, Kind: payload})
}

func (f *remoteFile) Rename(ctx context.Context, value storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.RenameWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) RenameWithBarrier(ctx context.Context, value storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := fileRenameOf(value)
	return f.action(ctx, fileRequest{Op: storage.OpFileRename, Action: id, Rename: payload})
}

func (f *remoteFile) PrepareRemoval(ctx context.Context, value storage.PrepareRemovalRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.PrepareRemovalWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) PrepareRemovalWithBarrier(ctx context.Context, value storage.PrepareRemovalRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &value
	return f.action(ctx, fileRequest{Op: storage.OpFilePrepareRemoval, Action: id, Prepare: payload})
}

func (f *remoteFile) CancelPrepared(ctx context.Context, value storage.RemovalIntentID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.CancelPreparedWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) CancelPreparedWithBarrier(ctx context.Context, value storage.RemovalIntentID, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &value
	return f.action(ctx, fileRequest{Op: storage.OpFileCancelPrepared, Action: id, Intent: payload})
}

func (f *remoteFile) DrainEntry(ctx context.Context, value storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.DrainEntryWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) DrainEntryWithBarrier(ctx context.Context, value storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &value
	return f.action(ctx, fileRequest{Op: storage.OpFileDrainEntry, Action: id, Drain: payload})
}

func (f *remoteFile) CancelDrain(ctx context.Context, value storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.CancelDrainWithBarrier(ctx, value, id)
	return r, err
}
func (f *remoteFile) CancelDrainWithBarrier(ctx context.Context, value storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload := &value
	return f.action(ctx, fileRequest{Op: storage.OpFileCancelDrain, Action: id, CancelDrain: payload})
}

func (s *remoteFileSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.QueryActionWithBarrier(ctx, id)
	return r, err
}
func (s *remoteFileSession) QueryActionWithBarrier(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	return s.action(ctx, fileRequest{Op: storage.OpFileQueryAction, Action: id})
}

func (s *remoteFileSession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.CancelActionWithBarrier(ctx, id)
	return r, err
}
func (s *remoteFileSession) CancelActionWithBarrier(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	return s.action(ctx, fileRequest{Op: storage.OpFileCancelAction, Action: id})
}

func (s *remoteFileSession) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.CloseWithBarrier(ctx, id)
	return r, err
}
func (s *remoteFileSession) CloseWithBarrier(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	return s.action(ctx, fileRequest{Op: storage.OpFileSessionClose, Action: id})
}

func (s *remoteFileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	r, err := s.call(ctx, fileRequest{Op: storage.OpFileStatus})
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	return *r.Status, nil
}

func (s *remoteFileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	r, err := s.call(ctx, fileRequest{Op: storage.OpFileRenew})
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	return *r.Status, nil
}

func (f *remoteFile) ReplaceClaim(ctx context.Context, value storage.AccessClaim, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.action(ctx, fileRequest{Op: storage.OpFileReplaceClaim, Action: id, Claim: &value})
	return r, err
}

func (f *remoteFile) ReplaceRanges(ctx context.Context, value storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.action(ctx, fileRequest{Op: storage.OpFileReplaceRanges, Action: id, Ranges: &value})
	return r, err
}

func (f *remoteFile) WaitRanges(ctx context.Context, value storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.action(ctx, fileRequest{Op: storage.OpFileWaitRanges, Action: id, Wait: &value})
	return r, err
}
func (s *remoteFileSession) SetNodeAttr(ctx context.Context, node uint64, change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.SetNodeAttrWithBarrier(ctx, node, change, id)
	return r, err
}
func (s *remoteFileSession) SetNodeAttrWithBarrier(ctx context.Context, node uint64, change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	payload, err := AttrChangeOf(change)
	if err != nil {
		return storage.FileActionReceipt{}, nil, fileLocalFailure(err)
	}
	return s.action(ctx, fileRequest{Op: storage.OpFileSetNodeAttr, Node: node, Change: payload, Action: id})
}
func (s *remoteFileSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileReference, Reference: id})
	if err != nil {
		return nil, err
	}
	return &remoteFile{session: s, id: id, node: response.Node}, nil
}
func (s *remoteFileSession) StatNode(ctx context.Context, node uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	r, err := s.call(ctx, fileRequest{Op: storage.OpFileStatNode, Node: node, Observation: &options})
	if err != nil {
		return storage.FileObservation{}, err
	}
	return r.Observation.storage()
}
func (s *remoteFileSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := s.action(ctx, fileRequest{Op: storage.OpFileRetireRangeOwner, Owner: &owner, Action: id})
	return r, err
}
func (f *remoteFile) Reference() storage.FileReferenceID { return f.id }
func (f *remoteFile) NodeID() uint64                     { return f.node }
func (f *remoteFile) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileStat, Observation: &options})
	if err != nil {
		return storage.FileObservation{}, err
	}
	return r.Observation.storage()
}
func (f *remoteFile) CheckObservation(ctx context.Context, condition storage.ObservationCondition) (storage.FileObservation, error) {
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileCheckObservation, Check: &condition})
	if err != nil {
		return storage.FileObservation{}, err
	}
	return r.Observation.storage()
}
func (f *remoteFile) ReadAt(ctx context.Context, request storage.FileReadRequest) (storage.FileRead, error) {
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileRead, Read: &fileReadRequest{Offset: request.Offset, Length: request.Length, Owner: request.Owner}})
	if err != nil {
		return storage.FileRead{}, err
	}
	observation, err := r.Observation.storage()
	if err != nil {
		return storage.FileRead{}, err
	}
	return storage.FileRead{Attr: observation.Attr, Data: r.Data}, nil
}
func (f *remoteFile) ListAt(ctx context.Context, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileListAt, List: &request})
	if err != nil {
		return storage.DirectoryPage{}, err
	}
	return r.Page.storage()
}
func (f *remoteFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileRangeSnapshot, Owner: &owner, Scope: &scope})
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	return *r.Ranges, nil
}
func (f *remoteFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.action(ctx, fileRequest{Op: storage.OpFileRetireRanges, Owner: &owner, Scope: &scope, Action: id})
	return r, err
}
func (f *remoteFile) Sync(ctx context.Context) error {
	_, err := f.call(ctx, fileRequest{Op: storage.OpFileSync})
	return err
}
func (f *remoteFile) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	r, _, err := f.CloseWithBarrier(ctx, id)
	return r, err
}
func (f *remoteFile) CloseWithBarrier(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error) {
	return f.action(ctx, fileRequest{Op: storage.OpFileClose, Action: id})
}

func (f *remoteFile) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileLookupAt, Name: name})
	if err != nil {
		return storage.EntryLookup{}, err
	}
	return response.Lookup.storage(name)
}

func fileLocalFailure(err error) error {
	return &storage.FileError{Code: storage.ErrnoOf(err), NotAdmitted: true, Cause: err}
}
