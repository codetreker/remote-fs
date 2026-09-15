package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.WindowsStorage = (*Storage)(nil)
var _ storage.WindowsSession = (*remoteWindowsSession)(nil)
var _ storage.WindowsFile = (*remoteWindowsFile)(nil)

type remoteWindowsSession struct {
	storage *Storage
	id      string
	mu      sync.Mutex
	closed  bool
}

type remoteWindowsFile struct {
	session *remoteWindowsSession
	id      string
	node    uint64
}

func (s *Storage) CheckWindowsStorage() error {
	_, err := s.WindowsState(context.Background())
	return err
}

func (s *Storage) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	response, err := s.windowsCall(ctx, windowsRequest{Op: storage.OpWindowsState})
	if err != nil {
		return storage.WindowsState{}, err
	}
	required, err := maxChangeFrameBytes(response.State.MaxEventBytes)
	if err != nil {
		return storage.WindowsState{}, unreachable(Request{Op: OpWindowsControl}, err)
	}
	if s.maxFrameBytes < required || s.maxBodyBytes < windowsControlResponseLimit() {
		return storage.WindowsState{}, syscall.EFBIG
	}
	return *response.State, nil
}

func (s *Storage) EnableWindows(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	return s.windowsActivation(ctx, storage.OpWindowsEnable, id)
}
func (s *Storage) QueryWindowsActivation(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	return s.windowsActivation(ctx, storage.OpWindowsQueryActivation, id)
}
func (s *Storage) windowsActivation(ctx context.Context, op storage.Operation, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	response, err := s.windowsCall(ctx, windowsRequest{Op: op, Action: id})
	if response.Activation == nil {
		return storage.WindowsActivation{}, err
	}
	a := response.Activation
	errno, _ := windowsErrno(a.Errno)
	return storage.WindowsActivation{Action: a.Action, State: a.State, Enabled: a.Enabled, Errno: errno, HistoryRemaining: a.HistoryRemaining}, err
}

func (s *Storage) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	response, err := s.windowsCall(ctx, windowsRequest{Op: storage.OpWindowsSessionOpen, Options: &options})
	if err != nil {
		return nil, err
	}
	if response.Status.Fenced || response.Status.Retired || response.Status.Remaining <= 0 {
		return nil, unreachable(Request{Op: OpWindowsControl}, errors.New("Windows enrollment returned no usable lifetime"))
	}
	return &remoteWindowsSession{storage: s, id: response.Session}, nil
}

func (s *remoteWindowsSession) call(ctx context.Context, r windowsRequest) (windowsResponse, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return windowsResponse{}, syscall.ESTALE
	}
	r.Session = s.id
	return s.storage.windowsCall(ctx, r)
}
func (s *remoteWindowsSession) Open(ctx context.Context, request storage.WindowsOpenRequest, id storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	if err := request.Check(); err != nil {
		return storage.WindowsOpenResult{}, err
	}
	response, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsOpen, Open: windowsOpenOf(request), Action: id})
	if err != nil {
		return storage.WindowsOpenResult{}, err
	}
	attr := response.Attr.storage()
	return storage.WindowsOpenResult{File: &remoteWindowsFile{session: s, id: response.File, node: attr.ID}, Attr: attr, CreateAction: response.CreateAction}, nil
}
func (s *remoteWindowsSession) QueryAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	response, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsQueryAction, Action: id})
	return s.actionResult(response, err)
}
func (s *remoteWindowsSession) CancelAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	response, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsCancelAction, Action: id})
	return s.actionResult(response, err)
}
func (s *remoteWindowsSession) actionResult(response windowsResponse, err error) (storage.WindowsActionResult, error) {
	if response.Action == nil {
		return storage.WindowsActionResult{}, err
	}
	a := response.Action
	errno, _ := windowsErrno(a.Errno)
	result := storage.WindowsActionResult{Action: a.Action, State: a.State, CreateAction: a.CreateAction, Errno: errno, Failure: a.Failure, Applied: a.Applied, HistoryRemaining: a.HistoryRemaining}
	if a.Symlink != nil {
		result.Symlink = &storage.WindowsSymlinkInfo{Target: a.Symlink.Target, Location: a.Symlink.Location, Unparsed: a.Symlink.Unparsed}
	}
	if a.Attr != nil {
		result.Attr = a.Attr.storage()
	}
	if a.File != "" {
		result.File = &remoteWindowsFile{session: s, id: a.File, node: result.Attr.ID}
	}
	return result, err
}
func (s *remoteWindowsSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	response, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsRenew})
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	return *response.Status, nil
}
func (s *remoteWindowsSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	response, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsStatus})
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	return *response.Status, nil
}
func (s *remoteWindowsSession) Close(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}
	_, err := s.call(ctx, windowsRequest{Op: storage.OpWindowsSessionClose})
	if err == nil {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
	return err
}

func (f *remoteWindowsFile) Reference() string { return f.id }
func (f *remoteWindowsFile) call(ctx context.Context, r windowsRequest) (windowsResponse, error) {
	r.File = f.id
	response, err := f.session.call(ctx, r)
	if response.Attr != nil && response.Attr.Basic.Attr.ID != f.node || response.ReadAttr != nil && response.ReadAttr.ID != f.node || response.Action != nil && response.Action.Attr != nil && response.Action.Attr.Basic.Attr.ID != f.node {
		return windowsResponse{}, unreachable(Request{Op: OpWindows}, errors.New("Windows response changed the retained object identity"))
	}
	return response, err
}
func (f *remoteWindowsFile) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	response, err := f.call(ctx, windowsRequest{Op: storage.OpWindowsStat})
	if err != nil {
		return storage.WindowsAttr{}, err
	}
	return response.Attr.storage(), nil
}
func (f *remoteWindowsFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	response, err := f.call(ctx, windowsRequest{Op: storage.OpWindowsRead, Offset: offset, Length: length})
	if err != nil {
		return storage.FileRead{}, err
	}
	return storage.FileRead{Attr: response.ReadAttr.Storage(), Data: response.Data}, nil
}
func (f *remoteWindowsFile) action(ctx context.Context, r windowsRequest) (storage.WindowsActionResult, error) {
	response, err := f.call(ctx, r)
	return f.session.actionResult(response, err)
}
func (f *remoteWindowsFile) WriteAt(ctx context.Context, offset int64, data []byte, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsWrite, Offset: offset, Data: data, Action: id})
}
func (f *remoteWindowsFile) Truncate(ctx context.Context, size int64, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsTruncate, Offset: size, Action: id})
}
func (f *remoteWindowsFile) SetAttr(ctx context.Context, change storage.WindowsAttrChange, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := change.Check(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsSetAttr, Change: windowsAttrChangeOf(change), Action: id})
}
func (f *remoteWindowsFile) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	response, err := f.call(ctx, windowsRequest{Op: storage.OpWindowsReadLink})
	if err != nil {
		return storage.WindowsSymlinkInfo{}, err
	}
	return storage.WindowsSymlinkInfo{Target: response.Symlink.Target, Location: response.Symlink.Location}, nil
}
func (f *remoteWindowsFile) SetLink(ctx context.Context, target string, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := storage.CheckWindowsLinkTarget(target); err != nil {
		return storage.WindowsActionResult{}, err
	}
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsSetLink, Target: target, Action: id})
}

func (f *remoteWindowsFile) Rename(ctx context.Context, request storage.WindowsRenameRequest, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := request.Check(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsRename, Rename: &request, Action: id})
}
func (f *remoteWindowsFile) SetDeletePending(ctx context.Context, pending bool, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsSetDeletePending, DeletePending: pending, Action: id})
}
func (f *remoteWindowsFile) LockBatch(ctx context.Context, batch storage.WindowsLockBatch, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := batch.Check(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsLockBatch, Ranges: batch.Ranges, Action: id})
}
func (f *remoteWindowsFile) Sync(ctx context.Context) error {
	_, err := f.call(ctx, windowsRequest{Op: storage.OpWindowsSync})
	return err
}
func (f *remoteWindowsFile) Close(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.action(ctx, windowsRequest{Op: storage.OpWindowsClose, Action: id})
}
func (f *remoteWindowsFile) ListBounded(ctx context.Context, result *storage.WindowsListResult) error {
	if result == nil {
		return syscall.EINVAL
	}
	response, err := f.call(ctx, windowsRequest{Op: storage.OpWindowsList})
	if err != nil {
		return result.Fail(err)
	}
	defer response.bodyRelease()
	for _, entry := range response.Entries {
		if err := result.Add(storage.WindowsEntry{Name: entry.Name, Attr: entry.Attr.storage()}); err != nil {
			return result.Fail(err)
		}
	}
	return nil
}

func (s *Storage) windowsCall(ctx context.Context, req windowsRequest) (windowsResponse, error) {
	if req.Data == nil {
		req.Data = []byte{}
	}
	if req.Ranges == nil {
		req.Ranges = []storage.WindowsLockRange{}
	}
	maximum := storage.DefaultFileSessionOptions()
	if req.Options != nil {
		maximum = *req.Options
	}
	if err := validateWindowsRequest(req, maximum); err != nil {
		return windowsResponse{}, err
	}
	if int64(len(req.Data)) > s.maxWriteBytes {
		return windowsResponse{}, syscall.EFBIG
	}
	if req.Op == storage.OpWindowsRead && int64(req.Length) > windowsReadLimit(s.maxBodyBytes) {
		return windowsResponse{}, syscall.EFBIG
	}
	op, requestLimit, responseLimit, admission := OpWindows, s.maxBodyBytes, s.maxBodyBytes, s.fileRequests
	if windowsControl(req.Op) {
		op, requestLimit, responseLimit, admission = OpWindowsControl, DefaultMaxLockControlBytes, windowsControlResponseLimit(), s.windowsControls
	}
	if s.maxBodyBytes < windowsControlResponseLimit() {
		return windowsResponse{}, syscall.EFBIG
	}
	request := Request{Op: op}
	release, err := admission.acquire(ctx, retainedResponseMultiplier*responseLimit)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) {
			return windowsResponse{}, &operationError{req: request, errno: syscall.EAGAIN, detail: err.Error()}
		}
		return windowsResponse{}, windowsExchangeFailure(request, err, true)
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	body, err := json.Marshal(req)
	if err != nil {
		return windowsResponse{}, err
	}
	if int64(len(body)) > requestLimit {
		return windowsResponse{}, syscall.EFBIG
	}
	endpoint, err := request.URL(s.base)
	if err != nil {
		return windowsResponse{}, err
	}
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return windowsResponse{}, windowsExchangeFailure(request, err, true)
	}
	outgoing.Header.Set("Content-Type", contentJSON)
	outgoing.Header.Set("Accept", contentJSON)
	if windowsMutation(req.Op) {
		scope := locking.ScopeFromContext(ctx)
		if !locking.HasScope(ctx) && s.scope != nil {
			scope = *s.scope
		}
		if scope.Owner != (locking.OwnerRef{}) || len(scope.Grants) != 0 {
			encoded, err := encodeMutationScope(scope)
			if err != nil {
				return windowsResponse{}, err
			}
			outgoing.Header.Set(HeaderMutationScope, encoded)
		}
	}
	if err := ctx.Err(); err != nil {
		return windowsResponse{}, windowsExchangeFailure(request, err, true)
	}
	started := time.Now()
	incoming, err := s.http.Do(outgoing)
	if err != nil {
		return windowsResponse{}, windowsExchangeFailure(request, err, windowsReadOnly(req.Op))
	}
	defer incoming.Body.Close()
	if incoming.Header.Get(HeaderProtocol) != Version || incoming.Header.Get("Content-Type") != contentJSON {
		return windowsResponse{}, unreachable(request, errors.New("Windows response protocol or media type is invalid"))
	}
	if incoming.StatusCode != http.StatusOK && incoming.StatusCode != StatusStorageError {
		return windowsResponse{}, unreachable(request, errors.New("Windows response status is invalid"))
	}
	content, err := readWhole(incoming, responseLimit)
	if err != nil {
		return windowsResponse{}, windowsExchangeFailure(request, err, windowsReadOnly(req.Op))
	}
	response := windowsResponse{}
	if incoming.StatusCode == StatusStorageError {
		var failure windowsErrorResponse
		if err := decodeWindowsJSON(content, &failure); err != nil {
			return response, unreachable(request, err)
		}
		errno, err := windowsErrno(failure.Errno)
		if err != nil || errno == 0 || !validWindowsFailure(failure.Failure) || len(failure.Message) > windowsDiagnosticBytes {
			return response, unreachable(request, errors.New("Windows error response has an invalid classification"))
		}
		if failure.Action != nil {
			if !windowsActionRequired(req.Op) || failure.Action.Action != req.Action {
				return response, unreachable(request, errors.New("Windows error carries an unrelated action"))
			}
			if err := validateWindowsAction(*failure.Action); err != nil {
				return response, unreachable(request, err)
			}
			if err := validateWindowsBatchAction(req, failure.Action); err != nil {
				return windowsResponse{}, unreachable(request, err)
			}
			response.Action = failure.Action
		}
		if failure.Activation != nil {
			if req.Op != storage.OpWindowsEnable && req.Op != storage.OpWindowsQueryActivation {
				return windowsResponse{}, unreachable(request, errors.New("Windows error carries an unrelated activation"))
			}
			if err := validateWindowsActivation(req.Action, failure.Activation); err != nil {
				return windowsResponse{}, unreachable(request, err)
			}
			response.Activation = failure.Activation
		}
		var result error = &operationError{req: request, errno: errno, detail: failure.Message}
		if failure.Failure != "" {
			result = &storage.WindowsError{Failure: failure.Failure, Err: result}
		}
		if failure.Symlink != nil {
			if errno != syscall.ELOOP || failure.Failure != "" || validateWindowsSymlink(failure.Symlink) != nil {
				return windowsResponse{}, unreachable(request, errors.New("Windows symlink response is invalid"))
			}
			result = &storage.WindowsSymlinkError{WindowsSymlinkInfo: storage.WindowsSymlinkInfo{Target: failure.Symlink.Target, Location: failure.Symlink.Location, Unparsed: failure.Symlink.Unparsed}, Err: result}
		}
		adjustWindowsLifetime(&response, time.Since(started))
		return response, result
	}
	if err := decodeWindowsJSON(content, &response); err != nil {
		return windowsResponse{}, unreachable(request, err)
	}
	if err := validateWindowsResponse(req, response); err != nil {
		return windowsResponse{}, unreachable(request, err)
	}
	if response.Action != nil && response.Action.Errno != "" || response.Activation != nil && response.Activation.Errno != "" {
		return windowsResponse{}, unreachable(request, errors.New("Windows failed action arrived as a successful response"))
	}
	adjustWindowsLifetime(&response, time.Since(started))
	if req.Op == storage.OpWindowsList {
		response.bodyRelease = release
		retained = true
	}
	return response, nil
}

func adjustWindowsLifetime(response *windowsResponse, elapsed time.Duration) {
	if response.Status != nil {
		response.Status.Remaining = maxDuration(response.Status.Remaining - elapsed)
		response.Status.HistoryRemaining = maxDuration(response.Status.HistoryRemaining - elapsed)
	}
	if response.Action != nil {
		response.Action.HistoryRemaining = maxDuration(response.Action.HistoryRemaining - elapsed)
	}
	if response.Activation != nil {
		response.Activation.HistoryRemaining = maxDuration(response.Activation.HistoryRemaining - elapsed)
	}
}

func windowsExchangeFailure(request Request, cause error, interruptible bool) error {
	failure := operationFailure(request, cause, interruptible).(*operationError)
	failure.detail = "Windows exchange failed"
	return failure
}
