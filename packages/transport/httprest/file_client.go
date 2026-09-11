package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"sync"
	"syscall"
	"time"
)

type fileScopeKey struct{}
type fileReadOnlyKey struct{}

func fileReadOnly(op storage.Operation) bool {
	switch op {
	case storage.OpFileRead, storage.OpFileStat, storage.OpFileStatNode, storage.OpFileGetLock, storage.OpFileQueryLock, storage.OpFileStatus:
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
	storage     *Storage
	id          string
	mu          sync.Mutex
	epoch       uint64
	failed      error
	closed      bool
	closeAction storage.LockRequestID
}

type remoteFile struct {
	session     *remoteFileSession
	id          string
	mu          sync.Mutex
	closed      bool
	closeAction storage.LockRequestID
}

func (s *Storage) CheckFileStorage() error { return nil }

var _ storage.FileStorage = (*Storage)(nil)
var _ FileSessionWithBarrier = (*remoteFileSession)(nil)
var _ FileWithBarrier = (*remoteFile)(nil)

func (s *Storage) fileCall(ctx context.Context, req fileRequest) (fileResponse, error) {
	if int64(len(req.Path)) > s.maxBodyBytes || int64(len(req.Data)) > s.maxWriteBytes {
		return fileResponse{}, syscall.EFBIG
	}
	if req.Op == storage.OpFileRead && (req.Length < 0 || int64(req.Length) > fileReadLimit(s.maxBodyBytes)) {
		return fileResponse{}, syscall.EFBIG
	}
	op := OpFile
	if fileControl(req.Op) {
		op = OpFileControl
	} else {
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
		limit = min(limit, DefaultMaxLockControlBytes)
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
	answer, err := s.call(ctx, Request{Op: op}, body)
	if err != nil {
		return fileResponse{}, err
	}
	defer answer.release()
	var response fileResponse
	if err := decodeFileJSON(answer.content, &response); err != nil {
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
	return &remoteFileSession{storage: s, id: response.Session, epoch: response.Epoch}, nil
}

func (s *remoteFileSession) call(ctx context.Context, req fileRequest) (fileResponse, error) {
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
	if fileActionRequired(req.Op) && req.Action == "" {
		var err error
		req.Action, err = storage.NewLockRequestID(s.epoch)
		if err != nil {
			s.mu.Unlock()
			return fileResponse{}, err
		}
	}
	s.mu.Unlock()
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
	var failure *operationError
	if err != nil && errors.As(err, &failure) && failure.unknown && (req.Action != "" || req.Op == storage.OpFileAck) {
		recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		recovered, recoveryErr := s.storage.fileCall(recovery, req)
		cancel()
		if recoveryErr == nil && !recovered.Retry {
			response, err = recovered, nil
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
		interruptible := !req.Open.Create && !req.Open.Truncate &&
			errors.Is(err, context.Canceled) && storage.ErrnoOf(err) == syscall.EINTR && cleanupErr == nil
		if cleanupErr != nil && !errors.Is(cleanupErr, syscall.ESTALE) {
			err = errors.Join(err, cleanupErr)
		}
		return nil, nil, operationFailure(Request{Op: OpFile}, err, interruptible)
	}
	return &remoteFile{session: s, id: response.File}, response.Barrier, nil
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.closeAction == "" {
		var err error
		s.closeAction, err = storage.NewLockRequestID(s.epoch)
		if err != nil {
			s.mu.Unlock()
			return err
		}
	}
	action := s.closeAction
	s.mu.Unlock()
	r, e := s.storage.fileCall(ctx, fileRequest{Op: storage.OpFileSessionClose, Session: s.id, Action: action})
	_ = r
	if e == nil || errors.Is(e, syscall.ESTALE) {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		return nil
	}
	return e
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
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileStat})
	return responseFileAttr(r, e)
}
func (f *remoteFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if offset < 0 || length < 0 {
		return storage.FileRead{}, syscall.EINVAL
	}
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileRead, Offset: offset, Length: length})
	a, e := responseFileAttr(r, e)
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
	a, e := responseFileAttr(r, e)
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
	a, e := responseFileAttr(r, e)
	return a, r.Barrier, e
}
func (f *remoteFile) SetAttr(ctx context.Context, c storage.AttrChange) (storage.Attr, error) {
	a, _, e := f.SetAttrWithBarrier(ctx, c)
	return a, e
}
func (f *remoteFile) SetAttrWithBarrier(ctx context.Context, c storage.AttrChange) (storage.Attr, *MutationBarrier, error) {
	change := AttrChangeOf(c)
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileSetAttr, Change: change})
	a, e := responseFileAttr(r, e)
	return a, r.Barrier, e
}
func (f *remoteFile) Sync(ctx context.Context) error {
	_, e := f.call(ctx, fileRequest{Op: storage.OpFileSync})
	return e
}
func (f *remoteFile) GetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock) (storage.LockConflict, error) {
	if err := lock.Check(); err != nil {
		return storage.LockConflict{}, err
	}
	r, e := f.call(ctx, fileRequest{Op: storage.OpFileGetLock, Owner: owner, Lock: lock})
	if e != nil {
		return storage.LockConflict{}, e
	}
	if r.Conflict == nil {
		return storage.LockConflict{}, unreachable(Request{Op: OpFile}, errors.New("lock query returned no conflict result"))
	}
	return *r.Conflict, nil
}
func (f *remoteFile) lockCall(ctx context.Context, r fileRequest) (storage.LockAttempt, error) {
	if _, e := r.LockID.Epoch(); e != nil {
		return storage.LockAttempt{}, e
	}
	response, e := f.call(ctx, r)
	if e != nil {
		return storage.LockAttempt{}, e
	}
	if response.Attempt == nil || response.Attempt.Request != r.LockID || response.Attempt.State < storage.LockPending || response.Attempt.State > storage.LockReleased {
		return storage.LockAttempt{}, unreachable(Request{Op: OpFile}, fmt.Errorf("lock response has no matching action outcome"))
	}
	return response.Attempt.storage()
}
func (f *remoteFile) SetLock(ctx context.Context, o storage.LockOwner, l storage.FileLock, id storage.LockRequestID) (storage.LockAttempt, error) {
	if e := l.Check(); e != nil {
		return storage.LockAttempt{}, e
	}
	op := storage.OpFileSetLock
	if l.Type == storage.Unlock {
		op = storage.OpFileUnlock
	}
	return f.lockCall(ctx, fileRequest{Op: op, Owner: o, Lock: l, LockID: id})
}
func (f *remoteFile) QueryLock(ctx context.Context, o storage.LockOwner, id storage.LockRequestID) (storage.LockAttempt, error) {
	return f.lockCall(ctx, fileRequest{Op: storage.OpFileQueryLock, Owner: o, LockID: id})
}
func (f *remoteFile) CancelLock(ctx context.Context, o storage.LockOwner, id storage.LockRequestID) (storage.LockAttempt, error) {
	return f.lockCall(ctx, fileRequest{Op: storage.OpFileCancelLock, Owner: o, LockID: id})
}
func (f *remoteFile) DropLocks(ctx context.Context, o storage.LockOwner, family storage.LockFamily) error {
	_, e := f.call(ctx, fileRequest{Op: storage.OpFileDropLocks, Owner: o, Family: family})
	return e
}
func (f *remoteFile) Close(ctx context.Context) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	if f.closeAction == "" {
		f.session.mu.Lock()
		epoch := f.session.epoch
		f.session.mu.Unlock()
		var err error
		f.closeAction, err = storage.NewLockRequestID(epoch)
		if err != nil {
			f.mu.Unlock()
			return err
		}
	}
	action := f.closeAction
	f.mu.Unlock()
	_, e := f.session.call(ctx, fileRequest{Op: storage.OpFileClose, File: f.id, Action: action})
	if e == nil || errors.Is(e, syscall.ESTALE) {
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
		return nil
	}
	return e
}
