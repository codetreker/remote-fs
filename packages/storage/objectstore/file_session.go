package objectstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	maxFileControlOperations = 2
	maxFileRangeOperations   = 2
	maxFileCleanupOperations = 2
)

type fileOperationClass uint8

const (
	fileDataOperation fileOperationClass = iota
	fileControlOperation
	fileRangeOperation
	fileCleanupOperation
	fileWaitOperation
)

type fileSession struct {
	storage        *Storage
	authority      metastore.FileStore
	native         metastore.FileSession
	options        storage.FileSessionOptions
	cleanup        context.Context
	mu             sync.Mutex
	active, closed bool
	expires        time.Time
	revision       uint64
	operations     [5]int
	idle           chan struct{}
	files          map[storage.FileReferenceID]*openFile
	timer          *time.Timer
	closePermit    chan struct{}
	closeErr       error
}

var _ storage.FileStorage = (*Storage)(nil)
var _ storage.FileSession = (*fileSession)(nil)

func (s *Storage) CheckFileStorage() error {
	native, ok := s.meta.(metastore.FileStore)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckFileStore(); err != nil {
		return err
	}
	if _, ok := s.objects.(BoundedObjects); !ok {
		return syscall.EOPNOTSUPP
	}
	return s.CheckBounded()
}

func (s *Storage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	if err := s.CheckFileStorage(); err != nil {
		return storage.FileVolumeState{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.FileVolumeState{}, err
	}
	defer s.endOperation()
	return s.meta.(metastore.FileStore).FileState(ctx)
}

func (s *Storage) Usage(ctx context.Context) (int64, error) {
	native, ok := s.meta.(metastore.FileStore)
	if !ok {
		return 0, syscall.EOPNOTSUPP
	}
	if err := s.beginOperation(); err != nil {
		return 0, err
	}
	defer s.endOperation()
	return native.Usage(ctx)
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	if err := options.Check(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	if err := s.CheckFileStorage(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	defer s.endOperation()
	authority := s.meta.(metastore.FileStore)
	maxBytes, _, _ := authority.FileOperationLimits()
	options.MaxFileSize = min(options.MaxFileSize, maxBytes)
	s.fileMu.Lock()
	if s.filesClosing {
		s.fileMu.Unlock()
		return nil, storage.FileSessionStatus{}, syscall.ESTALE
	}
	if s.fileEnrollmentContext == nil {
		s.fileEnrollmentContext, s.cancelFileEnrollment = context.WithCancel(s.cleanupContext)
	}
	enrollmentContext := s.fileEnrollmentContext
	s.fileEnrollment.Add(1)
	s.fileMu.Unlock()
	ctx, cancelEnrollment := context.WithCancel(ctx)
	stopEnrollment := context.AfterFunc(enrollmentContext, cancelEnrollment)
	defer func() { stopEnrollment(); cancelEnrollment() }()
	defer s.fileEnrollment.Done()
	started := time.Now()
	native, status, err := authority.NewFileSession(ctx, options)
	if err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	idle := make(chan struct{})
	close(idle)
	session := &fileSession{storage: s, authority: authority, native: native, options: options,
		cleanup: locking.WithScope(context.WithoutCancel(ctx), locking.MutationScope{}),
		active:  true, expires: started.Add(status.Remaining), revision: status.Revision, idle: idle,
		files: make(map[storage.FileReferenceID]*openFile), closePermit: make(chan struct{}, 1)}
	session.closePermit <- struct{}{}
	s.fileMu.Lock()
	if s.fileSessions == nil {
		s.fileSessions = make(map[*fileSession]struct{})
	}
	s.fileSessions[session] = struct{}{}
	closing := s.filesClosing
	s.fileMu.Unlock()
	session.mu.Lock()
	session.timer = time.AfterFunc(time.Until(session.expires), session.expire)
	session.mu.Unlock()
	if closing {
		return nil, storage.FileSessionStatus{}, errors.Join(syscall.ESTALE, session.dispose(ctx))
	}
	session.mu.Lock()
	status.Remaining = min(status.Remaining, max(time.Until(session.expires), 0))
	status.Retired = status.Retired || !session.active || status.Remaining <= 0
	session.mu.Unlock()
	return session, status, nil
}

func (s *fileSession) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	_, _, timeout := s.authority.FileOperationLimits()
	return context.WithTimeout(ctx, timeout)
}

func (s *fileSession) cleanupOperation(ctx context.Context) (context.Context, context.CancelFunc) {
	return s.operationContext(locking.WithScope(s.cleanup, locking.ScopeFromContext(ctx)))
}

func (s *fileSession) begin(ctx context.Context, class fileOperationClass) (context.Context, func(), error) {
	return s.admit(ctx, class, class == fileCleanupOperation)
}

func (s *fileSession) admit(ctx context.Context, class fileOperationClass, allowRetired bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var admissionErr error
	if allowRetired {
		admissionErr = s.storage.beginFileCleanup()
	} else {
		admissionErr = s.storage.beginOperation()
	}
	if admissionErr != nil {
		return nil, nil, admissionErr
	}
	s.mu.Lock()
	if !allowRetired && (!s.active || !time.Now().Before(s.expires)) {
		s.mu.Unlock()
		s.storage.endOperation()
		return nil, nil, syscall.ESTALE
	}
	limit := s.options.MaxOperations
	switch class {
	case fileControlOperation:
		limit = maxFileControlOperations
	case fileRangeOperation:
		limit = maxFileRangeOperations
	case fileCleanupOperation:
		limit = maxFileCleanupOperations
	case fileWaitOperation:
		limit = s.options.MaxWaiters
	}
	if s.operations[class] >= limit {
		s.mu.Unlock()
		s.storage.endOperation()
		return nil, nil, syscall.EAGAIN
	}
	if class != fileControlOperation && class != fileCleanupOperation && s.drainingOperations() == 0 {
		s.idle = make(chan struct{})
	}
	s.operations[class]++
	s.mu.Unlock()
	var cancel context.CancelFunc
	if class == fileWaitOperation {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = s.operationContext(ctx)
	}
	ctx = metastore.WithFilePublicationGuard(ctx, s.publicationAllowed)
	return ctx, func() {
		cancel()
		s.mu.Lock()
		s.operations[class]--
		if class != fileControlOperation && class != fileCleanupOperation && s.drainingOperations() == 0 {
			close(s.idle)
		}
		s.mu.Unlock()
		s.storage.endOperation()
	}, nil
}

func (s *fileSession) drainingOperations() int {
	return s.operations[fileDataOperation] + s.operations[fileRangeOperation] + s.operations[fileWaitOperation]
}

func (s *fileSession) publicationAllowed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || !time.Now().Before(s.expires) {
		return syscall.ESTALE
	}
	return nil
}

func (s *fileSession) Retain(ctx context.Context, request storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := s.native.Retain(ctx, request, id)
	if result.Effects&(storage.EffectContentChanged|storage.EffectEntryDetached) != 0 {
		s.storage.sweepAfterMutation()
	}
	return result, err
}

func (s *fileSession) RetainAt(ctx context.Context, request storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := s.native.RetainAt(ctx, request, id)
	if result.Effects&(storage.EffectContentChanged|storage.EffectEntryDetached) != 0 {
		s.storage.sweepAfterMutation()
	}
	return result, err
}

func (s *fileSession) CreateAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := s.native.CreateAndRetainAt(ctx, request, id)
	if result.Effects&(storage.EffectContentChanged|storage.EffectEntryDetached) != 0 {
		s.storage.sweepAfterMutation()
	}
	return result, err
}

func (s *fileSession) ResetAndRetainAt(ctx context.Context, request storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := s.native.ResetAndRetainAt(ctx, request, id)
	if result.Effects&(storage.EffectContentChanged|storage.EffectEntryDetached) != 0 {
		s.storage.sweepAfterMutation()
	}
	return result, err
}

func (s *fileSession) ReplaceAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := s.native.ReplaceAndRetainAt(ctx, request, id)
	if result.Effects&(storage.EffectContentChanged|storage.EffectEntryDetached) != 0 {
		s.storage.sweepAfterMutation()
	}
	return result, err
}

func (s *fileSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	ctx, done, err := s.begin(ctx, fileCleanupOperation)
	if err != nil {
		return nil, err
	}
	defer done()
	native, live, err := s.native.Reference(ctx, id)
	if err != nil {
		return nil, err
	}
	if native.Reference() != id || id == 0 {
		return nil, syscall.EIO
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.files[id]; f != nil {
		return f, nil
	}
	active := live && s.active && !s.closed && time.Now().Before(s.expires)
	if active && len(s.files) >= s.options.MaxFiles {
		return nil, syscall.EMFILE
	}
	idle := make(chan struct{})
	close(idle)
	f := &openFile{session: s, native: native, active: active, idle: idle, closePermit: make(chan struct{}, 1)}
	f.closePermit <- struct{}{}
	if active {
		s.files[id] = f
	}
	return f, nil
}

func (s *fileSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	return s.native.StatNode(ctx, id, options)
}

func (s *fileSession) SetNodeAttr(ctx context.Context, node uint64, change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return s.native.SetNodeAttr(ctx, node, change, id)
}

func (s *fileSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	return s.native.QueryAction(ctx, id)
}

func (s *fileSession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return s.native.CancelAction(ctx, id)
}

func (s *fileSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := s.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return s.native.RetireRangeOwner(ctx, owner, id)
}

func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.begin(ctx, fileControlOperation)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	started := time.Now()
	status, err := s.native.Renew(ctx)
	if err != nil {
		return status, err
	}
	s.mu.Lock()
	if s.active && status.Revision > s.revision {
		s.revision = status.Revision
		s.expires = started.Add(status.Remaining)
		s.timer.Reset(time.Until(s.expires))
	}
	status.Remaining = min(status.Remaining, max(time.Until(s.expires), 0))
	status.Retired = status.Retired || !s.active || status.Remaining <= 0
	s.mu.Unlock()
	return status, nil
}

func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.admit(ctx, fileControlOperation, true)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	status, err := s.native.Status(ctx)
	s.mu.Lock()
	status.Remaining = min(status.Remaining, max(time.Until(s.expires), 0))
	status.Retired = status.Retired || !s.active || status.Remaining <= 0
	s.mu.Unlock()
	return status, err
}

func (s *fileSession) expire() {
	s.mu.Lock()
	if s.active && time.Now().Before(s.expires) {
		s.timer.Reset(time.Until(s.expires))
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	ctx, cancel := s.operationContext(s.cleanup)
	defer cancel()
	_ = s.dispose(ctx)
}

func (s *fileSession) retireAndDrain(ctx context.Context) error {
	s.mu.Lock()
	s.active = false
	s.timer.Stop()
	idle := s.idle
	s.mu.Unlock()
	if err := s.native.Retire(ctx); err != nil {
		return err
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *fileSession) finalize(err error) {
	s.mu.Lock()
	s.closeErr = err
	if err == nil {
		s.closed = true
		clear(s.files)
	}
	s.mu.Unlock()
	if err == nil {
		s.storage.fileMu.Lock()
		delete(s.storage.fileSessions, s)
		s.storage.fileMu.Unlock()
		s.storage.sweepAfterMutation()
	}
}

func (s *fileSession) dispose(ctx context.Context) error {
	select {
	case <-s.closePermit:
		defer func() { s.closePermit <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}
	if err := s.retireAndDrain(ctx); err != nil {
		s.finalize(err)
		return err
	}
	cleanup, cancel := s.cleanupOperation(ctx)
	stop := context.AfterFunc(ctx, cancel)
	err := s.native.Dispose(cleanup)
	stop()
	cancel()
	s.finalize(err)
	return err
}

func (s *fileSession) Close(ctx context.Context, id storage.FileActionID) (receipt storage.FileActionReceipt, err error) {
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return storage.FileActionReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}, nil
	}
	if err := s.storage.beginFileCleanup(); err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer s.storage.endOperation()
	select {
	case <-s.closePermit:
		defer func() { s.closePermit <- struct{}{} }()
	case <-ctx.Done():
		return storage.FileActionReceipt{}, beforeFileAdmission(ctx.Err())
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	admitted, proceed, err := s.native.BeginClose(ctx, id)
	if !proceed || err != nil {
		if err == nil && admitted.Errno == 0 && (admitted.State == storage.FileActionCompleted || admitted.State == storage.FileActionRetired) {
			s.mu.Lock()
			inactive := !s.active
			s.mu.Unlock()
			if inactive {
				s.finalize(nil)
			}
		}
		return admitted, err
	}
	defer func() { err = afterFileAdmission(err) }()
	s.mu.Lock()
	s.active = false
	s.timer.Stop()
	idle := s.idle
	s.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		s.finalize(ctx.Err())
		return admitted, ctx.Err()
	}
	cleanup, finish := s.cleanupOperation(ctx)
	stop := context.AfterFunc(ctx, finish)
	result, err := s.native.Close(cleanup, id)
	stop()
	finish()
	if (result.State == storage.FileActionCompleted || result.State == storage.FileActionRetired) && result.Errno == 0 {
		s.finalize(nil)
	} else {
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
	}
	return result, err
}

// CloseFileSessions keeps failed cleanup owned until a later shutdown retry.
func (s *Storage) CloseFileSessions() error {
	s.fileCloseMu.Lock()
	defer s.fileCloseMu.Unlock()
	s.fileMu.Lock()
	s.filesClosing = true
	if s.cancelFileEnrollment != nil {
		s.cancelFileEnrollment()
	}
	s.fileMu.Unlock()
	s.fileEnrollment.Wait()
	s.fileMu.Lock()
	sessions := make([]*fileSession, 0, len(s.fileSessions))
	for session := range s.fileSessions {
		sessions = append(sessions, session)
	}
	s.fileMu.Unlock()
	var errs []error
	for _, session := range sessions {
		ctx, cancel := session.operationContext(session.cleanup)
		errs = append(errs, session.dispose(ctx))
		cancel()
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("closing retained file sessions: %w", err)
	}
	return nil
}

func (s *Storage) beginFileCleanup() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closeDone != nil && !s.fileCloseRetry {
		return fmt.Errorf("the object-store volume is closed: %w", syscall.EIO)
	}
	s.operations.RLock()
	return nil
}
