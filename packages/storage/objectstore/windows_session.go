package objectstore

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsAuthority interface {
	metastore.WindowsStore
	Advisory(context.Context) (*advisory.Coordinator, error)
}

type windowsSession struct {
	storage              *Storage
	native               metastore.WindowsSession
	domain               *advisory.Coordinator
	options              storage.FileSessionOptions
	cleanup              context.Context
	mu                   sync.Mutex
	active, closed       bool
	operations, controls int
	idle                 chan struct{}
	files                map[string]*windowsFile
	expires              time.Time
	timer                *time.Timer
	closePermit          chan struct{}
	closeErr             error
}

var _ storage.WindowsStorage = (*Storage)(nil)
var _ storage.WindowsSession = (*windowsSession)(nil)

func (s *Storage) CheckWindowsStorage() error {
	native, ok := s.meta.(windowsAuthority)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckWindowsStore(); err != nil {
		return err
	}
	if _, ok := s.objects.(BoundedObjects); !ok {
		return syscall.EOPNOTSUPP
	}
	return s.CheckBounded()
}

func (s *Storage) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsState{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.WindowsState{}, err
	}
	defer s.endOperation()
	return s.meta.(windowsAuthority).WindowsState(ctx)
}

func (s *Storage) EnableWindows(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.WindowsActivation{}, err
	}
	defer s.endOperation()
	return s.meta.(windowsAuthority).EnableWindows(ctx, id)
}

func (s *Storage) QueryWindowsActivation(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.WindowsActivation{}, err
	}
	defer s.endOperation()
	return s.meta.(windowsAuthority).QueryWindowsActivation(ctx, id)
}

func (s *Storage) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if err := s.CheckWindowsStorage(); err != nil {
		return nil, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	native := s.meta.(windowsAuthority)
	domain, err := native.Advisory(ctx)
	if err != nil {
		return nil, err
	}
	maxBytes, _, _ := domain.FileOperationLimits()
	options.MaxFileSize = min(options.MaxFileSize, maxBytes)
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.filesClosing {
		return nil, syscall.ESTALE
	}
	started := time.Now()
	inner, err := native.NewWindowsSession(ctx, options)
	if err != nil {
		return nil, err
	}
	idle := make(chan struct{})
	close(idle)
	ws := &windowsSession{storage: s, native: inner, domain: domain, options: options,
		cleanup: locking.WithScope(context.WithoutCancel(ctx), locking.MutationScope{}), active: true, idle: idle,
		files: make(map[string]*windowsFile), expires: started.Add(options.Lease), closePermit: make(chan struct{}, 1)}
	ws.closePermit <- struct{}{}
	if s.windowsSessions == nil {
		s.windowsSessions = make(map[*windowsSession]struct{})
	}
	s.windowsSessions[ws] = struct{}{}
	ws.mu.Lock()
	ws.timer = time.AfterFunc(time.Until(ws.expires), ws.expire)
	ws.mu.Unlock()
	return ws, nil
}

func (s *windowsSession) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	_, _, timeout := s.domain.FileOperationLimits()
	return context.WithTimeout(ctx, timeout)
}

func (s *windowsSession) cleanupOperation(ctx context.Context) (context.Context, context.CancelFunc) {
	return s.operationContext(locking.WithScope(s.cleanup, locking.ScopeFromContext(ctx)))
}

func (s *windowsSession) begin(ctx context.Context, control bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := s.storage.beginOperation(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if !s.active || !time.Now().Before(s.expires) {
		s.mu.Unlock()
		s.storage.endOperation()
		return nil, nil, syscall.ESTALE
	}
	count, limit := &s.operations, s.options.MaxOperations
	if control {
		count, limit = &s.controls, maxFileControlOperations
	}
	if *count >= limit {
		s.mu.Unlock()
		s.storage.endOperation()
		return nil, nil, syscall.EAGAIN
	}
	if s.operations+s.controls == 0 {
		s.idle = make(chan struct{})
	}
	*count++
	s.mu.Unlock()
	ctx, cancel := s.operationContext(ctx)
	return ctx, func() {
		cancel()
		s.mu.Lock()
		*count--
		if s.operations+s.controls == 0 {
			close(s.idle)
		}
		s.mu.Unlock()
		s.storage.endOperation()
	}, nil
}

func (s *windowsSession) result(result metastore.WindowsResult, err error) (storage.WindowsActionResult, error) {
	r := result.WindowsActionResult
	r.File = nil
	if result.Reference == nil {
		return r, err
	}
	reference := result.Reference.Reference()
	if reference == "" || len(reference) > storage.WindowsMaxReferenceBytes {
		return r, errors.Join(err, syscall.EIO)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.files[reference]; f != nil {
		r.File = f
		return r, err
	}
	now := time.Now()
	for key, f := range s.files {
		if f.closed && !now.Before(f.forgetAfter) {
			delete(s.files, key)
		}
	}
	// Completed open receipts can outlive a closed reference. Their wrappers
	// retain its closed state only for the same finite action-history interval.
	if len(s.files) >= s.options.MaxFiles+s.options.MaxLockActions {
		return r, errors.Join(err, syscall.EMFILE)
	}
	idle := make(chan struct{})
	close(idle)
	f := &windowsFile{session: s, native: result.Reference, idle: idle, closePermit: make(chan struct{}, 1)}
	f.closePermit <- struct{}{}
	s.files[reference] = f
	r.File = f
	return r, err
}

func (s *windowsSession) Open(ctx context.Context, request storage.WindowsOpenRequest, id storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	ctx, done, err := s.begin(ctx, false)
	if err != nil {
		return storage.WindowsOpenResult{}, err
	}
	defer done()
	r, err := s.result(s.native.Open(ctx, request, id))
	if err == nil && r.File == nil {
		err = syscall.EIO
	}
	return storage.WindowsOpenResult{File: r.File, Attr: r.Attr, CreateAction: r.CreateAction}, err
}

func (s *windowsSession) QueryAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return s.result(s.native.QueryAction(ctx, id))
}

func (s *windowsSession) CancelAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return s.result(s.native.CancelAction(ctx, id))
}

func (s *windowsSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.begin(ctx, true)
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
	if s.active {
		s.expires = started.Add(status.Remaining)
		s.timer.Reset(time.Until(s.expires))
	}
	status.Remaining = min(status.Remaining, max(time.Until(s.expires), 0))
	status.Retired = status.Retired || !s.active
	s.mu.Unlock()
	return status, nil
}

func (s *windowsSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	status, err := s.native.Status(ctx)
	s.mu.Lock()
	status.Remaining = min(status.Remaining, max(time.Until(s.expires), 0))
	status.Retired = status.Retired || !s.active
	s.mu.Unlock()
	return status, err
}

func (s *windowsSession) expire() {
	s.mu.Lock()
	if s.active && time.Now().Before(s.expires) {
		s.timer.Reset(time.Until(s.expires))
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	ctx, cancel := s.operationContext(s.cleanup)
	defer cancel()
	// Close retains the failure and ownership for the storage's shutdown retry.
	_ = s.Close(ctx)
}

func (s *windowsSession) Close(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}
	select {
	case <-s.closePermit:
		defer func() { s.closePermit <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.active = false
	s.timer.Stop()
	idle := s.idle
	s.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		return s.recordClose(ctx.Err())
	}
	cleanup, cancel := s.cleanupOperation(ctx)
	stop := context.AfterFunc(ctx, cancel)
	err := s.native.Close(cleanup)
	stop()
	cancel()
	if err == nil {
		s.storage.fileMu.Lock()
		delete(s.storage.windowsSessions, s)
		s.storage.fileMu.Unlock()
		s.storage.sweepAfterMutation()
	}
	return s.recordClose(err)
}

func (s *windowsSession) recordClose(err error) error {
	s.mu.Lock()
	s.closeErr = err
	if err == nil {
		s.closed = true
	}
	s.mu.Unlock()
	return err
}
