package replicated

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

var _ storage.FileStorage = (*Storage)(nil)
var _ storage.FileStorage = (*scopedStorage)(nil)
var _ storage.FileSession = (*fileSession)(nil)
var _ storage.File = (*retainedFile)(nil)

// CheckFileStorage requires native retained references from the authority.
// The local tree cannot supply this capability for detached file objects.
func (s *Storage) CheckFileStorage() error { return s.remote.CheckFileStorage() }

func (s *scopedStorage) CheckFileStorage() error { return s.base.CheckFileStorage() }

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	return s.newFileSession(ctx, options, s.remote)
}

func (s *scopedStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	remote, err := s.base.remote.WithScope(s.scope)
	if err != nil {
		return nil, err
	}
	return s.base.newFileSession(ctx, options, remote)
}

func (s *Storage) newFileSession(ctx context.Context, options storage.FileSessionOptions, remote *httprest.Storage) (storage.FileSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if err := s.CheckFileStorage(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closing || len(s.fileSessions)+len(s.fileCleanupSessions)+s.fileSessionOpening >= s.options.MaxFileSessions {
		s.mu.Unlock()
		return nil, fmt.Errorf("replicated file session admission is closed or full: %w", syscall.EAGAIN)
	}
	s.fileSessionOpening++
	s.mu.Unlock()
	registered := false
	defer func() {
		if !registered {
			s.mu.Lock()
			s.fileSessionOpening--
			s.wakeConfirmationCapacity()
			s.mu.Unlock()
		}
	}()
	confirmation, err := s.expect(ctx, "file-session", "")
	if err != nil {
		return nil, err
	}
	defer s.forget(confirmation)
	if err := ctx.Err(); err != nil {
		return nil, confirmationContextError("file-session", "", ctx)
	}
	sendCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	defer func() { stop(); cancel() }()
	remoteSession, err := remote.NewFileSession(sendCtx, options)
	if nilReference(remoteSession) {
		remoteSession = nil
	}
	if err != nil {
		if remoteSession == nil {
			return nil, err
		}
		return s.cleanupFailedOpenSession(remoteSession, err)
	}
	if remoteSession == nil {
		return nil, fmt.Errorf("file session open returned no session: %w", syscall.EIO)
	}
	barriers, ok := remoteSession.(httprest.FileSessionWithBarrier)
	if !ok {
		return s.cleanupFailedOpenSession(remoteSession, fmt.Errorf("file session has no replication barriers: %w", syscall.EOPNOTSUPP))
	}
	lifetime, stopSession := context.WithCancel(context.Background())
	session := &fileSession{base: s, remote: barriers, lifetime: lifetime, stop: stopSession, changed: make(chan struct{})}
	s.mu.Lock()
	if s.fileSessions == nil {
		s.fileSessions = make(map[*fileSession]struct{})
	}
	s.fileSessions[session] = struct{}{}
	s.fileSessionOpening--
	s.wakeConfirmationCapacity()
	registered = true
	closing := s.closing
	s.mu.Unlock()
	if closing {
		cleanup, done := s.fileCleanupContext()
		defer done()
		result, closeErr := session.CloseWithResult(cleanup)
		failure := errors.Join(fmt.Errorf("replicated storage closed while opening a file session: %w", syscall.EIO), closeErr, result.Check(closeErr))
		if result.Released {
			return nil, failure
		}
		return session, failure
	}
	return session, nil
}

func (s *Storage) cleanupFailedOpenSession(native storage.FileSession, failure error) (storage.FileSession, error) {
	cleanup, done := s.fileCleanupContext()
	defer done()
	result, settled, closeErr := s.closeFailedOpenSession(cleanup, native)
	failure = errors.Join(failure, closeErr)
	if settled {
		return nil, failure
	}
	session := &failedOpenSession{base: s, native: native, failure: failure, result: result}
	s.mu.Lock()
	if s.fileCleanupSessions == nil {
		s.fileCleanupSessions = make(map[*failedOpenSession]struct{})
	}
	s.fileCleanupSessions[session] = struct{}{}
	s.mu.Unlock()
	return session, failure
}

func (s *Storage) closeFailedOpenSession(ctx context.Context, native storage.FileSession) (storage.ReferenceCloseResult, bool, error) {
	if remote, ok := native.(httprest.FileSessionWithBarrier); ok {
		result, barrier, err := remote.CloseWithBarrier(ctx)
		if !result.Released {
			return result, false, errors.Join(err, result.Check(err))
		}
		settled, err := s.confirmReleasedClose(ctx, "close-failed-open-session", barrier, err)
		return result, settled, err
	}
	result, err := native.CloseWithResult(ctx)
	return result, result.Released, errors.Join(err, result.Check(err))
}

type failedOpenSession struct {
	base    *Storage
	native  storage.FileSession
	failure error
	mu      sync.Mutex
	run     chan struct{}
	result  storage.ReferenceCloseResult
	err     error
	settled bool
}

func (s *failedOpenSession) OpenFile(context.Context, string, storage.FileOpenOptions) (storage.File, error) {
	return nil, s.failure
}

func (s *failedOpenSession) OpenNode(context.Context, uint64, storage.FileOpenOptions) (storage.File, error) {
	return nil, s.failure
}

func (s *failedOpenSession) StatNode(context.Context, uint64) (storage.Attr, error) {
	return storage.Attr{}, s.failure
}

func (s *failedOpenSession) SetNodeAttr(context.Context, uint64, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, s.failure
}

func (s *failedOpenSession) Renew(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.failure
}

func (s *failedOpenSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.failure
}

func (s *failedOpenSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}

func (s *failedOpenSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.mu.Lock()
	for s.run != nil {
		run := s.run
		s.mu.Unlock()
		select {
		case <-run:
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
		s.mu.Lock()
	}
	if s.settled {
		result, err := s.result, s.err
		s.mu.Unlock()
		return result, err
	}
	previous := s.result
	s.run = make(chan struct{})
	s.mu.Unlock()
	result, settled, err := s.base.closeFailedOpenSession(ctx, s.native)
	if previous.Released && !result.Released {
		result = previous
		settled = false
		err = errors.Join(err, fmt.Errorf("released failed-open session lost barrier replay: %w", syscall.EIO))
	}
	err = errors.Join(err, result.Check(err))
	if settled {
		s.base.mu.Lock()
		delete(s.base.fileCleanupSessions, s)
		s.base.mu.Unlock()
	}
	s.mu.Lock()
	if result.Released {
		s.result = result
	}
	if settled {
		s.err = err
		s.settled = true
	}
	close(s.run)
	s.run = nil
	s.mu.Unlock()
	return result, err
}

func (s *Storage) fileCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.options.ConfirmationGrace)
}

func (s *Storage) closeFileSessions() error {
	s.mu.Lock()
	sessions := make([]*fileSession, 0, len(s.fileSessions))
	for session := range s.fileSessions {
		sessions = append(sessions, session)
	}
	cleanupSessions := make([]*failedOpenSession, 0, len(s.fileCleanupSessions))
	for session := range s.fileCleanupSessions {
		cleanupSessions = append(cleanupSessions, session)
	}
	s.mu.Unlock()
	var result error
	for _, session := range sessions {
		ctx, cancel := s.fileCleanupContext()
		result = errors.Join(result, session.Close(ctx))
		cancel()
	}
	for _, session := range cleanupSessions {
		ctx, cancel := s.fileCleanupContext()
		result = errors.Join(result, session.Close(ctx))
		cancel()
	}
	return result
}

type fileSession struct {
	base              *Storage
	remote            httprest.FileSessionWithBarrier
	lifetime          context.Context
	stop              context.CancelFunc
	mu                sync.Mutex
	active            int
	closing           bool
	closed            bool
	closeResult       storage.ReferenceCloseResult
	closeErr          error
	closeBarrier      *httprest.MutationBarrier
	closeAuthorityErr error
	changed           chan struct{}
	closeRun          chan struct{}
}

// The session drains local calls as well as server references. Reconciliation and
// cleanup do not depend on the stream, whose health cannot establish ownership.
func (s *fileSession) begin(ctx context.Context, ordinary bool) (context.Context, func(), error) {
	if ordinary {
		if err := s.base.usable("file", ""); err != nil {
			return nil, nil, err
		}
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, nil, syscall.ESTALE
	}
	s.active++
	s.mu.Unlock()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	return operation, func() {
		stop()
		cancel()
		s.mu.Lock()
		s.active--
		close(s.changed)
		s.changed = make(chan struct{})
		s.mu.Unlock()
	}, nil
}

func (s *fileSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}

func (s *fileSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.mu.Lock()
	for s.closeRun != nil {
		finished := s.closeRun
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		case <-finished:
		}
		s.mu.Lock()
	}
	if s.closed {
		result, err := s.closeResult, s.closeErr
		s.mu.Unlock()
		return result, err
	}
	previous := s.closeResult
	previousBarrier := s.closeBarrier
	previousAuthorityErr := s.closeAuthorityErr
	s.closing = true
	s.stop()
	s.closeRun = make(chan struct{})
	defer func() {
		s.mu.Lock()
		close(s.closeRun)
		s.closeRun = nil
		s.mu.Unlock()
	}()
	for s.active != 0 {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		case <-changed:
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	var result storage.ReferenceCloseResult
	var barrier *httprest.MutationBarrier
	var authorityErr error
	if previous.Released && previousBarrier != nil {
		result, barrier, authorityErr = previous, previousBarrier, previousAuthorityErr
	} else {
		result, barrier, authorityErr = s.remote.CloseWithBarrier(ctx)
	}
	if previous.Released && !result.Released {
		return previous, errors.Join(authorityErr, fmt.Errorf("released session close lost barrier replay: %w", syscall.EIO))
	}
	err := errors.Join(authorityErr, result.Check(authorityErr))
	if !result.Released {
		return result, err
	}
	settled, err := s.base.confirmReleasedClose(ctx, "close-file-session", barrier, authorityErr)
	s.mu.Lock()
	s.closeResult = result
	s.closeBarrier = barrier
	s.closeAuthorityErr = authorityErr
	if !settled {
		s.mu.Unlock()
		return result, err
	}
	s.closed = true
	s.closeErr = err
	s.mu.Unlock()
	s.base.mu.Lock()
	delete(s.base.fileSessions, s)
	s.base.mu.Unlock()
	return result, err
}

func (s *fileSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if _, err := storage.CleanPath(path); err != nil {
		return nil, err
	}
	return s.open(ctx, func(ctx context.Context) (storage.File, *httprest.MutationBarrier, error) {
		return s.remote.OpenFileWithBarrier(ctx, path, options)
	})
}

func (s *fileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	if err := options.CheckNode(id); err != nil {
		return nil, err
	}
	return s.open(ctx, func(ctx context.Context) (storage.File, *httprest.MutationBarrier, error) {
		return s.remote.OpenNodeWithBarrier(ctx, id, options)
	})
}

func (s *fileSession) open(ctx context.Context, open func(context.Context) (storage.File, *httprest.MutationBarrier, error)) (storage.File, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer done()
	var remote storage.File
	err = s.confirm(ctx, "open-file", func(ctx context.Context) (*httprest.MutationBarrier, error) {
		var barrier *httprest.MutationBarrier
		remote, barrier, err = open(ctx)
		return barrier, err
	})
	if nilReference(remote) {
		remote = nil
	}
	if err != nil {
		if remote != nil {
			return s.cleanupFailedOpenFile(remote, err)
		}
		return nil, err
	}
	barriers, ok := remote.(httprest.FileWithBarrier)
	if !ok {
		if remote == nil {
			return nil, fmt.Errorf("opened file has no replication barriers: %w", syscall.EIO)
		}
		return s.cleanupFailedOpenFile(remote, fmt.Errorf("opened file has no replication barriers: %w", syscall.EIO))
	}
	return &retainedFile{session: s, remote: barriers}, nil
}

func (s *fileSession) confirm(ctx context.Context, op string, send func(context.Context) (*httprest.MutationBarrier, error)) error {
	return s.base.change(ctx, op, "", func(ctx context.Context) (httprest.MutationBarrier, error) {
		barrier, err := send(ctx)
		if err != nil {
			return httprest.MutationBarrier{}, err
		}
		if barrier == nil {
			return httprest.MutationBarrier{}, fmt.Errorf("file operation returned no replication barrier: %w", syscall.EIO)
		}
		return *barrier, nil
	})
}

// Cleanup can publish a pending unlink and must remain possible without a live
// replica stream. A healthy replica waits for the returned barrier; a stopped or
// failed follower records the authority result without making cleanup depend on it.
func (s *Storage) confirmCleanup(ctx context.Context, op string, barrier *httprest.MutationBarrier) error {
	if barrier == nil {
		return fmt.Errorf("%s returned no replication barrier: %w", op, syscall.EIO)
	}
	captured := &confirmation{}
	if err := s.setBarrier(captured, *barrier); err != nil {
		return err
	}
	s.mu.Lock()
	closing, failed := s.closing, s.failure != nil
	s.mu.Unlock()
	if closing || failed {
		return nil
	}
	admitted, err := s.expect(ctx, op, "")
	if err != nil {
		return &cleanupConfirmationFailure{cause: err}
	}
	defer s.forget(admitted)
	if err := s.await(ctx, op, "", captured); err != nil {
		return &cleanupConfirmationFailure{cause: err}
	}
	return nil
}

func (s *Storage) confirmReleasedClose(ctx context.Context, op string, barrier *httprest.MutationBarrier, authorityErr error) (bool, error) {
	if barrier == nil {
		var pending *httprest.CloseBarrierPendingError
		if errors.As(authorityErr, &pending) {
			return false, authorityErr
		}
		return false, errors.Join(authorityErr, fmt.Errorf("%s returned no replication barrier: %w", op, syscall.EIO))
	}
	confirmationErr := s.confirmCleanup(ctx, op, barrier)
	return confirmationErr == nil, errors.Join(authorityErr, confirmationErr)
}

type cleanupConfirmationFailure struct{ cause error }

func (e *cleanupConfirmationFailure) Error() string {
	return fmt.Sprintf("authority cleanup completed but replica visibility was not confirmed: %v", e.cause)
}

func (e *cleanupConfirmationFailure) Unwrap() []error       { return []error{syscall.EIO, e.cause} }
func (e *cleanupConfirmationFailure) Classification() error { return syscall.EIO }

func (s *fileSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	return s.remote.StatNode(ctx, id)
}

func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	ctx, done, err := s.begin(ctx, true)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	var attr storage.Attr
	err = s.confirm(ctx, "set-node-attr", func(ctx context.Context) (*httprest.MutationBarrier, error) {
		var barrier *httprest.MutationBarrier
		attr, barrier, err = s.remote.SetNodeAttrWithBarrier(ctx, id, change)
		return barrier, err
	})
	if err != nil {
		return storage.Attr{}, err
	}
	return attr, nil
}

func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.begin(ctx, false)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	return s.remote.Renew(ctx)
}

func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, done, err := s.begin(ctx, false)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	return s.remote.Status(ctx)
}
