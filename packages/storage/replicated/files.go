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
	if s.closing || len(s.fileSessions)+s.fileSessionOpening >= s.options.MaxFileSessions {
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
	if err != nil {
		return nil, err
	}
	barriers, ok := remoteSession.(httprest.FileSessionWithBarrier)
	if !ok {
		cleanup, done := s.fileCleanupContext()
		defer done()
		return nil, errors.Join(fmt.Errorf("file session has no replication barriers: %w", syscall.EOPNOTSUPP), remoteSession.Close(cleanup))
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
		return nil, errors.Join(fmt.Errorf("replicated storage closed while opening a file session: %w", syscall.EIO), session.Close(cleanup))
	}
	return session, nil
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
	s.mu.Unlock()
	var result error
	for _, session := range sessions {
		ctx, cancel := s.fileCleanupContext()
		result = errors.Join(result, session.Close(ctx))
		cancel()
	}
	return result
}

type fileSession struct {
	base     *Storage
	remote   httprest.FileSessionWithBarrier
	lifetime context.Context
	stop     context.CancelFunc
	mu       sync.Mutex
	active   int
	closing  bool
	closed   bool
	changed  chan struct{}
	closeRun chan struct{}
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
	s.mu.Lock()
	for s.closeRun != nil {
		finished := s.closeRun
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-finished:
		}
		s.mu.Lock()
	}
	if s.closed {
		s.mu.Unlock()
		return nil
	}
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
			return ctx.Err()
		case <-changed:
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	if err := s.remote.Close(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.base.mu.Lock()
	delete(s.base.fileSessions, s)
	s.base.mu.Unlock()
	return nil
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
	if err != nil {
		if remote != nil {
			cleanup, cancel := s.base.fileCleanupContext()
			err = errors.Join(err, remote.Close(cleanup))
			cancel()
		}
		return nil, err
	}
	barriers, ok := remote.(httprest.FileWithBarrier)
	if !ok {
		cleanup, cancel := s.base.fileCleanupContext()
		defer cancel()
		return nil, errors.Join(fmt.Errorf("opened file has no replication barriers: %w", syscall.EIO), remote.Close(cleanup))
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
