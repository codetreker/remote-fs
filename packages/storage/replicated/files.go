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

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	return s.newFileSession(ctx, options, s.remote)
}

func (s *scopedStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	remote, err := s.base.remote.WithScope(s.scope)
	if err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	return s.base.newFileSession(ctx, options, remote)
}

func (s *Storage) newFileSession(ctx context.Context, options storage.FileSessionOptions, remote *httprest.Storage) (storage.FileSession, storage.FileSessionStatus, error) {
	if err := options.Check(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	if err := s.CheckFileStorage(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	s.mu.Lock()
	if s.closing || len(s.fileSessions)+s.fileSessionOpening >= s.options.MaxFileSessions {
		s.mu.Unlock()
		return nil, storage.FileSessionStatus{}, fmt.Errorf("replicated file session admission is closed or full: %w", syscall.EAGAIN)
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
		return nil, storage.FileSessionStatus{}, err
	}
	defer s.forget(confirmation)
	if err := ctx.Err(); err != nil {
		return nil, storage.FileSessionStatus{}, confirmationContextError("file-session", "", ctx)
	}
	sendCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	defer func() { stop(); cancel() }()
	remoteSession, status, err := remote.NewFileSession(sendCtx, options)
	if remoteSession == nil {
		return nil, status, err
	}
	barriers := remoteSession.(httprest.FileSessionWithBarrier)
	lifetime, stopSession := context.WithCancel(context.Background())
	session := &fileSession{actionEpoch: status.ActionEpoch, base: s, remote: barriers, lifetime: lifetime, stop: stopSession, changed: make(chan struct{}), closing: err != nil}
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
		failure := errors.Join(err, fmt.Errorf("replicated storage closed while opening a file session: %w", syscall.EIO))
		if cleanupErr := session.cleanup(cleanup); cleanupErr != nil {
			return session, status, errors.Join(failure, cleanupErr)
		}
		return nil, status, failure
	}
	return session, status, err
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
		result = errors.Join(result, session.cleanup(ctx))
		cancel()
	}
	return result
}

type fileSession struct {
	base          *Storage
	remote        httprest.FileSessionWithBarrier
	lifetime      context.Context
	stop          context.CancelFunc
	mu            sync.Mutex
	active        int
	closing       bool
	closed        bool
	changed       chan struct{}
	closeRun      chan struct{}
	closeReceipt  storage.FileActionReceipt
	closeBarrier  *httprest.MutationBarrier
	cleanupAction storage.FileActionID
	actionEpoch   uint64
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

func (s *Storage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	return s.remote.FileState(ctx)
}
func (s *scopedStorage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	return s.base.remote.FileState(ctx)
}

func (s *fileSession) cleanup(ctx context.Context) error {
	s.mu.Lock()
	if s.cleanupAction == "" {
		action, err := storage.NewFileActionID(s.actionEpoch)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.cleanupAction = action
	}
	action := s.cleanupAction
	s.mu.Unlock()
	_, err := s.Close(ctx, action)
	return err
}

func (s *fileSession) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	if _, err := action.Epoch(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	s.mu.Lock()
	for s.closeRun != nil {
		finished := s.closeRun
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return storage.FileActionReceipt{}, ctx.Err()
		case <-finished:
		}
		s.mu.Lock()
	}
	if s.closed {
		receipt, barrier := s.closeReceipt.Clone(), s.closeBarrier
		s.mu.Unlock()
		if receipt.Action == action {
			s.base.mu.Lock()
			closing := s.base.closing
			s.base.mu.Unlock()
			if closing {
				return receipt, nil
			}
			return receipt, s.confirmReceipt(ctx, "close-session", receipt, barrier, nil)
		}
		return s.remote.Close(ctx, action)
	}
	if s.cleanupAction == "" {
		s.cleanupAction = action
	}
	s.closing = true
	s.stop()
	s.closeRun = make(chan struct{})
	defer func() { s.mu.Lock(); close(s.closeRun); s.closeRun = nil; s.mu.Unlock() }()
	for s.active != 0 {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return storage.FileActionReceipt{}, ctx.Err()
		case <-changed:
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	receipt, barrier, err := s.remote.CloseWithBarrier(ctx, action)
	if err != nil {
		return receipt, err
	}
	s.mu.Lock()
	s.closed = true
	s.closeReceipt = receipt.Clone()
	s.closeBarrier = barrier
	s.mu.Unlock()
	s.base.mu.Lock()
	delete(s.base.fileSessions, s)
	closing := s.base.closing
	s.base.mu.Unlock()
	if closing {
		return receipt, nil
	}
	return receipt, s.confirmReceipt(ctx, "close-session", receipt, barrier, nil)
}

const replicatedEffects = storage.EffectCreated | storage.EffectContentChanged | storage.EffectMetadataChanged | storage.EffectEntryMoved | storage.EffectEntryDetached

// Control cleanup remains admitted when the replica cannot serve ordinary I/O.
// Its barrier observation is bounded by the session's active control operations.
func (s *fileSession) perform(ctx context.Context, op string, ordinary bool, send func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error)) (storage.FileActionReceipt, error) {
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if ordinary || !closing {
		operation, done, err := s.begin(ctx, ordinary)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		defer done()
		ctx = operation
	}
	var err error
	var confirmation *confirmation
	if ordinary {
		confirmation, err = s.base.expect(ctx, op, "")
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		defer s.base.forget(confirmation)
	}
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, confirmationContextError(op, "", ctx)
	}
	receipt, barrier, callErr := send(ctx)
	s.base.mu.Lock()
	volumeClosing := s.base.closing
	s.base.mu.Unlock()
	if !ordinary && volumeClosing {
		return receipt, callErr
	}
	confirmErr := s.confirmReceipt(ctx, op, receipt, barrier, confirmation)
	return receipt, errors.Join(callErr, confirmErr)
}

func (s *fileSession) confirmReceipt(ctx context.Context, op string, receipt storage.FileActionReceipt, barrier *httprest.MutationBarrier, pending *confirmation) error {
	if receipt.Effects&replicatedEffects == 0 {
		return nil
	}
	if barrier == nil {
		return fmt.Errorf("file operation returned effects without a replication barrier: %w", syscall.EIO)
	}
	if pending == nil {
		pending = new(confirmation)
	}
	if err := s.base.setBarrier(pending, *barrier); err != nil {
		return err
	}
	return s.base.await(ctx, op, "", pending)
}

func (s *fileSession) Retain(ctx context.Context, req storage.RetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "Retain", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.RetainWithBarrier(ctx, req, action)
	})
}

func (s *fileSession) RetainAt(ctx context.Context, req storage.RetainAtRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "RetainAt", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.RetainAtWithBarrier(ctx, req, action)
	})
}

func (s *fileSession) CreateAndRetainAt(ctx context.Context, req storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "CreateAndRetainAt", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.CreateAndRetainAtWithBarrier(ctx, req, action)
	})
}

func (s *fileSession) ResetAndRetainAt(ctx context.Context, req storage.ResetAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "ResetAndRetainAt", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.ResetAndRetainAtWithBarrier(ctx, req, action)
	})
}

func (s *fileSession) ReplaceAndRetainAt(ctx context.Context, req storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "ReplaceAndRetainAt", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.ReplaceAndRetainAtWithBarrier(ctx, req, action)
	})
}

func (s *fileSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	return fileCall(ctx, s, false, func(ctx context.Context) (storage.File, error) {
		file, err := s.remote.Reference(ctx, id)
		if err != nil {
			return nil, err
		}
		remote, ok := file.(httprest.FileWithBarrier)
		if !ok {
			return nil, fmt.Errorf("retained reference has no replication barriers: %w", syscall.EIO)
		}
		return &retainedFile{session: s, remote: remote}, nil
	})
}
func (s *fileSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	return fileCall(ctx, s, true, func(ctx context.Context) (storage.FileObservation, error) { return s.remote.StatNode(ctx, id, options) })
}
func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, req storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "set-node-attr", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.SetNodeAttrWithBarrier(ctx, id, req, action)
	})
}
func (s *fileSession) QueryAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "query-action", false, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.QueryActionWithBarrier(ctx, action)
	})
}
func (s *fileSession) CancelAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.perform(ctx, "cancel-action", false, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return s.remote.CancelActionWithBarrier(ctx, action)
	})
}
func (s *fileSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileCall(ctx, s, false, func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.remote.RetireRangeOwner(ctx, owner, action)
	})
}
func (s *fileSession) observedStatus(status storage.FileSessionStatus, err error) (storage.FileSessionStatus, error) {
	if err == nil {
		s.mu.Lock()
		s.actionEpoch = max(s.actionEpoch, status.ActionEpoch)
		s.mu.Unlock()
	}
	return status, err
}
func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := fileCall(ctx, s, false, s.remote.Renew)
	return s.observedStatus(status, err)
}
func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := fileCall(ctx, s, false, s.remote.Status)
	return s.observedStatus(status, err)
}
