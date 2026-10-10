package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type handleState uint32

const (
	handleReserved handleState = iota
	handleLive
	handleCleanupOnly
	handleBarrierOnly
)

type fileHandle struct {
	state           atomic.Uint32
	lastStatus      atomic.Uint32
	treeID          uint32
	sessionID       uint64
	ownerSession    *session
	identity        storage.FileSessionIdentityResult
	nodeID          uint64
	diagnostic      atomic.Pointer[HandleOwnerStatus]
	capacity        *Server
	use             storage.UseClaim
	attempt         *storage.CloseAttempt
	attemptAdmitted bool
	remintEpoch     uint64
	priorAttempt    *storage.CloseAttempt
	released        bool
	published       bool
	semanticErr     error
	id              wire.FileID
	file            storage.File
	node            storage.NodeReference
	pendingOpen     func(context.Context) (bool, error)
	access          uint32
	action          storage.FileActionID
	closing         bool
	requestCloseMu  contextLock
	cleanup         cleanupGate
}

func (t *tree) beginFileWork(s *session) bool {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	if t.fileStopping {
		return false
	}
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired {
		return false
	}
	if t.fileActive == 0 {
		t.fileIdle = make(chan struct{})
	}
	t.fileActive++
	return true
}

func (t *tree) endFileWork() {
	t.fileMu.Lock()
	t.fileActive--
	if t.fileActive == 0 {
		close(t.fileIdle)
		t.fileIdle = nil
	}
	t.fileMu.Unlock()
}

func (t *tree) fenceFileWork() <-chan struct{} {
	t.fileMu.Lock()
	t.fileStopping = true
	idle := t.fileIdle
	t.fileMu.Unlock()
	return idle
}

func waitFileWork(ctx context.Context, idle <-chan struct{}) error {
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A reserved slot exists before a name-changing authority action starts. The
// FileId never transfers to another open, including when the action fails.
func (t *tree) reserveFileHandle(s *session, limit int) (*fileHandle, error) {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	if t.fileStopping || t.fileActive == 0 || len(t.handles) >= limit {
		return nil, syscall.EMFILE
	}
	s.mu.Lock()
	if s.retired || s.id == 0 || s.id == math.MaxUint64 || s.nextFileID >= math.MaxUint64-1 {
		s.mu.Unlock()
		return nil, syscall.EMFILE
	}
	if s.fileIncarnation == 0 {
		var nonce [8]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.fileIncarnation = binary.LittleEndian.Uint64(nonce[:])
		if s.fileIncarnation == 0 || s.fileIncarnation == math.MaxUint64 {
			s.mu.Unlock()
			return nil, syscall.EIO
		}
	}
	s.nextFileID++
	sequence, incarnation := s.nextFileID, s.fileIncarnation
	s.mu.Unlock()
	id := wire.FileID{}
	binary.LittleEndian.PutUint64(id[:8], incarnation)
	binary.LittleEndian.PutUint64(id[8:], sequence)
	handle := &fileHandle{id: id, treeID: t.id, sessionID: s.id, ownerSession: s}
	if t.authority != nil {
		handle.identity = t.authority.identity
	}
	if t.export != nil && t.export.server != nil {
		server := t.export.server
		server.handleMu.Lock()
		limits := server.config.Limits
		if len(server.handleOwners) >= min(limits.MaxHandles, limits.MaxUnresolvedOwners, limits.MaxDiagnosticBytes/diagnosticOwnerBytes) {
			server.handleMu.Unlock()
			return nil, syscall.EMFILE
		}
		if server.handleOwners == nil {
			server.handleOwners = make(map[*fileHandle]struct{})
		}
		server.handleOwners[handle] = struct{}{}
		handle.capacity = server
		server.handleMu.Unlock()
	}
	handle.diagnostic.Store(&HandleOwnerStatus{FileID: id, TreeID: t.id, SessionID: s.id})
	if t.handles == nil {
		t.handles = make(map[wire.FileID]*fileHandle)
	}
	t.handles[id] = handle
	return handle, nil
}

func (t *tree) findFileHandle(id wire.FileID) *fileHandle {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	return t.handles[id]
}

func (t *tree) releaseFileHandle(handle *fileHandle) {
	t.fileMu.Lock()
	if t.handles[handle.id] == handle {
		delete(t.handles, handle.id)
		if handle.capacity != nil {
			handle.capacity.handleMu.Lock()
			delete(handle.capacity.handleOwners, handle)
			handle.capacity.handleMu.Unlock()
		}
	}
	t.fileMu.Unlock()
}

func (h *fileHandle) bindReference(ctx context.Context, t *tree, attr storage.Attr, use storage.UseClaim) error {
	var reference any = h.file
	if h.node != nil {
		reference = h.node
	}
	identity, ok := reference.(storage.ReferenceIdentity)
	if !ok {
		return syscall.EIO
	}
	nodeID, err := identity.ReferenceNodeID()
	if err != nil || attr.ID == 0 || nodeID != attr.ID || h.treeID != t.id || h.identity != t.authority.identity {
		return syscall.EIO
	}
	a := t.authority
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	h.ownerSession.mu.Lock()
	defer h.ownerSession.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if t.fileStopping || h.ownerSession.retired || a.stopping || a.closed || !time.Now().Before(a.deadline) || t.handles[h.id] != h {
		return syscall.ESTALE
	}
	h.nodeID, h.use = attr.ID, use
	h.diagnostic.Store(&HandleOwnerStatus{FileID: h.id, TreeID: h.treeID, SessionID: h.sessionID, NodeID: h.nodeID, OpenAction: h.action})
	h.published = true
	h.state.Store(uint32(handleLive))
	return nil
}

func (h *fileHandle) closeActions() (storage.ReferenceCloseActions, error) {
	var reference any = h.file
	if h.node != nil {
		reference = h.node
	}
	actions, ok := reference.(storage.ReferenceCloseActions)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	return actions, nil
}

func (t *tree) closeFileHandle(ctx context.Context, handle *fileHandle) (closeErr error) {
	defer func() { handle.lastStatus.Store(closeStatusError(closeErr)) }()
	return handle.cleanup.run(ctx, func() error {
		handle.closing = true
		if !handle.released {
			handle.state.Store(uint32(handleCleanupOnly))
		}
		var pendingErr error
		if handle.pendingOpen != nil {
			noReference, recoverErr := handle.pendingOpen(ctx)
			if noReference {
				t.releaseFileHandle(handle)
				return recoverErr
			}
			if handle.file == nil && handle.node == nil {
				return recoverErr
			}
			pendingErr = recoverErr
			handle.pendingOpen = nil
		}
		actions, err := handle.closeActions()
		if err != nil {
			if handle.published {
				return errors.Join(pendingErr, err)
			}
			var result storage.ReferenceCloseResult
			var closeErr error
			switch {
			case handle.file != nil:
				result, closeErr = handle.file.CloseWithResult(ctx)
			case handle.node != nil:
				result, closeErr = handle.node.CloseWithResult(ctx)
			default:
				return errors.Join(pendingErr, syscall.EIO)
			}
			return errors.Join(pendingErr, t.acceptCloseResult(handle, result, closeErr))
		}
		if handle.attempt == nil {
			status, err := actions.CloseOwnerStatus(ctx)
			if err != nil {
				return errors.Join(pendingErr, err)
			}
			if status.Check() != nil {
				return errors.Join(pendingErr, syscall.EIO)
			}
			if status.Current != nil {
				attempt := *status.Current
				handle.attempt = &attempt
				handle.attemptAdmitted = true
			} else {
				if status.Released {
					var result storage.ReferenceCloseResult
					var settled error
					if handle.file != nil {
						result, settled = handle.file.CloseWithResult(ctx)
					} else {
						result, settled = handle.node.CloseWithResult(ctx)
					}
					return errors.Join(pendingErr, t.acceptCloseResult(handle, result, settled))
				}
				if !status.Ready {
					return errors.Join(pendingErr, syscall.EIO)
				}
				action, err := storage.NewFileActionID(status.CurrentEpoch)
				if err != nil {
					return errors.Join(pendingErr, err)
				}
				handle.attempt = &storage.CloseAttempt{Action: action, Generation: status.NextGeneration}
			}
		} else if !handle.released && handle.remintEpoch == 0 {
			receipt, err := actions.QueryCloseAttempt(ctx, *handle.attempt)
			if err != nil {
				return errors.Join(pendingErr, err)
			}
			if receipt.Check() != nil || receipt.Action != handle.attempt.Action || receipt.Operation != "" && receipt.Operation != storage.OpFileClose || receipt.Outcome == storage.FileActionRetired {
				return errors.Join(pendingErr, syscall.EIO)
			}
			if receipt.Operation == storage.OpFileClose && receipt.Outcome != storage.FileActionNotExecuted {
				handle.attemptAdmitted = true
			}
		}
		result, closeErr := handle.runCloseAttempt(ctx, actions)

		if closeErr != nil && !result.Released && !result.Determined && handle.remintEpoch == 0 {
			receipt, queryErr := actions.QueryCloseAttempt(ctx, *handle.attempt)
			if queryErr == nil && receipt.Check() == nil && receipt.Action == handle.attempt.Action && receipt.Outcome == storage.FileActionNotExecuted && (receipt.Operation == "" || receipt.Operation == storage.OpFileClose) {
				status, statusErr := actions.CloseOwnerStatus(ctx)
				if statusErr == nil && status.Check() == nil {
					old := *handle.attempt
					if status.Current != nil {
						handle.priorAttempt = &old
						adopted := *status.Current
						handle.attempt = &adopted
						handle.attemptAdmitted = true
						handle.recordCloseDiagnostic()
						result, closeErr = handle.runCloseAttempt(ctx, actions)
					} else if status.Released {
						handle.priorAttempt = &old
						handle.attempt = nil
						if handle.file != nil {
							result, closeErr = handle.file.CloseWithResult(ctx)
						} else {
							result, closeErr = handle.node.CloseWithResult(ctx)
						}
					} else if status.Ready && status.NextGeneration > old.Generation {
						action, mintErr := storage.NewFileActionID(status.CurrentEpoch)
						if mintErr != nil {
							return errors.Join(pendingErr, mintErr)
						}
						handle.priorAttempt = &old
						handle.attempt = &storage.CloseAttempt{Action: action, Generation: status.NextGeneration}
						handle.attemptAdmitted = false
						handle.recordCloseDiagnostic()
						result, closeErr = handle.runCloseAttempt(ctx, actions)
					}
				}
			}
		}
		return errors.Join(pendingErr, t.acceptCloseResult(handle, result, closeErr))
	})
}

func (h *fileHandle) runCloseAttempt(ctx context.Context, actions storage.ReferenceCloseActions) (storage.ReferenceCloseResult, error) {
	for retry := 0; retry < 2; retry++ {
		if h.remintEpoch != 0 {
			if err := h.remintCloseAttempt(); err != nil {
				return storage.ReferenceCloseResult{}, err
			}
		}
		h.recordCloseDiagnostic()
		result, err := actions.CloseWithAction(ctx, *h.attempt)
		var missed *storage.CloseActionNotExecutedError
		if !errors.As(err, &missed) {
			return result, err
		}
		if h.attemptAdmitted || result.Released || h.released {
			return storage.ReferenceCloseResult{}, errors.Join(err, syscall.EIO)
		}
		epoch, epochErr := h.attempt.Action.Epoch()
		if epochErr != nil || missed.CurrentEpoch <= epoch {
			return storage.ReferenceCloseResult{}, errors.Join(err, syscall.EIO)
		}
		h.remintEpoch = missed.CurrentEpoch
		if retry == 1 {
			return storage.ReferenceCloseResult{}, err
		}
	}
	return storage.ReferenceCloseResult{}, syscall.EIO
}

func (h *fileHandle) remintCloseAttempt() error {
	action, err := storage.NewFileActionID(h.remintEpoch)
	if err != nil {
		return err
	}
	old := *h.attempt
	h.priorAttempt = &old
	h.attempt = &storage.CloseAttempt{Action: action, Generation: old.Generation}
	h.attemptAdmitted = false
	h.remintEpoch = 0
	return nil
}

func (t *tree) acceptCloseResult(handle *fileHandle, result storage.ReferenceCloseResult, closeErr error) error {
	if err := result.Check(closeErr); err != nil {
		return errors.Join(closeErr, err, syscall.EIO)
	}
	if result.Released {
		handle.released = true
		var unsettled *storage.CloseSettlementError
		if errors.As(closeErr, &unsettled) {
			handle.state.Store(uint32(handleBarrierOnly))
			if handle.semanticErr == nil {
				handle.semanticErr = unsettled.SemanticErr
			}
			return closeErr
		}
		t.releaseFileHandle(handle)
		return errors.Join(closeErr, handle.semanticErr)
	}
	if handle.released {
		return errors.Join(closeErr, syscall.EIO)
	}
	if result.Determined && handle.attempt != nil {
		old := *handle.attempt
		handle.priorAttempt = &old
		handle.attempt = nil
		handle.attemptAdmitted = false
		handle.remintEpoch = 0
	}
	return closeErr
}

func (t *tree) closeFileHandles(ctx context.Context) error {
	t.fileMu.Lock()
	handles := make([]*fileHandle, 0, len(t.handles))
	for _, handle := range t.handles {
		handles = append(handles, handle)
	}
	t.fileMu.Unlock()
	var errs []error
	for _, handle := range handles {
		if err := t.closeFileHandle(ctx, handle); err != nil {
			errs = append(errs, err)
		}
	}
	t.fileMu.Lock()
	retained := len(t.handles)
	t.fileMu.Unlock()
	if retained != 0 {
		errs = append(errs, syscall.EIO)
	}
	return errors.Join(errs...)
}
