package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Server) reserveDeleteRecoverySlot() error {
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	limits := s.config.Limits
	if len(s.handleOwners)+s.deleteRecoverySlots >= limits.MaxUnresolvedOwners ||
		len(s.handleOwners)+s.deleteRecoverySlots+s.writes.usedDiagnosticSlots() >= limits.MaxDiagnosticBytes/diagnosticOwnerBytes {
		return syscall.EMFILE
	}
	s.deleteRecoverySlots++
	return nil
}

func (s *Server) releaseDeleteRecoverySlot() {
	s.handleMu.Lock()
	s.deleteRecoverySlots--
	s.handleMu.Unlock()
}

// The caller holds server.mu while removing the export.
func (e *Export) releaseDeleteRecoveryReservation() {
	if e.deleteRecoveryReserved {
		e.server.releaseDeleteRecoverySlot()
		e.deleteRecoveryReserved = false
	}
}

func (e *Export) admitDeleteOpen() error {
	s := e.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.share.DeleteIntentOwner == "" {
		return syscall.EOPNOTSUPP
	}
	if e.stopping || s.stopping || s.stopped {
		return ErrStopped
	}
	if !e.deleteRecovery.ready.Load() || e.deleteRecovery.busy.Load() {
		return syscall.EAGAIN
	}
	if e.deleteActive == 0 {
		e.deleteIdle = make(chan struct{})
	}
	e.deleteActive++
	return nil
}

func (e *Export) endDeleteOpen() {
	e.server.mu.Lock()
	e.deleteActive--
	if e.deleteActive == 0 {
		close(e.deleteIdle)
		e.deleteIdle = nil
	}
	e.server.mu.Unlock()
}

func (e *Export) beginDeleteRecovery(ctx context.Context) error {
	e.server.mu.Lock()
	idle := e.deleteIdle
	e.server.mu.Unlock()
	if idle == nil {
		return ctx.Err()
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *fileHandle) deleteIntentID() storage.DeleteIntentID {
	if h.deleteAttempt == nil {
		return ""
	}
	return h.deleteAttempt.id
}

func (e *Export) deleteIntentOwned(id storage.DeleteIntentID) bool {
	e.server.handleMu.Lock()
	defer e.server.handleMu.Unlock()
	for h := range e.server.handleOwners {
		d := h.diagnostic.Load()
		if h.export == e && d != nil && d.DeleteIntent == id && !h.deleteRefSettled.Load() {
			return true
		}
	}
	return false
}

func (e *Export) finishRecoveredDeleteIntent(status storage.DeleteIntentStatus) {
	e.server.handleMu.Lock()
	defer e.server.handleMu.Unlock()
	for h := range e.server.handleOwners {
		d := h.diagnostic.Load()
		if h.export == e && d != nil && d.DeleteIntent == status.ID && h.deleteRefSettled.Load() {
			current := *d
			current.DeleteOutcome = status.Outcome
			h.diagnostic.Store(&current)
			delete(e.server.handleOwners, h)
		}
	}
}

func (e *Export) pendingDeleteError() error {
	e.server.handleMu.Lock()
	defer e.server.handleMu.Unlock()
	for h := range e.server.handleOwners {
		if d := h.diagnostic.Load(); h.export == e && d != nil && d.DeleteIntent != "" {
			return syscall.EIO
		}
	}
	return nil
}

func (t *tree) settleHandleDeleteIntent(ctx context.Context, h *fileHandle) (settleErr error) {
	if h.deleteAttempt == nil {
		return nil
	}
	e := t.export
	defer func() {
		d := h.diagnostic.Load()
		e.logDeleteIntent("reference-settlement", h.deleteAttempt.id, h.action, d.DeleteOutcome, settleErr)
	}()
	if !e.deleteRecovery.busy.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer e.deleteRecovery.busy.Store(false)
	if !e.deleteRecovery.ready.Load() {
		return ErrBusy
	}
	principal, ok := PrincipalFromContext(ctx)
	if !ok || !principal.Valid() {
		return ErrIdentityDenied
	}
	if err := e.server.config.AuthorizeIdentity.AuthorizeIdentity(ctx, principal); err != nil {
		return err
	}
	if err := e.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: e.share.Volume, Operation: storage.OpFileQueryDeleteIntent}); err != nil {
		return err
	}
	actions := t.authority.raw.(storage.FileActions)
	status, err := actions.QueryDeleteIntent(ctx, e.share.DeleteIntentOwner, h.deleteAttempt.id)
	if err != nil {
		return err
	}
	if status.Check() != nil || status.ID != h.deleteAttempt.id || h.nodeID != 0 && status.NodeID != 0 && status.NodeID != h.nodeID {
		return syscall.EIO
	}
	d := h.diagnostic.Load()
	current := *d
	current.DeleteOutcome = status.Outcome
	h.diagnostic.Store(&current)
	switch status.Outcome {
	case storage.DeleteIntentArmed, storage.DeleteIntentPending:
		e.finishRecoveredDeleteIntent(status)
		return nil
	case storage.DeleteIntentCompleted, storage.DeleteIntentNotExecuted:
		if err := e.ackDeleteIntent(ctx, t.authority.raw, principal, h.deleteAttempt); err != nil {
			return err
		}
		e.finishRecoveredDeleteIntent(status)
		return nil
	case storage.DeleteIntentCleanupFailed:
		return status.Failure
	case storage.DeleteIntentUnknown:
		if h.deleteAttempt.terminalObserved && h.deleteAttempt.issued {
			e.finishRecoveredDeleteIntent(status)
			return nil
		}
	}
	return syscall.EIO
}

// Parent settlement retires the protocol identifier but cannot discard a
// durable intent whose query or ACK still belongs to this endpoint.
func (t *tree) retireHandleDeleteIntent(ctx context.Context, h *fileHandle) error {
	h.deleteRefSettled.Store(true)
	t.releaseFileHandle(h)
	return errors.Join(retirePendingFileIO(h, true, true), t.settleHandleDeleteIntent(ctx, h))
}

// The export's recovery gate protects ACK attempts while the bounded discovery
// pass processes the original retained handle owners.
func (e *Export) retainedDeleteAttempts() []*deleteIntentAttempt {
	e.server.handleMu.Lock()
	defer e.server.handleMu.Unlock()
	var attempts []*deleteIntentAttempt
	for h := range e.server.handleOwners {
		if h.export == e && h.deleteRefSettled.Load() && h.deleteAttempt != nil {
			attempts = append(attempts, h.deleteAttempt)
		}
	}
	return attempts
}

func (e *Export) logDeleteIntent(phase string, id storage.DeleteIntentID, action storage.FileActionID, outcome storage.DeleteIntentOutcome, err error) {
	if e.server.config.Logger != nil {
		e.server.config.Logger.Info("SMB delete intent", "component", "smb", "volume", e.share.Volume,
			"intent", id, "action", action, "phase", phase, "outcome", outcome, "status", statusError(err))
	}
}
