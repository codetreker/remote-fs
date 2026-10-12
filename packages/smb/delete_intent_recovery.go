package smb

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type deleteIntentAttempt struct {
	id               storage.DeleteIntentID
	outcome          storage.DeleteIntentOutcome
	action           storage.FileActionID
	priorUnknown     storage.FileActionID
	terminalObserved bool
	issued           bool
	acked            bool
}

type deleteRecoverySession struct {
	raw         storage.FileSession
	backend     storage.BackendIdentityResult
	closing     bool
	closeErr    error
	semanticErr error
}

type deleteRecoveryState struct {
	busy          atomic.Bool
	ready         atomic.Bool
	cleanupOwners atomic.Int32
	current       *deleteRecoverySession
	retired       *deleteRecoverySession
	extraSlot     bool
	cursor        storage.DeleteIntentCursor
	page          storage.DeleteIntentPage
	index         int
	attempt       *deleteIntentAttempt
	complete      bool
	locals        []*deleteIntentAttempt
	localIndex    int
	localCaptured bool
}

func (e *Export) addDeleteStatus(out *Status) {
	if e.share.DeleteIntentOwner == "" {
		return
	}
	if e.deleteRecovery.ready.Load() {
		out.DeleteRecoveryReadyExports++
	} else {
		out.DeleteRecoveryPendingExports++
	}
	out.DeleteRecoveryCleanupOwners += int(e.deleteRecovery.cleanupOwners.Load())
}

func (e *Export) deletePrincipal(ctx context.Context) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	p, ok := PrincipalFromContext(ctx)
	if !ok || !p.Valid() {
		return Principal{}, ErrIdentityDenied
	}
	if err := e.server.config.AuthorizeIdentity.AuthorizeIdentity(ctx, p); err != nil {
		return Principal{}, err
	}
	return p, nil
}

func (e *Export) authorizeDelete(ctx context.Context, op storage.Operation) error {
	return e.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: e.share.Volume, Operation: op})
}

func (e *Export) queryDeleteIntent(ctx context.Context, raw storage.FileSession, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	if err := e.authorizeDelete(ctx, storage.OpFileQueryDeleteIntent); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	actions, ok := raw.(storage.FileActions)
	if !ok {
		return storage.DeleteIntentStatus{}, syscall.EOPNOTSUPP
	}
	status, err := actions.QueryDeleteIntent(ctx, e.share.DeleteIntentOwner, id)
	if err == nil && (status.Check() != nil || status.ID != id) {
		err = syscall.EIO
	}
	return status, err
}

func (e *Export) deleteSessionStatus(ctx context.Context, raw storage.FileSession) (storage.FileSessionStatus, error) {
	if err := e.authorizeDelete(ctx, storage.OpFileStatus); err != nil {
		return storage.FileSessionStatus{}, err
	}
	status, err := raw.Status(ctx)
	if err == nil && (status.Epoch == "" || status.Revision == 0 || status.ActionEpoch == 0 || status.Remaining <= 0 || status.Fenced || status.Retired) {
		err = syscall.ESTALE
	}
	return status, err
}

// A terminal witness and an issued ACK permit recovery of a missing record.
// Missing records alone never establish whether an open accepted an intent.
func (e *Export) ackDeleteIntent(ctx context.Context, raw storage.FileSession, principal Principal, attempt *deleteIntentAttempt) (ackErr error) {
	defer func() { e.logDeleteIntent("ack", attempt.id, attempt.action, attempt.outcome, ackErr) }()
	ctx = WithPrincipal(ctx, principal)
	if _, err := e.deletePrincipal(ctx); err != nil {
		return err
	}
	if e.deleteIntentOwned(attempt.id) {
		return ErrBusy
	}
	status, err := e.queryDeleteIntent(ctx, raw, attempt.id)
	if err != nil {
		return err
	}
	if status.Outcome == storage.DeleteIntentUnknown && attempt.terminalObserved && attempt.issued {
		attempt.acked = true
		return nil
	}
	if status.Outcome != storage.DeleteIntentCompleted && status.Outcome != storage.DeleteIntentNotExecuted {
		return deleteOutcomeError(status)
	}
	if attempt.terminalObserved && attempt.outcome != status.Outcome {
		return syscall.EIO
	}
	attempt.terminalObserved, attempt.outcome = true, status.Outcome
	actions, ok := raw.(storage.FileActions)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if attempt.issued {
		if err := e.authorizeDelete(ctx, storage.OpFileQueryAction); err != nil {
			return err
		}
		receipt, err := actions.QueryFileAction(ctx, attempt.action)
		if err != nil && !errors.Is(err, syscall.ESTALE) {
			return err
		}
		if err == nil && (receipt.Check() != nil || receipt.Action != attempt.action || receipt.Operation != "" && receipt.Operation != storage.OpFileAcknowledgeDeleteIntent) {
			return syscall.EIO
		}
		if err == nil && receipt.Outcome == storage.FileActionCompleted {
			return syscall.EIO
		}
		if err != nil || receipt.Outcome == storage.FileActionUnknown || receipt.Outcome == storage.FileActionRetired || receipt.Outcome == storage.FileActionNotExecuted {
			if attempt.priorUnknown == "" {
				attempt.priorUnknown = attempt.action
			}
			attempt.action = ""
		}
	}
	if attempt.action == "" {
		current, err := e.deleteSessionStatus(ctx, raw)
		if err != nil {
			return err
		}
		attempt.action, err = storage.NewFileActionID(current.ActionEpoch)
		if err != nil {
			return err
		}
	}
	if err := e.authorizeDelete(ctx, storage.OpFileAcknowledgeDeleteIntent); err != nil {
		return err
	}
	attempt.issued = true
	err = actions.AcknowledgeDeleteIntent(ctx, storage.AcknowledgeDeleteIntentCommand{Action: attempt.action, Owner: e.share.DeleteIntentOwner, Intent: attempt.id})
	if err == nil {
		attempt.acked = true
	}
	return err
}

func deleteOutcomeError(status storage.DeleteIntentStatus) error {
	if status.Outcome == storage.DeleteIntentCleanupFailed {
		return status.Failure
	}
	if status.Outcome == storage.DeleteIntentArmed || status.Outcome == storage.DeleteIntentPending {
		return ErrBusy
	}
	return syscall.EIO
}

func (e *Export) processRecoveredDeleteIntent(ctx context.Context, raw storage.FileSession, principal Principal, attempt *deleteIntentAttempt) (processErr error) {
	status, err := e.queryDeleteIntent(ctx, raw, attempt.id)
	defer func() { e.logDeleteIntent("discovery-intent", attempt.id, attempt.action, status.Outcome, processErr) }()
	if err != nil {
		return err
	}
	switch status.Outcome {
	case storage.DeleteIntentArmed, storage.DeleteIntentPending:
		if attempt.terminalObserved {
			return syscall.EIO
		}
		e.finishRecoveredDeleteIntent(status)
	case storage.DeleteIntentCompleted, storage.DeleteIntentNotExecuted, storage.DeleteIntentUnknown:
		if err := e.ackDeleteIntent(ctx, raw, principal, attempt); err != nil {
			return err
		}
		e.finishRecoveredDeleteIntent(storage.DeleteIntentStatus{ID: attempt.id, Outcome: attempt.outcome})
	default:
		return deleteOutcomeError(status)
	}
	return nil
}

func (r *deleteRecoveryState) resetScan() {
	r.complete, r.cursor, r.page, r.index = false, 0, storage.DeleteIntentPage{}, 0
	r.locals, r.localIndex, r.localCaptured = nil, 0, false
}

func (e *Export) closeRecoverySession(ctx context.Context, owner *deleteRecoverySession) (bool, error) {
	owner.closing = true
	if err := e.authorizeDelete(ctx, storage.OpFileSessionClose); err != nil {
		owner.closeErr = err
		return false, err
	}
	result, err := owner.raw.CloseWithResult(ctx)
	if checkErr := result.Check(err); checkErr != nil {
		err = errors.Join(err, checkErr)
		owner.closeErr = err
		return false, err
	}
	var settlement *storage.CloseSettlementError
	settled := result.Released && !errors.As(err, &settlement)
	if settlement != nil && owner.semanticErr == nil {
		owner.semanticErr = settlement.SemanticErr
	}
	if settled {
		err = errors.Join(owner.semanticErr, err)
	}
	owner.closeErr = err
	return settled, err
}

func (e *Export) updateDeleteRecoveryOwners() {
	r := &e.deleteRecovery
	count := int32(0)
	if r.current != nil {
		count++
	}
	if r.retired != nil {
		count++
	}
	r.cleanupOwners.Store(count)
}

func (e *Export) retryRetiredDeleteRecovery(ctx context.Context) error {
	r := &e.deleteRecovery
	if r.retired == nil {
		return nil
	}
	settled, err := e.closeRecoverySession(ctx, r.retired)
	if settled {
		r.retired = nil
		if r.extraSlot {
			e.server.releaseDeleteRecoverySlot()
			r.extraSlot = false
		}
		e.updateDeleteRecoveryOwners()
	}
	return err
}

func (e *Export) prepareDeleteRecoverySession(ctx context.Context, backend storage.BackendIdentityResult) error {
	r := &e.deleteRecovery
	if r.current != nil && r.current.backend != backend {
		settled, err := e.closeRecoverySession(ctx, r.current)
		if settled {
			r.current = nil
		} else {
			if r.retired != nil {
				return errors.Join(r.retired.closeErr, err)
			}
			if err := e.server.reserveDeleteRecoverySlot(); err != nil {
				return errors.Join(r.current.closeErr, err)
			}
			r.extraSlot = true
			r.retired, r.current = r.current, nil
		}
		if r.complete {
			r.resetScan()
		}
		e.updateDeleteRecoveryOwners()
		if settled && err != nil {
			return err
		}
	}
	if r.current == nil {
		if err := e.authorizeDelete(ctx, storage.OpFileSessionOpen); err != nil {
			return err
		}
		raw, err := e.share.Backend.NewFileSession(ctx, e.server.config.Limits.FileSession)
		if !interfaceNil(raw) {
			r.current = &deleteRecoverySession{raw: raw, backend: backend}
			e.updateDeleteRecoveryOwners()
		}
		if err == nil && r.current == nil {
			err = syscall.EIO
		}
		if err != nil {
			if r.current != nil {
				settled, closeErr := e.closeRecoverySession(ctx, r.current)
				if settled {
					r.current = nil
					e.updateDeleteRecoveryOwners()
				}
				return errors.Join(err, closeErr)
			}
			return err
		}
	}
	if r.current.closing {
		settled, err := e.closeRecoverySession(ctx, r.current)
		if settled {
			r.current = nil
			e.updateDeleteRecoveryOwners()
			if r.complete && err == nil {
				r.ready.Store(true)
				r.resetScan()
			}
		}
		if err == nil {
			return syscall.EAGAIN
		}
		return err
	}
	raw := r.current.raw
	actions, ok := raw.(storage.FileActions)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := actions.CheckFileActions(); err != nil {
		return err
	}
	identity, ok := raw.(storage.FileSessionIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := identity.CheckFileSessionIdentity(); err != nil {
		return err
	}
	if err := e.authorizeDelete(ctx, storage.OpFileBackendIdentity); err != nil {
		return err
	}
	descriptor, err := identity.FileSessionIdentity(ctx)
	if err != nil {
		return err
	}
	if descriptor.Check() != nil || descriptor.Backend != backend {
		return syscall.EIO
	}
	status, err := e.deleteSessionStatus(ctx, raw)
	if err == nil && status.Epoch != descriptor.SessionEpoch {
		err = syscall.EIO
	}
	if errors.Is(err, syscall.ESTALE) {
		r.current.closing = true
	}
	return err
}

// RecoverDeleteIntents performs one finite, bounded pass using the host's
// current principal. EAGAIN retains the exact page and unfinished ACK attempt.
func (e *Export) RecoverDeleteIntents(ctx context.Context) (recoverErr error) {
	if e.share.DeleteIntentOwner == "" {
		return syscall.EOPNOTSUPP
	}
	r := &e.deleteRecovery
	if !r.busy.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer r.busy.Store(false)
	defer func() {
		phase := "discovery-failed"
		if r.ready.Load() {
			phase = "discovery-ready"
		} else if errors.Is(recoverErr, syscall.EAGAIN) {
			phase = "discovery-partial"
		}
		e.logDeleteIntent(phase, "", "", 0, recoverErr)
	}()
	s := e.server
	s.mu.Lock()
	if s.exports[e.key] != e || !e.published || s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	stopping := e.stopping || s.stopping
	e.active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		e.active--
		s.mu.Unlock()
	}()
	r.ready.Store(false)
	ctx, cancel := context.WithTimeout(ctx, e.server.config.Limits.CleanupTimeout)
	defer cancel()
	principal, err := e.deletePrincipal(ctx)
	if err != nil {
		return err
	}
	if err := e.beginDeleteRecovery(ctx); err != nil {
		return err
	}
	if stopping && r.attempt == nil && e.pendingDeleteError() == nil {
		oldErr := e.retryRetiredDeleteRecovery(ctx)
		if r.current == nil {
			return oldErr
		}
		settled, err := e.closeRecoverySession(ctx, r.current)
		if settled {
			r.current = nil
			e.updateDeleteRecoveryOwners()
		}
		return errors.Join(oldErr, err)
	}
	oldErr := e.retryRetiredDeleteRecovery(ctx)
	if err := e.authorizeDelete(ctx, storage.OpFileBackendIdentity); err != nil {
		return errors.Join(oldErr, err)
	}
	backend, err := e.share.Backend.(storage.BackendIdentity).BackendIdentity(ctx)
	if err == nil && (backend.Check() != nil || backend.Volume != e.share.BackendVolume || backend.RootNodeID != e.share.RootNodeID) {
		err = syscall.EIO
	}
	if err != nil {
		return errors.Join(oldErr, err)
	}
	if err := e.prepareDeleteRecoverySession(ctx, backend); err != nil {
		if r.retired != nil {
			oldErr = errors.Join(oldErr, r.retired.closeErr)
		}
		if errors.Is(err, syscall.EAGAIN) && r.current == nil && r.ready.Load() {
			return oldErr
		}
		return errors.Join(oldErr, err)
	}
	if r.retired != nil {
		oldErr = errors.Join(oldErr, r.retired.closeErr)
	}
	raw := r.current.raw
	actions := raw.(storage.FileActions)
	if !r.localCaptured {
		r.locals, r.localCaptured = e.retainedDeleteAttempts(), true
	}
	for remaining := e.server.config.Limits.MaxUnresolvedOwners; remaining > 0; {
		if err := ctx.Err(); err != nil {
			return errors.Join(oldErr, err)
		}
		if r.localIndex < len(r.locals) {
			if err := e.processRecoveredDeleteIntent(ctx, raw, principal, r.locals[r.localIndex]); err != nil {
				return errors.Join(oldErr, err)
			}
			r.localIndex++
			remaining--
			continue
		}
		if r.index == len(r.page.Intents) {
			if err := e.authorizeDelete(ctx, storage.OpFileListDeleteIntents); err != nil {
				return errors.Join(oldErr, err)
			}
			limit := min(storage.MaxDeleteIntentPageEntries, remaining)
			page, err := actions.ListDeleteIntents(ctx, e.share.DeleteIntentOwner, r.cursor, limit)
			if err == nil && page.Check(e.share.DeleteIntentOwner, r.cursor, limit) != nil {
				err = syscall.EIO
			}
			if err != nil {
				return errors.Join(oldErr, err)
			}
			if len(page.Intents) == 0 {
				if late := e.retainedDeleteAttempts(); len(late) != 0 {
					r.locals, r.localIndex = late, 0
					continue
				}
				r.complete = true
				settled, err := e.closeRecoverySession(ctx, r.current)
				if settled {
					r.current = nil
					e.updateDeleteRecoveryOwners()
					if err == nil {
						r.ready.Store(true)
						r.resetScan()
					}
				}
				return errors.Join(oldErr, err)
			}
			r.page, r.index = page, 0
		}
		id := r.page.Intents[r.index].ID
		if r.attempt == nil {
			r.attempt = &deleteIntentAttempt{id: id}
		}
		if err := e.processRecoveredDeleteIntent(ctx, raw, principal, r.attempt); err != nil {
			return errors.Join(oldErr, err)
		}
		r.attempt = nil
		r.index++
		remaining--
		if r.index == len(r.page.Intents) {
			r.cursor, r.page, r.index = r.page.Next, storage.DeleteIntentPage{}, 0
		}
	}
	return errors.Join(oldErr, syscall.EAGAIN)
}

func (e *Export) closeDeleteRecovery(ctx context.Context) error {
	r := &e.deleteRecovery
	if !r.busy.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer r.busy.Store(false)
	if r.current == nil && r.retired == nil && r.attempt == nil {
		return nil
	}
	principal, err := e.deletePrincipal(ctx)
	if err != nil {
		return err
	}
	oldErr := e.retryRetiredDeleteRecovery(ctx)
	if r.attempt != nil {
		if r.current == nil {
			return errors.Join(oldErr, ErrBusy)
		}
		if err := e.processRecoveredDeleteIntent(ctx, r.current.raw, principal, r.attempt); err != nil {
			return errors.Join(oldErr, err)
		}
		r.attempt = nil
	}
	if r.current == nil {
		return oldErr
	}
	settled, err := e.closeRecoverySession(ctx, r.current)
	if settled {
		r.current = nil
		e.updateDeleteRecoveryOwners()
	}
	return errors.Join(oldErr, err)
}
