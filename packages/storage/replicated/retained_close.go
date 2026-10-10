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

type retainedClose struct {
	mu                 sync.Mutex
	closed             bool
	closedByParent     bool
	closeResult        storage.ReferenceCloseResult
	closeErr           error
	closeBarrier       *httprest.MutationBarrier
	closeAuthorityErr  error
	closeOriginalErr   error
	closeRun           chan struct{}
	closeAttempt       storage.CloseAttempt
	closeAttemptSet    bool
	activeReserved     bool
	closeCurrentEpoch  uint64
	closeProofUnstored bool
	implicitClose      bool
	implicitSuccessor  bool
	rejectedCandidate  *closeCandidateState
}

type closeCandidateState struct {
	attempt           storage.CloseAttempt
	attemptSet        bool
	result            storage.ReferenceCloseResult
	barrier           *httprest.MutationBarrier
	authorityErr      error
	originalErr       error
	implicitClose     bool
	implicitSuccessor bool
	currentEpoch      uint64
	proofUnstored     bool
	rejectionErr      error
}

func (c *retainedClose) restoreRejectedCandidate(session *fileSession, previous closeCandidateState) {
	c.closeAttempt = previous.attempt
	c.closeAttemptSet = previous.attemptSet
	c.closeResult = previous.result
	c.closeBarrier = previous.barrier
	c.closeAuthorityErr = previous.authorityErr
	c.closeOriginalErr = previous.originalErr
	c.implicitClose = previous.implicitClose
	c.implicitSuccessor = previous.implicitSuccessor
	c.closeCurrentEpoch = previous.currentEpoch
	c.closeProofUnstored = previous.proofUnstored
	c.rejectedCandidate = nil
	if c.activeReserved {
		session.releaseCloseAttempt()
		c.activeReserved = false
	}
}

type closeBarrierAuthority interface {
	CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
}

type closeActionBarrier interface {
	CloseWithActionAndBarrier(context.Context, storage.CloseAttempt) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
}

func persistentCloseError(err error) error {
	return closeErrorPart(err, true)
}

func closeErrorPart(err error, semantic bool) error {
	var pending *httprest.CloseBarrierPendingError
	var unsettled *storage.CloseSettlementError
	if !errors.As(err, &pending) && !errors.As(err, &unsettled) {
		return err
	}
	switch err := err.(type) {
	case *httprest.CloseBarrierPendingError:
		if semantic {
			return closeErrorPart(err.SemanticErr, semantic)
		}
		return closeErrorPart(err.Cause, semantic)
	case *storage.CloseSettlementError:
		if semantic {
			return closeErrorPart(err.SemanticErr, semantic)
		}
		return closeErrorPart(err.Cause, semantic)
	case interface{ Unwrap() []error }:
		children := err.Unwrap()
		parts := make([]error, len(children))
		for i, child := range children {
			parts[i] = closeErrorPart(child, semantic)
		}
		return errors.Join(parts...)
	case interface{ Unwrap() error }:
		return closeErrorPart(err.Unwrap(), semantic)
	default:
		return err
	}
}

func lowerCloseSettlement(err error) *storage.CloseSettlementError {
	var lower *storage.CloseSettlementError
	if !errors.As(err, &lower) {
		return nil
	}
	state, cause := lower.State, closeErrorPart(lower.Cause, false)
	if invalid := lower.Check(); invalid != nil {
		state = storage.CloseSettlementUnknown
		cause = errors.Join(cause, fmt.Errorf("invalid lower close settlement: %w", invalid), syscall.EIO)
	}
	return &storage.CloseSettlementError{State: state, SemanticErr: persistentCloseError(err), Cause: cause}
}

func settledAuthorityBarrier(barrier *httprest.MutationBarrier, err error) *httprest.MutationBarrier {
	var lower *storage.CloseSettlementError
	if errors.As(err, &lower) {
		return nil
	}
	return barrier
}

func replayHistoricalClose(ctx context.Context, remote closeActionBarrier, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	result, _, err := remote.CloseWithActionAndBarrier(ctx, attempt)
	if result.Released {
		return result, &storage.CloseSettlementError{
			State: storage.CloseSettlementUnknown, SemanticErr: persistentCloseError(err),
			Cause: errors.Join(closeErrorPart(err, false), fmt.Errorf("historical close replay contradicts a later attempt: %w", syscall.EIO)),
		}
	}
	return result, errors.Join(err, result.Check(err))
}

func retiredSameGenerationClose(ctx context.Context, remote storage.ReferenceCloseActions, current, requested storage.CloseAttempt) error {
	currentEpoch, currentErr := current.Action.Epoch()
	requestedEpoch, requestedErr := requested.Action.Epoch()
	if currentErr != nil || requestedErr != nil || requestedEpoch >= currentEpoch {
		return syscall.EINVAL
	}
	receipt, err := remote.QueryCloseAttempt(ctx, requested)
	if err != nil {
		return err
	}
	if receipt.Action != requested.Action || receipt.Operation != storage.OpFileClose {
		return fmt.Errorf("close history query changed reference identity: %w", syscall.EIO)
	}
	if receipt.Outcome == storage.FileActionRetired {
		return syscall.ESTALE
	}
	if receipt.Outcome == storage.FileActionNotExecuted {
		return syscall.EINVAL
	}
	return syscall.EIO
}

func (c *retainedClose) closeWithAction(ctx context.Context, session *fileSession, remote closeBarrierAuthority, kind string, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if err := attempt.Check(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	if _, ok := remote.(closeActionBarrier); !ok {
		return storage.ReferenceCloseResult{}, syscall.EOPNOTSUPP
	}
	if _, ok := remote.(storage.ReferenceCloseActions); !ok {
		return storage.ReferenceCloseResult{}, syscall.EOPNOTSUPP
	}
	return c.close(ctx, session, remote, kind, &attempt)
}

func queryCloseAttempt(ctx context.Context, remote any, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if err := attempt.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	actions, err := optional[storage.ReferenceCloseActions](remote)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	return actions.QueryCloseAttempt(ctx, attempt)
}

func closeOwnerStatus(ctx context.Context, remote any) (storage.CloseOwnerStatus, error) {
	actions, err := optional[storage.ReferenceCloseActions](remote)
	if err != nil {
		return storage.CloseOwnerStatus{}, err
	}
	return actions.CloseOwnerStatus(ctx)
}

func (c *retainedClose) ownerStatus(ctx context.Context, session *fileSession, remote any) (storage.CloseOwnerStatus, error) {
	status, err := closeOwnerStatus(ctx, remote)
	if err != nil || !status.Ready {
		return status, err
	}
	c.mu.Lock()
	localReady := !c.closed && !c.implicitClose && !c.activeReserved && !c.closeProofUnstored &&
		(!c.closeAttemptSet || (!c.closeResult.Released && (c.closeResult.Determined || c.closeCurrentEpoch != 0)))
	c.mu.Unlock()
	if !localReady || !session.canReserveCloseAttempt() {
		status.Ready = false
	}
	return status, nil
}

func (c *retainedClose) close(ctx context.Context, session *fileSession, remote closeBarrierAuthority, kind string, attempt *storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	requestedAction := attempt != nil
	c.mu.Lock()
	for c.closeRun != nil {
		run := c.closeRun
		c.mu.Unlock()
		select {
		case <-run:
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
		c.mu.Lock()
	}
	if requestedAction && c.rejectedCandidate != nil && c.closeAttemptSet && *attempt == c.closeAttempt {
		receipt, queryErr := remote.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, *attempt)
		if queryErr != nil {
			original := c.rejectedCandidate.rejectionErr
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, errors.Join(original, queryErr)
		}
		if receipt.Action != attempt.Action || receipt.Operation != storage.OpFileClose {
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, fmt.Errorf("rejected close query changed reference identity: %w", syscall.EIO)
		}
		if receipt.Outcome == storage.FileActionNotExecuted {
			previous := *c.rejectedCandidate
			c.restoreRejectedCandidate(session, previous)
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, previous.rejectionErr
		}
		if receipt.Outcome != storage.FileActionPending && receipt.Outcome != storage.FileActionCompleted {
			original := c.rejectedCandidate.rejectionErr
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, errors.Join(original, syscall.EAGAIN)
		}
	}
	if attempt != nil && c.closeAttemptSet && c.closeAttempt != *attempt {
		session.mu.Lock()
		retired := session.closed && session.closeResult.Released && session.closeResult.Determined
		session.mu.Unlock()
		if retired {
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, syscall.ESTALE
		}
	}
	if c.closed {
		if requestedAction && c.implicitSuccessor && c.closeAttemptSet && *attempt == c.closeAttempt {
			c.mu.Unlock()
			return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
		}
		if requestedAction && !c.closeAttemptSet {
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, syscall.EINVAL
		}
		if requestedAction && c.closedByParent {
			if attempt.Generation > c.closeAttempt.Generation {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if attempt.Generation == c.closeAttempt.Generation && *attempt != c.closeAttempt {
				if !session.hasCloseTombstone(c, *attempt) {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EINVAL
				}
				c.mu.Unlock()
				return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
			}
			c.mu.Unlock()
			result, barrier, err := remote.(closeActionBarrier).CloseWithActionAndBarrier(ctx, *attempt)
			if result.Released {
				_, err = session.base.confirmReleasedClose(ctx, "close-"+kind, barrier, err)
			}
			return result, errors.Join(err, result.Check(err))
		}
		if attempt != nil && c.closeAttemptSet && c.closeAttempt != *attempt {
			if attempt.Generation < c.closeAttempt.Generation {
				c.mu.Unlock()
				return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
			}
			if attempt.Generation == c.closeAttempt.Generation {
				// Only an ID that reached the native epoch fence may be replayed
				// after a replacement takes over the same generation.
				if session.hasCloseTombstone(c, *attempt) {
					c.mu.Unlock()
					return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
				}
				current := c.closeAttempt
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, retiredSameGenerationClose(ctx, remote.(storage.ReferenceCloseActions), current, *attempt)
			}
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, syscall.EINVAL
		}
		result, err := c.closeResult, c.closeErr
		c.mu.Unlock()
		return result, err
	}
	if requestedAction && c.implicitSuccessor && c.closeAttemptSet && *attempt == c.closeAttempt {
		c.mu.Unlock()
		return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
	}
	previous := c.closeResult
	previousBarrier := c.closeBarrier
	previousAuthorityErr := c.closeAuthorityErr
	previousOriginalErr := c.closeOriginalErr
	priorCandidate := closeCandidateState{
		attempt: c.closeAttempt, attemptSet: c.closeAttemptSet,
		result: previous, barrier: previousBarrier, authorityErr: previousAuthorityErr, originalErr: previousOriginalErr,
		implicitClose: c.implicitClose, implicitSuccessor: c.implicitSuccessor,
		currentEpoch: c.closeCurrentEpoch, proofUnstored: c.closeProofUnstored,
	}
	adoptingImplicit := false
	freshCandidate := false
	freshCurrentEpoch := uint64(0)
	if attempt != nil {
		if c.closeAttemptSet && c.closeAttempt != *attempt {
			if c.closeAttempt.Action == attempt.Action {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if attempt.Generation < c.closeAttempt.Generation {
				c.mu.Unlock()
				return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
			}
			if attempt.Generation == c.closeAttempt.Generation {
				if session.hasCloseTombstone(c, *attempt) {
					c.mu.Unlock()
					return replayHistoricalClose(ctx, remote.(closeActionBarrier), *attempt)
				}
				currentEpoch, currentErr := c.closeAttempt.Action.Epoch()
				requestedEpoch, requestedErr := attempt.Action.Epoch()
				if currentErr == nil && requestedErr == nil && requestedEpoch < currentEpoch {
					current := c.closeAttempt
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, retiredSameGenerationClose(ctx, remote.(storage.ReferenceCloseActions), current, *attempt)
				}
			}
			if previous.Released {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			status, statusErr := remote.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
			if statusErr != nil {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, statusErr
			}
			if status.Released {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if status.Current != nil && *status.Current != *attempt {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EBUSY
			}
			if status.Current != nil {
				adoptingImplicit = c.implicitClose
				if attempt.Generation == c.closeAttempt.Generation {
					if !session.hasCloseTombstone(c, c.closeAttempt) {
						if !c.activeReserved && !c.closeProofUnstored {
							c.mu.Unlock()
							return storage.ReferenceCloseResult{}, fmt.Errorf("same-generation close adoption has no old attempt proof: %w", syscall.EIO)
						}
						if err := session.reserveCloseTombstone(ctx, c, remote.(storage.ReferenceCloseActions), c.closeAttempt); err != nil {
							c.mu.Unlock()
							return storage.ReferenceCloseResult{}, err
						}
						if c.activeReserved {
							session.releaseCloseAttempt()
							c.activeReserved = false
						}
						c.closeProofUnstored = false
					}
				} else {
					if c.activeReserved {
						session.releaseCloseAttempt()
						c.activeReserved = false
					}
					c.closeProofUnstored = false
				}
			} else {
				if !status.Ready {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EAGAIN
				}
				if attempt.Generation != status.NextGeneration {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EINVAL
				}
				if attempt.Generation == c.closeAttempt.Generation {
					epoch, epochErr := attempt.Action.Epoch()
					if c.closeCurrentEpoch == 0 || epochErr != nil || epoch != c.closeCurrentEpoch {
						c.mu.Unlock()
						return storage.ReferenceCloseResult{}, syscall.EINVAL
					}
				} else if c.closeCurrentEpoch != 0 {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EINVAL
				}
				if !previous.Determined && c.closeCurrentEpoch == 0 {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EBUSY
				}
				freshCandidate = true
				freshCurrentEpoch = status.CurrentEpoch
			}
		} else if !c.closeAttemptSet {
			status, statusErr := remote.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
			if statusErr != nil {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, statusErr
			}
			if status.Released {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if status.Current != nil {
				if *status.Current != *attempt {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, syscall.EBUSY
				}
				adoptingImplicit = c.implicitClose
			} else if !status.Ready {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EAGAIN
			} else if attempt.Generation != status.NextGeneration {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			} else {
				freshCandidate = true
				freshCurrentEpoch = status.CurrentEpoch
			}
		}
		if freshCandidate {
			epoch, epochErr := attempt.Action.Epoch()
			if epochErr != nil || epoch > freshCurrentEpoch {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			receipt, queryErr := remote.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, *attempt)
			if queryErr != nil {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, queryErr
			}
			if receipt.Action != attempt.Action || receipt.Operation != storage.OpFileClose {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, fmt.Errorf("close candidate query changed reference identity: %w", syscall.EIO)
			}
			if receipt.Outcome != storage.FileActionNotExecuted {
				c.mu.Unlock()
				if receipt.Outcome == storage.FileActionRetired {
					return storage.ReferenceCloseResult{}, syscall.ESTALE
				}
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
		}
		if c.implicitClose && !adoptingImplicit {
			c.mu.Unlock()
			return storage.ReferenceCloseResult{}, syscall.EINVAL
		}
		if !c.closeAttemptSet || c.closeAttempt != *attempt {
			if c.closeProofUnstored {
				if err := session.reserveCloseTombstone(ctx, c, remote.(storage.ReferenceCloseActions), c.closeAttempt); err != nil {
					c.mu.Unlock()
					return storage.ReferenceCloseResult{}, errors.Join(syscall.EAGAIN, err)
				}
				c.closeProofUnstored = false
			}
			session.mu.Lock()
			retired := session.closed && session.closeResult.Released && session.closeResult.Determined
			session.mu.Unlock()
			if retired {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if c.activeReserved {
				unstored := c.closeProofUnstored
				c.mu.Unlock()
				if unstored {
					return storage.ReferenceCloseResult{}, syscall.EAGAIN
				}
				return storage.ReferenceCloseResult{}, fmt.Errorf("previous close attempt still holds admission: %w", syscall.EIO)
			}
			if err := session.reserveCloseAttempt(); err != nil {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, err
			}
			c.activeReserved = true
			c.closeCurrentEpoch = 0
			c.closeProofUnstored = false
			c.implicitSuccessor = false
		}
		c.closeAttempt, c.closeAttemptSet, c.implicitClose = *attempt, true, false
	} else if c.closeAttemptSet {
		if !previous.Released {
			status, statusErr := remote.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
			if statusErr != nil {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, statusErr
			}
			if status.Current != nil {
				if *status.Current != c.closeAttempt {
					adopt := *status.Current
					c.mu.Unlock()
					return c.close(ctx, session, remote, kind, &adopt)
				}
				attempt = &c.closeAttempt
			} else if status.Ready {
				c.implicitClose = true
				c.implicitSuccessor = true
			} else if status.Released {
				c.implicitClose = true
				c.implicitSuccessor = true
			} else {
				c.mu.Unlock()
				return storage.ReferenceCloseResult{}, syscall.EAGAIN
			}
		} else {
			attempt = &c.closeAttempt
		}
	} else {
		c.implicitClose = true
	}
	c.closeRun = make(chan struct{})
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		close(c.closeRun)
		c.closeRun = nil
		c.mu.Unlock()
	}()
	session.mu.Lock()
	sessionClosed := session.closing || session.closed
	parentReleased := session.closed && session.closeResult.Released && session.closeResult.Determined
	parentErr := session.closeErr
	session.mu.Unlock()
	if parentReleased && !previous.Released && !requestedAction {
		// A settled authority session close proves release of every child.
		// Individual action receipts still belong to their original IDs.
		result := storage.ReferenceCloseResult{Released: true, Determined: true}
		c.mu.Lock()
		c.closed = true
		c.closedByParent = true
		c.rejectedCandidate = nil
		c.closeResult = result
		c.closeErr = parentErr
		if c.activeReserved {
			session.releaseCloseAttempt()
			c.activeReserved = false
		}
		c.mu.Unlock()
		return result, parentErr
	}
	var result storage.ReferenceCloseResult
	var barrier *httprest.MutationBarrier
	var authorityErr error
	if previous.Released && previousBarrier != nil {
		result, barrier, authorityErr = previous, previousBarrier, previousAuthorityErr
	} else {
		call := func(ctx context.Context) (storage.ReferenceCloseResult, error) {
			if attempt != nil {
				remote := remote.(interface {
					CloseWithActionAndBarrier(context.Context, storage.CloseAttempt) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
				})
				result, barrier, authorityErr = remote.CloseWithActionAndBarrier(ctx, *attempt)
			} else {
				result, barrier, authorityErr = remote.CloseWithBarrier(ctx)
			}
			return result, authorityErr
		}
		if sessionClosed || previous.Released {
			result, authorityErr = call(ctx)
		} else {
			result, authorityErr = fileCall(ctx, session, false, call)
		}
	}
	if previous.Released && !result.Released {
		return previous, &storage.CloseSettlementError{
			State: storage.CloseSettlementUnknown, SemanticErr: persistentCloseError(previousOriginalErr),
			Cause: errors.Join(closeErrorPart(authorityErr, false), fmt.Errorf("released %s close lost barrier replay: %w", kind, syscall.EIO)),
		}
	}
	rawAuthorityErr := authorityErr
	if previous.Released && previousBarrier == nil {
		authorityErr = errors.Join(persistentCloseError(previousOriginalErr), authorityErr)
	}
	err := errors.Join(authorityErr, result.Check(authorityErr))
	if !result.Released {
		if freshCandidate && !result.Determined && errors.Is(authorityErr, syscall.EINVAL) {
			receipt, queryErr := remote.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, *attempt)
			if queryErr == nil && receipt.Action == attempt.Action && receipt.Operation == storage.OpFileClose && receipt.Outcome == storage.FileActionNotExecuted {
				c.mu.Lock()
				c.restoreRejectedCandidate(session, priorCandidate)
				c.mu.Unlock()
				return result, err
			}
			if queryErr != nil || receipt.Action != attempt.Action || receipt.Operation != storage.OpFileClose || receipt.Outcome == storage.FileActionUnknown || receipt.Outcome == storage.FileActionRetired {
				priorCandidate.rejectionErr = err
				c.mu.Lock()
				c.rejectedCandidate = &priorCandidate
				c.mu.Unlock()
			}
		}
		c.mu.Lock()
		c.closeResult = result
		if result.Determined {
			c.rejectedCandidate = nil
		}
		if attempt == nil && result.Determined {
			c.implicitClose = false
		}
		var proof *storage.CloseActionNotExecutedError
		if attempt != nil && errors.As(authorityErr, &proof) {
			if proof.CurrentEpoch == 0 {
				c.mu.Unlock()
				return result, errors.Join(err, fmt.Errorf("close proof has no current epoch: %w", syscall.EIO))
			}
			if !session.hasCloseTombstone(c, *attempt) {
				if !c.activeReserved && !c.closeProofUnstored {
					c.mu.Unlock()
					return result, errors.Join(err, fmt.Errorf("close proof has no reserved attempt: %w", syscall.EIO))
				}
				if reserveErr := session.reserveCloseTombstone(ctx, c, remote.(storage.ReferenceCloseActions), *attempt); reserveErr != nil {
					c.closeProofUnstored = true
					c.closeCurrentEpoch = proof.CurrentEpoch
					if c.activeReserved {
						session.releaseCloseAttempt()
						c.activeReserved = false
					}
					c.mu.Unlock()
					return result, errors.Join(err, fmt.Errorf("close tombstone capacity exhausted: %w", reserveErr))
				}
				if c.activeReserved {
					session.releaseCloseAttempt()
					c.activeReserved = false
				}
				c.closeProofUnstored = false
			}
			c.closeCurrentEpoch = proof.CurrentEpoch
		} else if result.Determined && c.activeReserved {
			session.releaseCloseAttempt()
			c.activeReserved = false
		}
		c.mu.Unlock()
		return result, err
	}
	settled, err := session.base.confirmReleasedClose(ctx, "close-"+kind, barrier, authorityErr)
	c.mu.Lock()
	c.closeResult = result
	c.closeBarrier = settledAuthorityBarrier(barrier, authorityErr)
	c.closeAuthorityErr = rawAuthorityErr
	if !previous.Released {
		c.closeOriginalErr = rawAuthorityErr
	}
	c.rejectedCandidate = nil
	if c.activeReserved {
		session.releaseCloseAttempt()
		c.activeReserved = false
	}
	if settled {
		c.closed = true
		c.closeErr = err
	}
	c.mu.Unlock()
	return result, err
}
