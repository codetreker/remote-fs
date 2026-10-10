package httprest

import (
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func closeSemanticError(err error) error {
	if err == nil {
		return nil
	}
	if pending, ok := err.(*CloseBarrierPendingError); ok {
		return pending.SemanticErr
	}
	if pending, ok := err.(*storage.CloseSettlementError); ok {
		return pending.SemanticErr
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var semantic []error
		for _, child := range joined.Unwrap() {
			semantic = append(semantic, closeSemanticError(child))
		}
		return errors.Join(semantic...)
	}
	return err
}

func closeReplayError(previous, cause error) error {
	var pending *CloseBarrierPendingError
	if !errors.As(previous, &pending) {
		return errors.Join(previous, cause)
	}
	if cause == nil {
		cause = syscall.EIO
	}
	return &CloseBarrierPendingError{State: storage.CloseSettlementUnknown, SemanticErr: closeSemanticError(previous), Cause: cause}
}

func closeReplayResultError(result *referenceCloseResult, wasReleased bool, previous, current error) error {
	projected := closeBarrierResultError(result, current)
	if !wasReleased {
		return projected
	}
	semantic := closeSemanticError(previous)
	if semantic == nil {
		return projected
	}
	if pending, ok := projected.(*CloseBarrierPendingError); ok {
		pending.SemanticErr = semantic
		return pending
	}
	return semantic
}

func neutralCloseSettlement(err error) error {
	if pending, ok := err.(*CloseBarrierPendingError); ok {
		state := pending.State
		if state == 0 {
			state = storage.CloseSettlementPending
		}
		return &storage.CloseSettlementError{State: state, SemanticErr: pending.SemanticErr, Cause: pending.Cause}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var projected []error
		for _, child := range joined.Unwrap() {
			projected = append(projected, neutralCloseSettlement(child))
		}
		return errors.Join(projected...)
	}
	return err
}
