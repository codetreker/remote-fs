package storage

import (
	"errors"
	"syscall"
)

// InlineCloseSettlement promises that every released result from the session's
// CloseWithResult and its returned File and NodeReference CloseWithResult or
// CloseWithAction has completed settlement through the entire backend chain.
// Semantic errors may remain after settlement. Checks must verify every layer;
// physical release alone cannot establish this promise.
type InlineCloseSettlement interface {
	CheckInlineCloseSettlement() error
}

type CloseSettlementState uint8

const (
	CloseSettlementPending CloseSettlementState = iota + 1
	CloseSettlementUnknown
)

// CloseSettlementError accompanies confirmed physical release while an adapter
// still owes settlement. Pending means known work is unfinished; Unknown means
// required evidence is unavailable. The same close attempt continues settlement.
// SemanticErr preserves the original native outcome independently of transient
// Cause. Confirmed settlement removes this marker and returns SemanticErr alone.
// Every released, unsettled adapter result must carry this marker; its absence
// confirms settlement even when the close returns a semantic error.
type CloseSettlementError struct {
	State       CloseSettlementState
	SemanticErr error
	Cause       error
}

func (e *CloseSettlementError) Check() error {
	if e == nil || e.State != CloseSettlementPending && e.State != CloseSettlementUnknown || e.Cause == nil {
		return syscall.EINVAL
	}
	var nested *CloseSettlementError
	if errors.As(e.SemanticErr, &nested) || errors.As(e.Cause, &nested) {
		return syscall.EINVAL
	}
	return nil
}

func (e *CloseSettlementError) Error() string {
	switch e.State {
	case CloseSettlementPending:
		return "released close settlement is pending"
	case CloseSettlementUnknown:
		return "released close settlement is unknown"
	default:
		return "released close settlement state is invalid"
	}
}

func (e *CloseSettlementError) Unwrap() []error {
	var causes []error
	if e.SemanticErr != nil {
		causes = append(causes, e.SemanticErr)
	}
	if e.Cause != nil {
		causes = append(causes, e.Cause)
	}
	return causes
}
