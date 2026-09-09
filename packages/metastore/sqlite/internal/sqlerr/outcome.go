package sqlerr

import (
	"errors"
	"syscall"
)

func IsUncertainCommit(err error) bool {
	var uncertain *UncertainCommitError
	return errors.As(err, &uncertain)
}

type UncertainCommitError struct{ err error }

func (e *UncertainCommitError) Error() string         { return e.err.Error() }
func (e *UncertainCommitError) Unwrap() error         { return e.err }
func (e *UncertainCommitError) Classification() error { return syscall.EIO }

type DurabilityFailure struct{ err error }

func (e *DurabilityFailure) Error() string         { return e.err.Error() }
func (e *DurabilityFailure) Unwrap() error         { return e.err }
func (e *DurabilityFailure) Is(target error) bool  { return target == syscall.EIO }
func (e *DurabilityFailure) Classification() error { return syscall.EIO }

type ReadCleanupFailure struct{ cause error }

func (e *ReadCleanupFailure) Error() string         { return e.cause.Error() }
func (e *ReadCleanupFailure) Unwrap() error         { return e.cause }
func (e *ReadCleanupFailure) Is(target error) bool  { return target == syscall.EIO }
func (e *ReadCleanupFailure) Classification() error { return syscall.EIO }

func NewUncertainCommit(err error) *UncertainCommitError {
	return &UncertainCommitError{err: err}
}

func NewDurabilityFailure(err error) *DurabilityFailure {
	return &DurabilityFailure{err: err}
}

func NewReadCleanupFailure(cause error) *ReadCleanupFailure {
	return &ReadCleanupFailure{cause: cause}
}
