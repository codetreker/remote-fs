package httprest

import (
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

var capabilityErrors = map[string]error{
	"use-conflict":       storage.ErrUseConflict,
	"range-conflict":     storage.ErrRangeConflict,
	"pending-delete":     storage.ErrPendingDelete,
	"condition-conflict": storage.ErrConditionConflict,
	"invalid-scope":      storage.ErrInvalidScope,
}

func capabilityErrorCode(err error) string {
	result := ""
	for code, candidate := range capabilityErrors {
		if storage.ErrnoOf(err) == storage.ErrnoOf(candidate) && errors.Is(err, candidate) {
			if result != "" {
				return ""
			}
			result = code
		}
	}
	return result
}

type fileEffectError struct{ cause error }

func (e *fileEffectError) Error() string         { return e.cause.Error() }
func (e *fileEffectError) Unwrap() error         { return e.cause }
func (e *fileEffectError) Classification() error { return syscall.EIO }
func uncertainFileEffect(cause error) error      { return &fileEffectError{cause: cause} }
func (e *fileEffectError) Is(target error) bool  { return target == syscall.EIO }
