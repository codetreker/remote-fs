package httprest

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var pendingDeleteCapabilityError = fmt.Errorf("node pending unlink: %w", syscall.EBUSY)

var capabilityErrors = map[string]error{
	"use-conflict":       storage.ErrUseConflict,
	"range-conflict":     storage.ErrRangeConflict,
	"pending-delete":     pendingDeleteCapabilityError,
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
