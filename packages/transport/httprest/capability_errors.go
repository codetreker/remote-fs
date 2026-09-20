package httprest

import (
	"errors"

	"github.com/codetreker/remote-fs/packages/storage"
)

var capabilityErrors = map[string]error{
	"use-conflict":       storage.ErrUseConflict,
	"range-conflict":     storage.ErrRangeConflict,
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
