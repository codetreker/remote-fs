package storage

import "syscall"

// FileError carries authority facts independently of a client's platform error
// vocabulary. Cause preserves diagnostics; Code owns the operation result.
type FileError struct {
	// NotAdmitted proves this invocation was rejected before action admission.
	// It says nothing about an earlier invocation using the same action ID.
	NotAdmitted bool
	Code        syscall.Errno
	Conflict    *FileConflict
	Cause       error
}

func (e *FileError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code.Error()
}
func (e *FileError) Unwrap() error         { return e.Cause }
func (e *FileError) Is(target error) bool  { return target == e.Code }
func (e *FileError) Classification() error { return e.Code }

// IsFileCallNotAdmitted preserves proof through wrapping. Independent joined
// failures must all carry the proof; one marker cannot hide another outcome.
func IsFileCallNotAdmitted(err error) bool {
	if err == nil {
		return false
	}
	if file, ok := err.(*FileError); ok {
		return file.NotAdmitted
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !IsFileCallNotAdmitted(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return IsFileCallNotAdmitted(wrapped.Unwrap())
	}
	return false
}
