package storage

import "errors"

// WindowsError preserves a specific access failure alongside the ordinary
// storage error chain. Failure is a semantic code, never a platform status value.
type WindowsError struct {
	Failure WindowsFailure
	Err     error
}

func (e *WindowsError) Error() string { return string(e.Failure) + ": " + e.Err.Error() }
func (e *WindowsError) Unwrap() error { return e.Err }

// WindowsSymlinkInfo is a bounded authoritative stopped-on-symlink observation.
// Target uses volume path syntax and is captured with Location. Unparsed is the
// remaining canonical slash path, empty or starting with "/". Target and Unparsed
// each contain at most WindowsMaxLinkTargetBytes bytes; Location.Path is bounded
// by WindowsMaxNameInfoBytes. Native authority must prove volume confinement
// before returning target data.
type WindowsSymlinkInfo struct {
	Target   string
	Location WindowsNameInfo
	Unparsed string
}

// WindowsSymlinkError preserves ELOOP and its original cause while exposing the
// same data recorded in a rejected open action for later reconciliation.
type WindowsSymlinkError struct {
	WindowsSymlinkInfo
	Err error
}

func (e *WindowsSymlinkError) Error() string { return "Windows path encounters a symbolic link" }
func (e *WindowsSymlinkError) Unwrap() error { return e.Err }

// WindowsFailureOf returns only a consistent known classification. Conflicting
// joined failures cannot be represented by an arbitrarily chosen first match.
func WindowsFailureOf(err error) WindowsFailure {
	if err == nil {
		return ""
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result WindowsFailure
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			failure := WindowsFailureOf(child)
			if failure == "" || result != "" && result != failure {
				return ""
			}
			result = failure
		}
		return result
	}
	if classified, ok := err.(*WindowsError); ok {
		switch classified.Failure {
		case WindowsSharingViolation, WindowsLockConflict, WindowsDeletePending, WindowsRangeNotLocked, WindowsNotReparsePoint:
			return classified.Failure
		default:
			return ""
		}
	}
	return WindowsFailureOf(errors.Unwrap(err))
}
