package storage

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestWindowsFailurePreservesErrorChain(t *testing.T) {
	for _, failure := range []WindowsFailure{WindowsSharingViolation, WindowsLockConflict, WindowsDeletePending, WindowsRangeNotLocked, WindowsNotReparsePoint} {
		err := &WindowsError{Failure: failure, Err: syscall.EACCES}
		if err.Error() != string(failure)+": "+syscall.EACCES.Error() || !errors.Is(err, syscall.EACCES) {
			t.Fatalf("lost error chain: %v", err)
		}
		if got := WindowsFailureOf(fmt.Errorf("transport: %w", err)); got != failure {
			t.Fatalf("got %q, want %q", got, failure)
		}
		if got := WindowsFailureOf(errors.Join(err, err)); got != failure {
			t.Fatalf("equal joined classifications: %q", got)
		}
	}
	for _, err := range []error{nil, syscall.EIO, &WindowsError{Failure: "unknown", Err: syscall.EIO}, errors.Join(&WindowsError{Failure: WindowsSharingViolation, Err: syscall.EACCES}, &WindowsError{Failure: WindowsDeletePending, Err: syscall.EACCES}), errors.Join(&WindowsError{Failure: WindowsSharingViolation, Err: syscall.EACCES}, syscall.EIO)} {
		if got := WindowsFailureOf(err); got != "" {
			t.Fatalf("invented classification %q for %v", got, err)
		}
	}
}

func TestWindowsSymlinkErrorPreservesAuthoritativeTargetAndCause(t *testing.T) {
	lookup := &WindowsSymlinkError{
		WindowsSymlinkInfo: WindowsSymlinkInfo{
			Target: "../target", Location: WindowsNameInfo{State: WindowsNameLinked, Path: "dir/link"}, Unparsed: "/child",
		},
		Err: syscall.ELOOP,
	}
	wrapped := fmt.Errorf("open: %w", lookup)
	var actual *WindowsSymlinkError
	if !errors.As(wrapped, &actual) || actual != lookup || !errors.Is(wrapped, syscall.ELOOP) {
		t.Fatalf("symbolic-link failure lost its observation or cause: %v", wrapped)
	}
	if lookup.Error() != "Windows path encounters a symbolic link" || lookup.Target != "../target" ||
		lookup.Location.Path != "dir/link" || lookup.Unparsed != "/child" {
		t.Fatalf("symbolic-link observation changed: %+v", lookup)
	}
	if failure := WindowsFailureOf(wrapped); failure != "" {
		t.Fatalf("symbolic-link data was collapsed into unrelated failure %q", failure)
	}
}
