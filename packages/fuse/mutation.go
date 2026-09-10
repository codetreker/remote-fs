package fuse

import (
	"fmt"
	"syscall"
)

// A compound operation cannot invite a retry after an earlier stage changed the
// volume or a handle buffer. The original cancellation remains available for diagnostics.
func afterMutation(changed bool, err error) error {
	if changed && errnoOf(err) == syscall.EINTR {
		return &incompleteMutation{cause: err}
	}
	return err
}

type incompleteMutation struct{ cause error }

func (e *incompleteMutation) Error() string {
	return fmt.Sprintf("the operation was interrupted after a change: %v: %v", e.cause, syscall.EIO)
}

func (e *incompleteMutation) Unwrap() []error { return []error{e.cause, syscall.EIO} }

func (e *incompleteMutation) Classification() error { return syscall.EIO }
