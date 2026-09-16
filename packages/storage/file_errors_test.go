package storage_test

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileErrorsKeepAuthoritativeClassificationAndDiagnosticCause(t *testing.T) {
	cause := errors.New("diagnostic failure")
	err := &storage.FileError{Code: syscall.EACCES, Conflict: &storage.FileConflict{Kind: storage.ConflictClaim}, Cause: errors.Join(cause, context.Canceled)}
	if storage.ErrnoOf(err) != syscall.EACCES || !errors.Is(err, syscall.EACCES) || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || err.Error() != err.Cause.Error() {
		t.Fatal("file error lost result/cause")
	}
	if storage.ErrnoOf(errors.Join(err, syscall.EIO)) != syscall.EIO {
		t.Fatal("independent failure was hidden")
	}
	plain := &storage.FileError{Code: syscall.EAGAIN}
	if plain.Error() != syscall.EAGAIN.Error() || plain.Unwrap() != nil || storage.ErrnoOf(plain) != syscall.EAGAIN {
		t.Fatal("plain error changed")
	}
}

func TestFileNoAdmissionProofIsForThisCallAndPreservesErrorTreeBoundaries(t *testing.T) {
	proof := &storage.FileError{Code: syscall.EINVAL, NotAdmitted: true, Cause: errors.New("action operands differ")}
	another := &storage.FileError{Code: syscall.EAGAIN, NotAdmitted: true}
	for _, err := range []error{proof, fmt.Errorf("wrapped: %w", proof), errors.Join(proof), errors.Join(proof, another)} {
		if !storage.IsFileCallNotAdmitted(err) {
			t.Fatalf("explicit call rejection lost proof: %v", err)
		}
	}
	for _, err := range []error{nil, syscall.EINVAL, context.Canceled, errors.New("unknown"), &storage.FileError{Code: syscall.EIO, Cause: proof}, errors.Join(proof, context.Canceled), errors.Join(proof, syscall.EIO), emptyErrorChildren{}, nilErrorChild{}} {
		if storage.IsFileCallNotAdmitted(err) {
			t.Fatalf("unproved independent outcome treated as no admission: %v", err)
		}
	}
	if storage.ErrnoOf(proof) != syscall.EINVAL || !errors.Is(proof, syscall.EINVAL) {
		t.Fatal("admission fact changed original rejection")
	}
}

type emptyErrorChildren struct{}

func (emptyErrorChildren) Error() string   { return "empty error children" }
func (emptyErrorChildren) Unwrap() []error { return nil }

type nilErrorChild struct{}

func (nilErrorChild) Error() string { return "nil error child" }
func (nilErrorChild) Unwrap() error { return nil }
