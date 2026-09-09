package sqlerr

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestUncertainCommitErrorPreservesItsCauseAndClassification(t *testing.T) {
	cause := errors.New("commit result unavailable")
	err := &UncertainCommitError{err: cause}
	if err.Error() != cause.Error() {
		t.Fatalf("uncertain commit says %q, want %q", err.Error(), cause.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatalf("uncertain commit does not retain its cause: %v", err)
	}
	if !IsUncertainCommit(errors.Join(errors.New("open failed"), err)) {
		t.Fatal("a joined uncertain commit was not classified as uncertain")
	}
}

func TestDurabilityOutcomesKeepTheirCauseAndOverrideCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		wrap      func(error) error
		matchesIO bool
		uncertain bool
	}{
		{"commit", func(err error) error { return NewUncertainCommit(err) }, false, true},
		{"durability", func(err error) error { return NewDurabilityFailure(err) }, true, false},
		{"read cleanup", func(err error) error { return NewReadCleanupFailure(err) }, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cause := fmt.Errorf("storage failure: %w", context.Canceled)
			err := test.wrap(cause)
			if err.Error() != cause.Error() || errors.Unwrap(err) != cause {
				t.Fatalf("outcome lost its exact cause: %v", err)
			}
			if !errors.Is(err, context.Canceled) || errors.Is(err, syscall.EIO) != test.matchesIO || errors.Is(err, syscall.ENOENT) {
				t.Fatalf("outcome matches incorrect causes: %v", err)
			}
			var classified interface{ Classification() error }
			if !errors.As(err, &classified) || classified.Classification() != syscall.EIO {
				t.Fatalf("outcome lacks EIO classification: %v", err)
			}
			if IsUncertainCommit(errors.Join(errors.New("open failed"), err)) != test.uncertain {
				t.Fatalf("outcome has incorrect uncertainty classification: %v", err)
			}
			for _, joined := range []error{
				errors.Join(context.Canceled, err),
				errors.Join(err, context.Canceled),
			} {
				if storage.ErrnoOf(joined) != syscall.EIO || !errors.Is(joined, cause) {
					t.Fatalf("joined cancellation displaced the durability cause: %v", joined)
				}
			}
		})
	}
}
