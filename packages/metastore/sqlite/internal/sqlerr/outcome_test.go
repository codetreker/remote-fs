package sqlerr

import (
	"errors"
	"testing"
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
