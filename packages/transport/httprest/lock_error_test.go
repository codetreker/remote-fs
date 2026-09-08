package httprest

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type classifiedPublicationFailure struct{ cause error }

func (e classifiedPublicationFailure) Error() string {
	return "publication outcome unknown: " + e.cause.Error()
}
func (e classifiedPublicationFailure) Unwrap() error         { return e.cause }
func (e classifiedPublicationFailure) Classification() error { return syscall.EIO }

func TestNamespaceJoinedLockFailurePreservesWholeOutcome(t *testing.T) {
	conflict := &locking.Error{Code: locking.Conflict, Message: "file occupied"}
	cleanup := fmt.Errorf("rollback evidence failed: %w", syscall.EIO)
	for name, failure := range map[string]error{
		"conflict then cleanup":  errors.Join(conflict, cleanup),
		"cleanup then conflict":  errors.Join(cleanup, conflict),
		"classified uncertainty": classifiedPublicationFailure{cause: errors.Join(conflict, cleanup)},
		"wrapped join":           fmt.Errorf("namespace commit: %w", errors.Join(conflict, cleanup)),
	} {
		t.Run(name, func(t *testing.T) {
			h := &Handler{maxBodyBytes: 1024}
			answer := httptest.NewRecorder()
			h.writeOperationError(answer, failure)
			if answer.Code != StatusStorageError {
				t.Fatalf("status = %d", answer.Code)
			}
			if strings.Contains(answer.Body.String(), "lockCode") || !strings.Contains(answer.Body.String(), "rollback evidence failed") {
				t.Fatalf("whole error was replaced by a nested lock failure: %s", answer.Body.String())
			}
			err := (&Storage{}).storageError(Request{Op: OpWrite, Path: "f"}, answer.Body.Bytes())
			if storage.ErrnoOf(err) != syscall.EIO || errors.Is(err, syscall.EBUSY) || !strings.Contains(err.Error(), "rollback evidence failed") {
				t.Fatalf("wire lost publication failure: %v", err)
			}
			var typed *locking.Error
			if errors.As(err, &typed) {
				t.Fatal("joined uncertainty became a typed lock refusal")
			}
		})
	}
}

func TestNamespaceWrappedLockFailureRetainsItsDiagnostic(t *testing.T) {
	original := fmt.Errorf("replace file: %w", &locking.Error{Code: locking.StaleGrant, Message: "grant expired"})
	h := &Handler{maxBodyBytes: 1024}
	answer := httptest.NewRecorder()
	h.writeOperationError(answer, original)
	err := (&Storage{}).storageError(Request{Op: OpWrite, Path: "f"}, answer.Body.Bytes())
	var failure *locking.Error
	if !errors.As(err, &failure) || failure.Code != locking.StaleGrant || storage.ErrnoOf(err) != syscall.ESTALE {
		t.Fatalf("lost exact lock classification: %v", err)
	}
	if !strings.Contains(err.Error(), "replace file:") || !strings.Contains(err.Error(), "grant expired") {
		t.Fatalf("lost enclosing diagnostic: %v", err)
	}
}

func TestNamespaceLockDiagnosticUsesTheNamespaceBodyBound(t *testing.T) {
	diagnostic := strings.Repeat("path-segment/", 100) + ": grant expired"
	h := &Handler{maxBodyBytes: 1 << 20}
	answer := httptest.NewRecorder()
	original := &locking.Error{Code: locking.StaleGrant, Message: diagnostic}
	h.writeOperationError(answer, original)
	err := (&Storage{}).storageError(Request{Op: OpWrite, Path: "f"}, answer.Body.Bytes())
	var failure *locking.Error
	if !errors.As(err, &failure) || failure.Code != locking.StaleGrant || failure.Message != original.Error() {
		t.Fatalf("bounded namespace diagnostic lost its exact outcome: %v", err)
	}
}

func TestNamespaceLockFailureRejectsDamagedClassifications(t *testing.T) {
	for _, body := range []string{
		`{"errno":"ESTALE","message":"expired","lockCode":"staleGrant"}`,
		`{"errno":"ESTALE","message":"expired","lockCode":"staleGrant","recorded":null}`,
		`{"errno":"ESTALE","message":"expired","lockCode":"staleGrant","recorded":false,"recorded":false}`,
		`{"errno":"EIO","message":"expired","lockCode":"staleGrant","recorded":false}`,
		`{"errno":"ESTALE","message":"expired","lockCode":"staleGrant","recorded":false,"action":{}}`,
	} {
		err := (&Storage{}).storageError(Request{Op: OpWrite, Path: "f"}, []byte(body))
		var failure *locking.Error
		if storage.ErrnoOf(err) != syscall.EIO || errors.As(err, &failure) {
			t.Fatalf("malformed namespace lock error accepted: %v", err)
		}
	}
}

func TestNamespaceSingletonJoinedLockFailureRetainsItsCode(t *testing.T) {
	original := &locking.Error{Code: locking.Conflict, Message: "file occupied"}
	for _, err := range []error{
		errors.Join(original, nil),
		fmt.Errorf("rename source target: %w", errors.Join(original, nil)),
		errors.Join(errors.Join(original, nil), nil),
	} {
		h := &Handler{maxBodyBytes: 1024}
		answer := httptest.NewRecorder()
		h.writeOperationError(answer, err)
		decoded := (&Storage{}).storageError(Request{Op: OpWrite, Path: "f"}, answer.Body.Bytes())
		var failure *locking.Error
		if !errors.As(decoded, &failure) || failure.Code != locking.Conflict || failure.Message != err.Error() {
			t.Fatalf("successful cleanup erased exact lock refusal: %v", decoded)
		}
	}
}
