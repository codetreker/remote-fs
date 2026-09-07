package locking_test

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

type classifiedLockFailure struct {
	cause          error
	classification error
}

func (e classifiedLockFailure) Error() string         { return "native outcome: " + e.cause.Error() }
func (e classifiedLockFailure) Unwrap() error         { return e.cause }
func (e classifiedLockFailure) Classification() error { return e.classification }

func TestCodeOfPreservesWholeFailureInsteadOfNestedLockRefusal(t *testing.T) {
	refusal := locking.Wrap(locking.Conflict, "file occupied", nil)
	cleanup := fmt.Errorf("rollback evidence failed: %w", syscall.EIO)
	for name, failure := range map[string]error{
		"refusal then cleanup":                  errors.Join(refusal, cleanup),
		"cleanup then refusal":                  errors.Join(cleanup, refusal),
		"transparent wrapper around join":       fmt.Errorf("native guard: %w", errors.Join(refusal, cleanup)),
		"authoritative EIO":                     classifiedLockFailure{cause: refusal, classification: syscall.EIO},
		"authoritative EIO with both failures":  classifiedLockFailure{cause: errors.Join(refusal, cleanup), classification: syscall.EIO},
		"matching errno has no exact lock code": classifiedLockFailure{cause: refusal, classification: syscall.EBUSY},
		"missing classification":                classifiedLockFailure{cause: refusal},
		"independent matching lock codes":       errors.Join(refusal, locking.Wrap(locking.Conflict, "another target occupied", nil)),
	} {
		t.Run(name, func(t *testing.T) {
			if code := locking.CodeOf(failure); code != locking.Unavailable {
				t.Fatalf("combined failure became %q: %v", code, failure)
			}
			wrapped := locking.Wrap(locking.CodeOf(failure), "management action failed", failure)
			if !errors.Is(wrapped, refusal) {
				t.Fatal("diagnostic refusal cause was lost")
			}
			if errors.Is(failure, cleanup) && !errors.Is(wrapped, cleanup) {
				t.Fatal("independent cleanup cause was lost")
			}
		})
	}
}

func TestCodeOfRetainsTransparentSingleFailureAndOuterLockClassification(t *testing.T) {
	refusal := locking.Wrap(locking.StaleResource, "target was removed", nil)
	for _, failure := range []error{
		refusal,
		fmt.Errorf("native target: %w", refusal),
		errors.Join(refusal, nil),
		fmt.Errorf("native target: %w", errors.Join(errors.Join(refusal, nil), nil)),
	} {
		if code := locking.CodeOf(failure); code != locking.StaleResource {
			t.Fatalf("transparent failure became %q: %v", code, failure)
		}
	}
	outer := locking.Wrap(locking.Unavailable, "native transition is uncertain", refusal)
	if code := locking.CodeOf(fmt.Errorf("operation: %w", outer)); code != locking.Unavailable {
		t.Fatalf("outer lock classification became %q", code)
	}
	if code := locking.CodeOf(nil); code != locking.Unavailable {
		t.Fatalf("nil error invented outcome %q", code)
	}
}

type failedGuardNative struct {
	*contractNative
	failure error
}

func (n failedGuardNative) Guard(context.Context, locking.BackendKey, func() error) error {
	return n.failure
}

func TestAuthorityGuardFailureRetainsCombinedUnavailableReceipt(t *testing.T) {
	refusal := locking.Wrap(locking.Conflict, "metadata writer occupied", nil)
	cleanup := fmt.Errorf("witness rollback failed: %w", syscall.EIO)
	for name, failure := range map[string]error{
		"lock then cleanup": errors.Join(refusal, cleanup),
		"cleanup then lock": errors.Join(cleanup, refusal),
		"authoritative EIO": classifiedLockFailure{cause: refusal, classification: syscall.EIO},
	} {
		t.Run(name, func(t *testing.T) {
			clock := newContractClock()
			options := locking.DefaultOptions()
			options.Clock = clock
			store := &contractPersistence{max: options.MaxLease, start: clock.Now().Add(-time.Hour)}
			native := newContractNative()
			authority, err := locking.New(context.Background(), options, failedGuardNative{native, failure}, store)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := authority.Close(); err != nil {
					t.Fatal(err)
				}
			})
			h := &contractHarness{t: t, a: authority, clock: clock, native: native, store: store}
			owner := h.owner()
			request := h.request(owner, "failed", "a", locking.Exclusive)
			result, err := h.a.Acquire(context.Background(), request)
			contractRejected(t, result, err, locking.Unavailable)
			if !errors.Is(err, failure) || !errors.Is(err, refusal) {
				t.Fatalf("retained rejection lost native cause: %v", err)
			}
			if result.Grant != nil {
				t.Fatal("uncertain native guard failure created a grant")
			}
			replayed, err := h.a.QueryAction(context.Background(), owner, request.Request)
			contractRejected(t, replayed, err, locking.Unavailable)
			if !errors.Is(err, failure) {
				t.Fatalf("replayed failure lost native cause: %v", err)
			}
		})
	}
}
