package fuse

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type actionSessionProbe struct {
	storage.FileSession
	query  func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
	cancel func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s actionSessionProbe) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.query(ctx, id)
}
func (s actionSessionProbe) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.cancel(ctx, id)
}

func TestFileActionReconciliationPreservesConfirmedEffects(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		state   storage.FileActionState
		errno   syscall.Errno
		want    syscall.Errno
		pending bool
		unknown bool
	}{
		{"completed write", storage.FileActionCompleted, 0, 0, false, false},
		{"known rejection", storage.FileActionNotApplied, syscall.EACCES, syscall.EACCES, false, false},
		{"cancel pending", storage.FileActionNotApplied, syscall.EINTR, syscall.EINTR, true, false},
		{"unknown", storage.FileActionUnknown, 0, syscall.EIO, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var queries, cancellations int
			result := storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: test.state, Errno: test.errno}
			if test.state == storage.FileActionCompleted {
				result.Effects = storage.EffectContentChanged
			}
			check := func(ctx context.Context, got storage.FileActionID) {
				t.Helper()
				if got != id || ctx.Err() != nil {
					t.Fatalf("cleanup identity/context = %s/%v", got, ctx.Err())
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("cleanup is unbounded")
				}
			}
			v := &volume{flushTimeout: time.Second, stop: make(chan struct{}), deadline: time.Now().Add(time.Minute)}
			v.files = actionSessionProbe{
				query: func(ctx context.Context, got storage.FileActionID) (storage.FileActionReceipt, error) {
					check(ctx, got)
					queries++
					if test.pending {
						return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionPending}, nil
					}
					return result, nil
				},
				cancel: func(ctx context.Context, got storage.FileActionID) (storage.FileActionReceipt, error) {
					check(ctx, got)
					cancellations++
					return result, nil
				},
			}
			got, err := v.issueAction(ctx, id, storage.OpFileWrite, func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{}, context.Canceled
			})
			if errnoOf(err) != test.want || got.Action != id {
				t.Fatalf("result=%+v err=%v", got, err)
			}
			if queries != 1 || (cancellations == 1) != (test.pending || test.unknown) {
				t.Fatalf("queries/cancellations=%d/%d", queries, cancellations)
			}
			if test.unknown && errnoOf(v.check()) != syscall.EIO {
				t.Fatal("unknown action did not fence")
			}
		})
	}
}

func TestFileActionRejectsMismatchedAndContradictoryReceipts(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, receipt := range []storage.FileActionReceipt{
		{State: storage.FileActionCompleted},
		{Action: id, Operation: storage.OpFileTruncate, State: storage.FileActionCompleted},
		{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionPending},
		{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionUnknown},
		{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionNotApplied, Effects: storage.EffectRangesChanged},
	} {
		if terminal, _ := actionResult(receipt, id, storage.OpFileWrite, nil); terminal {
			t.Fatalf("accepted %+v", receipt)
		}
	}
	cause := fmt.Errorf("quota source: %w", syscall.EDQUOT)
	if terminal, got := actionResult(storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionNotApplied, Errno: syscall.EDQUOT}, id, storage.OpFileWrite, cause); !terminal || got != cause {
		t.Fatal("lost original known cause")
	}
	if terminal, got := actionResult(storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted}, id, storage.OpFileWrite, context.Canceled); !terminal || got != nil {
		t.Fatal("confirmed result became interruption")
	}
	if terminal, got := actionResult(storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionNotApplied}, id, storage.OpFileWrite, nil); !terminal || !errors.Is(got, syscall.EINTR) {
		t.Fatal("cancelled action became success")
	}
}

func TestFileActionRetirementIsACleanupFact(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileClose, storage.OpFileSessionClose} {
		receipt := storage.FileActionReceipt{Operation: operation, State: storage.FileActionRetired}
		terminal, err := cleanupResult(receipt, operation, 0, nil)
		if !terminal || err != nil {
			t.Fatalf("cleanup %s=%v/%v", operation, terminal, err)
		}
		if terminal, _ := actionResult(receipt, "unrelated", operation, nil); terminal {
			t.Fatal("cleanup fact became historical execution")
		}
	}
	for _, receipt := range []storage.FileActionReceipt{
		{Operation: storage.OpFileClose, Reference: 2, State: storage.FileActionRetired},
		{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired},
		{Operation: storage.OpFileClose, State: storage.FileActionRetired, Action: "old"},
		{Operation: storage.OpFileClose, State: storage.FileActionRetired, Effects: storage.EffectReferenceRetired},
	} {
		if terminal, _ := cleanupResult(receipt, storage.OpFileClose, 0, nil); terminal {
			t.Fatalf("accepted wrong cleanup identity %+v", receipt)
		}
	}
	cause := fmt.Errorf("cleanup accounting: %w", syscall.EIO)
	terminal, err := cleanupResult(storage.FileActionReceipt{Operation: storage.OpFileClose, State: storage.FileActionRetired, Errno: syscall.EIO}, storage.OpFileClose, 0, cause)
	if !terminal || err != cause {
		t.Fatal("retired identity hid cleanup failure")
	}
	terminal, err = cleanupResult(storage.FileActionReceipt{Operation: storage.OpFileClose, State: storage.FileActionRetired, Errno: syscall.EDQUOT}, storage.OpFileClose, 0, nil)
	if !terminal || errnoOf(err) != syscall.EDQUOT {
		t.Fatal("lost retired cleanup error")
	}
}

func TestFileActionRequiresAdmissionProofBeforeSkippingReconciliation(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, proof := range []bool{false, true} {
		t.Run(fmt.Sprintf("proof=%v", proof), func(t *testing.T) {
			queries := 0
			v := &volume{flushTimeout: time.Second, deadline: time.Now().Add(time.Minute), stop: make(chan struct{})}
			v.files = actionSessionProbe{query: func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
				queries++
				return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged}, nil
			}}
			cause := error(syscall.EAGAIN)
			if proof {
				cause = &storage.FileError{Code: syscall.EIO, NotAdmitted: true, Cause: errors.New("admission unavailable")}
			}
			_, err := v.issueAction(t.Context(), id, storage.OpFileWrite, func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{}, cause
			})
			if proof {
				if err != cause || queries != 0 {
					t.Fatalf("proven refusal=%v queries%d", err, queries)
				}
			} else if err != nil || queries != 1 {
				t.Fatalf("bare errno was trusted: %v queries%d", err, queries)
			}
		})
	}
}

func TestFileActionQueryAdmissionRefusalDoesNotSettleEarlierUnknownMutation(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	refusal := &storage.FileError{Code: syscall.EACCES, NotAdmitted: true, Cause: syscall.EACCES}
	v := &volume{flushTimeout: time.Second, deadline: time.Now().Add(time.Minute), stop: make(chan struct{})}
	calls := 0
	denied := func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		return storage.FileActionReceipt{}, refusal
	}
	v.files = actionSessionProbe{query: denied, cancel: denied}
	_, err = v.issueAction(t.Context(), id, storage.OpFileWrite, func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, syscall.EIO
	})
	if errnoOf(err) != syscall.EIO || errnoOf(v.check()) != syscall.EIO || calls != 2 {
		t.Fatalf("unknown mutation=%v health=%v calls%d", err, v.check(), calls)
	}
}
