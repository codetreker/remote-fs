package smb

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type ownerActionFile struct {
	handleFileStub
	status func() (storage.CloseOwnerStatus, error)
	query  func(storage.CloseAttempt) (storage.FileActionReceipt, error)
	close  func(storage.CloseAttempt) (storage.ReferenceCloseResult, error)
}

func (f *ownerActionFile) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	if f.status == nil {
		return storage.CloseOwnerStatus{}, syscall.ENOSYS
	}
	return f.status()
}
func (f *ownerActionFile) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if f.query == nil {
		return storage.FileActionReceipt{}, syscall.ENOSYS
	}
	return f.query(attempt)
}
func (f *ownerActionFile) CloseWithAction(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if f.close == nil {
		return storage.ReferenceCloseResult{}, syscall.ENOSYS
	}
	return f.close(attempt)
}

func closeOwnerAttempt(t *testing.T, epoch, generation uint64) storage.CloseAttempt {
	t.Helper()
	action, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return storage.CloseAttempt{Action: action, Generation: generation}
}

func closeOwnerFixture(t *testing.T, file storage.File) (*tree, *fileHandle) {
	t.Helper()
	s := &session{id: 47}
	tree := &tree{id: 13}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	tree.endFileWork()
	handle.file, handle.published = file, true
	handle.state.Store(uint32(handleLive))
	return tree, handle
}

func requireOwnedClose(t *testing.T, tree *tree, handle *fileHandle, attempt storage.CloseAttempt) {
	t.Helper()
	if tree.findFileHandle(handle.id) != handle || handle.attempt == nil || *handle.attempt != attempt {
		t.Fatalf("close ownership changed: retained=%v attempt=%+v want=%+v", tree.findFileHandle(handle.id) == handle, handle.attempt, attempt)
	}
}

func TestCloseOwnerAdoptsBoundCurrentAttempt(t *testing.T) {
	for _, outcome := range []storage.FileActionOutcome{storage.FileActionPending, storage.FileActionUnknown} {
		t.Run(map[storage.FileActionOutcome]string{storage.FileActionPending: "pending", storage.FileActionUnknown: "unknown"}[outcome], func(t *testing.T) {
			original := closeOwnerAttempt(t, 8, 3)
			statuses, closes := 0, 0
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) {
					statuses++
					return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: outcome, NextGeneration: 3, CurrentEpoch: 9}, nil
				},
				close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					closes++
					if got != original {
						t.Fatalf("adopted attempt: %+v want %+v", got, original)
					}
					return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			if err := tree.closeFileHandle(t.Context(), handle); err != nil {
				t.Fatal(err)
			}
			if statuses != 1 || closes != 1 || tree.findFileHandle(handle.id) != nil {
				t.Fatalf("adopted close: statuses=%d closes=%d retained=%v", statuses, closes, tree.findFileHandle(handle.id) != nil)
			}
		})
	}
}

func TestCloseOwnerReadyUsesBoundEpochAndGeneration(t *testing.T) {
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 6, CurrentEpoch: 23}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			epoch, err := got.Action.Epoch()
			if err != nil || epoch != 23 || got.Generation != 6 {
				t.Fatalf("new attempt: %+v epoch=%d err=%v", got, epoch, err)
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); err != nil || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("ready close: retained=%v err=%v", tree.findFileHandle(handle.id) != nil, err)
	}
}

func TestCloseOwnerUnknownReplaysExactAttemptEvenWithCompletedReceipt(t *testing.T) {
	original := closeOwnerAttempt(t, 11, 4)
	closes, queries, statuses := 0, 0, 0
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 4, CurrentEpoch: 12}, nil
		},
		query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
			queries++
			if got != original {
				t.Fatalf("queried another attempt: %+v", got)
			}
			return storage.FileActionReceipt{Action: got.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionCompleted}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if got != original {
				t.Fatalf("replayed another attempt: %+v", got)
			}
			if closes == 1 {
				return storage.ReferenceCloseResult{}, syscall.ECONNRESET
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ENOTEMPTY
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("unknown close: %v", err)
	}
	requireOwnedClose(t, tree, handle, original)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("typed semantic close result: %v", err)
	}
	if closes != 2 || queries != 2 || statuses != 1 || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("typed replay counts: close=%d query=%d status=%d retained=%v", closes, queries, statuses, tree.findFileHandle(handle.id) != nil)
	}
}

func TestCloseOwnerDeterminedRetainedUsesNextGeneration(t *testing.T) {
	original := closeOwnerAttempt(t, 11, 4)
	statuses, closes := 0, 0
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			if statuses == 1 {
				return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionPending, NextGeneration: 4, CurrentEpoch: 11}, nil
			}
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 5, CurrentEpoch: 19}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if closes == 1 {
				if got != original {
					t.Fatalf("initial attempt: %+v", got)
				}
				return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
			}
			epoch, err := got.Action.Epoch()
			if err != nil || epoch != 19 || got.Generation != 5 || got.Action == original.Action {
				t.Fatalf("next generation: %+v epoch=%d err=%v", got, epoch, err)
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("determined retained result: %v", err)
	}
	if tree.findFileHandle(handle.id) != handle || handle.attempt != nil || handle.priorAttempt == nil || *handle.priorAttempt != original || handle.released {
		t.Fatalf("retained generation state: %+v", handle)
	}
	if err := tree.closeFileHandle(t.Context(), handle); err != nil || statuses != 2 || closes != 2 || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("next generation close: status=%d closes=%d retained=%v err=%v", statuses, closes, tree.findFileHandle(handle.id) != nil, err)
	}
}

func TestCloseOwnerRemintsOnlyWithBoundNonexecutionProof(t *testing.T) {
	statuses, closes := 0, 0
	var original, replacement storage.CloseAttempt
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 7, CurrentEpoch: 20}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if closes == 1 {
				original = got
				return storage.ReferenceCloseResult{}, &storage.CloseActionNotExecutedError{CurrentEpoch: 21}
			}
			replacement = got
			epoch, err := got.Action.Epoch()
			if err != nil || epoch != 21 || got.Generation != original.Generation || got.Action == original.Action {
				t.Fatalf("reminted attempt: %+v epoch=%d err=%v", got, epoch, err)
			}
			return storage.ReferenceCloseResult{}, syscall.ECONNRESET
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("reminted result: %v", err)
	}
	requireOwnedClose(t, tree, handle, replacement)
	if closes != 2 || statuses != 1 || handle.priorAttempt == nil || *handle.priorAttempt != original {
		t.Fatalf("nonexecution proof state: closes=%d statuses=%d prior=%+v", closes, statuses, handle.priorAttempt)
	}
}

func TestCloseOwnerGenericErrorsNeverRemintUnknownAttempt(t *testing.T) {
	for _, fault := range []error{syscall.ESTALE, syscall.EINVAL, context.Canceled, &storage.CloseActionNotExecutedError{CurrentEpoch: 29}} {
		t.Run(fault.Error(), func(t *testing.T) {
			original := closeOwnerAttempt(t, 25, 2)
			closes, statuses := 0, 0
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) {
					statuses++
					return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 2, CurrentEpoch: 26}, nil
				},
				query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
					return storage.FileActionReceipt{Action: got.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionUnknown}, nil
				},
				close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					closes++
					if got != original {
						t.Fatalf("unknown attempt was reminted: %+v", got)
					}
					return storage.ReferenceCloseResult{}, fault
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			for range 2 {
				if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, fault) {
					t.Fatalf("unknown error: %v", err)
				}
				requireOwnedClose(t, tree, handle, original)
			}
			if closes != 2 || statuses != 1 || handle.priorAttempt != nil {
				t.Fatalf("unknown close counts: closes=%d statuses=%d prior=%+v", closes, statuses, handle.priorAttempt)
			}
		})
	}
}

func TestCloseOwnerRetiredOrMalformedReceiptPreservesExactOwner(t *testing.T) {
	for _, name := range []string{"retired", "different-action", "different-operation", "malformed", "unreachable"} {
		t.Run(name, func(t *testing.T) {
			original, different := closeOwnerAttempt(t, 31, 2), closeOwnerAttempt(t, 31, 2)
			closes := 0
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) {
					return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 2, CurrentEpoch: 32}, nil
				},
				query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
					receipt := storage.FileActionReceipt{Action: got.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionUnknown}
					switch name {
					case "retired":
						receipt.Outcome = storage.FileActionRetired
					case "different-action":
						receipt.Action = different.Action
					case "different-operation":
						receipt.Operation = storage.OpFileOpenAt
					case "malformed":
						receipt.Outcome = 0
					case "unreachable":
						return storage.FileActionReceipt{}, syscall.ECONNRESET
					}
					return receipt, nil
				},
				close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					closes++
					return storage.ReferenceCloseResult{}, syscall.EIO
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			if err := tree.closeFileHandle(t.Context(), handle); err == nil {
				t.Fatal("unknown close reported success")
			}
			if err := tree.closeFileHandle(t.Context(), handle); err == nil {
				t.Fatal("unusable receipt reported success")
			}
			requireOwnedClose(t, tree, handle, original)
			if closes != 1 || handle.priorAttempt != nil {
				t.Fatalf("unusable receipt replayed close: closes=%d prior=%+v", closes, handle.priorAttempt)
			}
		})
	}
}

func TestCloseOwnerBarrierSettlementKeepsAttemptAndSemanticError(t *testing.T) {
	original := closeOwnerAttempt(t, 40, 1)
	statuses, closes, queries := 0, 0, 0
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionPending, NextGeneration: 1, CurrentEpoch: 40}, nil
		},
		query: func(storage.CloseAttempt) (storage.FileActionReceipt, error) {
			queries++
			return storage.FileActionReceipt{}, syscall.ENOSYS
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if got != original {
				t.Fatalf("barrier settlement changed attempt: %+v", got)
			}
			if closes <= 2 {
				state := storage.CloseSettlementPending
				var semantic error = syscall.ENOTEMPTY
				if closes == 2 {
					state, semantic = storage.CloseSettlementUnknown, nil
				}
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{State: state, SemanticErr: semantic, Cause: syscall.ECONNRESET}
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	for range 2 {
		err := tree.closeFileHandle(t.Context(), handle)
		var marker *storage.CloseSettlementError
		if !errors.As(err, &marker) || !errors.Is(err, syscall.ECONNRESET) || !errors.Is(handle.semanticErr, syscall.ENOTEMPTY) {
			t.Fatalf("unsettled errors: %v", err)
		}
		requireOwnedClose(t, tree, handle, original)
		if !handle.released || handleState(handle.state.Load()) != handleBarrierOnly {
			t.Fatalf("barrier owner: released=%v state=%v", handle.released, handle.state.Load())
		}
	}
	err := tree.closeFileHandle(t.Context(), handle)
	var marker *storage.CloseSettlementError
	if !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.ECONNRESET) || errors.As(err, &marker) {
		t.Fatalf("settled semantic result: %v", err)
	}
	if statuses != 1 || closes != 3 || queries != 0 || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("barrier settlement: statuses=%d closes=%d queries=%d retained=%v", statuses, closes, queries, tree.findFileHandle(handle.id) != nil)
	}
}

func TestCloseOwnerMalformedResultRetainsAttempt(t *testing.T) {
	cases := []struct {
		name   string
		result storage.ReferenceCloseResult
		err    error
	}{
		{"retained-success", storage.ReferenceCloseResult{}, nil},
		{"marker-without-release", storage.ReferenceCloseResult{Determined: true}, &storage.CloseSettlementError{State: storage.CloseSettlementPending, Cause: syscall.EIO}},
		{"invalid-settlement-state", storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{State: 99, Cause: syscall.EIO}},
		{"missing-settlement-cause", storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{State: storage.CloseSettlementPending}},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			original := closeOwnerAttempt(t, 41, 1)
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) {
					return storage.CloseOwnerStatus{Current: &original, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 1, CurrentEpoch: 41}, nil
				},
				close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					return item.result, item.err
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed result: %v", err)
			}
			requireOwnedClose(t, tree, handle, original)
			if handle.released || handle.priorAttempt != nil {
				t.Fatalf("malformed facts advanced ownership: released=%v prior=%+v", handle.released, handle.priorAttempt)
			}
		})
	}
}

func TestCloseOwnerCannotInferSettlementFromOwnerStatus(t *testing.T) {
	cases := []struct {
		name   string
		status storage.CloseOwnerStatus
		err    error
	}{
		{"not-ready", storage.CloseOwnerStatus{NextGeneration: 1, CurrentEpoch: 44}, nil},

		{"invalid-ready-release", storage.CloseOwnerStatus{Ready: true, Released: true, NextGeneration: 1, CurrentEpoch: 44}, nil},
		{"unreachable", storage.CloseOwnerStatus{}, syscall.ECONNRESET},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			closes := 0
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) { return item.status, item.err },
				close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					closes++
					return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			if err := tree.closeFileHandle(t.Context(), handle); err == nil {
				t.Fatal("owner status reported settlement")
			}
			if tree.findFileHandle(handle.id) != handle || handle.attempt != nil || handle.released || closes != 0 {
				t.Fatalf("owner status advanced release: retained=%v attempt=%+v released=%v closes=%d", tree.findFileHandle(handle.id) == handle, handle.attempt, handle.released, closes)
			}
		})
	}
}

type resultOnlyOwnerFile struct {
	storage.File
	closes int
	result func(int) (storage.ReferenceCloseResult, error)
}

func (f *resultOnlyOwnerFile) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	f.closes++
	if f.result != nil {
		return f.result(f.closes)
	}
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func TestCloseOwnerGenericFallbackRequiresUnpublishedReference(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "unpublished", true: "published"}[published], func(t *testing.T) {
			file := &resultOnlyOwnerFile{}
			tree, handle := closeOwnerFixture(t, file)
			handle.published = published
			err := tree.closeFileHandle(t.Context(), handle)
			if published {
				if !errors.Is(err, syscall.EOPNOTSUPP) || file.closes != 0 || tree.findFileHandle(handle.id) != handle {
					t.Fatalf("published fallback: closes=%d retained=%v err=%v", file.closes, tree.findFileHandle(handle.id) == handle, err)
				}
			} else if err != nil || file.closes != 1 || tree.findFileHandle(handle.id) != nil {
				t.Fatalf("unpublished cleanup: closes=%d retained=%v err=%v", file.closes, tree.findFileHandle(handle.id) != nil, err)
			}
		})
	}
}

func TestCloseOwnerAdoptsInternalWinnerOnlyAfterCandidateNotExecuted(t *testing.T) {
	winner := closeOwnerAttempt(t, 51, 1)
	var candidate storage.CloseAttempt
	statuses, queries, closes := 0, 0, 0
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			if statuses == 1 {
				return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 51}, nil
			}
			return storage.CloseOwnerStatus{Current: &winner, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 1, CurrentEpoch: 51}, nil
		},
		query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
			queries++
			if got != candidate {
				t.Fatalf("candidate proof queried different attempt: %+v", got)
			}
			return storage.FileActionReceipt{Action: got.Action, Outcome: storage.FileActionNotExecuted}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if closes == 1 {
				candidate = got
				if got == winner {
					t.Fatal("new explicit candidate equals admitted winner")
				}
				return storage.ReferenceCloseResult{}, syscall.EINVAL
			}
			if got != winner {
				t.Fatalf("internal winner not adopted: %+v", got)
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); err != nil {
		t.Fatal(err)
	}
	if statuses != 2 || queries != 1 || closes != 2 || handle.priorAttempt == nil || *handle.priorAttempt != candidate || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("internal winner close: statuses=%d queries=%d closes=%d prior=%+v retained=%v", statuses, queries, closes, handle.priorAttempt, tree.findFileHandle(handle.id) != nil)
	}
}

func TestCloseOwnerReleasedStatusRetrievesTypedSettlement(t *testing.T) {
	file := &ownerActionFile{
		handleFileStub: handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.ECONNRESET}},
		status: func() (storage.CloseOwnerStatus, error) {
			return storage.CloseOwnerStatus{Released: true, NextGeneration: 2, CurrentEpoch: 61}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("released typed settlement: %v", err)
	}
	if !handle.released || handleState(handle.state.Load()) != handleBarrierOnly || tree.findFileHandle(handle.id) != handle || file.closes.Load() != 1 {
		t.Fatalf("released status bypassed settlement: released=%v state=%v retained=%v closes=%d", handle.released, handle.state.Load(), tree.findFileHandle(handle.id) == handle, file.closes.Load())
	}
	file.err = nil
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.ECONNRESET) || tree.findFileHandle(handle.id) != nil || file.closes.Load() != 2 {
		t.Fatalf("released status settlement retry: closes=%d retained=%v err=%v", file.closes.Load(), tree.findFileHandle(handle.id) != nil, err)
	}
}

func TestCloseOwnerConsecutiveEpochProofsResumeBeforeRetiredQuery(t *testing.T) {
	var attempts []storage.CloseAttempt
	statuses, queries := 0, 0
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 3, CurrentEpoch: 71}, nil
		},
		query: func(storage.CloseAttempt) (storage.FileActionReceipt, error) {
			queries++
			t.Fatal("proven unexecuted action queried after its epoch retired")
			return storage.FileActionReceipt{}, syscall.EIO
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			attempts = append(attempts, got)
			epoch, err := got.Action.Epoch()
			wantEpoch := uint64(70 + len(attempts))
			if err != nil || epoch != wantEpoch || got.Generation != 3 {
				t.Fatalf("epoch proof retry: %+v epoch=%d want=%d err=%v", got, epoch, wantEpoch, err)
			}
			if len(attempts) <= 2 {
				return storage.ReferenceCloseResult{}, &storage.CloseActionNotExecutedError{CurrentEpoch: epoch + 1}
			}
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("bounded repeated proof: %v", err)
	}
	if len(attempts) != 2 || tree.findFileHandle(handle.id) != handle {
		t.Fatalf("first proof call: attempts=%d retained=%v", len(attempts), tree.findFileHandle(handle.id) == handle)
	}
	if err := tree.closeFileHandle(t.Context(), handle); err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 || statuses != 1 || queries != 0 || tree.findFileHandle(handle.id) != nil || attempts[0].Action == attempts[1].Action || attempts[1].Action == attempts[2].Action {
		t.Fatalf("proof continuation: attempts=%+v statuses=%d queries=%d retained=%v", attempts, statuses, queries, tree.findFileHandle(handle.id) != nil)
	}
}

type resultOnlyOwnerNode struct {
	storage.NodeReference
	closes int
	result func(int) (storage.ReferenceCloseResult, error)
}

func (n *resultOnlyOwnerNode) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	n.closes++
	return n.result(n.closes)
}

func TestCloseOwnerUnpublishedTypedReferencesRetainBarrierCapacity(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		t.Run(kind, func(t *testing.T) {
			result := func(call int) (storage.ReferenceCloseResult, error) {
				if call <= 2 {
					state := storage.CloseSettlementPending
					if call == 2 {
						state = storage.CloseSettlementUnknown
					}
					return storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{State: state, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.ECONNRESET}
				}
				return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
			}
			file := &resultOnlyOwnerFile{result: result}
			node := &resultOnlyOwnerNode{result: result}
			tree, handle := closeOwnerFixture(t, file)
			handle.published = false
			if kind == "node" {
				handle.file, handle.node = nil, node
			}
			for range 2 {
				if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ECONNRESET) {
					t.Fatalf("unpublished settlement: %v", err)
				}
				if !handle.released || tree.findFileHandle(handle.id) != handle || handleState(handle.state.Load()) != handleBarrierOnly {
					t.Fatalf("unpublished barrier owner: retained=%v released=%v state=%v", tree.findFileHandle(handle.id) == handle, handle.released, handle.state.Load())
				}
				s := &session{id: 47}
				if !tree.beginFileWork(s) {
					t.Fatal("capacity probe admission refused")
				}
				_, err := tree.reserveFileHandle(s, 1)
				tree.endFileWork()
				if !errors.Is(err, syscall.EMFILE) {
					t.Fatalf("unsettled owner did not retain capacity: %v", err)
				}
			}
			if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.ECONNRESET) || tree.findFileHandle(handle.id) != nil {
				t.Fatalf("unpublished settled result: retained=%v err=%v", tree.findFileHandle(handle.id) != nil, err)
			}
			if kind == "file" && file.closes != 3 || kind == "node" && node.closes != 3 {
				t.Fatalf("typed generic close count: file=%d node=%d", file.closes, node.closes)
			}
		})
	}
}

func TestCloseOwnerRejectedCandidateRetrievesReleasedInternalWinner(t *testing.T) {
	var candidate storage.CloseAttempt
	statuses, queries, explicitCloses := 0, 0, 0
	file := &ownerActionFile{
		handleFileStub: handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}, err: syscall.ENOTEMPTY},
		status: func() (storage.CloseOwnerStatus, error) {
			statuses++
			if statuses == 1 {
				return storage.CloseOwnerStatus{Ready: true, NextGeneration: 2, CurrentEpoch: 81}, nil
			}
			return storage.CloseOwnerStatus{Released: true, NextGeneration: 3, CurrentEpoch: 82}, nil
		},
		query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
			queries++
			if got != candidate {
				t.Fatalf("rejected candidate query: %+v want %+v", got, candidate)
			}
			return storage.FileActionReceipt{Action: got.Action, Outcome: storage.FileActionNotExecuted}, nil
		},
		close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			explicitCloses++
			candidate = got
			return storage.ReferenceCloseResult{}, syscall.EINVAL
		},
	}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EINVAL) {
		t.Fatalf("internal released winner: %v", err)
	}
	if statuses != 2 || queries != 1 || explicitCloses != 1 || file.closes.Load() != 1 || handle.attempt != nil || handle.priorAttempt == nil || *handle.priorAttempt != candidate || tree.findFileHandle(handle.id) != nil {
		t.Fatalf("released winner recovery: statuses=%d queries=%d explicit=%d implicit=%d attempt=%+v prior=%+v retained=%v", statuses, queries, explicitCloses, file.closes.Load(), handle.attempt, handle.priorAttempt, tree.findFileHandle(handle.id) != nil)
	}
}

func TestCloseOwnerRejectedCandidateAdvancesOnlyWithLaterBoundGeneration(t *testing.T) {
	for _, next := range []uint64{1, 2, 3} {
		t.Run(map[uint64]string{1: "older", 2: "same", 3: "later"}[next], func(t *testing.T) {
			var candidate storage.CloseAttempt
			statuses, queries, closes := 0, 0, 0
			file := &ownerActionFile{
				status: func() (storage.CloseOwnerStatus, error) {
					statuses++
					if statuses == 1 {
						return storage.CloseOwnerStatus{Ready: true, NextGeneration: 2, CurrentEpoch: 91}, nil
					}
					return storage.CloseOwnerStatus{Ready: true, NextGeneration: next, CurrentEpoch: 92}, nil
				},
				query: func(got storage.CloseAttempt) (storage.FileActionReceipt, error) {
					queries++
					if got != candidate {
						t.Fatalf("candidate proof: %+v want %+v", got, candidate)
					}
					return storage.FileActionReceipt{Action: got.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
				},
				close: func(got storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
					closes++
					if closes == 1 {
						candidate = got
						return storage.ReferenceCloseResult{}, syscall.EINVAL
					}
					epoch, err := got.Action.Epoch()
					if next <= candidate.Generation || got.Generation != next || got.Action == candidate.Action || epoch != 92 || err != nil {
						t.Fatalf("status did not prove later attempt: %+v epoch=%d err=%v", got, epoch, err)
					}
					return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
				},
			}
			tree, handle := closeOwnerFixture(t, file)
			err := tree.closeFileHandle(t.Context(), handle)
			if statuses != 2 || queries != 1 {
				t.Fatalf("bound recovery checks: statuses=%d queries=%d", statuses, queries)
			}
			if next <= candidate.Generation {
				if !errors.Is(err, syscall.EINVAL) || closes != 1 || handle.priorAttempt != nil {
					t.Fatalf("nonadvancing generation minted: closes=%d prior=%+v err=%v", closes, handle.priorAttempt, err)
				}
				requireOwnedClose(t, tree, handle, candidate)
			} else if err != nil || closes != 2 || handle.priorAttempt == nil || *handle.priorAttempt != candidate || tree.findFileHandle(handle.id) != nil {
				t.Fatalf("later generation recovery: closes=%d prior=%+v retained=%v err=%v", closes, handle.priorAttempt, tree.findFileHandle(handle.id) != nil, err)
			}
		})
	}
}

func TestCloseOwnerReleasedFactDoesNotRequireRetainedDetermination(t *testing.T) {
	file := &ownerActionFile{status: func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
	}, close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
		return storage.ReferenceCloseResult{Released: true}, syscall.ENOTEMPTY
	}}
	tree, handle := closeOwnerFixture(t, file)
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("semantic outcome %v", err)
	}
	if tree.findFileHandle(handle.id) != nil {
		t.Fatal("confirmed release retained owner")
	}
}
