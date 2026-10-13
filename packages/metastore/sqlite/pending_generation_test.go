package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openOrdinaryPendingReference(t *testing.T) (*LockingStore, *retainedFile) {
	t.Helper()
	store, _, node, _ := guardedFileFixture(t)
	opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.Reference.(*retainedNodeReference).file
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return store, file
}

func clearPending(t *testing.T, file *retainedFile, generation []byte) metastore.ReferenceState {
	t.Helper()
	state, err := file.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func setOrdinaryPending(t *testing.T, file *retainedFile) metastore.ReferenceState {
	t.Helper()
	state, err := file.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: fileAction(t), Condition: storage.UnlinkFile})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func requirePendingGeneration(t *testing.T, state metastore.ReferenceState, generation uint64) {
	t.Helper()
	if !state.PendingUnlink || len(state.PendingGeneration) != 8 || binary.BigEndian.Uint64(state.PendingGeneration) != generation {
		t.Fatalf("pending generation = %x, pending=%t; want %d", state.PendingGeneration, state.PendingUnlink, generation)
	}
}

func requirePendingClearNoEffect(t *testing.T, store *LockingStore, file *retainedFile, generation []byte) metastore.ReferenceState {
	t.Helper()
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var state metastore.ReferenceState
	if err := store.inspect(t.Context(), func(tx *sql.Tx) error {
		var err error
		state, err = store.returnedReferenceState(t.Context(), tx, file.id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	publications := 0
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		publications++
		return func(storage.PublicationResult) error { return nil }, nil
	})
	after, err := file.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: generation})
	if err != nil || !reflect.DeepEqual(after, state) || publications != 0 {
		t.Fatalf("no-op clear = %+v, %v; before %+v, publications %d", after, err, state, publications)
	}
	if got, err := store.DurableState(t.Context()); err != nil || got != before {
		t.Fatalf("no-op clear advanced durable generation: %+v, %v", got, err)
	}
	if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
		t.Fatalf("no-op clear changed event position: %d, %v", got, err)
	}
	return after
}

func TestOrdinaryPendingGenerationRejectsABAAndNilClearRaces(t *testing.T) {
	store, file := openOrdinaryPendingReference(t)
	clear := requirePendingClearNoEffect(t, store, file, nil)
	if clear.PendingUnlink || clear.PendingGeneration != nil {
		t.Fatalf("nil clear synthesized pending: %+v", clear)
	}
	first := setOrdinaryPending(t, file)
	requirePendingGeneration(t, first, 1)
	repeated := setOrdinaryPending(t, file)
	requirePendingGeneration(t, repeated, 1)
	if _, err := file.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t)}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("nil clear accepted a newly pending object: %v", err)
	}
	cleared := clearPending(t, file, first.PendingGeneration)
	if cleared.PendingUnlink || cleared.PendingGeneration != nil {
		t.Fatalf("ordinary clear = %+v", cleared)
	}
	requirePendingClearNoEffect(t, store, file, nil)
	second := setOrdinaryPending(t, file)
	requirePendingGeneration(t, second, 2)
	if _, err := file.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: first.PendingGeneration}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale clear erased a new ordinary episode: %v", err)
	}
	current, err := file.State(t.Context(), storage.StateScalars)
	if err != nil || !bytes.Equal(current.PendingGeneration, second.PendingGeneration) {
		t.Fatalf("stale clear changed pending state: %+v, %v", current, err)
	}
	clearPending(t, file, current.PendingGeneration)
}

func TestOrdinaryPendingGenerationAdvancesWhileCloseIntentStaysPending(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	doc, docReference, intent := openCloseUnlinkReference(t, store, false, storage.NodeRegular,
		storage.UseClaim{Uses: storage.ReadData | storage.DeleteName})
	t.Cleanup(func() {
		if err := doc.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	opened, err := store.OpenNodeRef(t.Context(), uint64(doc.id), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Action: fileAction(t), Use: storage.UseClaim{Uses: storage.DeleteName},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(doc.id)},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.Reference.(*retainedNodeReference).file
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if result, err := docReference.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("close intent trigger = %+v, %v", result, err)
	}
	initial, err := file.State(t.Context(), storage.StateScalars)
	if err != nil {
		t.Fatal(err)
	}
	requirePendingGeneration(t, initial, 1)
	requirePendingClearNoEffect(t, store, file, initial.PendingGeneration)
	first := setOrdinaryPending(t, file)
	requirePendingGeneration(t, first, 2)
	repeated := setOrdinaryPending(t, file)
	requirePendingGeneration(t, repeated, 2)
	cleared := clearPending(t, file, first.PendingGeneration)
	requirePendingGeneration(t, cleared, 2)
	requirePendingClearNoEffect(t, store, file, cleared.PendingGeneration)
	second := setOrdinaryPending(t, file)
	requirePendingGeneration(t, second, 3)
	if _, err := file.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: first.PendingGeneration}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("DOC aggregate hid ordinary ABA: %v", err)
	}
	current, err := file.State(t.Context(), storage.StateScalars)
	if err != nil {
		t.Fatal(err)
	}
	requirePendingGeneration(t, current, 3)
	clearPending(t, file, current.PendingGeneration)
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent)
	if err != nil || status.Outcome != storage.DeleteIntentPending {
		t.Fatalf("ordinary clear changed accepted close obligation: %+v, %v", status, err)
	}
	if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("final reference did not execute original close obligation: %+v, %v", result, err)
	}
	status, err = store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("original close obligation did not complete: %+v, %v", status, err)
	}
}

func TestPendingClearNoOpStillChecksNativeDeleteUseAndFinalPublication(t *testing.T) {
	store, file := openOrdinaryPendingReference(t)
	withoutDelete, err := store.OpenNodeRef(t.Context(), uint64(file.id), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Action: fileAction(t), Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(file.id)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer withoutDelete.Reference.Close(context.Background())
	if _, err := withoutDelete.Reference.(metastore.DeleteIntent).ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t)}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("nil clear without DELETE = %v", err)
	}
	for _, test := range []struct {
		uses []storage.TargetUse
		want error
	}{
		{[]storage.TargetUse{{NodeID: uint64(file.id + 1), Scope: file.scope}}, syscall.EINVAL},
		{[]storage.TargetUse{{NodeID: uint64(file.id), Scope: storage.UseScope{Token: "not-live"}}}, storage.ErrInvalidScope},
	} {
		if _, err := file.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t), Uses: test.uses}); !errors.Is(err, test.want) {
			t.Fatalf("nil clear with invalid use %v = %v", test.uses, err)
		}
	}
	checks := 0
	refused := errors.New("final clear publication refused")
	ctx := metastore.WithFilePublicationGuard(t.Context(), func() error {
		checks++
		if checks == 2 {
			return refused
		}
		return nil
	})
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Action: fileAction(t)}); !errors.Is(err, refused) || checks != 2 {
		t.Fatalf("no-op clear omitted final publication check: %v, checks %d", err, checks)
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("refused no-op clear changed durable state: %+v, %v", after, err)
	}
	requirePendingClearNoEffect(t, store, file, nil)
}

func TestPendingScalarTokenBypassesPayloadBudgetWhileMutationResultsRemainFull(t *testing.T) {
	store, file := openOrdinaryPendingReference(t)
	payload := bytes.Repeat([]byte("x"), 1024)
	if _, err := store.SetMetadata(t.Context(), uint64(file.id), "test.pending", nil, payload); err != nil {
		t.Fatal(err)
	}
	budgetCalls := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, metadataBytes int64) error {
		budgetCalls++
		if attr.ID != uint64(file.id) || attr.Metadata != nil || metadataBytes < int64(len(payload)) {
			t.Errorf("FULL result budget did not receive unloaded payload facts: %+v, %d", attr, metadataBytes)
		}
		return syscall.EFBIG
	})
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{Action: fileAction(t), Condition: storage.UnlinkFile}); !errors.Is(err, syscall.EFBIG) || budgetCalls != 1 {
		t.Fatalf("SET omitted FULL result budget: %v, calls %d", err, budgetCalls)
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("refused FULL result changed generation: %+v, %v", after, err)
	}
	state, err := file.State(ctx, storage.StateScalars)
	if err != nil || state.PendingUnlink || budgetCalls != 1 {
		t.Fatalf("refused SET or scalar observation changed pending: %+v, %v, calls %d", state, err, budgetCalls)
	}
	pending := setOrdinaryPending(t, file)
	if !bytes.Equal(pending.State.Metadata["test.pending"].Data, payload) {
		t.Fatal("SET no longer returns its original FULL metadata result")
	}
	state, err = file.State(ctx, storage.StateScalars)
	if err != nil || !bytes.Equal(state.PendingGeneration, pending.PendingGeneration) || state.State.Metadata != nil ||
		state.State.BirthTime != nil || state.State.ChangeTime != nil || !state.State.AccessTime.IsZero() || !state.State.ModTime.IsZero() || state.LinkTarget != nil || budgetCalls != 1 {
		t.Fatalf("scalar token loaded full payload or invoked its budget: %+v, %v, calls %d", state, err, budgetCalls)
	}
	if _, err := file.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: pending.PendingGeneration}); !errors.Is(err, syscall.EFBIG) || budgetCalls != 2 {
		t.Fatalf("CLEAR omitted FULL result budget: %v, calls %d", err, budgetCalls)
	}
	state, err = file.State(t.Context(), storage.StateScalars)
	if err != nil || !bytes.Equal(state.PendingGeneration, pending.PendingGeneration) {
		t.Fatalf("refused CLEAR changed pending state: %+v, %v", state, err)
	}
	clearPending(t, file, pending.PendingGeneration)
	if _, err := file.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Action: fileAction(t)}); !errors.Is(err, syscall.EFBIG) || budgetCalls != 3 {
		t.Fatalf("no-op CLEAR omitted original FULL result budget: %v, calls %d", err, budgetCalls)
	}
}
