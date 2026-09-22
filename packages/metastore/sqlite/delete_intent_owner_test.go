package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func armAndCloseDeleteIntent(
	t *testing.T,
	store *LockingStore,
	name string,
	owner storage.DeleteIntentOwner,
	id storage.DeleteIntentID,
) error {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: directoryTarget(root), RawLeaf: []byte(name),
	}}, storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Existing: storage.Keep,
		Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{
			ID: id, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile,
		},
	})
	if err != nil {
		return err
	}
	return opened.File.Close(t.Context())
}

func TestDeleteIntentDiscoveryIsOwnerScopedAndSequenceOrdered(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner := storage.DeleteIntentOwner("owner-a")
	other := storage.DeleteIntentOwner("owner-b")
	first := storage.DeleteIntentID("ffffffffffffffffffffffffffffffff")
	lower := storage.DeleteIntentID("00000000000000000000000000000000")
	foreign := storage.DeleteIntentID("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err := armAndCloseDeleteIntent(t, store, "first", owner, first); err != nil {
		t.Fatal(err)
	}

	page, err := store.ListDeleteIntents(t.Context(), owner, 0, 1)
	if err != nil || len(page.Intents) != 1 || page.Intents[0].ID != first || page.Next == 0 {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	if err := armAndCloseDeleteIntent(t, store, "lower", owner, lower); err != nil {
		t.Fatal(err)
	}
	if err := armAndCloseDeleteIntent(t, store, "foreign", other, foreign); err != nil {
		t.Fatal(err)
	}

	next, err := store.ListDeleteIntents(t.Context(), owner, page.Next, storage.MaxDeleteIntentPageEntries)
	if err != nil || len(next.Intents) != 1 || next.Intents[0].ID != lower || next.Next <= page.Next {
		t.Fatalf("page after concurrent lower ID = %+v, %v", next, err)
	}
	empty, err := store.ListDeleteIntents(t.Context(), owner, next.Next, 1)
	if err != nil || len(empty.Intents) != 0 || empty.Next != next.Next {
		t.Fatalf("empty tail page = %+v, %v", empty, err)
	}
	foreignPage, err := store.ListDeleteIntents(t.Context(), other, 0, 2)
	if err != nil || len(foreignPage.Intents) != 1 || foreignPage.Intents[0].ID != foreign {
		t.Fatalf("foreign owner page = %+v, %v", foreignPage, err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), other, first)
	if err != nil || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("wrong-owner query = %+v, %v", status, err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: other, Intent: first,
	}); err != nil {
		t.Fatalf("wrong-owner acknowledgement = %v", err)
	}
	status, err = store.QueryDeleteIntent(t.Context(), owner, first)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("wrong owner removed the intent: %+v, %v", status, err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: owner, Intent: first,
	}); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.ListDeleteIntents(t.Context(), owner, 0, 2)
	if err != nil || len(remaining.Intents) != 1 || remaining.Intents[0].ID != lower {
		t.Fatalf("owner page after acknowledgement = %+v, %v", remaining, err)
	}
}

func TestDeleteIntentSequenceIsNotReusedAfterCapacityIsReleased(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.MaxDeleteIntents = 1
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner := storage.DeleteIntentOwner("bounded-owner")
	first := storage.DeleteIntentID("11111111111111111111111111111111")
	second := storage.DeleteIntentID("00000000000000000000000000000000")
	if err := armAndCloseDeleteIntent(t, store, "first", owner, first); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListDeleteIntents(t.Context(), owner, 0, 1)
	if err != nil || len(page.Intents) != 1 {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	if err := armAndCloseDeleteIntent(t, store, "blocked", owner, second); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("capacity exhaustion = %v", err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: owner, Intent: first,
	}); err != nil {
		t.Fatal(err)
	}
	if err := armAndCloseDeleteIntent(t, store, "second", owner, second); err != nil {
		t.Fatal(err)
	}
	next, err := store.ListDeleteIntents(t.Context(), owner, page.Next, 1)
	if err != nil || len(next.Intents) != 1 || next.Intents[0].ID != second || next.Next <= page.Next {
		t.Fatalf("replacement page = %+v, %v", next, err)
	}
}

func TestDeleteIntentOwnerOperationsHonorCancellation(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner := storage.DeleteIntentOwner("cancel-owner")
	id := storage.DeleteIntentID("cccccccccccccccccccccccccccccccc")
	if err := armAndCloseDeleteIntent(t, store, "file", owner, id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.QueryDeleteIntent(ctx, owner, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query = %v", err)
	}
	if _, err := store.ListDeleteIntents(ctx, owner, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled list = %v", err)
	}
	if err := store.AcknowledgeDeleteIntent(ctx, storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: owner, Intent: id,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acknowledgement = %v", err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), owner, id)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("cancelled acknowledgement removed the intent: %+v, %v", status, err)
	}
}
