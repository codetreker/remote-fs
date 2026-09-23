package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func createOwnedIntent(t *testing.T, store *LockingStore, name string, owner storage.DeleteIntentOwner, id storage.DeleteIntentID) error {
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
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{ID: id, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		return err
	}
	return opened.File.Close(t.Context())
}

func TestOwnedDeleteIntentsRemainDiscoverableThroughPaginationAndAck(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner, other := storage.DeleteIntentOwner("owner-a"), storage.DeleteIntentOwner("owner-b")
	first := storage.DeleteIntentID("ffffffffffffffffffffffffffffffff")
	second := storage.DeleteIntentID("00000000000000000000000000000000")
	foreign := storage.DeleteIntentID("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err := createOwnedIntent(t, store, "first", owner, first); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListDeleteIntents(t.Context(), owner, 0, 1)
	if err != nil || len(page.Intents) != 1 || page.Intents[0].ID != first || page.Next == 0 {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	if err := createOwnedIntent(t, store, "second", owner, second); err != nil {
		t.Fatal(err)
	}
	if err := createOwnedIntent(t, store, "foreign", other, foreign); err != nil {
		t.Fatal(err)
	}
	page2, err := store.ListDeleteIntents(t.Context(), owner, page.Next, 1)
	if err != nil || len(page2.Intents) != 1 || page2.Intents[0].ID != second || page2.Next <= page.Next {
		t.Fatalf("second page = %+v, %v", page2, err)
	}
	if hidden, err := store.QueryDeleteIntent(t.Context(), other, first); err != nil || hidden.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("wrong-owner query = %+v, %v", hidden, err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: other, Intent: first,
	}); err != nil {
		t.Fatalf("wrong-owner ack = %v", err)
	}
	if status, err := store.QueryDeleteIntent(t.Context(), owner, first); err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("wrong-owner ack changed intent = %+v, %v", status, err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: owner, Intent: first,
	}); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.ListDeleteIntents(t.Context(), owner, 0, 2)
	if err != nil || len(remaining.Intents) != 1 || remaining.Intents[0].ID != second {
		t.Fatalf("page after ack = %+v, %v", remaining, err)
	}
	foreignPage, err := store.ListDeleteIntents(t.Context(), other, 0, 1)
	if err != nil || len(foreignPage.Intents) != 1 || foreignPage.Intents[0].ID != foreign {
		t.Fatalf("foreign page = %+v, %v", foreignPage, err)
	}
}

func TestDeleteIntentCursorIsNotReusedAfterCapacityReturns(t *testing.T) {
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
	if err := createOwnedIntent(t, store, "first", owner, first); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListDeleteIntents(t.Context(), owner, 0, 1)
	if err != nil || len(page.Intents) != 1 {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	if err := createOwnedIntent(t, store, "full", owner, second); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("full intent history = %v", err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Owner: owner, Intent: first,
	}); err != nil {
		t.Fatal(err)
	}
	if err := createOwnedIntent(t, store, "second", owner, second); err != nil {
		t.Fatal(err)
	}
	next, err := store.ListDeleteIntents(t.Context(), owner, page.Next, 1)
	if err != nil || len(next.Intents) != 1 || next.Intents[0].ID != second || next.Next <= page.Next {
		t.Fatalf("post-ack page = %+v, %v", next, err)
	}
	empty, err := store.ListDeleteIntents(t.Context(), owner, next.Next, 1)
	if err != nil || len(empty.Intents) != 0 || empty.Next != next.Next {
		t.Fatalf("empty tail = %+v, %v", empty, err)
	}
}

func TestOwnedDeleteIntentOperationsHonorCancellation(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	owner := storage.DeleteIntentOwner("cancel-owner")
	id := storage.DeleteIntentID("cccccccccccccccccccccccccccccccc")
	if err := createOwnedIntent(t, store, "file", owner, id); err != nil {
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
	if status, err := store.QueryDeleteIntent(t.Context(), owner, id); err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("cancelled acknowledgement changed intent = %+v, %v", status, err)
	}
}
