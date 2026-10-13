package sqlite

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNodeReferenceAdmissionRefusalProvesNoEffects(t *testing.T) {
	for _, failure := range []struct {
		name  string
		cause error
		errno syscall.Errno
		ctx   func(context.Context) (context.Context, context.CancelFunc)
	}{
		{"canceled", context.Canceled, syscall.EINTR, func(parent context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(parent)
			cancel()
			return ctx, cancel
		}},
		{"deadline", context.DeadlineExceeded, syscall.EIO, func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithDeadline(parent, time.Now().Add(-time.Nanosecond))
		}},
	} {
		for _, withIntent := range []bool{false, true} {
			name := failure.name
			if withIntent {
				name += "/close-intent"
			}
			t.Run(name, func(t *testing.T) {
				store, directory := nodeReferenceAdmissionStore(t)
				options := nodeReferenceAdmissionOptions(t, directory)
				owner := storage.DeleteIntentOwner("admission-refusal")
				intent, err := storage.NewDeleteIntentID()
				if err != nil {
					t.Fatal(err)
				}
				if withIntent {
					options.Use.Uses |= storage.DeleteName
					options.CloseIntent = &storage.CloseIntent{
						ID: intent, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty,
					}
				}
				if err := options.Check(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := failure.ctx(t.Context())
				defer cancel()
				result, err := func() (metastore.NodeOpenResult, error) {
					if err := store.coordinator.commit.acquire(t.Context()); err != nil {
						t.Fatal(err)
					}
					defer store.coordinator.commit.release()
					return store.OpenNodeRef(ctx, uint64(directory.ID), options)
				}()
				if err != failure.cause || !errors.Is(err, failure.cause) || storage.ErrnoOf(err) != failure.errno {
					t.Fatalf("admission error = %v (%v), want original %v (%v)", err, storage.ErrnoOf(err), failure.cause, failure.errno)
				}
				if !reflect.DeepEqual(result, metastore.NodeOpenResult{NotExecuted: true}) {
					t.Fatalf("refused admission result = %+v, want only NotExecuted", result)
				}
				status, err := store.QueryDeleteIntent(t.Context(), owner, intent)
				if err != nil || status.Outcome != storage.DeleteIntentUnknown {
					t.Fatalf("unaccepted close intent = %+v, %v", status, err)
				}
				page, err := store.ListDeleteIntents(t.Context(), owner, 0, 1)
				if err != nil || len(page.Intents) != 0 || page.Next != 0 {
					t.Fatalf("close intents after admission refusal = %+v, %v", page, err)
				}
				assertNodeReferenceAdmissionRecovery(t, store, directory)
			})
		}
	}
}

func TestNodeReferenceFailureAfterAdmissionDoesNotClaimNotExecuted(t *testing.T) {
	store, directory := nodeReferenceAdmissionStore(t)
	calls := 0
	ctx := metastore.WithFilePublicationGuard(t.Context(), func() error {
		calls++
		return context.Canceled
	})
	result, err := store.OpenNodeRef(ctx, uint64(directory.ID), nodeReferenceAdmissionOptions(t, directory))
	if calls != 1 || !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR {
		t.Fatalf("post-admission failure = %v (%v), publication checks %d", err, storage.ErrnoOf(err), calls)
	}
	if !reflect.DeepEqual(result, metastore.NodeOpenResult{}) {
		t.Fatalf("post-admission failure result = %+v, want no nonexecution proof", result)
	}
	assertNodeReferenceAdmissionRecovery(t, store, directory)
}

func nodeReferenceAdmissionStore(t *testing.T) (*LockingStore, metastore.Node) {
	t.Helper()
	config := lockingTestConfig(t)
	config.SQLite.MaxRetainedFiles = 1
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing admission fixture: %v", err)
		}
	})
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	return store, directory
}

func nodeReferenceAdmissionOptions(t *testing.T, directory metastore.Node) storage.NodeRefOptions {
	t.Helper()
	return storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadEntries}, MetadataAccess: storage.ReadMetadata,
	}
}

func assertNodeReferenceAdmissionRecovery(t *testing.T, store *LockingStore, directory metastore.Node) {
	t.Helper()
	assertNoNativeNodeReferences(t, store)
	options := nodeReferenceAdmissionOptions(t, directory)
	options.Use.Deny = storage.ReadEntries
	opened, err := store.OpenNodeRef(t.Context(), uint64(directory.ID), options)
	if opened.Reference != nil {
		t.Cleanup(func() {
			if err := opened.Reference.Close(context.Background()); err != nil {
				t.Errorf("closing recovery reference: %v", err)
			}
		})
	}
	if err != nil || opened.Reference == nil || opened.NotExecuted || opened.Outcome != storage.Opened || opened.State.ID != directory.ID {
		t.Fatalf("directory recovery open = %+v, %v", opened, err)
	}
	result, err := opened.Reference.CloseWithResult(t.Context())
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("directory recovery close = %+v, %v", result, err)
	}
	assertNoNativeNodeReferences(t, store)
}

func assertNoNativeNodeReferences(t *testing.T, store *LockingStore) {
	t.Helper()
	if err := store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer store.coordinator.commit.release()
	if len(store.files) != 0 || store.fileDomain.files != 0 || len(store.coordinator.pins) != 0 {
		t.Fatalf("native reference state: retained=%d volume=%d pins=%v", len(store.files), store.fileDomain.files, store.coordinator.pins)
	}
}
