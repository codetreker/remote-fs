package sqlite

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func armRecoveryIntent(t *testing.T, store *LockingStore, name string, kind storage.NodeKind) (*retainedFile, closeUnlinkReference, storage.DeleteIntentID) {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	condition := storage.UnlinkFile
	if kind == storage.NodeDirectory {
		condition = storage.UnlinkIfEmpty
	}
	opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{
		Parent: directoryTarget(root), RawLeaf: []byte(name),
	}), storage.NodeRefOptions{
		Kind: kind, Create: true, Exclusive: true, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.Absent},
		Use:    storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: id, Trigger: storage.OnReferenceClose, Condition: condition},
	})
	if err != nil {
		t.Fatal(err)
	}
	return opened.Reference.(*retainedNodeReference).file, opened.Reference, id
}

func requireIntentOutcome(t *testing.T, store *LockingStore, intent storage.DeleteIntentID, outcome storage.DeleteIntentOutcome) {
	t.Helper()
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent)
	if err != nil || status.Outcome != outcome {
		t.Fatalf("intent = %+v, %v; want %v", status, err, outcome)
	}
}

func TestStartupClassifiesArmedBeforeBoundedReclamation(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.MaxDeleteIntents = 1
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "older"); err != nil {
		t.Fatal(err)
	}
	older, err := store.OpenFile(t.Context(), "older", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := older.(metastore.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
	}); err != nil {
		t.Fatal(err)
	}
	armed, _, intent := armRecoveryIntent(t, store, "newer", storage.NodeRegular)
	id := armed.id
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Stat(t.Context(), "older"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("older reclamation = %v", err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentPending)
	if f, err := store.OpenNode(t.Context(), uint64(id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); f != nil || !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("new pin = %v, %v", f, err)
	}
	if err := store.Remove(t.Context(), "newer"); !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("SDK unlink = %v", err)
	}
	var generation int64
	if err := store.read.QueryRowContext(t.Context(), `SELECT pending_generation FROM nodes WHERE volume=? AND id=?`, store.volume, id).Scan(&generation); err != nil || generation != 1 {
		t.Fatalf("pending generation = %d, %v", generation, err)
	}
	if err := store.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentCompleted)
}

func TestStartupArmedNonemptyDirectoryIsNotExecuted(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	_, _, intent := armRecoveryIntent(t, store, "directory", storage.NodeDirectory)
	if err := store.Create(t.Context(), "directory/child"); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireIntentOutcome(t, store, intent, storage.DeleteIntentNotExecuted)
	if _, err := store.Stat(t.Context(), "directory/child"); err != nil {
		t.Fatal(err)
	}
}

func TestStartupClassifiesMoreThanPublicationTargetLimit(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.MaxDeleteIntents = 4
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "older"); err != nil {
		t.Fatal(err)
	}
	older, err := store.OpenFile(t.Context(), "older", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := older.(metastore.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
	}); err != nil {
		t.Fatal(err)
	}
	var last *retainedFile
	var intent storage.DeleteIntentID
	for i := 0; i < 4; i++ {
		last, _, intent = armRecoveryIntent(t, store, fmt.Sprintf("armed-%d", i), storage.NodeRegular)
	}
	id := last.id
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireIntentOutcome(t, store, intent, storage.DeleteIntentPending)
	if reference, err := store.OpenNode(t.Context(), uint64(id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); reference != nil || !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("last armed admission = %v, %v", reference, err)
	}
}

func TestMaintenancePreservesLiveArmed(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, reference, intent := armRecoveryIntent(t, store, "live", storage.NodeRegular)
	if err := store.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentArmed)
	if _, err := reference.CloseWithResult(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentCompleted)
}

func TestArmedStartupRetainsOriginalLeaseRecoveryWindow(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	_, _, intent := armRecoveryIntent(t, store, "protected", storage.NodeRegular)
	if err := store.RaiseMaxLease(t.Context(), 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	store, err = OpenLocking(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if !store.file.Acquired().Equal(store.RecoveryStart()) || !store.file.Acquired().Equal(store.anchor.RecoveryStart()) {
		t.Fatal("startup replaced native acquisition")
	}
	if elapsed := time.Since(store.file.Acquired()); elapsed < 150*time.Millisecond {
		t.Fatalf("exposed armed volume before recovery: %v", elapsed)
	}
	if status, err := store.locks.Status(t.Context()); err != nil || status.Recovering {
		t.Fatalf("exposed recovering authority = %+v, %v", status, err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentCompleted)
}

func TestArmedStartupCancellationReleasesPrivateOwnership(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	_, _, intent := armRecoveryIntent(t, store, "protected", storage.NodeRegular)
	if err := store.RaiseMaxLease(t.Context(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if opened, err := OpenLocking(ctx, config); opened != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled startup = %v, %v", opened, err)
	}
	assertNativeOwnerReleased(t, config.Database)
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireIntentOutcome(t, store, intent, storage.DeleteIntentCompleted)
}

func TestStartupWithoutArmedDoesNotWaitForLeaseRecovery(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RaiseMaxLease(t.Context(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	store, err = OpenLocking(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if status, err := store.locks.Status(t.Context()); err != nil || !status.Recovering {
		t.Fatalf("no armed startup waited for recovery = %+v, %v", status, err)
	}
}

func TestArmedClassificationHonorsStrongConflict(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	_, _, intent := armRecoveryIntent(t, store, "protected", storage.NodeRegular)
	acquireLeaseTestGrant(t, store.LockService(), "protected", time.Second)
	if err := store.recoverArmedDeleteIntents(t.Context()); locking.CodeOf(err) != locking.Conflict {
		t.Fatalf("strong conflict = %v", err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentArmed)
}

func TestPhysicalDiscardSettlesArmedBeforeNodeDeletion(t *testing.T) {
	for _, operation := range []string{"Remove", "NameRemove", "RenameReplace", "NameRenameReplace", "AtomicReplace", "DetachedReclaim"} {
		t.Run(operation, func(t *testing.T) {
			config := lockingTestConfig(t)
			store, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			file, reference, intent := armRecoveryIntent(t, store, "victim", storage.NodeRegular)
			id := file.id
			// Suppress the in-memory trigger to isolate physical discard from the
			// ordinary reference-close transition; the obligation was accepted by OpenChildRef.
			file.closeIntent = ""
			if operation == "DetachedReclaim" {
				if err := store.Remove(t.Context(), "victim"); err != nil {
					t.Fatal(err)
				}
				requireIntentOutcome(t, store, intent, storage.DeleteIntentArmed)
			}
			if _, err := reference.CloseWithResult(t.Context()); err != nil {
				t.Fatal(err)
			}
			if operation != "DetachedReclaim" {
				requireIntentOutcome(t, store, intent, storage.DeleteIntentArmed)
			}
			root, err := store.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("victim")}
			switch operation {
			case "Remove":
				err = store.Remove(t.Context(), "victim")
			case "NameRemove":
				_, err = store.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Action: fileAction(t), Name: name, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(id)}})
			case "RenameReplace", "NameRenameReplace":
				if err := store.Create(t.Context(), "source"); err != nil {
					t.Fatal(err)
				}
				if operation == "RenameReplace" {
					err = store.Rename(t.Context(), "source", "victim")
				} else {
					_, err = store.MutateName(t.Context(), storage.NameCommand{
						Kind: storage.NameRename, Action: fileAction(t),
						Name:   storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("source")},
						Target: storage.ChildCondition{State: storage.Any}, Destination: &storage.RenameTarget{
							Parent: directoryTarget(root), ObservedLeaf: name.RawLeaf, OutputLeaf: name.RawLeaf,
							Expected: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(id)},
						},
					})
				}
			case "AtomicReplace":
				var result metastore.OpenResult
				result, err = store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
					Read: true, Write: true, Create: true, Existing: storage.ReplaceNode, Action: fileAction(t),
					Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(id)},
					Use:    storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
				})
				if result.File != nil {
					err = errors.Join(err, result.File.Close(t.Context()))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			requireIntentOutcome(t, store, intent, storage.DeleteIntentNotExecuted)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			config.Initialize = false
			store, err = OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatalf("persistent integrity reopen: %v", err)
			}
			defer store.Close()
			requireIntentOutcome(t, store, intent, storage.DeleteIntentNotExecuted)
			if operation == "RenameReplace" || operation == "NameRenameReplace" || operation == "AtomicReplace" {
				if replacement, err := store.Stat(t.Context(), "victim"); err != nil || replacement.ID == id {
					t.Fatalf("replacement identity = %+v, %v", replacement, err)
				}
			} else if _, err := store.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("discarded name = %v", err)
			}
		})
	}
}

func TestPinnedDiscardPreservesArmedUntilClose(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, reference, intent := armRecoveryIntent(t, store, "victim", storage.NodeRegular)
	if err := store.Remove(t.Context(), "victim"); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentArmed)
	if err := store.Create(t.Context(), "victim"); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.Stat(t.Context(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.CloseWithResult(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentNotExecuted)
	if after, err := store.Stat(t.Context(), "victim"); err != nil || after.ID != replacement.ID {
		t.Fatalf("replacement after close = %+v, %v", after, err)
	}
}

func TestPendingCloseDeleteDirectoryRejectsLegacyNameAddition(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	file, initiator, intent := armRecoveryIntent(t, store, "directory", storage.NodeDirectory)
	last, err := store.OpenNodeRef(t.Context(), uint64(file.id), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Action: fileAction(t), Target: storage.ChildCondition{State: storage.Any},
		MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := initiator.CloseWithResult(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentPending)
	for _, operation := range []struct {
		name  string
		apply func() error
	}{
		{"Commit", func() error { return store.Commit(t.Context(), "directory/commit", metastore.Object{}) }},
		{"Create", func() error { return store.Create(t.Context(), "directory/create") }},
		{"Mkdir", func() error { return store.Mkdir(t.Context(), "directory/mkdir") }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.apply(); !errors.Is(err, storage.ErrPendingDelete) {
				t.Fatalf("pending directory name addition = %v", err)
			}
		})
	}
	if children, err := store.List(t.Context(), "directory"); err != nil || len(children) != 0 {
		t.Fatalf("pending directory changed = %+v, %v", children, err)
	}
	if result, err := last.Reference.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("last close = %+v, %v", result, err)
	}
	requireIntentOutcome(t, store, intent, storage.DeleteIntentCompleted)
}
