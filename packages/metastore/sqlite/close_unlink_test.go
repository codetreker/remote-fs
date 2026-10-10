package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type closeUnlinkReference interface {
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}

func openCloseUnlinkReference(t *testing.T, store *LockingStore, nodeReference bool, kind storage.NodeKind, use storage.UseClaim) (*retainedFile, closeUnlinkReference, storage.DeleteIntentID) {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	selection := selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("victim")})
	id := storage.DeleteIntentID("dddddddddddddddddddddddddddddddd")
	condition := storage.UnlinkFile
	if kind == storage.NodeDirectory {
		condition = storage.UnlinkIfEmpty
	}
	intent := &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: id, Trigger: storage.OnReferenceClose, Condition: condition}
	if nodeReference {
		opened, err := store.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{
			Kind: kind, Create: true, Exclusive: true, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.Absent}, Use: use,
			MetadataAccess: storage.ReadMetadata, CloseIntent: intent,
		})
		if err != nil {
			t.Fatal(err)
		}
		return opened.Reference.(*retainedNodeReference).file, opened.Reference, id
	}
	opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Existing: storage.Keep, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.Absent}, Use: use, CloseIntent: intent,
	})
	if err != nil {
		t.Fatal(err)
	}
	return opened.File.(*retainedFile), opened.File, id
}

func TestDeleteOnCloseExemptsItsRetainedClaimThroughFinalization(t *testing.T) {
	for _, test := range []struct {
		name          string
		nodeReference bool
		kind          storage.NodeKind
		uses          storage.Uses
	}{
		{"File", false, storage.NodeRegular, storage.ReadData | storage.DeleteName},
		{"RegularNodeReference", true, storage.NodeRegular, storage.ReadData | storage.DeleteName},
		{"DirectoryNodeReference", true, storage.NodeDirectory, storage.ReadEntries | storage.DeleteName},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := OpenLocking(t.Context(), lockingTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			})
			claim := storage.UseClaim{Uses: test.uses, Deny: storage.DeleteName}
			file, reference, intent := openCloseUnlinkReference(t, store, test.nodeReference, test.kind, claim)
			publications := 0
			ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
				publications++
				if err := store.fileDomain.coordinator.CheckCloseUnlink(t.Context(), uint64(file.id), file.scope, claim); err != nil {
					t.Errorf("finalization lost exact original claim: %v", err)
				}
				if err := store.fileDomain.coordinator.CheckUse(t.Context(), uint64(file.id), storage.UseScope{}, storage.DeleteName); !errors.Is(err, storage.ErrUseConflict) {
					t.Errorf("finalization exposed anonymous delete: %v", err)
				}
				return func(storage.PublicationResult) error { return nil }, nil
			})
			result, err := reference.CloseWithResult(ctx)
			if err != nil || !result.Released || !result.Determined || publications != 2 {
				t.Fatalf("self-denying close = %+v, %v, publications %d", result, err, publications)
			}
			if status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent); err != nil || status.Outcome != storage.DeleteIntentCompleted {
				t.Fatalf("close intent = %+v, %v", status, err)
			}
			if _, err := store.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("deleted name = %v", err)
			}
			if _, err := store.StatNode(t.Context(), uint64(file.id)); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("deleted object = %v", err)
			}
			if err := store.fileDomain.coordinator.CheckCloseUnlink(t.Context(), uint64(file.id), file.scope, claim); !errors.Is(err, storage.ErrInvalidScope) {
				t.Fatalf("released reference retained use claim: %v", err)
			}
			if count := store.coordinator.pins[retainedNode{store.volume, file.id}]; count != 0 || store.fileDomain.files != 0 {
				t.Fatalf("released reference retained resources: pins %d, files %d", count, store.fileDomain.files)
			}
		})
	}
}

func TestDeleteOnCloseSettlesOnReadOnlyLastReferenceAndHonorsOtherClaims(t *testing.T) {
	for _, nodeReference := range []bool{false, true} {
		name := "File"
		if nodeReference {
			name = "NodeReference"
		}
		t.Run(name, func(t *testing.T) {
			store, err := OpenLocking(t.Context(), lockingTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			})
			armed, armingReference, intent := openCloseUnlinkReference(t, store, nodeReference, storage.NodeRegular,
				storage.UseClaim{Uses: storage.ReadData | storage.DeleteName})
			var last *retainedFile
			var lastReference closeUnlinkReference
			if nodeReference {
				opened, err := store.OpenNodeRef(t.Context(), uint64(armed.id), storage.NodeRefOptions{
					Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(armed.id)},
					Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
				})
				if err != nil {
					t.Fatal(err)
				}
				last, lastReference = opened.Reference.(*retainedNodeReference).file, opened.Reference
			} else {
				opened, err := store.OpenNode(t.Context(), uint64(armed.id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
				if err != nil {
					t.Fatal(err)
				}
				last, lastReference = opened.(*retainedFile), opened
			}
			if result, err := armingReference.CloseWithResult(t.Context()); err != nil || !result.Released {
				t.Fatalf("arming reference close = %+v, %v", result, err)
			}
			if status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent); err != nil || status.Outcome != storage.DeleteIntentPending {
				t.Fatalf("obligation before last reference = %+v, %v", status, err)
			}
			// A separate native claim exercises publication denial independently of
			// the reference's pin; it must never inherit the closing exemption.
			other := storage.UseScope{Token: "other-native-holder"}
			claim := storage.UseClaim{Uses: storage.ReadData, Deny: storage.DeleteName}
			if err := store.fileDomain.coordinator.AddUse(t.Context(), uint64(last.id), other, claim); err != nil {
				t.Fatal(err)
			}
			result, err := lastReference.CloseWithResult(t.Context())
			if result.Released || !result.Determined || !errors.Is(err, storage.ErrUseConflict) {
				t.Fatalf("other holder's deny = %+v, %v", result, err)
			}
			if err := store.fileDomain.coordinator.CheckUse(t.Context(), uint64(last.id), last.scope, storage.ReadData); err != nil {
				t.Fatalf("failed close lost original claim = %v", err)
			}
			if count := store.coordinator.pins[retainedNode{store.volume, last.id}]; count != 1 {
				t.Fatalf("failed close lost pin = %d", count)
			}
			if _, err := store.Stat(t.Context(), "victim"); err != nil {
				t.Fatalf("conflicting close removed name = %v", err)
			}
			if err := store.fileDomain.coordinator.DropUseExact(t.Context(), uint64(last.id), other, claim); err != nil {
				t.Fatal(err)
			}
			if result, err := lastReference.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
				t.Fatalf("read-only last reference close = %+v, %v", result, err)
			}
			if status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intent); err != nil || status.Outcome != storage.DeleteIntentCompleted {
				t.Fatalf("completed obligation = %+v, %v", status, err)
			}
		})
	}
}

func TestCloseUnlinkUnverifiableClaimFencesBeforeDeletingName(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "Missing"
		if changed {
			name = "Changed"
		}
		t.Run(name, func(t *testing.T) {
			store, err := OpenLocking(t.Context(), lockingTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			file, reference, _ := openCloseUnlinkReference(t, store, false, storage.NodeRegular,
				storage.UseClaim{Uses: storage.ReadData | storage.DeleteName, Deny: storage.DeleteName})
			poisoned := false
			t.Cleanup(func() {
				if poisoned {
					if err := store.coordinator.commit.acquire(context.Background()); err != nil {
						t.Errorf("cleanup poisoned reference: %v", err)
					} else {
						delete(store.files, file)
						delete(store.coordinator.pins, retainedNode{store.volume, file.id})
						store.fileDomain.files--
						store.coordinator.commit.release()
					}
				} else if _, err := reference.CloseWithResult(context.Background()); err != nil {
					t.Errorf("close reference: %v", err)
				}
				if err := store.Close(); poisoned && (err == nil || !store.Terminal()) || !poisoned && err != nil {
					t.Errorf("close store after injected claim inconsistency: %v, terminal %v", err, store.Terminal())
				}
			})
			if err := file.Retire(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := store.fileDomain.coordinator.DropUseExact(t.Context(), uint64(file.id), file.scope, file.use); err != nil {
				t.Fatal(err)
			}
			if changed {
				if err := store.fileDomain.coordinator.AddUse(t.Context(), uint64(file.id), file.scope, storage.UseClaim{Uses: storage.ReadData}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := reference.CloseWithResult(t.Context())
			poisoned = true
			if result.Released || result.Determined || !errors.Is(err, storage.ErrInvalidScope) || storage.ErrnoOf(err) != syscall.EIO {
				t.Fatalf("unverifiable claim = %+v, %v", result, err)
			}
			var named int
			if err := store.read.QueryRowContext(t.Context(), `SELECT count(*) FROM entries WHERE volume=? AND node=?`, store.volume, file.id).Scan(&named); err != nil || named != 1 {
				t.Fatalf("unverifiable claim removed name: count %d, error %v", named, err)
			}
			if count := store.coordinator.pins[retainedNode{store.volume, file.id}]; count != 1 {
				t.Fatalf("unverifiable claim released pin = %d", count)
			}
			if opened, err := store.OpenFile(t.Context(), "victim", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); opened != nil || storage.ErrnoOf(err) != syscall.EIO {
				t.Fatalf("unverifiable authority admitted open: reference %v, error %v", opened, err)
			}
		})
	}
}
