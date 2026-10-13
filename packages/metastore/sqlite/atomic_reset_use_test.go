package sqlite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAtomicDeleteResetPreservesIdentityMetadataAndActualDataAccess(t *testing.T) {
	store, _, node, selection := guardedFileFixture(t)
	birth, accessed := time.Unix(10, 0), time.Unix(20, 0)
	modified := time.Unix(30, 0)
	if _, err := store.SetNodeAttr(t.Context(), uint64(node.ID), storage.AttrChange{BirthTime: &birth, AccessTime: &accessed}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetMetadata(t.Context(), uint64(node.ID), "test.preserved", nil, []byte("unchanged")); err != nil {
		t.Fatal(err)
	}
	previous, err := store.SetMetadata(t.Context(), uint64(node.ID), "test.reset", nil, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	observer, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
		Action: fileAction(t), MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Deny: storage.WriteData},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Reference.Close(context.Background())
	before, err := observer.Reference.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var accounting [][2]int64
	opened, err := store.OpenAt(accountingContext(t, &accounting), selectChild(selection.Name), storage.OpenAtOptions{
		Action: fileAction(t), Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
		Use: storage.UseClaim{Uses: storage.DeleteName, Deny: storage.DeleteName}, Existing: storage.ResetContent, ResetUse: storage.DeleteName,
		MetadataAccess: storage.WriteMetadata,
		Initial: storage.InitialState{OnReset: storage.InitialFields{
			Attr: storage.AttrChange{ModTime: &modified}, Metadata: map[string][]byte{"test.reset": []byte("after")},
		}},
	})
	if err != nil || opened.File == nil || opened.Outcome != storage.Reset || opened.State.ID != node.ID || opened.State.Size != 0 {
		t.Fatalf("delete-only reset = %+v, %v", opened, err)
	}
	defer opened.File.Close(context.Background())
	if !reflect.DeepEqual(accounting, [][2]int64{{4, 0}}) {
		t.Fatalf("reset publication accounting = %v", accounting)
	}
	after, err := observer.Reference.Node(t.Context())
	if err != nil || after.ID != node.ID || after.Size != 0 || after.Revision == before.Revision ||
		after.BirthTime == nil || !after.BirthTime.Equal(birth) || !after.AccessTime.Equal(accessed) || !after.ModTime.Equal(modified) ||
		after.ChangeTime == nil || before.ChangeTime == nil || !after.ChangeTime.After(*before.ChangeTime) || !bytes.Equal(after.Metadata["test.preserved"].Data, []byte("unchanged")) ||
		!bytes.Equal(after.Metadata["test.reset"].Data, []byte("after")) || bytes.Equal(after.Metadata["test.reset"].Version, previous.Version) {
		t.Fatalf("original reference after reset = %+v, %v", after, err)
	}
	if used, err := store.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("reset retained content quota = %d, %v", used, err)
	}
	for _, use := range []storage.Uses{storage.ReadData, storage.WriteData} {
		if _, err := opened.File.Node(metastore.WithFileAccess(t.Context(), metastore.FileAccess{Uses: use})); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("delete-only reset granted data use %d: %v", use, err)
		}
	}
	if _, err := opened.File.Reserve(t.Context(), 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete-only reset granted write reservation: %v", err)
	}
	if _, err := opened.File.(metastore.ReferenceStateAccess).State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete-only reset granted metadata read: %v", err)
	}
}

func TestAtomicResetUseEnforcesEffectSharingAndReturnedWriterSharing(t *testing.T) {
	for _, test := range []struct {
		name     string
		resetUse storage.Uses
		write    bool
		deny     storage.Uses
	}{
		{"delete denied", storage.DeleteName, false, storage.DeleteName},
		{"ordinary overwrite with delete claim still needs write", 0, true, storage.WriteData},
		{"delete reset with data writer still needs writer sharing", storage.DeleteName, true, storage.WriteData},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _, node, selection := guardedFileFixture(t)
			observer, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{
				Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
				Action: fileAction(t), Use: storage.UseClaim{Deny: test.deny},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Reference.Close(context.Background())
			before, err := store.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			position, err := store.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			uses := storage.DeleteName
			if test.write {
				uses |= storage.WriteData
			}
			var accounting [][2]int64
			opened, err := store.OpenAt(accountingContext(t, &accounting), selectChild(selection.Name), storage.OpenAtOptions{
				Action: fileAction(t), Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
				Write: test.write, Use: storage.UseClaim{Uses: uses}, Existing: storage.ResetContent, ResetUse: test.resetUse,
			})
			if !errors.Is(err, storage.ErrUseConflict) || opened.File != nil || opened.State.ID != 0 || opened.Outcome != 0 {
				t.Fatalf("denied reset = %+v, %v", opened, err)
			}
			if after, err := store.Stat(t.Context(), "file"); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("denied reset changed state = %+v, %v", after, err)
			}
			if after, err := store.CommittedPosition(t.Context()); err != nil || after != position || len(accounting) != 0 || store.nodePinCountLocked(node.ID) != 1 {
				t.Fatalf("denied reset changed publication or retention: position %d, accounting %v, %v", after, accounting, err)
			}
		})
	}
}

func TestAtomicDeleteResetRetainsStrongWritePublicationRules(t *testing.T) {
	store, _, node, selected := guardedFileFixture(t)
	fixture := publicationFixture{store: store.Store, service: store.LockService()}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Exclusive)
	options := storage.OpenAtOptions{Action: fileAction(t), Existing: storage.ResetContent, ResetUse: storage.DeleteName,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}, Use: storage.UseClaim{Uses: storage.DeleteName}}
	selection := selectChild(selected.Name)
	opened, err := fixture.store.OpenAt(t.Context(), selection, options)
	requirePublicationCode(t, err, locking.Conflict)
	if opened.File != nil {
		t.Fatal("rejected strong publication returned a reference")
	}
	if after := fixture.node(t, "file"); !reflect.DeepEqual(after, node) {
		t.Fatalf("rejected strong reset changed node: %+v", after)
	}
	options.Action = fileAction(t)
	opened, err = fixture.store.OpenAt(publicationScope(t.Context(), owner, grant), selection, options)
	if err != nil || opened.File == nil || opened.State.ID != node.ID || opened.State.Size != 0 || opened.Outcome != storage.Reset {
		t.Fatalf("exclusive-owner delete reset = %+v, %v", opened, err)
	}
	defer opened.File.Close(context.Background())
	status, err := fixture.service.QueryGrant(t.Context(), owner, grant)
	if err != nil || status.State != locking.Active {
		t.Fatalf("content reset retired the original strong resource: %+v, %v", status, err)
	}
}

func TestAtomicDeleteResetRollsBackMetadataQuotaAndReferenceOnPublicationRefusal(t *testing.T) {
	for _, failure := range []string{"publication", "accounting", "metadata CAS"} {
		t.Run(failure, func(t *testing.T) {
			store, _, node, selection := guardedFileFixture(t)
			before, err := store.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			position, err := store.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			refused := errors.New("reset publication refused")
			ctx := t.Context()
			options := storage.OpenAtOptions{Action: fileAction(t), Existing: storage.ResetContent, ResetUse: storage.DeleteName,
				Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}, Use: storage.UseClaim{Uses: storage.DeleteName},
				Initial: storage.InitialState{OnReset: storage.InitialFields{Metadata: map[string][]byte{"test.refused": []byte("new")}}}}
			want := refused
			switch failure {
			case "publication":
				ctx = metastore.WithFilePublicationGuard(ctx, func() error { return refused })
			case "accounting":
				ctx = storage.WithPublicationAccounting(ctx, func(int64, int64) (storage.PublicationSettlement, error) { return nil, refused })
			case "metadata CAS":
				options.Target.ExpectedMetadata = map[string][]byte{"test.missing": []byte("stale")}
				want = storage.ErrConditionConflict
			}
			opened, err := store.OpenAt(ctx, selectChild(selection.Name), options)
			if !errors.Is(err, want) || opened.File != nil || opened.State.ID != 0 || opened.Outcome != 0 {
				t.Fatalf("refused reset = %+v, %v", opened, err)
			}
			if after, err := store.Stat(t.Context(), "file"); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused reset changed node: %+v, %v", after, err)
			}
			if after, err := store.CommittedPosition(t.Context()); err != nil || after != position || store.fileDomain.files != 0 || store.nodePinCountLocked(node.ID) != 0 {
				t.Fatalf("refused reset changed log or reference ownership: %d, %v", after, err)
			}
			if used, err := store.Usage(t.Context()); err != nil || used != 4 {
				t.Fatalf("refused reset changed quota: %d, %v", used, err)
			}
		})
	}
}
