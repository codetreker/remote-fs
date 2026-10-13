package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func descendantFixture(t *testing.T) (*LockingStore, storage.NameCommand) {
	t.Helper()
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, path := range []string{"source", "source/child", "outside"} {
		if err := store.Mkdir(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"source/child/file", "outside/file"} {
		if err := store.Create(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.Stat(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	return store, storage.NameCommand{Kind: storage.NameRename, Action: fileAction(t), RequireNoOpenDescendants: true,
		Name:        storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("source")},
		Target:      storage.ChildCondition{State: storage.SameNode, NodeID: uint64(source.ID)},
		Destination: &storage.RenameTarget{Parent: directoryTarget(root), ObservedLeaf: []byte("moved"), OutputLeaf: []byte("moved"), Expected: storage.ChildCondition{State: storage.Absent}},
	}
}

func descendantReference(t *testing.T, store *Store, path string, use storage.UseClaim) metastore.NodeReference {
	t.Helper()
	node, err := store.Stat(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: node.Kind, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}, Use: use, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return opened.Reference
}

func rejectedDescendantMutation(t *testing.T, store *Store, command storage.NameCommand, want error) {
	t.Helper()
	before := observedDirectory(t, store, uint64(store.root))
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.MutateName(t.Context(), command)
	if !errors.Is(err, want) || result.Attr != nil {
		t.Fatalf("descendant mutation = %+v, %v; want %v", result, err, want)
	}
	if after := observedDirectory(t, store, uint64(store.root)); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected descendant mutation changed the namespace")
	}
	if after, err := store.CommittedPosition(t.Context()); err != nil || after != position {
		t.Fatalf("rejection advanced position: %v, %v", after, err)
	}
}

func TestNameRenameRejectsEveryRetainedDescendantKind(t *testing.T) {
	for _, test := range []struct {
		name, path string
		use        storage.UseClaim
	}{
		{"metadata file", "source/child/file", storage.UseClaim{}},
		{"read file sharing delete", "source/child/file", storage.UseClaim{Uses: storage.ReadData}},
		{"metadata directory", "source/child", storage.UseClaim{}},
		{"enumerated directory", "source/child", storage.UseClaim{Uses: storage.ReadEntries}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, command := descendantFixture(t)
			ref := descendantReference(t, store.Store, test.path, test.use)
			rejectedDescendantMutation(t, store.Store, command, syscall.EACCES)
			command.Destination.ObservedLeaf, command.Destination.OutputLeaf = []byte("source"), []byte("source")
			command.Destination.Expected = command.Target
			rejectedDescendantMutation(t, store.Store, command, syscall.EACCES)
			if err := ref.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := store.MutateName(t.Context(), command); err != nil {
				t.Fatalf("settled descendant remains blocking: %v", err)
			}
		})
	}
}

func TestNameRenameExcludesSourceReferenceAndPreservesItsSharingCheck(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(map[bool]string{false: "source only", true: "source sharing denies"}[denied], func(t *testing.T) {
			store, command := descendantFixture(t)
			use := storage.UseClaim{}
			if denied {
				use.Deny = storage.DeleteName
			}
			descendantReference(t, store.Store, "source", use)
			if denied {
				rejectedDescendantMutation(t, store.Store, command, storage.ErrUseConflict)
				return
			}
			if result, err := store.MutateName(t.Context(), command); err != nil || result.Attr == nil || result.Attr.ID != command.Target.NodeID {
				t.Fatalf("source reference rename = %+v, %v", result, err)
			}
		})
	}
}

func TestNameRenameIgnoresValidatedDetachedReference(t *testing.T) {
	store, command := descendantFixture(t)
	ref := descendantReference(t, store.Store, "source/child/file", storage.UseClaim{})
	if err := store.Remove(t.Context(), "source/child/file"); err != nil {
		t.Fatal(err)
	}
	observation, err := ref.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
	if err != nil || observation.State != storage.NameDetached {
		t.Fatalf("detached = %+v, %v", observation, err)
	}
	if _, err := store.MutateName(t.Context(), command); err != nil {
		t.Fatalf("detached reference blocked rename: %v", err)
	}
}

func TestNameRenameKeepsUnsettledAndRetiredReferencesInItsPredicate(t *testing.T) {
	store, command := descendantFixture(t)
	ref := descendantReference(t, store.Store, "source/child/file", storage.UseClaim{})
	if err := ref.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := ref.CloseWithResult(canceled); !errors.Is(err, context.Canceled) || result.Released {
		t.Fatalf("unsettled close = %+v, %v", result, err)
	}
	rejectedDescendantMutation(t, store.Store, command, syscall.EACCES)
	if result, err := ref.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("settled close = %+v, %v", result, err)
	}
	if _, err := store.MutateName(t.Context(), command); err != nil {
		t.Fatal(err)
	}
}

func TestNameRenameLeavesRawEntryPolicyUnchanged(t *testing.T) {
	store, command := descendantFixture(t)
	descendantReference(t, store.Store, "source/child/file", storage.UseClaim{})
	command.RequireNoOpenDescendants = false
	if _, err := store.MutateName(t.Context(), command); err != nil {
		t.Fatalf("raw rename with retained descendant = %v", err)
	}
}

func TestNameRenameDescendantCheckRevalidatesFinalTransaction(t *testing.T) {
	store, command := descendantFixture(t)
	if err := store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err := store.inspect(t.Context(), func(tx *sql.Tx) error { _, err := store.prepareNamespaceMutation(t.Context(), tx, command); return err })
	store.coordinator.commit.release()
	if err != nil {
		t.Fatal(err)
	}
	descendantReference(t, store.Store, "source/child/file", storage.UseClaim{})
	if err := store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = store.mutateTransactionLocked(t.Context(), t.Context(), nil, func(tx *sql.Tx) error {
		_, err := store.prepareNamespaceMutation(t.Context(), tx, command)
		return err
	})
	store.coordinator.commit.release()
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("final reference admission = %v", err)
	}
	if node, err := store.Stat(t.Context(), "source"); err != nil || uint64(node.ID) != command.Target.NodeID {
		t.Fatalf("final refusal changed source: %+v, %v", node, err)
	}
}

func TestNameRenameBoundsAggregateReferenceAncestryWork(t *testing.T) {
	for _, limit := range []int64{3, 4} {
		t.Run(map[int64]string{3: "exhausted", 4: "exact"}[limit], func(t *testing.T) {
			store, command := descendantFixture(t)
			descendantReference(t, store.Store, "outside/file", storage.UseClaim{})
			store.maxIntegrityRecords = limit
			if limit == 3 {
				rejectedDescendantMutation(t, store.Store, command, syscall.EFBIG)
				return
			}
			if _, err := store.MutateName(t.Context(), command); err != nil {
				t.Fatalf("exact candidate plus header budget = %v", err)
			}
		})
	}
}

func TestNameRenamePinCandidatesAreVolumeLocalAndBounded(t *testing.T) {
	store, command := descendantFixture(t)
	foreign := make(map[int64]int, store.fileDomain.maxFiles+1)
	for i := range store.fileDomain.maxFiles + 1 {
		foreign[int64(i)+1] = 1
	}
	store.coordinator.pins[store.volume+1] = foreign
	t.Cleanup(func() { delete(store.coordinator.pins, store.volume+1) })
	if _, err := store.MutateName(t.Context(), command); err != nil {
		t.Fatalf("foreign pins affected rename: %v", err)
	}
	command.Name.RawLeaf = []byte("moved")
	command.Destination.ObservedLeaf, command.Destination.OutputLeaf = []byte("source"), []byte("source")
	descendantReference(t, store.Store, "outside/file", storage.UseClaim{})
	maxFiles := store.fileDomain.maxFiles
	store.fileDomain.maxFiles = 1
	store.coordinator.pins[store.volume][9223372036854775807] = 1
	rejectedDescendantMutation(t, store.Store, command, syscall.EFBIG)
	delete(store.coordinator.pins[store.volume], 9223372036854775807)
	store.fileDomain.maxFiles = maxFiles
}

func TestNameRenameRejectsUnverifiableReferenceAncestry(t *testing.T) {
	for _, test := range []string{"missing binding", "multiple bindings", "bad parent kind", "detached with binding", "cycle"} {
		t.Run(test, func(t *testing.T) {
			store, command := descendantFixture(t)
			ref := descendantReference(t, store.Store, "outside/file", storage.UseClaim{})
			id := ref.(*retainedNodeReference).file.id
			outside, err := store.Stat(t.Context(), "outside")
			if err != nil {
				t.Fatal(err)
			}
			var restore func() error
			switch test {
			case "missing binding":
				_, err = store.write.ExecContext(t.Context(), `DELETE FROM entries WHERE volume=? AND node=?`, store.volume, id)
				restore = func() error {
					_, err := store.write.ExecContext(t.Context(), `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, store.volume, outside.ID, []byte("file"), id)
					return err
				}
			case "multiple bindings":
				_, err = store.write.ExecContext(t.Context(), `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, store.volume, store.root, []byte("second"), id)
				restore = func() error {
					_, err := store.write.ExecContext(t.Context(), `DELETE FROM entries WHERE volume=? AND parent=? AND name=?`, store.volume, store.root, []byte("second"))
					return err
				}
			case "bad parent kind":
				_, err = store.write.ExecContext(t.Context(), `UPDATE nodes SET kind=? WHERE volume=? AND id=?`, storage.NodeRegular, store.volume, outside.ID)
				restore = func() error {
					_, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET kind=? WHERE volume=? AND id=?`, storage.NodeDirectory, store.volume, outside.ID)
					return err
				}
			case "detached with binding":
				_, err = store.write.ExecContext(t.Context(), `UPDATE nodes SET detached=1 WHERE volume=? AND id=?`, store.volume, id)
				restore = func() error {
					_, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET detached=0 WHERE volume=? AND id=?`, store.volume, id)
					return err
				}
			case "cycle":
				other, lookupErr := store.Stat(t.Context(), "source/child")
				if lookupErr != nil {
					t.Fatal(lookupErr)
				}
				_, err = store.write.ExecContext(t.Context(), `UPDATE entries SET parent=? WHERE volume=? AND node=?`, other.ID, store.volume, outside.ID)
				if err == nil {
					_, err = store.write.ExecContext(t.Context(), `UPDATE entries SET parent=? WHERE volume=? AND node=?`, outside.ID, store.volume, other.ID)
				}
				restore = func() error {
					_, err := store.write.ExecContext(t.Context(), `UPDATE entries SET parent=? WHERE volume=? AND node=?`, store.root, store.volume, outside.ID)
					if err == nil {
						_, err = store.write.ExecContext(t.Context(), `UPDATE entries SET parent=? WHERE volume=? AND node=?`, command.Target.NodeID, store.volume, other.ID)
					}
					return err
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			result, mutationErr := store.MutateName(t.Context(), command)
			if err := restore(); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(mutationErr, syscall.EIO) || result.Attr != nil {
				t.Fatalf("corrupt ancestry = %+v, %v", result, mutationErr)
			}
			if node, err := store.Stat(t.Context(), "source"); err != nil || uint64(node.ID) != command.Target.NodeID {
				t.Fatalf("corrupt ancestry moved source: %+v, %v", node, err)
			}
		})
	}
}

func TestNameRenameOrdersAfterConcurrentReferenceAdmission(t *testing.T) {
	store, command := descendantFixture(t)
	node, err := store.Stat(t.Context(), "source/child/file")
	if err != nil {
		t.Fatal(err)
	}
	admitted, release := make(chan struct{}), make(chan struct{})
	ctx := metastore.WithFilePublicationGuard(t.Context(), func() error { close(admitted); <-release; return nil })
	type openResult struct {
		opened metastore.NodeOpenResult
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		result, err := store.OpenNodeRef(ctx, uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}, MetadataAccess: storage.ReadMetadata})
		opened <- openResult{result, err}
	}()
	<-admitted
	renamed := make(chan error, 1)
	go func() { _, err := store.MutateName(t.Context(), command); renamed <- err }()
	close(release)
	ref := <-opened
	if ref.err != nil || ref.opened.Reference == nil {
		t.Fatalf("concurrent open = %+v, %v", ref.opened, ref.err)
	}
	t.Cleanup(func() {
		if err := ref.opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := <-renamed; !errors.Is(err, syscall.EACCES) {
		t.Fatalf("rename missed admitted reference: %v", err)
	}
	if current, err := store.Stat(t.Context(), "source"); err != nil || uint64(current.ID) != command.Target.NodeID {
		t.Fatalf("concurrent refusal changed source: %+v, %v", current, err)
	}
}

func TestNameRenameRejectsMissingRetainedNode(t *testing.T) {
	store, command := descendantFixture(t)
	const missing = int64(9223372036854775807)
	store.pinNodeLocked(missing)
	t.Cleanup(func() { store.unpinNodeLocked(missing) })
	rejectedDescendantMutation(t, store.Store, command, syscall.EIO)
}

func TestNameRenameRespectsRetainedFileAdmissionLimit(t *testing.T) {
	store, command := descendantFixture(t)
	store.fileDomain.maxFiles = 1
	source := descendantReference(t, store.Store, "source", storage.UseClaim{})
	node, err := store.Stat(t.Context(), "source/child/file")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}, MetadataAccess: storage.ReadMetadata})
	if !errors.Is(err, syscall.EAGAIN) || opened.Reference != nil {
		t.Fatalf("retained limit = %+v, %v", opened, err)
	}
	if _, err := store.MutateName(t.Context(), command); err != nil {
		t.Fatalf("unadmitted reference blocked rename: %v", err)
	}
	if err := source.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(store.coordinator.pins[store.volume]) != 0 {
		t.Fatal("settled source reference left pin group")
	}
}
