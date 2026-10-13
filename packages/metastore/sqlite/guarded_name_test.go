package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func guardedNameFixture(t *testing.T) (*LockingStore, storage.NameCommand) {
	t.Helper()
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, path := range []string{"common", "common/left", "common/right"} {
		if err := store.Mkdir(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"common/left/source", "common/right/target"} {
		if err := store.Create(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	nodes := make(map[string]metastore.Node)
	guards := &storage.NamespaceGuards{}
	for _, path := range []string{"", "common", "common/left", "common/right"} {
		node, err := store.Stat(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		nodes[path] = node
		guards.Directories = append(guards.Directories, observedDirectory(t, store.Store, uint64(node.ID)).Observation)
	}
	guards.RootID = uint64(nodes[""].ID)
	for _, path := range []string{"common/left/source", "common/right/target"} {
		node, err := store.Stat(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		nodes[path] = node
	}
	for _, edge := range []struct{ parent, leaf, child string }{
		{"", "common", "common"}, {"common", "left", "common/left"}, {"common", "right", "common/right"},
		{"common/left", "source", "common/left/source"}, {"common/right", "target", "common/right/target"},
	} {
		guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: uint64(nodes[edge.parent].ID), RawLeaf: []byte(edge.leaf), ChildID: uint64(nodes[edge.child].ID)})
	}
	return store, storage.NameCommand{Kind: storage.NameRename, Action: fileAction(t), Guards: guards,
		Name:   storage.ChildName{Parent: directoryTarget(nodes["common/left"]), RawLeaf: []byte("source")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(nodes["common/left/source"].ID)},
		Destination: &storage.RenameTarget{Parent: directoryTarget(nodes["common/right"]), ObservedLeaf: []byte("target"), OutputLeaf: []byte("target"),
			Expected: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(nodes["common/right/target"].ID)}},
	}
}

func nameState(t *testing.T, store *Store) []storage.ObservedDirectory {
	t.Helper()
	var states []storage.ObservedDirectory
	for _, path := range []string{"", "common", "common/left", "common/right"} {
		node, err := store.Stat(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		states = append(states, observedDirectory(t, store, uint64(node.ID)))
	}
	return states
}

func TestGuardedNameMutationRejectsStaleUnionWithoutEffects(t *testing.T) {
	for _, path := range []string{"common/left/new", "common/right/new", "common/new"} {
		t.Run(path, func(t *testing.T) {
			store, command := guardedNameFixture(t)
			if err := store.Create(t.Context(), path); err != nil {
				t.Fatal(err)
			}
			before := nameState(t, store.Store)
			result, err := store.MutateName(t.Context(), command)
			if !errors.Is(err, storage.ErrConditionConflict) || result.Attr != nil {
				t.Fatalf("stale union = %+v, %v", result, err)
			}
			if after := nameState(t, store.Store); !reflect.DeepEqual(before, after) {
				t.Fatal("stale guarded rename changed namespace or attributes")
			}
		})
	}
}

func TestGuardedNameMutationAcceptsSharedAncestryAndTerminalFiles(t *testing.T) {
	store, command := guardedNameFixture(t)
	result, err := store.MutateName(t.Context(), command)
	if err != nil || result.Attr == nil || result.Attr.ID != command.Target.NodeID {
		t.Fatalf("guarded rename = %+v, %v", result, err)
	}
	if _, err := store.Stat(t.Context(), "common/left/source"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old association = %v", err)
	}
	if node, err := store.Stat(t.Context(), "common/right/target"); err != nil || uint64(node.ID) != command.Target.NodeID {
		t.Fatalf("new association = %+v, %v", node, err)
	}
}

func TestGuardedNameMutationRejectsSelfSelectedRoot(t *testing.T) {
	store, command := guardedNameFixture(t)
	command.Guards.RootID = command.Guards.Directories[1].ParentID
	command.Guards.Directories = command.Guards.Directories[1:]
	command.Guards.Edges = command.Guards.Edges[1:]
	if err := command.Check(); err != nil {
		t.Fatalf("structurally valid small root: %v", err)
	}
	before := nameState(t, store.Store)
	if result, err := store.MutateName(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) || result.Attr != nil {
		t.Fatalf("wrong root = %+v, %v", result, err)
	}
	if !reflect.DeepEqual(before, nameState(t, store.Store)) {
		t.Fatal("wrong root changed namespace")
	}
}

func TestGuardedNameMutationRetainsMetadataAndThirdOutputConditions(t *testing.T) {
	for _, test := range []string{"source metadata", "target metadata", "third output"} {
		t.Run(test, func(t *testing.T) {
			store, command := guardedNameFixture(t)
			switch test {
			case "source metadata", "target metadata":
				condition := &command.Target
				if test == "target metadata" {
					condition = &command.Destination.Expected
				}
				condition.ExpectedMetadata = map[string][]byte{"test.guard": nil}
				if _, err := store.SetMetadata(t.Context(), condition.NodeID, "test.guard", nil, []byte("current")); err != nil {
					t.Fatal(err)
				}
			case "third output":
				if err := store.Create(t.Context(), "common/right/output"); err != nil {
					t.Fatal(err)
				}
				command.Guards.Directories[3] = observedDirectory(t, store.Store, command.Destination.Parent.NodeID).Observation
				command.Destination.OutputLeaf = []byte("output")
			}
			before := nameState(t, store.Store)
			if result, err := store.MutateName(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) || result.Attr != nil {
				t.Fatalf("condition = %+v, %v", result, err)
			}
			if !reflect.DeepEqual(before, nameState(t, store.Store)) {
				t.Fatal("failed condition changed namespace")
			}
		})
	}
}

func TestGuardedNameMutationRechecksEvidenceInFinalWriteTransaction(t *testing.T) {
	store, command := guardedNameFixture(t)
	if err := store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err := store.inspect(t.Context(), func(tx *sql.Tx) error { _, err := store.prepareNamespaceMutation(t.Context(), tx, command); return err })
	if err != nil {
		store.coordinator.commit.release()
		t.Fatal(err)
	}
	token := bytes.Clone(command.Guards.Directories[2].Revision)
	token[len(token)-1]++
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET directory_revision=? WHERE volume=? AND id=?`, token, store.volume, command.Name.Parent.NodeID); err != nil {
		store.coordinator.commit.release()
		t.Fatal(err)
	}
	err = store.mutateTransactionLocked(t.Context(), t.Context(), nil, func(tx *sql.Tx) error {
		mutation, err := store.prepareNamespaceMutation(t.Context(), tx, command)
		if err != nil {
			return err
		}
		_, err = store.applyNamespaceMutation(t.Context(), tx, command, mutation, time.Now())
		return err
	})
	store.coordinator.commit.release()
	if !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("final transaction guard = %v", err)
	}
	if source, err := store.Stat(t.Context(), "common/left/source"); err != nil || uint64(source.ID) != command.Target.NodeID {
		t.Fatalf("source = %+v, %v", source, err)
	}
	if target, err := store.Stat(t.Context(), "common/right/target"); err != nil || uint64(target.ID) != command.Destination.Expected.NodeID {
		t.Fatalf("target = %+v, %v", target, err)
	}
}

func TestPendingNameMutationRejectsTheOriginalDeleteUseToken(t *testing.T) {
	for _, selection := range []string{"source", "target", "remove"} {
		t.Run(selection, func(t *testing.T) {
			store, command := guardedNameFixture(t)
			id := command.Target.NodeID
			if selection == "target" {
				id = command.Destination.Expected.NodeID
			}
			opened, err := store.OpenNodeRef(t.Context(), id, storage.NodeRefOptions{Kind: storage.NodeRegular, Action: fileAction(t),
				Target: storage.ChildCondition{State: storage.SameNode, NodeID: id}, Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := opened.Reference.Scope(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			pending, err := opened.Reference.(metastore.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: fileAction(t), Condition: storage.UnlinkFile})
			if err != nil {
				t.Fatal(err)
			}
			command.Uses = []storage.TargetUse{{NodeID: id, Scope: scope}}
			if selection == "remove" {
				command.Kind, command.Destination = storage.NameRemove, nil
				command.Guards = nil
			}
			before := nameState(t, store.Store)
			if result, err := store.MutateName(t.Context(), command); !errors.Is(err, storage.ErrPendingDelete) || result.Attr != nil {
				t.Fatalf("pending mutation = %+v, %v", result, err)
			}
			if !reflect.DeepEqual(before, nameState(t, store.Store)) {
				t.Fatal("pending mutation changed namespace")
			}
			if _, err := opened.Reference.(metastore.DeleteIntent).ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: fileAction(t), Generation: pending.PendingGeneration}); err != nil {
				t.Fatal(err)
			}
			if err := opened.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGuardedNameMutationPropagatesCanceledAdmission(t *testing.T) {
	store, command := guardedNameFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := store.MutateName(ctx, command); !errors.Is(err, context.Canceled) || result.Attr != nil {
		t.Fatalf("canceled rename = %+v, %v", result, err)
	}
}
