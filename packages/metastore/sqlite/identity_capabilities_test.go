package sqlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

const testDeleteIntentOwner storage.DeleteIntentOwner = "sqlite-test-owner"

func fileAction(t *testing.T) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func selectChild(name storage.ChildName) storage.ChildSelection {
	return storage.ChildSelection{Name: name}
}

func accountingContext(t *testing.T, calls *[][2]int64) context.Context {
	t.Helper()
	return storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		*calls = append(*calls, [2]int64{previous, next})
		return func(storage.PublicationResult) error { return nil }, nil
	})
}

func TestConditionalPublicationAppliesMetadataWithTheContentRevision(t *testing.T) {
	_, file := openPublicationFile(t)
	initial, err := file.SetMetadata(t.Context(), "test.cas", nil, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := before.Size
	command := storage.FileMutation{
		Action: fileAction(t), Kind: storage.MutateTruncate, Size: 0, ExpectedSize: &expectedSize,
		ExpectedMetadata: map[string][]byte{"test.cas": initial.Version},
		Metadata:         map[string]storage.OpaquePayload{"test.cas": {Version: initial.Version, Data: []byte("after")}},
	}
	mutator := file.(metastore.ConditionalFileMutation)
	after, err := mutator.CommitMutation(t.Context(), command, before.Revision, metastore.Object{ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision == before.Revision || bytes.Equal(after.Metadata["test.cas"].Version, initial.Version) ||
		!bytes.Equal(after.Metadata["test.cas"].Data, []byte("after")) {
		t.Fatalf("conditional mutation = before %+v after %+v", before, after)
	}
	stale := command
	stale.Action = fileAction(t)
	stale.Metadata = map[string]storage.OpaquePayload{"test.cas": {Version: initial.Version, Data: []byte("wrong")}}
	if _, err := mutator.CommitMutation(t.Context(), stale, after.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale metadata publication = %v", err)
	}
	unchanged, err := file.Node(t.Context())
	if err != nil || unchanged.Revision != after.Revision || !bytes.Equal(unchanged.Metadata["test.cas"].Data, []byte("after")) {
		t.Fatalf("failed conditional mutation changed state = %+v, %v", unchanged, err)
	}
}

func TestDeleteIntentHashCanonicalizesAbsentMetadataAndUseOrder(t *testing.T) {
	first := storage.CloseIntent{Owner: testDeleteIntentOwner, Condition: storage.UnlinkFile,
		ExpectedMetadata: map[string][]byte{"absent": nil},
		Uses:             []storage.TargetUse{{NodeID: 9, Scope: storage.UseScope{Token: "b"}}, {NodeID: 4, Scope: storage.UseScope{Token: "a"}}}}
	second := storage.CloseIntent{Owner: testDeleteIntentOwner, Condition: storage.UnlinkFile,
		ExpectedMetadata: map[string][]byte{"absent": {}},
		Uses:             []storage.TargetUse{{NodeID: 4, Scope: storage.UseScope{Token: "a"}}, {NodeID: 9, Scope: storage.UseScope{Token: "b"}}}}
	a, err := deleteIntentHash(3, 1, []byte("name"), first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := deleteIntentHash(3, 1, []byte("name"), second)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("equivalent delete intents produced different request hashes")
	}
}

func TestIdentityAddressedNamespaceAndAtomicOpenPreserveTheSelectedNode(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: directory.Attr().ID}, RawLeaf: []byte("file")}
	opened, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Exclusive: true,
		Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Existing: storage.Keep,
		Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"test.identity": []byte("first")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.File.Close(t.Context())
	original := opened.State.Clone()
	if opened.Outcome != storage.Created || original.ID == 0 || !original.AllocationKnown || original.AllocationSize != 0 ||
		!bytes.Equal(original.Metadata["test.identity"].Data, []byte("first")) {
		t.Fatalf("atomic create = %+v", opened)
	}

	if _, err := store.LookupAt(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	rename := storage.NameCommand{
		Kind: storage.NameRename, Action: fileAction(t),
		Name: name, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(original.ID)},
		Destination: &storage.RenameTarget{
			Parent: directoryTarget(directory), ObservedLeaf: []byte("renamed"),
			Expected: storage.ChildCondition{State: storage.Absent}, OutputLeaf: []byte("renamed"),
		},
	}
	if _, err := store.MutateName(t.Context(), rename); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LookupAt(t.Context(), name); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old name after rename = %v", err)
	}
	renamed := storage.ChildName{Parent: directoryTarget(directory), RawLeaf: []byte("renamed")}
	attr, err := store.LookupAt(t.Context(), renamed)
	if err != nil || attr.ID != uint64(original.ID) {
		t.Fatalf("renamed identity = %+v, %v", attr, err)
	}
	retained, err := opened.File.Node(t.Context())
	if err != nil || retained.ID != original.ID {
		t.Fatalf("retained identity = %+v, %v", retained, err)
	}
}

func TestAtomicIdentityOpensCheckAuthoritativeMetadataPredicates(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := store.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.SetMetadata(t.Context(), uint64(node.ID), "test.version", nil, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}

	tests := []struct {
		name string
		open func(storage.ChildCondition) (func() error, error)
	}{
		{"OpenAt", func(condition storage.ChildCondition) (func() error, error) {
			result, err := store.OpenAt(t.Context(), selectChild(name), storage.OpenAtOptions{
				Read: true, Target: condition, Action: fileAction(t), Existing: storage.Keep,
				Use: storage.UseClaim{Uses: storage.ReadData},
			})
			if result.File == nil {
				return nil, err
			}
			return func() error { return result.File.Close(t.Context()) }, err
		}},
		{"OpenChildRef", func(condition storage.ChildCondition) (func() error, error) {
			result, err := store.OpenChildRef(t.Context(), selectChild(name), storage.NodeRefOptions{
				Kind: storage.NodeRegular, Target: condition, Action: fileAction(t),
				Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
			})
			if result.Reference == nil {
				return nil, err
			}
			return func() error { return result.Reference.Close(t.Context()) }, err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, condition := range []storage.ChildCondition{
				{State: storage.SameNode, NodeID: uint64(node.ID), ExpectedMetadata: map[string][]byte{"test.version": version.Version}},
				{State: storage.SameNode, NodeID: uint64(node.ID), ExpectedMetadata: map[string][]byte{"test.absent": nil}},
			} {
				close, err := test.open(condition)
				if err != nil {
					t.Fatalf("matching condition %+v = %v", condition.ExpectedMetadata, err)
				}
				if err := close(); err != nil {
					t.Fatal(err)
				}
			}
			close, err := test.open(storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID),
				ExpectedMetadata: map[string][]byte{"test.version": []byte("stale")}})
			if close != nil || !errors.Is(err, storage.ErrConditionConflict) {
				t.Fatalf("stale condition opened reference=%v err=%v", close != nil, err)
			}
		})
	}
}

func TestUncertainIdentityMutationsPreserveTheirCapturedResults(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		store, err := OpenLocking(t.Context(), lockingTestConfig(t))
		if err != nil {
			t.Fatal(err)
		}
		root, err := store.Stat(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		acceptErr := errors.New("open acceptance unavailable")
		store.witness = &retainedFailureWitness{failure: acceptErr}
		result, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}), storage.OpenAtOptions{
			Read: true, Create: true, Exclusive: true, Existing: storage.Keep, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.Absent}, Use: storage.UseClaim{Uses: storage.ReadData},
		})
		if !errors.Is(err, acceptErr) || result.File == nil || result.State.ID == 0 || result.Outcome != storage.Created {
			t.Fatalf("uncertain atomic open = %+v, %v", result, err)
		}
		if err := result.File.Close(t.Context()); !errors.Is(err, acceptErr) {
			t.Fatalf("uncertain reference close = %v", err)
		}
		if err := store.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
			t.Fatal(err)
		}
		store.locks, store.witness, store.files = nil, nil, nil
		if err := store.Abort(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("name mutation", func(t *testing.T) {
		store, err := OpenLocking(t.Context(), lockingTestConfig(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Create(t.Context(), "source"); err != nil {
			t.Fatal(err)
		}
		root, err := store.Stat(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		source, err := store.Stat(t.Context(), "source")
		if err != nil {
			t.Fatal(err)
		}
		acceptErr := errors.New("rename acceptance unavailable")
		store.witness = &retainedFailureWitness{failure: acceptErr}
		result, err := store.MutateName(t.Context(), storage.NameCommand{
			Kind: storage.NameRename, Action: fileAction(t),
			Name:   storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("source")},
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(source.ID)},
			Destination: &storage.RenameTarget{Parent: directoryTarget(root), ObservedLeaf: []byte("destination"),
				Expected: storage.ChildCondition{State: storage.Absent}, OutputLeaf: []byte("destination")},
		})
		if !errors.Is(err, acceptErr) || result.Attr == nil || result.Attr.ID != uint64(source.ID) {
			t.Fatalf("uncertain name mutation = %+v, %v", result, err)
		}
		if err := store.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
			t.Fatal(err)
		}
		store.locks, store.witness, store.files = nil, nil, nil
		if err := store.Abort(); err != nil {
			t.Fatal(err)
		}
	})
}

func directoryTarget(node metastore.Node) storage.DirectoryTarget {
	return storage.DirectoryTarget{NodeID: uint64(node.ID)}
}

func TestNodeReferenceRetainsSymlinkTargetAfterUnlink(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("link")}
	opened, err := store.OpenChildRef(t.Context(), selectChild(name), storage.NodeRefOptions{
		Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Create: true, Exclusive: true, MetadataAccess: storage.ReadMetadata,
		InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: []byte("target")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := opened.Reference.(*retainedNodeReference)
	if _, err := store.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameRemove, Action: fileAction(t), Name: name,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.State.Attr().ID},
	}); err != nil {
		t.Fatal(err)
	}
	state, err := ref.file.State(t.Context())
	if err != nil || !state.State.Detached || !bytes.Equal(state.LinkTarget, []byte("target")) {
		t.Fatalf("detached symlink = %+v, %v", state, err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StatNode(t.Context(), opened.State.Attr().ID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released symlink identity = %v", err)
	}
}

func TestCloseDeleteIntentFollowsRenameWithoutDeletingTheSuccessor(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("name")}
	intentID := storage.DeleteIntentID("0123456789abcdef0123456789abcdef")
	opened, err := store.OpenAt(metastore.WithReferenceSession(t.Context(), nil), selectChild(name), storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
		Action: fileAction(t), Existing: storage.Keep,
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentArmed || status.NodeID != opened.State.Attr().ID {
		t.Fatalf("armed status = %+v, %v", status, err)
	}
	scope, err := opened.File.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	renamed := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("renamed")}
	if _, err := store.MutateName(metastore.WithReferenceSession(t.Context(), nil), storage.NameCommand{
		Kind: storage.NameRename, Action: fileAction(t), Name: name,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.State.Attr().ID},
		Destination: &storage.RenameTarget{Parent: directoryTarget(root), ObservedLeaf: renamed.RawLeaf,
			Expected: storage.ChildCondition{State: storage.Absent}, OutputLeaf: renamed.RawLeaf},
		Uses: []storage.TargetUse{{NodeID: opened.State.Attr().ID, Scope: scope}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "name"); err != nil {
		t.Fatal(err)
	}
	successor, err := store.Stat(t.Context(), "name")
	if err != nil {
		t.Fatal(err)
	}
	if successor.ID == int64(opened.State.Attr().ID) {
		t.Fatal("successor reused the pending node identity")
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(t.Context(), "renamed"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("renamed original after close = %v", err)
	}
	if got, err := store.Stat(t.Context(), "name"); err != nil || got.ID != successor.ID {
		t.Fatalf("successor after original close = %+v, %v", got, err)
	}
	status, err = store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("completed status = %+v, %v", status, err)
	}
}

func TestRecoveredDeleteIntentCompletesAndRemainsQueryable(t *testing.T) {
	config := lockingTestConfig(t)
	config.Database = filepath.Join(t.TempDir(), "delete-recovery.db")
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "victim"); err != nil {
		t.Fatal(err)
	}
	node, err := store.Stat(t.Context(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	intentID := storage.DeleteIntentID("abcdef0123456789abcdef0123456789")
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET pending_generation=1 WHERE volume=? AND id=?`, store.volume, node.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `INSERT INTO delete_intents
		(intent,volume,owner,sequence,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,0,0)`, string(intentID), store.volume, string(testDeleteIntentOwner), 1, node.ID, store.root, []byte("victim"),
		make([]byte, 16), make([]byte, 32), false, storage.DeleteIntentArmed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET delete_intent_high_water=1 WHERE id=?`, store.volume); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("recovered name = %v", err)
	}
	status, err := reopened.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted || status.NodeID != uint64(node.ID) {
		t.Fatalf("recovered status = %+v, %v", status, err)
	}
	if err := reopened.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Owner: testDeleteIntentOwner,
		Action: fileAction(t), Intent: intentID,
	}); err != nil {
		t.Fatal(err)
	}
	status, err = reopened.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentUnknown || status.NodeID != 0 {
		t.Fatalf("acknowledged status = %+v, %v", status, err)
	}
}

func TestDeleteCleanupFailureIsReportedAndRetryable(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	intentID := storage.DeleteIntentID("11111111111111111111111111111111")
	opened, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("victim")}), storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Existing: storage.Keep, Action: fileAction(t),
		Target:      storage.ChildCondition{State: storage.Absent},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	publications := 0
	refused := errors.New("cleanup accounting refused")
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		publications++
		if publications == 2 {
			return nil, refused
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	if result, err := opened.File.CloseWithResult(ctx); result.Released || !errors.Is(err, refused) {
		t.Fatalf("failed close = %+v, %v", result, err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCleanupFailed || status.Failure == 0 {
		t.Fatalf("cleanup-failed status = %+v, %v", status, err)
	}
	if err := store.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Owner: testDeleteIntentOwner,
		Action: fileAction(t), Intent: intentID,
	}); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("cleanup-failed acknowledgement = %v", err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err = store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("retried cleanup status = %+v, %v", status, err)
	}
}

func TestDetachedReferenceCannotEnterExplicitPendingDelete(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Create: true},
		Use:        storage.UseClaim{Uses: storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(t.Context())
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	deleter := file.(metastore.DeleteIntent)
	if _, err := deleter.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: fileAction(t), Condition: storage.UnlinkFile,
	}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached pending delete = %v", err)
	}
}

func TestNotExecutedCloseIntentStillReleasesTheReference(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "dir/child"); err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	intentID := storage.DeleteIntentID("22222222222222222222222222222222")
	opened, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("dir")}), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := opened.Reference.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("nonempty close = %+v, %v", result, err)
	}
	if result, err := opened.Reference.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("replayed nonempty close = %+v, %v", result, err)
	}
	if _, err := opened.Reference.Node(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reference remained active after terminal close result: %v", err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), testDeleteIntentOwner, intentID)
	if err != nil || status.Outcome != storage.DeleteIntentNotExecuted {
		t.Fatalf("not-executed status = %+v, %v", status, err)
	}
	if _, err := store.Stat(t.Context(), "dir/child"); err != nil {
		t.Fatalf("failed deletion changed directory: %v", err)
	}
}

func TestIdentityRemovalPublishesOnlyRegularContentBytes(t *testing.T) {
	store, file := openPublicationFile(t)
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Commit(t.Context(), before.Revision, metastore.Object{Key: key, Size: 7, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	var replacement [][2]int64
	replaced, err := store.OpenAt(accountingContext(t, &replacement), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("file")}), storage.OpenAtOptions{
		Read: true, Create: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(current.ID)},
		Action: fileAction(t), Existing: storage.ReplaceNode,
		Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replaced.File.Close(t.Context())
	if len(replacement) != 1 || replacement[0] != [2]int64{7, 0} {
		t.Fatalf("regular replacement accounting = %+v", replacement)
	}

	linkName := storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("link")}
	link, err := store.OpenChildRef(t.Context(), selectChild(linkName), storage.NodeRefOptions{
		Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Create: true, InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: []byte("target")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var renamed [][2]int64
	if _, err := store.MutateName(accountingContext(t, &renamed), storage.NameCommand{
		Kind: storage.NameRename, Action: fileAction(t), Name: linkName,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: link.State.Attr().ID},
		Destination: &storage.RenameTarget{Parent: directoryTarget(root), ObservedLeaf: []byte("renamed-link"),
			Expected: storage.ChildCondition{State: storage.Absent}, OutputLeaf: []byte("renamed-link")},
	}); err != nil {
		t.Fatal(err)
	}
	if len(renamed) != 1 || renamed[0] != [2]int64{0, 0} {
		t.Fatalf("symlink rename accounting = %+v", renamed)
	}
	var removed [][2]int64
	if _, err := store.MutateName(accountingContext(t, &removed), storage.NameCommand{
		Kind: storage.NameRemove, Action: fileAction(t),
		Name:   storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("renamed-link")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: link.State.Attr().ID},
	}); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != [2]int64{0, 0} {
		t.Fatalf("symlink removal accounting = %+v", removed)
	}
	var cleanup [][2]int64
	if err := link.Reference.Close(accountingContext(t, &cleanup)); err != nil {
		t.Fatal(err)
	}
	if len(cleanup) != 1 || cleanup[0] != [2]int64{0, 0} {
		t.Fatalf("detached symlink cleanup accounting = %+v", cleanup)
	}
}

func TestPendingDeletePublishesTheRemovedRegularBytes(t *testing.T) {
	store, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("victim")}), storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Exclusive: true, Existing: storage.Keep, Action: fileAction(t),
		Target:      storage.ChildCondition{State: storage.Absent},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{Owner: testDeleteIntentOwner, ID: storage.DeleteIntentID("33333333333333333333333333333333"), Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := opened.File.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := opened.File.Reserve(t.Context(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.Commit(t.Context(), before.Revision, metastore.Object{Key: key, Size: 5, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	var calls [][2]int64
	if err := opened.File.Close(accountingContext(t, &calls)); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != [2]int64{0, 0} || calls[1] != [2]int64{5, 0} {
		t.Fatalf("pending-delete accounting = %+v", calls)
	}
}
