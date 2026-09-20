package sqlite

import (
	"bytes"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func fileAction(t *testing.T) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return id
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
	first := storage.CloseIntent{Condition: storage.UnlinkFile,
		ExpectedMetadata: map[string][]byte{"absent": nil},
		Uses:             []storage.TargetUse{{NodeID: 9, Scope: storage.UseScope{Token: "b"}}, {NodeID: 4, Scope: storage.UseScope{Token: "a"}}}}
	second := storage.CloseIntent{Condition: storage.UnlinkFile,
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
	opened, err := store.OpenAt(t.Context(), name, storage.OpenAtOptions{
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
	if opened.Outcome != storage.Created || original.ID == 0 || !bytes.Equal(original.Metadata["test.identity"].Data, []byte("first")) {
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
	opened, err := store.OpenChildRef(t.Context(), name, storage.NodeRefOptions{
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
	opened, err := store.OpenAt(metastore.WithReferenceSession(t.Context(), nil), name, storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
		Action: fileAction(t), Existing: storage.Keep,
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), intentID)
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
	status, err = store.QueryDeleteIntent(t.Context(), intentID)
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
		(intent,volume,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES(?,?,?,?,?,?,?,?,?,NULL,0,0)`, string(intentID), store.volume, node.ID, store.root, []byte("victim"),
		make([]byte, 16), make([]byte, 32), false, storage.DeleteIntentArmed); err != nil {
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
	status, err := reopened.QueryDeleteIntent(t.Context(), intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCompleted || status.NodeID != uint64(node.ID) {
		t.Fatalf("recovered status = %+v, %v", status, err)
	}
	if err := reopened.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{
		Action: fileAction(t), Intent: intentID,
	}); err != nil {
		t.Fatal(err)
	}
	status, err = reopened.QueryDeleteIntent(t.Context(), intentID)
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
	opened, err := store.OpenAt(t.Context(), storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("victim")}, storage.OpenAtOptions{
		Read: true, Create: true, Exclusive: true, Existing: storage.Keep, Action: fileAction(t),
		Target:      storage.ChildCondition{State: storage.Absent},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
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
	if err := opened.File.Close(ctx); !errors.Is(err, refused) {
		t.Fatalf("failed close = %v", err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), intentID)
	if err != nil || status.Outcome != storage.DeleteIntentCleanupFailed || status.Failure == 0 {
		t.Fatalf("cleanup-failed status = %+v, %v", status, err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err = store.QueryDeleteIntent(t.Context(), intentID)
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
	opened, err := store.OpenChildRef(t.Context(), storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("dir")}, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{ID: intentID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Reference.Close(t.Context()); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("nonempty close = %v", err)
	}
	if err := opened.Reference.Close(t.Context()); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("replayed nonempty close = %v", err)
	}
	if _, err := opened.Reference.Node(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reference remained active after terminal close result: %v", err)
	}
	status, err := store.QueryDeleteIntent(t.Context(), intentID)
	if err != nil || status.Outcome != storage.DeleteIntentNotExecuted {
		t.Fatalf("not-executed status = %+v, %v", status, err)
	}
	if _, err := store.Stat(t.Context(), "dir/child"); err != nil {
		t.Fatalf("failed deletion changed directory: %v", err)
	}
}
