package replicated_test

import (
	"context"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func replicatedFileActionFor(t *testing.T, session storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func TestIdentityCapabilitiesConfirmAuthorityResultsThroughTheReplica(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	if err := s.storage.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	if err := s.storage.Write(t.Context(), "dir/file", []byte("old")); err != nil {
		t.Fatal(err)
	}
	mounted, _ := mount(t, s)
	session := retainedSession(t, mounted)

	opener := session.(storage.AtomicFileOpener)
	namespace := session.(storage.NamespaceAccess)
	references := session.(storage.NodeReferences)
	actions := session.(storage.FileActions)
	for name, check := range map[string]func() error{
		"atomic open": opener.CheckAtomicFileOpen,
		"namespace":   namespace.CheckNamespaceAccess,
		"references":  references.CheckNodeReferences,
		"actions":     actions.CheckFileActions,
	} {
		if err := check(); err != nil {
			t.Fatalf("%s capability: %v", name, err)
		}
	}

	root, err := mounted.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := mounted.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	fileAttr, err := mounted.Stat(t.Context(), "dir/file")
	if err != nil {
		t.Fatal(err)
	}

	nameResult, err := namespace.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Action: replicatedFileActionFor(t, session),
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link")},
		Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("dir/file")},
	})
	if err != nil || nameResult.Attr == nil {
		t.Fatalf("create symlink=%+v error=%v", nameResult, err)
	}
	lookedUp, err := namespace.LookupAt(t.Context(), storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link"),
	})
	if err != nil || lookedUp.ID != nameResult.Attr.ID || lookedUp.Kind != storage.NodeSymlink {
		t.Fatalf("lookup=%+v error=%v", lookedUp, err)
	}

	opened, err := opener.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: directory.ID}, RawLeaf: []byte("file"),
	}},

		storage.OpenAtOptions{
			Read: true, Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: fileAttr.ID},
			Action: replicatedFileActionFor(t, session), Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}, Existing: storage.Keep,
		})

	if err != nil || opened.File == nil || opened.Attr.ID != fileAttr.ID || opened.Outcome != storage.Opened {
		t.Fatalf("open-at=%+v error=%v", opened, err)
	}
	file := opened.File
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	scoped := file.(storage.ScopedReference)
	if err := scoped.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := scoped.Scope(t.Context()); err != nil || scope.Check() != nil {
		t.Fatalf("scope=%+v error=%v", scope, err)
	}
	stateAccess := file.(storage.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	if state, err := stateAccess.State(t.Context()); err != nil || state.Attr.ID != fileAttr.ID || state.Detached || state.PendingUnlink {
		t.Fatalf("state=%+v error=%v", state, err)
	}
	metadata := file.(storage.ReferenceMetadataAccess)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	payload, err := metadata.SetMetadata(t.Context(), "test.replica", nil, []byte("value"))
	if err != nil || len(payload.Version) == 0 || string(payload.Data) != "value" {
		t.Fatalf("metadata=%+v error=%v", payload, err)
	}

	mutationAction := replicatedFileActionFor(t, session)
	expectedSize := int64(3)
	conditional := file.(storage.ConditionalFileMutation)
	if err := conditional.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	changed, err := conditional.MutateFile(t.Context(), storage.FileMutation{
		Action: mutationAction, Kind: storage.MutateWriteAt, ExpectedSize: &expectedSize, Offset: 3, Data: []byte("-new"),
	})
	if err != nil || changed.Size != 7 {
		t.Fatalf("conditional mutation=%+v error=%v", changed, err)
	}
	if replicated, err := mounted.Stat(t.Context(), "dir/file"); err != nil || replicated.Size != 7 {
		t.Fatalf("mutation returned before its barrier: %+v, %v", replicated, err)
	}
	receipt, err := actions.QueryFileAction(t.Context(), mutationAction)
	if err != nil || receipt.Operation != storage.OpFileMutate || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("mutation receipt=%+v error=%v", receipt, err)
	}

	deleteAccess := file.(storage.DeleteIntent)
	if err := deleteAccess.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	pending, err := deleteAccess.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: replicatedFileActionFor(t, session), Condition: storage.UnlinkFile,
	})
	if err != nil || !pending.PendingUnlink || len(pending.PendingGeneration) == 0 {
		t.Fatalf("pending unlink=%+v error=%v", pending, err)
	}
	cleared, err := deleteAccess.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{
		Action: replicatedFileActionFor(t, session), Generation: pending.PendingGeneration,
	})
	if err != nil || cleared.PendingUnlink || cleared.Attr.ID != fileAttr.ID {
		t.Fatalf("clear pending unlink=%+v error=%v", cleared, err)
	}

	verifyNodeReference(t, session, references, *nameResult.Attr, root.ID, "link")
	verifyNodeReference(t, session, references, directory, root.ID, "dir")
	verifyDeleteIntentLifecycle(t, session, actions, opener, directory.ID)
}

func verifyNodeReference(t *testing.T, session storage.FileSession, references storage.NodeReferences, attr storage.Attr, parent uint64, leaf string) {
	t.Helper()
	opened, err := references.OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: parent}, RawLeaf: []byte(leaf),
	}},

		storage.NodeRefOptions{
			Kind: attr.Kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
			Action: replicatedFileActionFor(t, session), Use: storage.UseClaim{Uses: storage.DeleteName},
			MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
		})

	if err != nil || opened.Reference == nil || opened.Attr.ID != attr.ID {
		t.Fatalf("open child reference=%+v error=%v", opened, err)
	}
	reference := opened.Reference
	defer func() {
		if err := reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := reference.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := reference.Scope(t.Context()); err != nil || scope.Check() != nil {
		t.Fatalf("reference scope=%+v error=%v", scope, err)
	}
	if err := reference.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	state, err := reference.State(t.Context())
	if err != nil || state.Attr.ID != attr.ID || state.Detached || state.PendingUnlink {
		t.Fatalf("reference state=%+v error=%v", state, err)
	}
	observed, err := reference.Stat(t.Context())
	if err != nil || observed.ID != attr.ID || observed.Kind != attr.Kind {
		t.Fatalf("reference stat=%+v error=%v", observed, err)
	}
	modified := time.Unix(345, 678).UTC()
	observed, err = reference.SetAttr(t.Context(), storage.AttrChange{ModTime: &modified})
	if err != nil || observed.ID != attr.ID || !observed.ModTime.Equal(modified) {
		t.Fatalf("reference setattr=%+v error=%v", observed, err)
	}

	metadata := reference.(storage.ReferenceMetadataAccess)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	payload, err := metadata.SetMetadata(t.Context(), "test.reference", nil, []byte(leaf))
	if err != nil || len(payload.Version) == 0 || string(payload.Data) != leaf {
		t.Fatalf("reference metadata=%+v error=%v", payload, err)
	}
	conditional, ok := reference.(storage.ConditionalFileMutation)
	if !ok {
		t.Fatalf("%v reference lost conditional file mutation", attr.Kind)
	}
	if err := conditional.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	atomicallyModified := time.Unix(789, 123).UTC()
	atomicValue := []byte("atomic-" + leaf)
	observed, err = conditional.MutateFile(t.Context(), storage.FileMutation{
		Action: replicatedFileActionFor(t, session), Kind: storage.MutateAttributes,
		Attr: storage.AttrChange{ModTime: &atomicallyModified},
		Metadata: map[string]storage.OpaquePayload{
			"test.atomic": {Data: atomicValue},
		},
	})
	if err != nil || observed.ID != attr.ID || observed.Kind != attr.Kind || !observed.ModTime.Equal(atomicallyModified) ||
		string(observed.Metadata["test.atomic"].Data) != string(atomicValue) {
		t.Fatalf("reference conditional mutation=%+v error=%v", observed, err)
	}
	direct, err := references.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{
		Kind: attr.Kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
		Action: replicatedFileActionFor(t, session), MetadataAccess: storage.ReadMetadata,
	})
	if err != nil || direct.Reference == nil || direct.Attr.ID != attr.ID {
		t.Fatalf("open direct reference=%+v error=%v", direct, err)
	}
	if err := direct.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if attr.Kind == storage.NodeDirectory {
		return
	}

	deleteAccess := reference.(storage.DeleteIntent)
	if err := deleteAccess.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	pending, err := deleteAccess.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{
		Action: replicatedFileActionFor(t, session), Condition: storage.UnlinkFile,
	})
	if err != nil || !pending.PendingUnlink || len(pending.PendingGeneration) == 0 {
		t.Fatalf("reference pending unlink=%+v error=%v", pending, err)
	}
	cleared, err := deleteAccess.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{
		Action: replicatedFileActionFor(t, session), Generation: pending.PendingGeneration,
	})
	if err != nil || cleared.PendingUnlink || cleared.Attr.ID != attr.ID {
		t.Fatalf("reference clear pending unlink=%+v error=%v", cleared, err)
	}

}

func verifyDeleteIntentLifecycle(t *testing.T, session storage.FileSession, actions storage.FileActions, opener storage.AtomicFileOpener, parent uint64) {
	t.Helper()
	owner := storage.DeleteIntentOwner("replicated-delete-test")
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	createAction := replicatedFileActionFor(t, session)
	opened, err := opener.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: parent}, RawLeaf: []byte("delete-on-close"),
	}},

		storage.OpenAtOptions{
			Read: true, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent},
			Action: createAction, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName}, Existing: storage.Keep,
			CloseIntent: &storage.CloseIntent{ID: intent, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
		})

	if err != nil || opened.File == nil || opened.Outcome != storage.Created {
		t.Fatalf("close-intent open=%+v error=%v", opened, err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err := actions.QueryDeleteIntent(t.Context(), owner, intent)
	if err != nil || status.NodeID != opened.Attr.ID || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("delete intent=%+v error=%v", status, err)
	}
	ackAction := replicatedFileActionFor(t, session)
	if err := actions.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: ackAction, Owner: owner, Intent: intent}); err != nil {
		t.Fatal(err)
	}
	receipt, err := actions.QueryFileAction(t.Context(), ackAction)
	if err != nil || receipt.Operation != storage.OpFileAcknowledgeDeleteIntent || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("acknowledgement receipt=%+v error=%v", receipt, err)
	}
}
