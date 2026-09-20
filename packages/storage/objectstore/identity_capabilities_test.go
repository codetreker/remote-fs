package objectstore_test

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func fileActionFor(t *testing.T, session storage.FileSession) storage.FileActionID {
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

func TestAtomicOpenJournalPreservesIdentityAndRejectsChangedIntent(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	want, err := volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	opener := session.(storage.AtomicFileOpener)
	action := fileActionFor(t, session)
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}
	options := storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: want.ID},
		Action: action, Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	}
	first, err := opener.OpenAt(t.Context(), name, options)
	if err != nil || first.File == nil || first.Attr.ID != want.ID || first.Outcome != storage.Opened {
		t.Fatalf("first open=%+v error=%v", first, err)
	}
	replayed, err := opener.OpenAt(t.Context(), name, options)
	if err != nil || replayed.File != first.File || replayed.Attr.ID != want.ID || replayed.Outcome != storage.Opened {
		t.Fatalf("replayed open=%+v error=%v", replayed, err)
	}
	changed := options
	changed.Use.Deny = storage.WriteData
	if result, err := opener.OpenAt(t.Context(), name, changed); !errors.Is(err, syscall.EINVAL) || result.File != nil {
		t.Fatalf("changed action intent=%+v error=%v", result, err)
	}
	receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), action)
	if err != nil || receipt.Operation != storage.OpFileOpenAt || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("action receipt=%+v error=%v", receipt, err)
	}
	if err := first.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCloseIntentDeletesOriginalIdentityAndCanBeAcknowledged(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	want, err := volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	options := storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: want.ID},
		Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName}, Existing: storage.Keep,
		CloseIntent: &storage.CloseIntent{ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file"),
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("close intent left the name: %v", err)
	}
	actions := session.(storage.FileActions)
	status, err := actions.QueryDeleteIntent(t.Context(), intent)
	if err != nil || status.NodeID != want.ID || status.Outcome != storage.DeleteIntentCompleted {
		t.Fatalf("delete intent=%+v error=%v", status, err)
	}
	ack := storage.AcknowledgeDeleteIntentCommand{Action: fileActionFor(t, session), Intent: intent}
	if err := actions.AcknowledgeDeleteIntent(t.Context(), ack); err != nil {
		t.Fatal(err)
	}
	if err := actions.AcknowledgeDeleteIntent(t.Context(), ack); err != nil {
		t.Fatalf("acknowledgement replay=%v", err)
	}
	receipt, err := actions.QueryFileAction(t.Context(), ack.Action)
	if err != nil || receipt.Operation != storage.OpFileAcknowledgeDeleteIntent || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("acknowledgement receipt=%+v error=%v", receipt, err)
	}
	otherIntent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	if err := actions.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: ack.Action, Intent: otherIntent}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed acknowledgement intent=%v", err)
	}
	status, err = actions.QueryDeleteIntent(t.Context(), intent)
	if err != nil || status.ID != intent || status.NodeID != 0 || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("acknowledged intent=%+v error=%v", status, err)
	}
}

func TestNonemptyDirectoryCloseIntentReleasesReferenceWithTerminalResult(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Create(t.Context(), "dir/child"); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := volume.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("dir"),
	}, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID},
		Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty},
	})
	if err != nil || opened.Reference == nil {
		t.Fatalf("open directory reference=%+v error=%v", opened, err)
	}
	if err := opened.Reference.Close(t.Context()); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("close nonempty directory=%v", err)
	}
	if err := opened.Reference.Close(t.Context()); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("replayed terminal close=%v", err)
	}
	if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference remained active: %v", err)
	}
	status, err := session.(storage.FileActions).QueryDeleteIntent(t.Context(), intent)
	if err != nil || status.NodeID != directory.ID || status.Outcome != storage.DeleteIntentNotExecuted {
		t.Fatalf("delete intent=%+v error=%v", status, err)
	}
	if _, err := volume.Stat(t.Context(), "dir/child"); err != nil {
		t.Fatalf("terminal close changed directory contents: %v", err)
	}
}
