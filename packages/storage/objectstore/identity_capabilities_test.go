package objectstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
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
	for _, name := range []string{"a", "b"} {
		if err := volume.Mkdir(t.Context(), name); err != nil {
			t.Fatal(err)
		}
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
	reader := session.(storage.DirectoryReader)
	rootObservation, err := reader.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root.ID})
	if err != nil {
		t.Fatal(err)
	}
	a, err := volume.Stat(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := volume.Stat(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	action := fileActionFor(t, session)
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}
	selection := storage.ChildSelection{Name: name, Guards: &storage.NamespaceGuards{
		Directories: []storage.DirectoryObservation{rootObservation.Observation},
		Edges: []storage.ObservedEdge{
			{ParentID: root.ID, RawLeaf: []byte("a"), ChildID: a.ID},
			{ParentID: root.ID, RawLeaf: []byte("b"), ChildID: b.ID},
		},
		RootID: root.ID,
	}}
	options := storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: want.ID},
		Action: action, Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	}
	first, err := opener.OpenAt(t.Context(), selection, options)
	if err != nil || first.File == nil || first.Attr.ID != want.ID || first.Outcome != storage.Opened {
		t.Fatalf("first open=%+v error=%v", first, err)
	}
	reordered := selection.Clone()
	reordered.Guards.Edges[0], reordered.Guards.Edges[1] = reordered.Guards.Edges[1], reordered.Guards.Edges[0]
	replayed, err := opener.OpenAt(t.Context(), reordered, options)
	if err != nil || replayed.File != first.File || replayed.Attr.ID != want.ID || replayed.Outcome != storage.Opened {
		t.Fatalf("replayed open=%+v error=%v", replayed, err)
	}
	changedSelection := selection.Clone()
	changedSelection.Guards.Directories[0].Revision[0]++
	if result, err := opener.OpenAt(t.Context(), changedSelection, options); !errors.Is(err, syscall.EINVAL) || result.File != nil {
		t.Fatalf("changed action intent=%+v error=%v", result, err)
	}
	changedOptions := options
	changedOptions.Use.Deny = storage.WriteData
	if result, err := opener.OpenAt(t.Context(), selection, changedOptions); !errors.Is(err, syscall.EINVAL) || result.File != nil {
		t.Fatalf("changed open options=%+v error=%v", result, err)
	}
	receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), action)
	if err != nil || receipt.Operation != storage.OpFileOpenAt || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("action receipt=%+v error=%v", receipt, err)
	}
	if err := first.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicOpenJournalTreatsEmptyAndAbsentGuardsAsTheSameIntent(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file, err := volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	opener := session.(storage.AtomicFileOpener)
	options := storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID},
		Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	}
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}
	first, err := opener.OpenAt(t.Context(), storage.ChildSelection{Name: name}, options)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := opener.OpenAt(t.Context(), storage.ChildSelection{Name: name, Guards: &storage.NamespaceGuards{}}, options)
	if err != nil || replayed.File != first.File {
		t.Fatalf("empty guard replay=%+v error=%v", replayed, err)
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
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file"),
	}}, options)
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
	opened, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("dir"),
	}}, storage.NodeRefOptions{
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

func TestReferenceActionJournalRejectsAnotherReceiver(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	for _, name := range []string{"first", "second"} {
		if err := volume.Write(t.Context(), name, []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	first := openFileFor(t, session, "first", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true}, Use: storage.UseClaim{Uses: storage.DeleteName},
	})
	second := openFileFor(t, session, "second", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true}, Use: storage.UseClaim{Uses: storage.DeleteName},
	})
	firstAgain := openFileFor(t, session, "first", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})

	modified := time.Unix(1_234, 567).UTC()
	mutation := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &modified}}
	if _, err := first.(storage.ConditionalFileMutation).MutateFile(t.Context(), mutation); err != nil {
		t.Fatal(err)
	}
	if _, err := firstAgain.(storage.ConditionalFileMutation).MutateFile(t.Context(), mutation); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("same action on another scope=%v", err)
	}

	pending := storage.PendingUnlinkCommand{Action: fileActionFor(t, session), Condition: storage.UnlinkFile}
	state, err := first.(storage.DeleteIntent).SetPendingUnlink(t.Context(), pending)
	if err != nil || !state.PendingUnlink || len(state.PendingGeneration) == 0 {
		t.Fatalf("set pending=%+v error=%v", state, err)
	}
	if replayed, err := second.(storage.DeleteIntent).SetPendingUnlink(t.Context(), pending); !errors.Is(err, syscall.EINVAL) || replayed.Attr.ID != 0 {
		t.Fatalf("same set action on another node=%+v error=%v", replayed, err)
	}
	clear := storage.ClearPendingUnlinkCommand{Action: fileActionFor(t, session), Generation: state.PendingGeneration}
	cleared, err := first.(storage.DeleteIntent).ClearPendingUnlink(t.Context(), clear)
	if err != nil || cleared.PendingUnlink {
		t.Fatalf("clear pending=%+v error=%v", cleared, err)
	}
	if replayed, err := second.(storage.DeleteIntent).ClearPendingUnlink(t.Context(), clear); !errors.Is(err, syscall.EINVAL) || replayed.Attr.ID != 0 {
		t.Fatalf("same clear action on another node=%+v error=%v", replayed, err)
	}

	for _, name := range []string{"first-dir", "second-dir"} {
		if err := volume.Mkdir(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	firstDir, err := volume.Stat(t.Context(), "first-dir")
	if err != nil {
		t.Fatal(err)
	}
	secondDir, err := volume.Stat(t.Context(), "second-dir")
	if err != nil {
		t.Fatal(err)
	}
	openDirectory := func(name string, attr storage.Attr) storage.NodeReference {
		opened, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{
			Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name),
		}}, storage.NodeRefOptions{
			Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
			Action: fileActionFor(t, session), Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		})
		if err != nil || opened.Reference == nil {
			t.Fatalf("open directory reference=%+v error=%v", opened, err)
		}
		return opened.Reference
	}
	firstReference := openDirectory("first-dir", firstDir)
	secondReference := openDirectory("second-dir", secondDir)
	directoryPending := storage.PendingUnlinkCommand{Action: fileActionFor(t, session), Condition: storage.UnlinkIfEmpty}
	directoryState, err := firstReference.(storage.DeleteIntent).SetPendingUnlink(t.Context(), directoryPending)
	if err != nil || !directoryState.PendingUnlink {
		t.Fatalf("set directory pending=%+v error=%v", directoryState, err)
	}
	if replayed, err := secondReference.(storage.DeleteIntent).SetPendingUnlink(t.Context(), directoryPending); !errors.Is(err, syscall.EINVAL) || replayed.Attr.ID != 0 {
		t.Fatalf("same node-reference set action on another node=%+v error=%v", replayed, err)
	}
	directoryClear := storage.ClearPendingUnlinkCommand{Action: fileActionFor(t, session), Generation: directoryState.PendingGeneration}
	if _, err := firstReference.(storage.DeleteIntent).ClearPendingUnlink(t.Context(), directoryClear); err != nil {
		t.Fatal(err)
	}
	if replayed, err := secondReference.(storage.DeleteIntent).ClearPendingUnlink(t.Context(), directoryClear); !errors.Is(err, syscall.EINVAL) || replayed.Attr.ID != 0 {
		t.Fatalf("same node-reference clear action on another node=%+v error=%v", replayed, err)
	}
}

func TestNodeReferenceConditionalMutationPreservesKindAndActionIdentity(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    storage.NodeKind
		initial storage.InitialState
	}{
		{name: "directory", kind: storage.NodeDirectory},
		{name: "symlink", kind: storage.NodeSymlink, initial: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: []byte("target")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			volume, _ := fileVolume(t, memory.New(), 4096, nil)
			root, err := volume.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			opened, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{
				Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(test.name),
			}}, storage.NodeRefOptions{
				Kind: test.kind, Target: storage.ChildCondition{State: storage.Absent}, Action: fileActionFor(t, session),
				Create: true, Exclusive: true, InitialState: test.initial,
				MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
			})
			if err != nil || opened.Reference == nil {
				t.Fatalf("open reference=%+v error=%v", opened, err)
			}
			second, err := session.(storage.NodeReferences).OpenNodeRef(t.Context(), opened.Attr.ID, storage.NodeRefOptions{
				Kind: test.kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.Attr.ID},
				Action: fileActionFor(t, session), MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
			})
			if err != nil || second.Reference == nil {
				t.Fatalf("open second reference=%+v error=%v", second, err)
			}
			mutator, ok := opened.Reference.(storage.ConditionalFileMutation)
			if !ok || mutator.CheckConditionalFileMutation() != nil {
				t.Fatal("node reference did not advertise conditional mutation")
			}
			modified := time.Unix(2_345, 678).UTC()
			command := storage.FileMutation{
				Action: fileActionFor(t, session), Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &modified},
				Metadata: map[string]storage.OpaquePayload{"test.reference": {Data: []byte(test.name)}},
			}
			changed, err := mutator.MutateFile(t.Context(), command)
			if err != nil || changed.ID != opened.Attr.ID || !changed.ModTime.Equal(modified) || string(changed.Metadata["test.reference"].Data) != test.name {
				t.Fatalf("conditional mutation=%+v error=%v", changed, err)
			}
			if result, err := second.Reference.(storage.ConditionalFileMutation).MutateFile(t.Context(), command); !errors.Is(err, syscall.EINVAL) || result.ID != 0 {
				t.Fatalf("same mutation action on another scope=%+v error=%v", result, err)
			}
			replayed, err := mutator.MutateFile(t.Context(), command)
			if err != nil || replayed.ID != changed.ID || !replayed.ModTime.Equal(modified) {
				t.Fatalf("conditional replay=%+v error=%v", replayed, err)
			}
			content := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateWriteAt, Data: []byte("x")}
			if result, err := mutator.MutateFile(t.Context(), content); !errors.Is(err, syscall.EBADF) || result.ID != 0 {
				t.Fatalf("node content mutation=%+v error=%v", result, err)
			}
			receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), content.Action)
			if err != nil || receipt.Operation != storage.OpFileMutate || receipt.Outcome != storage.FileActionNotExecuted {
				t.Fatalf("node content mutation receipt=%+v error=%v", receipt, err)
			}
		})
	}
}

type partialMutationStore struct {
	*sqlite.LockingStore
	failure        error
	cleanupFailure error
}

func (s *partialMutationStore) Abandon(ctx context.Context, key metastore.Key) error {
	return errors.Join(s.LockingStore.Abandon(ctx, key), s.cleanupFailure)
}

func (s *partialMutationStore) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (metastore.File, error) {
	file, err := s.LockingStore.OpenFile(ctx, name, options)
	if file == nil {
		return nil, err
	}
	return &partialMutationFile{File: file, mutation: file.(metastore.ConditionalFileMutation), failure: s.failure}, err
}

type partialMutationFile struct {
	metastore.File
	mutation metastore.ConditionalFileMutation
	failure  error
}

func (f *partialMutationFile) CheckConditionalFileMutation() error {
	return f.mutation.CheckConditionalFileMutation()
}

func (f *partialMutationFile) MutateFile(ctx context.Context, command storage.FileMutation) (metastore.FileState, error) {
	return f.mutation.MutateFile(ctx, command)
}

func (f *partialMutationFile) CommitMutation(ctx context.Context, command storage.FileMutation, revision uint64, object metastore.Object) (metastore.FileState, error) {
	state, err := f.mutation.CommitMutation(ctx, command, revision, object)
	if err != nil {
		return state, err
	}
	return state, f.failure
}

func TestConditionalContentMutationPreservesAppliedAttrWithError(t *testing.T) {
	failure := errors.New("post-apply confirmation failed")
	cleanupFailure := errors.New("staging cleanup failed")
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "files", Allowance: 4096,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), &partialMutationStore{
		LockingStore: meta, failure: failure, cleanupFailure: cleanupFailure,
	})
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close volume: %v", err)
		}
	})
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	file := openFileFor(t, session, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	command := storage.FileMutation{Action: fileActionFor(t, session), Kind: storage.MutateWriteAt, Data: []byte("applied")}
	changed, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), command)
	if !errors.Is(err, failure) || !errors.Is(err, cleanupFailure) || changed.ID == 0 || changed.Size != int64(len(command.Data)) {
		t.Fatalf("partial result=%+v error=%v", changed, err)
	}
	replayed, err := file.(storage.ConditionalFileMutation).MutateFile(t.Context(), command)
	if !errors.Is(err, failure) || !errors.Is(err, cleanupFailure) || replayed.ID != changed.ID || replayed.Size != changed.Size {
		t.Fatalf("replayed partial result=%+v error=%v", replayed, err)
	}
	receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), command.Action)
	if err != nil || receipt.Outcome != storage.FileActionUnknown || receipt.Operation != storage.OpFileMutate {
		t.Fatalf("partial result receipt=%+v error=%v", receipt, err)
	}
}
