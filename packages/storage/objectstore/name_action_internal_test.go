package objectstore

import (
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func guardedNameAction() storage.NameCommand {
	return storage.NameCommand{
		Kind:        storage.NameRename,
		Name:        storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 2}, RawLeaf: []byte("source")},
		Target:      storage.ChildCondition{State: storage.SameNode, NodeID: 4, ExpectedMetadata: map[string][]byte{"flags": {1}}},
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 3}, ObservedLeaf: []byte("target"), OutputLeaf: []byte("target"), Expected: storage.ChildCondition{State: storage.Absent}},
		Guards: &storage.NamespaceGuards{RootID: 1,
			Directories: []storage.DirectoryObservation{{ParentID: 3, Revision: []byte{3}}, {ParentID: 1, Revision: []byte{1}}, {ParentID: 2, Revision: []byte{2}}},
			Edges:       []storage.ObservedEdge{{ParentID: 2, RawLeaf: []byte("source"), ChildID: 4}, {ParentID: 1, RawLeaf: []byte("b"), ChildID: 3}, {ParentID: 1, RawLeaf: []byte("a"), ChildID: 2}},
		},
		RequireNoOpenDescendants: true,
	}
}

func TestNameActionCanonicalUnionOwnsEvidence(t *testing.T) {
	input := guardedNameAction()
	input.Action = actionID(t, actionTestSession(t, 10))
	if err := input.Check(); err != nil {
		t.Fatal(err)
	}
	frozen := canonicalFileActionInput(input).(storage.NameCommand)
	digest, err := fileActionDigest(storage.OpFileMutateName, input)
	if err != nil {
		t.Fatal(err)
	}
	reordered := canonicalFileActionInput(input).(storage.NameCommand)
	reordered.Guards.Directories[0], reordered.Guards.Directories[2] = reordered.Guards.Directories[2], reordered.Guards.Directories[0]
	reordered.Guards.Edges[0], reordered.Guards.Edges[2] = reordered.Guards.Edges[2], reordered.Guards.Edges[0]
	if actual, err := fileActionDigest(storage.OpFileMutateName, reordered); err != nil || actual != digest {
		t.Fatalf("reordered proof digest=%x,%v; want %x", actual, err, digest)
	}
	input.Name.RawLeaf[0] = 'x'
	input.Target.ExpectedMetadata["flags"][0]++
	input.Destination.ObservedLeaf[0] = 'x'
	input.Destination.OutputLeaf[0] = 'x'
	input.Guards.Directories[0].Revision[0]++
	input.Guards.Edges[0].RawLeaf[0] = 'x'
	input.Guards.RootID++
	if actual, err := fileActionDigest(storage.OpFileMutateName, frozen); err != nil || actual != digest {
		t.Fatalf("input mutation changed frozen proof=%x,%v", actual, err)
	}
	for name, change := range map[string]func(*storage.NameCommand){
		"guard":    func(c *storage.NameCommand) { c.Guards.Directories[0].Revision[0]++ },
		"flag":     func(c *storage.NameCommand) { c.RequireNoOpenDescendants = false },
		"metadata": func(c *storage.NameCommand) { c.Target.ExpectedMetadata["flags"][0]++ },
		"uses": func(c *storage.NameCommand) {
			c.Uses = []storage.TargetUse{{NodeID: 4, Scope: storage.UseScope{Token: "scope-token"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := canonicalFileActionInput(frozen).(storage.NameCommand)
			change(&changed)
			if actual, err := fileActionDigest(storage.OpFileMutateName, changed); err != nil || actual == digest {
				t.Fatalf("changed intent digest=%x,%v", actual, err)
			}
		})
	}
}

func TestNameActionJournalPreservesUncertainResultAndGuardedIntent(t *testing.T) {
	session := actionTestSession(t, 10)
	command := guardedNameAction()
	command.Action = actionID(t, session)
	failure := errors.New("namespace response lost")
	attr := storage.Attr{ID: 4, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{"flags": {Version: []byte{2}, Data: []byte{3}}}}
	calls := 0
	invoke := func(input storage.NameCommand) (storage.NameResult, error) {
		return runFileAction(t.Context(), session, input.Action, storage.OpFileMutateName, input, cloneNameResult,
			func(result storage.NameResult) bool { return result.Attr != nil },
			func() (storage.NameResult, error) { calls++; return storage.NameResult{Attr: &attr}, failure })
	}
	first, err := invoke(command)
	if err != failure || first.Attr == nil || first.Attr.ID != attr.ID {
		t.Fatalf("first result=%+v,%v", first, err)
	}
	first.Attr.Metadata["flags"].Data[0]++
	reordered := canonicalFileActionInput(command).(storage.NameCommand)
	reordered.Guards.Directories[0], reordered.Guards.Directories[2] = reordered.Guards.Directories[2], reordered.Guards.Directories[0]
	replayed, err := invoke(reordered)
	if err != failure || calls != 1 || !reflect.DeepEqual(replayed.Attr, &attr) {
		t.Fatalf("replay=%+v,%v calls=%d", replayed, err, calls)
	}
	for _, input := range []storage.NameCommand{
		func() storage.NameCommand {
			c := canonicalFileActionInput(command).(storage.NameCommand)
			c.Guards.Edges[0].ChildID++
			return c
		}(),
		func() storage.NameCommand { c := command; c.RequireNoOpenDescendants = false; return c }(),
	} {
		if _, err := invoke(input); !errors.Is(err, syscall.EINVAL) || calls != 1 {
			t.Fatalf("changed intent replay=%v calls=%d", err, calls)
		}
	}
	receipt, err := session.QueryFileAction(t.Context(), command.Action)
	if err != nil || receipt.Operation != storage.OpFileMutateName || receipt.Outcome != storage.FileActionUnknown {
		t.Fatalf("unknown receipt=%+v,%v", receipt, err)
	}
}

func TestOpenActionCanonicalResetUseAndInitialState(t *testing.T) {
	input := openAtActionInput{Options: storage.OpenAtOptions{
		Existing: storage.ResetContent, ResetUse: storage.DeleteName,
		Initial: storage.InitialState{OnReset: storage.InitialFields{Metadata: map[string][]byte{"flags": {3}}}},
	}}
	frozen := canonicalFileActionInput(input).(openAtActionInput)
	digest, err := fileActionDigest(storage.OpFileOpenAt, input)
	if err != nil {
		t.Fatal(err)
	}
	input.Options.Initial.OnReset.Metadata["flags"][0]++
	if actual, err := fileActionDigest(storage.OpFileOpenAt, frozen); err != nil || actual != digest {
		t.Fatalf("input mutated reset proof=%x,%v", actual, err)
	}
	changed := frozen
	changed.Options.ResetUse = 0
	if actual, err := fileActionDigest(storage.OpFileOpenAt, changed); err != nil || actual == digest {
		t.Fatalf("reset effect use excluded from intent=%x,%v", actual, err)
	}
}
