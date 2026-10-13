package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type guardedNameAuthority struct {
	*fileSessionStub
	command storage.NameCommand
	result  storage.NameResult
	barrier *httprest.MutationBarrier
	failure error
	calls   int
}

func (p *guardedNameAuthority) CheckNamespaceAccess() error { return nil }
func (p *guardedNameAuthority) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{}, nil
}
func (p *guardedNameAuthority) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	panic("name mutation bypassed barrier")
}
func (p *guardedNameAuthority) MutateNameWithBarrier(_ context.Context, command storage.NameCommand) (storage.NameResult, *httprest.MutationBarrier, error) {
	p.calls++
	p.command = command
	return p.result, p.barrier, p.failure
}

func TestGuardedNameMutationPreservesCommandAndConfirmation(t *testing.T) {
	command := storage.NameCommand{Kind: storage.NameRename,
		Name:                     storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 2}, RawLeaf: []byte("source")},
		Target:                   storage.ChildCondition{State: storage.SameNode, NodeID: 4, ExpectedMetadata: map[string][]byte{"app.flags": {2}}},
		Destination:              &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 3}, ObservedLeaf: []byte("target"), OutputLeaf: []byte("target"), Expected: storage.ChildCondition{State: storage.Absent}},
		Guards:                   &storage.NamespaceGuards{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}}, Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte("a"), ChildID: 2}}},
		RequireNoOpenDescendants: true,
	}
	failure := errors.New("authority result uncertain")
	for _, scenario := range []string{"confirmed", "missing barrier", "wrong log", "authority failure"} {
		t.Run(scenario, func(t *testing.T) {
			attr := storage.Attr{ID: 4, Kind: storage.NodeRegular}
			probe := &guardedNameAuthority{fileSessionStub: &fileSessionStub{}, result: storage.NameResult{Attr: &attr}, barrier: &httprest.MutationBarrier{Incarnation: "log"}}
			var want error
			switch scenario {
			case "missing barrier":
				probe.barrier, want = nil, syscall.EIO
			case "wrong log":
				probe.barrier.Incarnation, want = "elsewhere", syscall.EIO
			case "authority failure":
				probe.failure, want = failure, failure
			}
			session := retainedTestSession(t, probe)
			result, err := session.MutateName(t.Context(), command)
			if want == nil && err != nil || want != nil && !errors.Is(err, want) || result.Attr == nil || result.Attr.ID != 4 || probe.calls != 1 || !reflect.DeepEqual(probe.command, command) {
				t.Fatalf("name result=%+v,%v command=%+v calls=%d", result, err, probe.command, probe.calls)
			}
			if session.base.activeConfirmations != 0 {
				t.Fatal("name mutation retained confirmation capacity")
			}
		})
	}
}
