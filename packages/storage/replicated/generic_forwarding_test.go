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

type genericForwardCall struct {
	method  string
	request any
	action  storage.FileActionID
	value   any
}
type genericForwardContextKey struct{}
type genericForwardProbe struct {
	calls       []genericForwardCall
	receipt     storage.FileActionReceipt
	barrier     *httprest.MutationBarrier
	failure     error
	observation storage.FileObservation
}

func (p *genericForwardProbe) record(ctx context.Context, method string, request any, action storage.FileActionID) {
	p.calls = append(p.calls, genericForwardCall{method, request, action, ctx.Value(genericForwardContextKey{})})
}
func (p *genericForwardProbe) mutate(ctx context.Context, method string, request any, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	p.record(ctx, method, request, action)
	return p.receipt, p.barrier, p.failure
}
func (p *genericForwardProbe) control(ctx context.Context, method string, request any, action storage.FileActionID) (storage.FileActionReceipt, error) {
	p.record(ctx, method, request, action)
	return p.receipt, p.failure
}

type genericForwardFile struct {
	httprest.FileWithBarrier
	probe *genericForwardProbe
}
type genericForwardSession struct {
	httprest.FileSessionWithBarrier
	probe  *genericForwardProbe
	file   storage.File
	status storage.FileSessionStatus
}
type genericNodeChange struct {
	ID     uint64
	Change storage.AttrChange
}
type genericRangeOwner struct {
	Owner storage.RangeOwnerID
	Scope storage.RangeScope
}
type genericNodeObservation struct {
	ID      uint64
	Options storage.ObservationOptions
}

func (f *genericForwardFile) Reference() storage.FileReferenceID { return 97 }
func (f *genericForwardFile) Stat(ctx context.Context, request storage.ObservationOptions) (storage.FileObservation, error) {
	f.probe.record(ctx, "Stat", request, "")
	return f.probe.observation, f.probe.failure
}
func (f *genericForwardFile) CheckObservation(ctx context.Context, request storage.ObservationCondition) (storage.FileObservation, error) {
	f.probe.record(ctx, "CheckObservation", request, "")
	return f.probe.observation, f.probe.failure
}
func (f *genericForwardFile) ReadAt(ctx context.Context, request storage.FileReadRequest) (storage.FileRead, error) {
	f.probe.record(ctx, "ReadAt", request, "")
	return storage.FileRead{Attr: f.probe.observation.Attr, Data: []byte("partial")}, f.probe.failure
}
func (f *genericForwardFile) ListAt(ctx context.Context, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	f.probe.record(ctx, "ListAt", request, "")
	return storage.DirectoryPage{ParentID: 17, Revision: 31, Done: true}, f.probe.failure
}
func (f *genericForwardFile) Sync(ctx context.Context) error {
	f.probe.record(ctx, "Sync", nil, "")
	return f.probe.failure
}
func (f *genericForwardFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	f.probe.record(ctx, "RangeSnapshot", genericRangeOwner{owner, scope}, "")
	return storage.RangeSnapshot{Revision: 71, OwnerAvailable: 1}, f.probe.failure
}
func (s *genericForwardSession) Reference(ctx context.Context, reference storage.FileReferenceID) (storage.File, error) {
	s.probe.record(ctx, "Reference", reference, "")
	return s.file, s.probe.failure
}
func (s *genericForwardSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	s.probe.record(ctx, "StatNode", genericNodeObservation{id, options}, "")
	return s.probe.observation, s.probe.failure
}
func (s *genericForwardSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	s.probe.record(ctx, "Renew", nil, "")
	return s.status, s.probe.failure
}
func (s *genericForwardSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	s.probe.record(ctx, "Status", nil, "")
	return s.status, s.probe.failure
}

func (f *genericForwardFile) WriteAtWithBarrier(ctx context.Context, request storage.FileWriteRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "WriteAt", request, action)
}

func (f *genericForwardFile) TruncateWithBarrier(ctx context.Context, request storage.FileTruncateRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "Truncate", request, action)
}

func (f *genericForwardFile) SetAttrWithBarrier(ctx context.Context, request storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "SetAttr", request, action)
}

func (f *genericForwardFile) SetKindWithBarrier(ctx context.Context, request storage.SetKindRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "SetKind", request, action)
}

func (f *genericForwardFile) RenameWithBarrier(ctx context.Context, request storage.RenameRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "Rename", request, action)
}

func (f *genericForwardFile) PrepareRemovalWithBarrier(ctx context.Context, request storage.PrepareRemovalRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "PrepareRemoval", request, action)
}

func (f *genericForwardFile) DrainEntryWithBarrier(ctx context.Context, request storage.DrainEntryRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "DrainEntry", request, action)
}

func (f *genericForwardFile) CancelDrainWithBarrier(ctx context.Context, request storage.CancelDrainRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "CancelDrain", request, action)
}

func (f *genericForwardFile) CancelPreparedWithBarrier(ctx context.Context, request storage.RemovalIntentID, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "CancelPrepared", request, action)
}

func (s *genericForwardSession) RetainWithBarrier(ctx context.Context, request storage.RetainRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "Retain", request, action)
}

func (s *genericForwardSession) RetainAtWithBarrier(ctx context.Context, request storage.RetainAtRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "RetainAt", request, action)
}

func (s *genericForwardSession) CreateAndRetainAtWithBarrier(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "CreateAndRetainAt", request, action)
}

func (s *genericForwardSession) ResetAndRetainAtWithBarrier(ctx context.Context, request storage.ResetAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "ResetAndRetainAt", request, action)
}

func (s *genericForwardSession) ReplaceAndRetainAtWithBarrier(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "ReplaceAndRetainAt", request, action)
}

func (f *genericForwardFile) CloseWithBarrier(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.probe.mutate(ctx, "Close", nil, action)
}

func (s *genericForwardSession) QueryActionWithBarrier(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "QueryAction", nil, action)
}

func (s *genericForwardSession) CancelActionWithBarrier(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "CancelAction", nil, action)
}

func (s *genericForwardSession) SetNodeAttrWithBarrier(ctx context.Context, id uint64, request storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.probe.mutate(ctx, "SetNodeAttr", genericNodeChange{id, request}, action)
}

func (f *genericForwardFile) ReplaceClaim(ctx context.Context, request storage.AccessClaim, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.control(ctx, "ReplaceClaim", request, action)
}

func (f *genericForwardFile) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.control(ctx, "ReplaceRanges", request, action)
}

func (f *genericForwardFile) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.control(ctx, "WaitRanges", request, action)
}

func (f *genericForwardFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.control(ctx, "RetireFileRangeOwner", genericRangeOwner{owner, scope}, action)
}
func (s *genericForwardSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.control(ctx, "RetireSessionRangeOwner", owner, action)
}

type genericForwardMutation struct {
	name    string
	request any
	control bool
	call    func(context.Context, *retainedFile, *fileSession) (storage.FileActionReceipt, error)
}

func genericMutationCases() []genericForwardMutation {
	owner := storage.RangeOwnerID(41)
	claim := storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.WriteContent}
	target := storage.EntryTarget{Parent: 97, ParentID: 17, Name: []byte("exact"), DirectoryRevision: 31, ExpectedEntryID: 19, ExpectedNodeID: 23, ExpectedMetadataRevision: 29, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 17, NodeID: 17}}
	absent := storage.EntryTarget{Parent: target.Parent, ParentID: target.ParentID, Name: []byte("absent"), DirectoryRevision: target.DirectoryRevision, Witness: target.Witness}
	entry := storage.EntryCondition{ParentID: 17, EntryID: 19, NodeID: 23, DirectoryRevision: 31, Name: []byte("exact")}
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 17, NodeID: 23, Ancestors: []storage.EntryCondition{entry}}
	metadata := storage.Metadata{{Key: "client.test", Version: 1, Data: []byte("value")}}
	change := storage.AttrChange{ExpectedRevision: 29, Metadata: &metadata}
	ranges := storage.RangeReplaceRequest{Owner: 41, Scope: storage.RangeScope{Domain: 7}, ExpectedRevision: 71, Ranges: []storage.RangeAcquisition{{ID: 11, Start: 3, End: 13, Exclusive: true}}}
	return []genericForwardMutation{
		{name: "WriteAt", request: storage.FileWriteRequest{Offset: 3, Data: []byte("bytes"), Owner: &owner}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.WriteAt(ctx, storage.FileWriteRequest{Offset: 3, Data: []byte("bytes"), Owner: &owner}, testFileAction)
		}},
		{name: "Truncate", request: storage.FileTruncateRequest{Size: 51, Owner: &owner}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.Truncate(ctx, storage.FileTruncateRequest{Size: 51, Owner: &owner}, testFileAction)
		}},
		{name: "SetAttr", request: change, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.SetAttr(ctx, change, testFileAction)
		}},
		{name: "SetKind", request: storage.SetKindRequest{ExpectedRevision: 29, Kind: storage.NodeSymlink, LinkTarget: []byte("relative"), Metadata: *change.Metadata}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.SetKind(ctx, storage.SetKindRequest{ExpectedRevision: 29, Kind: storage.NodeSymlink, LinkTarget: []byte("relative"), Metadata: *change.Metadata}, testFileAction)
		}},
		{name: "Rename", request: storage.RenameRequest{Source: target, Destination: storage.EntryTarget{Parent: 97, ParentID: 17, Name: []byte("next"), DirectoryRevision: 31, Witness: target.Witness}}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.Rename(ctx, storage.RenameRequest{Source: target, Destination: storage.EntryTarget{Parent: 97, ParentID: 17, Name: []byte("next"), DirectoryRevision: 31, Witness: target.Witness}}, testFileAction)
		}},
		{name: "PrepareRemoval", request: storage.PrepareRemovalRequest{ExpectedMetadataRevision: 29, Entry: entry, Witness: location, Condition: storage.RemovalFile}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.PrepareRemoval(ctx, storage.PrepareRemovalRequest{ExpectedMetadataRevision: 29, Entry: entry, Witness: location, Condition: storage.RemovalFile}, testFileAction)
		}},
		{name: "DrainEntry", request: storage.DrainEntryRequest{ExpectedMetadataRevision: 29, Entry: entry, Witness: location, Condition: storage.RemovalFile}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.DrainEntry(ctx, storage.DrainEntryRequest{ExpectedMetadataRevision: 29, Entry: entry, Witness: location, Condition: storage.RemovalFile}, testFileAction)
		}},
		{name: "CancelDrain", request: storage.CancelDrainRequest{EntryID: 19, Generation: 5}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.CancelDrain(ctx, storage.CancelDrainRequest{EntryID: 19, Generation: 5}, testFileAction)
		}},
		{name: "Retain", request: storage.RetainRequest{NodeID: 23, ExpectedMetadataRevision: 29, Claim: claim}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.Retain(ctx, storage.RetainRequest{NodeID: 23, ExpectedMetadataRevision: 29, Claim: claim}, testFileAction)
		}},
		{name: "RetainAt", request: storage.RetainAtRequest{Target: target, Claim: claim}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.RetainAt(ctx, storage.RetainAtRequest{Target: target, Claim: claim}, testFileAction)
		}},
		{name: "CreateAndRetainAt", request: storage.CreateAndRetainRequest{Target: absent, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{Target: absent, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim}, testFileAction)
		}},
		{name: "ResetAndRetainAt", request: storage.ResetAndRetainRequest{Target: target, ExpectedRevision: 29, Change: change, Claim: claim}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.ResetAndRetainAt(ctx, storage.ResetAndRetainRequest{Target: target, ExpectedRevision: 29, Change: change, Claim: claim}, testFileAction)
		}},
		{name: "ReplaceAndRetainAt", request: storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.ReplaceAndRetainAt(ctx, storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim}, testFileAction)
		}},
		{name: "SetNodeAttr", request: genericNodeChange{23, change}, control: false, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.SetNodeAttr(ctx, 23, change, testFileAction)
		}},
		{name: "ReplaceClaim", request: claim, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.ReplaceClaim(ctx, claim, testFileAction)
		}},
		{name: "ReplaceRanges", request: ranges, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.ReplaceRanges(ctx, ranges, testFileAction)
		}},
		{name: "WaitRanges", request: storage.RangeWaitRequest{Owner: ranges.Owner, Scope: ranges.Scope, ExpectedRevision: ranges.ExpectedRevision, Ranges: ranges.Ranges, DetectDeadlock: true}, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.WaitRanges(ctx, storage.RangeWaitRequest{Owner: ranges.Owner, Scope: ranges.Scope, ExpectedRevision: ranges.ExpectedRevision, Ranges: ranges.Ranges, DetectDeadlock: true}, testFileAction)
		}},
		{name: "CancelPrepared", request: storage.RemovalIntentID(43), control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.CancelPrepared(ctx, storage.RemovalIntentID(43), testFileAction)
		}},
		{name: "Close", request: nil, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.Close(ctx, testFileAction)
		}},
		{name: "QueryAction", request: nil, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.QueryAction(ctx, testFileAction)
		}},
		{name: "CancelAction", request: nil, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.CancelAction(ctx, testFileAction)
		}},
		{name: "RetireFileRangeOwner", request: genericRangeOwner{41, storage.RangeScope{Domain: 7}}, control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return f.RetireRangeOwner(ctx, 41, storage.RangeScope{Domain: 7}, testFileAction)
		}},
		{name: "RetireSessionRangeOwner", request: storage.RangeOwnerID(41), control: true, call: func(ctx context.Context, f *retainedFile, s *fileSession) (storage.FileActionReceipt, error) {
			return s.RetireRangeOwner(ctx, storage.RangeOwnerID(41), testFileAction)
		}},
	}
}

func TestGenericFileActionsPreserveAuthorityIdentityAndPartialEffects(t *testing.T) {
	for _, test := range genericMutationCases() {
		t.Run(test.name, func(t *testing.T) {
			cause := errors.New("authority failed after recording its result")
			effects := storage.EffectMetadataChanged
			if test.control {
				effects = storage.EffectRangesChanged
			}
			want := storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: effects, Reference: 97, Observation: storage.FileObservation{Attr: storage.Attr{ID: 23, MetadataRevision: 29}}, RangeRevision: 73}
			probe := &genericForwardProbe{receipt: want, failure: cause}
			session := retainedTestSession(t, &genericForwardSession{probe: probe})
			file := &retainedFile{session: session, remote: &genericForwardFile{probe: probe}}
			ctx := context.WithValue(t.Context(), genericForwardContextKey{}, "caller-scope")
			got, err := test.call(ctx, file, session)
			if !errors.Is(err, cause) || !reflect.DeepEqual(got, want) {
				t.Fatalf("partial authority result changed: %+v, %v", got, err)
			}
			if !test.control && !errors.Is(err, syscall.EIO) {
				t.Fatalf("replicated effects lacked their required barrier: %v", err)
			}
			expected := []genericForwardCall{{method: test.name, request: test.request, action: testFileAction, value: "caller-scope"}}
			if !reflect.DeepEqual(probe.calls, expected) {
				t.Fatalf("authority request identity changed: got %#v, want %#v", probe.calls, expected)
			}
			if session.active != 0 || session.base.activeConfirmations != 0 {
				t.Fatal("completed forwarding retained admission capacity")
			}
		})
	}
}

func TestGenericFileActionsSeparateReplicaAdmissionFromAuthorityControls(t *testing.T) {
	for _, test := range genericMutationCases() {
		t.Run(test.name, func(t *testing.T) {
			want := storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectRangesChanged, Reference: 97}
			probe := &genericForwardProbe{receipt: want}
			session := retainedTestSession(t, &genericForwardSession{probe: probe})
			session.base.failure = errors.New("replication stream failed")
			session.base.options.MaxActiveConfirmations = 1
			session.base.activeConfirmations = 1
			file := &retainedFile{session: session, remote: &genericForwardFile{probe: probe}}
			got, err := test.call(t.Context(), file, session)
			if test.control {
				if err != nil || len(probe.calls) != 1 || !reflect.DeepEqual(got, want) {
					t.Fatalf("authority control depended on healthy replica: %+v, %v, calls %d", got, err, len(probe.calls))
				}
			} else if !errors.Is(err, syscall.EIO) || len(probe.calls) != 0 || !reflect.DeepEqual(got, storage.FileActionReceipt{}) {
				t.Fatalf("unhealthy mutation escaped replica admission: %+v, %v, calls %d", got, err, len(probe.calls))
			}
			if session.active != 0 || session.base.activeConfirmations != 1 {
				t.Fatal("forwarding changed another operation's capacity")
			}
			session.base.activeConfirmations = 0
		})
	}
}

func TestGenericFileMutationsRejectCancellationBeforeAuthorityDispatch(t *testing.T) {
	for _, test := range genericMutationCases() {
		if test.control {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			probe := &genericForwardProbe{}
			session := retainedTestSession(t, &genericForwardSession{probe: probe})
			file := &retainedFile{session: session, remote: &genericForwardFile{probe: probe}}
			ctx, cancel := context.WithCancelCause(t.Context())
			cause := errors.New("caller cancelled before dispatch")
			cancel(cause)
			_, err := test.call(ctx, file, session)
			if !errors.Is(err, cause) || storage.ErrnoOf(err) != syscall.EINTR || len(probe.calls) != 0 {
				t.Fatalf("cancelled mutation dispatched or lost cancellation: %v; calls %d", err, len(probe.calls))
			}
		})
	}
}

func TestGenericFileObservationRequestsPreservePartialAuthorityAnswers(t *testing.T) {
	owner := storage.RangeOwnerID(41)
	options := storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}
	condition := storage.ObservationCondition{MetadataRevision: 29, DirectoryRevision: 31, Location: storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 17, NodeID: 17}}
	page := storage.DirectoryPageRequest{Revision: 31, Cursor: storage.DirectoryCursor{ParentID: 17, Revision: 31, After: []byte("exact")}, MaxEntries: 7, MaxBytes: 4096}
	read := storage.FileReadRequest{Offset: 3, Length: 7, Owner: &owner}
	tests := []struct {
		name    string
		request any
		call    func(context.Context, *retainedFile, *fileSession) (any, error)
		want    func(storage.FileObservation) any
	}{
		{"Stat", options, func(ctx context.Context, f *retainedFile, _ *fileSession) (any, error) { return f.Stat(ctx, options) }, func(o storage.FileObservation) any { return o }},
		{"StatNode", genericNodeObservation{23, options}, func(ctx context.Context, _ *retainedFile, s *fileSession) (any, error) {
			return s.StatNode(ctx, 23, options)
		}, func(o storage.FileObservation) any { return o }},
		{"CheckObservation", condition, func(ctx context.Context, f *retainedFile, _ *fileSession) (any, error) {
			return f.CheckObservation(ctx, condition)
		}, func(o storage.FileObservation) any { return o }},
		{"ReadAt", read, func(ctx context.Context, f *retainedFile, _ *fileSession) (any, error) { return f.ReadAt(ctx, read) }, func(o storage.FileObservation) any { return storage.FileRead{Attr: o.Attr, Data: []byte("partial")} }},
		{"ListAt", page, func(ctx context.Context, f *retainedFile, _ *fileSession) (any, error) { return f.ListAt(ctx, page) }, func(storage.FileObservation) any {
			return storage.DirectoryPage{ParentID: 17, Revision: 31, Done: true}
		}},
		{"Sync", nil, func(ctx context.Context, f *retainedFile, _ *fileSession) (any, error) { return nil, f.Sync(ctx) }, func(storage.FileObservation) any { return nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cause := errors.New("authority observation failed")
			observation := storage.FileObservation{Attr: storage.Attr{ID: 23, MetadataRevision: 29}, LinkTarget: []byte("relative")}
			probe := &genericForwardProbe{observation: observation, failure: cause}
			session := retainedTestSession(t, &genericForwardSession{probe: probe})
			file := &retainedFile{session: session, remote: &genericForwardFile{probe: probe}}
			ctx := context.WithValue(t.Context(), genericForwardContextKey{}, "read-scope")
			got, err := test.call(ctx, file, session)
			if !errors.Is(err, cause) || !reflect.DeepEqual(got, test.want(observation)) {
				t.Fatalf("partial observation changed: %#v, %v", got, err)
			}
			expected := []genericForwardCall{{method: test.name, request: test.request, value: "read-scope"}}
			if !reflect.DeepEqual(probe.calls, expected) {
				t.Fatalf("observation request changed: %#v", probe.calls)
			}
			session.base.failure = errors.New("replica failed")
			_, err = test.call(ctx, file, session)
			if !errors.Is(err, syscall.EIO) || len(probe.calls) != 1 {
				t.Fatalf("unhealthy observation reached authority: %v", err)
			}
		})
	}
}

func TestGenericReferenceRequiresNativeBarriersWithoutLosingAuthorityErrors(t *testing.T) {
	cause := errors.New("reference history unavailable")
	for _, test := range []struct {
		name      string
		file      storage.File
		failure   error
		wantError error
	}{
		{name: "native", file: &genericForwardFile{probe: &genericForwardProbe{}}},
		{name: "missing barriers", file: &struct{ storage.File }{}, wantError: syscall.EIO},
		{name: "authority error", failure: cause, wantError: cause},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &genericForwardProbe{failure: test.failure}
			session := retainedTestSession(t, &genericForwardSession{probe: probe, file: test.file})
			session.base.failure = errors.New("replica failed")
			file, err := session.Reference(t.Context(), 97)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("reference error changed: %v", err)
			}
			if test.wantError == nil {
				if file == nil || file.Reference() != 97 {
					t.Fatalf("reference identity changed: %v", file)
				}
			} else if file != nil {
				t.Fatal("failed resolution returned a usable reference")
			}
			if len(probe.calls) != 1 || probe.calls[0].request != storage.FileReferenceID(97) {
				t.Fatalf("reference did not preserve authority identity: %#v", probe.calls)
			}
		})
	}
}

func TestGenericRangeSnapshotRemainsAuthoritativeAfterStreamFailure(t *testing.T) {
	cause := errors.New("range observation interrupted")
	probe := &genericForwardProbe{failure: cause}
	session := retainedTestSession(t, nil)
	session.base.failure = errors.New("replica failed")
	file := &retainedFile{session: session, remote: &genericForwardFile{probe: probe}}
	scope := storage.RangeScope{Domain: 7}
	got, err := file.RangeSnapshot(t.Context(), 41, scope)
	if !errors.Is(err, cause) || got.Revision != 71 || got.OwnerAvailable != 1 {
		t.Fatalf("range observation changed: %+v, %v", got, err)
	}
	if len(probe.calls) != 1 || !reflect.DeepEqual(probe.calls[0].request, genericRangeOwner{41, scope}) {
		t.Fatalf("range owner/domain changed: %#v", probe.calls)
	}
}

func TestGenericSessionStatusOnlyAdvancesEpochOnConfirmedAuthorityAnswers(t *testing.T) {
	for _, method := range []string{"Status", "Renew"} {
		t.Run(method, func(t *testing.T) {
			probe := &genericForwardProbe{}
			remote := &genericForwardSession{probe: probe, status: storage.FileSessionStatus{Epoch: "session", ActionEpoch: 9, Revision: 5}}
			session := retainedTestSession(t, remote)
			session.base.failure = errors.New("replica failed")
			call := session.Status
			if method == "Renew" {
				call = session.Renew
			}
			got, err := call(t.Context())
			if err != nil || got != remote.status || session.actionEpoch != 9 {
				t.Fatalf("confirmed status was lost: %+v, %v; epoch %d", got, err, session.actionEpoch)
			}
			remote.status.ActionEpoch = 3
			if _, err := call(t.Context()); err != nil || session.actionEpoch != 9 {
				t.Fatalf("old status regressed action admission: %v; epoch %d", err, session.actionEpoch)
			}
			cause := errors.New("renewal response could not be confirmed")
			probe.failure = cause
			remote.status.ActionEpoch = 12
			got, err = call(t.Context())
			if !errors.Is(err, cause) || got != remote.status || session.actionEpoch != 9 {
				t.Fatalf("unconfirmed status changed local epoch: %+v, %v; epoch %d", got, err, session.actionEpoch)
			}
			for _, call := range probe.calls {
				if call.method != method {
					t.Fatalf("status routed to %q", call.method)
				}
			}
		})
	}
}
