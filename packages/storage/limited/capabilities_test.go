package limited

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type capabilityCall struct {
	name string
	args any
}

type capabilityProbe struct {
	storage.FileSession
	calls                 []capabilityCall
	checkError, callError error
	before, after         int64
	open                  storage.OpenResult
	node                  storage.NodeOpenResult
	state                 storage.ReferenceState
	payload               storage.OpaquePayload
}

func (p *capabilityProbe) record(name string, args any) {
	p.calls = append(p.calls, capabilityCall{name, args})
}

func (p *capabilityProbe) publish(ctx context.Context) error {
	settle, err := storage.PreparePublication(ctx, p.before, p.after)
	if err != nil {
		return err
	}
	return errors.Join(p.callError, settle(storage.PublicationApplied))
}

func (p *capabilityProbe) CheckAtomicFileOpen() error  { return p.checkError }
func (p *capabilityProbe) CheckNamespaceAccess() error { return p.checkError }
func (p *capabilityProbe) CheckNodeReferences() error  { return p.checkError }
func (p *capabilityProbe) CheckMetadataAccess() error  { return p.checkError }
func (p *capabilityProbe) CheckUseOwners() error       { return p.checkError }
func (p *capabilityProbe) CheckRangeControl() error    { return p.checkError }

func (p *capabilityProbe) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.record("open", struct {
		Name    storage.ChildName
		Options storage.OpenAtOptions
	}{name, options})
	return p.open, p.publish(ctx)
}
func (p *capabilityProbe) LookupAt(_ context.Context, name storage.ChildName) (storage.Attr, error) {
	p.record("lookup", name)
	return p.state.Attr, p.callError
}
func (p *capabilityProbe) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	p.record("list", target)
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{7}}, Entries: []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: p.state.Attr}}}, p.callError
}
func (p *capabilityProbe) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	p.record("list-bounded", struct {
		Target storage.DirectoryTarget
		Result *storage.ListResult
	}{target, result})
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{7}}, p.callError
}
func (p *capabilityProbe) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	p.record("name", command)
	return storage.NameResult{Attr: &p.state.Attr}, p.publish(ctx)
}
func (p *capabilityProbe) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.record("node", struct {
		ID      uint64
		Options storage.NodeRefOptions
	}{id, options})
	return p.node, p.publish(ctx)
}
func (p *capabilityProbe) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.record("child", struct {
		Name    storage.ChildName
		Options storage.NodeRefOptions
	}{name, options})
	return p.node, p.publish(ctx)
}
func (p *capabilityProbe) SetMetadata(ctx context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	p.record("metadata", struct {
		ID            uint64
		Namespace     string
		Version, Data []byte
	}{id, namespace, version, data})
	return p.payload, p.publish(ctx)
}
func (p *capabilityProbe) NewUseOwner(_ context.Context, id uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	p.record("new-owner", struct {
		ID      uint64
		Scope   storage.UseScope
		Options storage.OwnerOptions
	}{id, scope, options})
	return 23, p.callError
}
func (p *capabilityProbe) RetireUseOwner(_ context.Context, owner storage.UseOwner) error {
	p.record("retire-owner", owner)
	return p.callError
}
func (p *capabilityProbe) GetConflict(_ context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	p.record("conflict", struct {
		Owner   storage.UseOwner
		Command storage.RangeCommand
	}{owner, command})
	return storage.RangeConflict{Found: true, Owner: 17, Range: command.Range, Mode: command.Mode}, p.callError
}
func (p *capabilityProbe) Apply(_ context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.record("apply", struct {
		Owner    storage.UseOwner
		Commands []storage.RangeCommand
		Request  storage.LockRequestID
	}{owner, commands, request})
	return storage.RangeAttempt{Request: request, State: storage.Granted, Commands: commands}, p.callError
}
func (p *capabilityProbe) Query(_ context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.record("query", struct {
		Owner   storage.UseOwner
		Request storage.LockRequestID
	}{owner, request})
	return storage.RangeAttempt{Request: request, State: storage.Granted}, p.callError
}
func (p *capabilityProbe) Cancel(_ context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.record("cancel", struct {
		Owner   storage.UseOwner
		Request storage.LockRequestID
	}{owner, request})
	return storage.RangeAttempt{Request: request, State: storage.Cancelled}, p.callError
}
func (p *capabilityProbe) Drop(_ context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	p.record("drop", struct {
		Owner  storage.UseOwner
		Domain storage.ConflictDomain
	}{owner, domain})
	return p.callError
}

func TestOptionalSessionCapabilitiesRejectMissingOrFailedBacking(t *testing.T) {
	failure := errors.New("native capability verification failed")
	for _, test := range []struct {
		name    string
		backing storage.FileSession
		want    error
	}{
		{"missing", &struct{ storage.FileSession }{}, syscall.EOPNOTSUPP},
		{"failed", &capabilityProbe{checkError: failure}, failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &fileSession{FileSession: test.backing, storage: &Storage{limit: MinLimit}}
			checks := []func() error{s.CheckAtomicFileOpen, s.CheckNamespaceAccess, s.CheckNodeReferences, s.CheckMetadataAccess, s.CheckUseOwners, s.CheckRangeControl}
			for _, check := range checks {
				if err := check(); !errors.Is(err, test.want) {
					t.Fatalf("check=%v", err)
				}
			}
			calls := []func() error{
				func() error { _, e := s.OpenAt(t.Context(), storage.ChildName{}, storage.OpenAtOptions{}); return e },
				func() error { _, e := s.LookupAt(t.Context(), storage.ChildName{}); return e },
				func() error { _, e := s.ReadDirNode(t.Context(), storage.DirectoryTarget{}); return e },
				func() error { _, e := s.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{}, nil); return e },
				func() error { _, e := s.MutateName(t.Context(), storage.NameCommand{}); return e },
				func() error { _, e := s.OpenNodeRef(t.Context(), 1, storage.NodeRefOptions{}); return e },
				func() error {
					_, e := s.OpenChildRef(t.Context(), storage.ChildName{}, storage.NodeRefOptions{})
					return e
				},
				func() error { _, e := s.SetMetadata(t.Context(), 1, "app", nil, nil); return e },
				func() error {
					_, e := s.NewUseOwner(t.Context(), 1, storage.UseScope{}, storage.OwnerOptions{})
					return e
				},
				func() error { return s.RetireUseOwner(t.Context(), 1) },
				func() error { _, e := s.GetConflict(t.Context(), 1, storage.RangeCommand{}); return e },
				func() error { _, e := s.Apply(t.Context(), 1, nil, ""); return e },
				func() error { _, e := s.Query(t.Context(), 1, ""); return e },
				func() error { _, e := s.Cancel(t.Context(), 1, ""); return e },
				func() error { return s.Drop(t.Context(), 1, storage.DomainRecord) },
			}
			for i, call := range calls {
				if err := call(); !errors.Is(err, test.want) {
					t.Fatalf("call%d=%v", i, err)
				}
			}
			if p, ok := test.backing.(*capabilityProbe); ok && len(p.calls) != 0 {
				t.Fatal("failed capability reached operation")
			}
		})
	}
}

func TestOptionalSessionMutationsPreserveInputsResultsAndNativeAccounting(t *testing.T) {
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 11}, RawLeaf: []byte{0xff, 'x'}}
	openOptions := storage.OpenAtOptions{Read: true, Write: true, Create: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.ResetContent, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Initial: storage.InitialState{OnReset: storage.InitialFields{Metadata: map[string][]byte{"app.tag": {3}}}}}
	nodeOptions := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 31}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries}, InitialState: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"app.tag": {9}}}}}
	command := storage.NameCommand{Kind: storage.NameRename, Name: name, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 31}, Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 12}, ObservedLeaf: []byte("target"), OutputLeaf: []byte("TARGET"), Expected: storage.ChildCondition{State: storage.Absent}}}
	for _, method := range []string{"open", "node", "child", "name", "metadata"} {
		t.Run(method, func(t *testing.T) {
			p := &capabilityProbe{before: 1024, after: 512, state: storage.ReferenceState{Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular, Size: 512}}, payload: storage.OpaquePayload{Version: []byte{7}, Data: []byte{0xff}}}
			r := &referenceProbe{probe: p}
			p.open = storage.OpenResult{File: r, Attr: p.state.Attr, Outcome: storage.Reset}
			p.node = storage.NodeOpenResult{Reference: r, Attr: p.state.Attr, Outcome: storage.Opened}
			s := &fileSession{FileSession: p, storage: &Storage{limit: MinLimit, count: 1024}}
			upstream := 0
			ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
				if previous != 1024 || next != 512 {
					t.Fatalf("native counts=%d,%d", previous, next)
				}
				upstream++
				return func(result storage.PublicationResult) error {
					if result != storage.PublicationApplied {
						t.Fatalf("result=%v", result)
					}
					return nil
				}, nil
			})
			var want any
			switch method {
			case "open":
				got, err := s.OpenAt(ctx, name, openOptions)
				if err != nil || got.File == r || got.File == nil || got.Outcome != p.open.Outcome || !reflect.DeepEqual(got.Attr, p.open.Attr) {
					t.Fatalf("open=%+v,%v", got, err)
				}
				want = struct {
					Name    storage.ChildName
					Options storage.OpenAtOptions
				}{name, openOptions}
			case "node":
				got, err := s.OpenNodeRef(ctx, 31, nodeOptions)
				if err != nil || got.Reference == r || got.Reference == nil || !reflect.DeepEqual(got.Attr, p.node.Attr) {
					t.Fatalf("node=%+v,%v", got, err)
				}
				want = struct {
					ID      uint64
					Options storage.NodeRefOptions
				}{31, nodeOptions}
			case "child":
				got, err := s.OpenChildRef(ctx, name, nodeOptions)
				if err != nil || got.Reference == r || got.Reference == nil || !reflect.DeepEqual(got.Attr, p.node.Attr) {
					t.Fatalf("child=%+v,%v", got, err)
				}
				want = struct {
					Name    storage.ChildName
					Options storage.NodeRefOptions
				}{name, nodeOptions}
			case "name":
				got, err := s.MutateName(ctx, command)
				if err != nil || got.Attr == nil || !reflect.DeepEqual(*got.Attr, p.state.Attr) {
					t.Fatalf("name=%+v,%v", got, err)
				}
				want = command
			case "metadata":
				got, err := s.SetMetadata(ctx, 31, "app.tag", []byte{1}, []byte{0xff})
				if err != nil || !reflect.DeepEqual(got, p.payload) {
					t.Fatalf("metadata=%+v,%v", got, err)
				}
				want = struct {
					ID            uint64
					Namespace     string
					Version, Data []byte
				}{31, "app.tag", []byte{1}, []byte{0xff}}
			}
			if len(p.calls) != 1 || p.calls[0].name != method || !reflect.DeepEqual(p.calls[0].args, want) {
				t.Fatalf("calls=%+v,want%+v", p.calls, want)
			}
			requireCount(t, s.storage, 512)
			if upstream != 1 {
				t.Fatalf("upstream hook calls=%d", upstream)
			}
		})
	}
}

func TestOptionalSessionObservationsAndRangeCommandsRetainTheirResults(t *testing.T) {
	failure := errors.New("native result delivery failed")
	p := &capabilityProbe{callError: failure, state: storage.ReferenceState{Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular}}}
	s := &fileSession{FileSession: p, storage: &Storage{limit: MinLimit, count: 512}}
	for _, check := range []func() error{s.CheckAtomicFileOpen, s.CheckNamespaceAccess, s.CheckNodeReferences, s.CheckMetadataAccess, s.CheckUseOwners, s.CheckRangeControl} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	target := storage.DirectoryTarget{NodeID: 11, Scope: &storage.UseScope{Token: "directory"}}
	name := storage.ChildName{Parent: target, RawLeaf: []byte{0xff}}
	attr, err := s.LookupAt(t.Context(), name)
	if !errors.Is(err, failure) || !reflect.DeepEqual(attr, p.state.Attr) {
		t.Fatalf("lookup=%+v,%v", attr, err)
	}
	listed, err := s.ReadDirNode(t.Context(), target)
	if !errors.Is(err, failure) || listed.Observation.ParentID != 11 || len(listed.Entries) != 1 || listed.Entries[0].RawLeaf[0] != 0xff {
		t.Fatalf("list=%+v,%v", listed, err)
	}
	scope := storage.UseScope{Token: "file"}
	options := storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Group: 99}
	owner, err := s.NewUseOwner(t.Context(), 31, scope, options)
	if !errors.Is(err, failure) || owner != 23 {
		t.Fatalf("owner=%d,%v", owner, err)
	}
	if err := s.RetireUseOwner(t.Context(), owner); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	request, _ := storage.NewLockRequestID(1)
	command := storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: 7, Length: 5}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}}
	conflict, err := s.GetConflict(t.Context(), owner, command)
	if !errors.Is(err, failure) || !conflict.Found || conflict.Range != command.Range || conflict.Owner != 17 {
		t.Fatalf("conflict=%+v,%v", conflict, err)
	}
	attempt, err := s.Apply(t.Context(), owner, []storage.RangeCommand{command}, request)
	if !errors.Is(err, failure) || attempt.Request != request || attempt.State != storage.Granted || !reflect.DeepEqual(attempt.Commands, []storage.RangeCommand{command}) {
		t.Fatalf("apply=%+v,%v", attempt, err)
	}
	attempt, err = s.Query(t.Context(), owner, request)
	if !errors.Is(err, failure) || attempt.Request != request || attempt.State != storage.Granted {
		t.Fatalf("query=%+v,%v", attempt, err)
	}
	attempt, err = s.Cancel(t.Context(), owner, request)
	if !errors.Is(err, failure) || attempt.Request != request || attempt.State != storage.Cancelled {
		t.Fatalf("cancel=%+v,%v", attempt, err)
	}
	if err := s.Drop(t.Context(), owner, storage.DomainEnforced); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	want := []capabilityCall{
		{"lookup", name},
		{"list", target},
		{"new-owner", struct {
			ID      uint64
			Scope   storage.UseScope
			Options storage.OwnerOptions
		}{31, scope, options}},
		{"retire-owner", owner},
		{"conflict", struct {
			Owner   storage.UseOwner
			Command storage.RangeCommand
		}{owner, command}},
		{"apply", struct {
			Owner    storage.UseOwner
			Commands []storage.RangeCommand
			Request  storage.LockRequestID
		}{owner, []storage.RangeCommand{command}, request}},
		{"query", struct {
			Owner   storage.UseOwner
			Request storage.LockRequestID
		}{owner, request}},
		{"cancel", struct {
			Owner   storage.UseOwner
			Request storage.LockRequestID
		}{owner, request}},
		{"drop", struct {
			Owner  storage.UseOwner
			Domain storage.ConflictDomain
		}{owner, storage.DomainEnforced}},
	}
	if !reflect.DeepEqual(p.calls, want) {
		t.Fatalf("calls=%+v,want%+v", p.calls, want)
	}
	requireCount(t, s.storage, 512)
}

func TestOptionalBoundedDirectoryPreservesCallerReservationAndInvalidatesFailures(t *testing.T) {
	failure := errors.New("directory capture failed")
	for _, name := range []string{"success", "native failure", "capability failure"} {
		t.Run(name, func(t *testing.T) {
			p := &capabilityProbe{}
			if name == "native failure" {
				p.callError = failure
			}
			if name == "capability failure" {
				p.checkError = failure
			}
			s := &fileSession{FileSession: p, storage: &Storage{limit: MinLimit}}
			result, err := storage.NewListResult(4096, 0, measurementEntryBytes)
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Add(storage.Entry{Name: "entry", Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular}}); err != nil {
				t.Fatal(err)
			}
			target := storage.DirectoryTarget{NodeID: 11, Scope: &storage.UseScope{Token: "directory"}}
			observation, err := s.ReadDirNodeBounded(t.Context(), target, result)
			if name == "success" {
				if err != nil || observation.ParentID != 11 {
					t.Fatalf("observation=%+v,%v", observation, err)
				}
				if entries, err := result.Entries(); err != nil || len(entries) != 1 {
					t.Fatalf("bounded result=%+v,%v", entries, err)
				}
			} else {
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				if entries, err := result.Entries(); entries != nil || !errors.Is(err, failure) {
					t.Fatalf("failed listing exposed partial result: %+v,%v", entries, err)
				}
			}
			if name == "capability failure" {
				if len(p.calls) != 0 {
					t.Fatal("failed capability called native")
				}
				return
			}
			want := capabilityCall{"list-bounded", struct {
				Target storage.DirectoryTarget
				Result *storage.ListResult
			}{target, result}}
			if len(p.calls) != 1 || !reflect.DeepEqual(p.calls[0], want) {
				t.Fatalf("bounded target/result changed: %+v", p.calls)
			}
		})
	}
}
