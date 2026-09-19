package locked

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type probeContextKey struct{}

type capabilityProbe struct {
	storage.FileSession
	checkErr, callErr error
	calls             int
	got               locking.MutationScope
	trace             any
	file              *capabilityFile
	node              *capabilityNode
	attr              storage.Attr
	openName          storage.ChildName
	openOptions       storage.OpenAtOptions
	ownerOptions      storage.OwnerOptions
	bounded           func(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error)
}

func (p *capabilityProbe) record(ctx context.Context) error {
	p.calls++
	p.got = locking.ScopeFromContext(ctx)
	p.trace = ctx.Value(probeContextKey{})
	return p.callErr
}
func (p *capabilityProbe) CheckAtomicFileOpen() error  { return p.checkErr }
func (p *capabilityProbe) CheckNamespaceAccess() error { return p.checkErr }
func (p *capabilityProbe) CheckNodeReferences() error  { return p.checkErr }
func (p *capabilityProbe) CheckMetadataAccess() error  { return p.checkErr }
func (p *capabilityProbe) CheckUseOwners() error       { return p.checkErr }
func (p *capabilityProbe) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.openName, p.openOptions = name, options
	return storage.OpenResult{File: p.file, Attr: p.attr, Outcome: storage.Replaced}, p.record(ctx)
}
func (p *capabilityProbe) ReadDirNode(ctx context.Context, _ storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: 7, Revision: []byte("directory")}, Entries: []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: p.attr}}}, p.record(ctx)
}
func (p *capabilityProbe) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	return p.bounded(ctx, target, result)
}
func (p *capabilityProbe) MutateName(ctx context.Context, _ storage.NameCommand) (storage.NameResult, error) {
	return storage.NameResult{Attr: &p.attr}, p.record(ctx)
}
func (p *capabilityProbe) OpenNodeRef(ctx context.Context, _ uint64, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return storage.NodeOpenResult{Reference: p.node, Attr: p.attr, Outcome: storage.Created}, p.record(ctx)
}
func (p *capabilityProbe) OpenChildRef(ctx context.Context, _ storage.ChildName, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return storage.NodeOpenResult{Reference: p.node, Attr: p.attr, Outcome: storage.Created}, p.record(ctx)
}
func (p *capabilityProbe) SetMetadata(ctx context.Context, _ uint64, _ string, _, _ []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{Version: []byte("version"), Data: []byte("payload")}, p.record(ctx)
}
func (p *capabilityProbe) NewUseOwner(ctx context.Context, _ uint64, _ storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	p.ownerOptions = options
	return 23, p.record(ctx)
}
func (p *capabilityProbe) RetireUseOwner(ctx context.Context, _ storage.UseOwner) error {
	return p.record(ctx)
}

type capabilityFile struct {
	storage.File
	probe *capabilityProbe
}

func (f *capabilityFile) WriteAt(ctx context.Context, _ int64, _ []byte) (storage.Attr, error) {
	return f.probe.attr, f.probe.record(ctx)
}

type capabilityNode struct {
	storage.NodeReference
	probe *capabilityProbe
}

func (n *capabilityNode) Stat(ctx context.Context) (storage.Attr, error) {
	return n.probe.attr, n.probe.record(ctx)
}
func (n *capabilityNode) SetAttr(ctx context.Context, _ storage.AttrChange) (storage.Attr, error) {
	return n.probe.attr, n.probe.record(ctx)
}
func (n *capabilityNode) Close(ctx context.Context) error { return n.probe.record(ctx) }

func capabilityView() (*Storage, locking.MutationScope, context.Context) {
	scope := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}, Grants: []locking.GrantRef{{ID: "grant", Resource: "resource", Generation: 1}}}
	ctx := context.WithValue(locking.WithScope(context.Background(), scope), probeContextKey{}, "request trace")
	return &Storage{scope: &scope}, scope, ctx
}
func probeSession() (*capabilityProbe, *fileSession, locking.MutationScope, context.Context) {
	view, scope, ctx := capabilityView()
	probe := &capabilityProbe{callErr: errors.New("known operation failure"), attr: storage.Attr{ID: 19, Kind: storage.NodeRegular, Size: 3, Metadata: map[string]storage.OpaquePayload{"test.tag": {Version: []byte("v"), Data: []byte("raw")}}}}
	probe.file = &capabilityFile{probe: probe}
	probe.node = &capabilityNode{probe: probe}
	return probe, &fileSession{FileSession: probe, storage: view}, scope, ctx
}

func TestOptionalSessionCapabilitiesRejectMissingAndRefusedBacking(t *testing.T) {
	probe, session, _, ctx := probeSession()
	for _, missing := range []bool{true, false} {
		cause := error(syscall.EOPNOTSUPP)
		if missing {
			session.FileSession = &struct{ storage.FileSession }{}
		} else {
			cause = errors.New("backing capability refused")
			probe.checkErr = cause
			session.FileSession = probe
		}
		for _, check := range []func() error{session.CheckAtomicFileOpen, session.CheckNamespaceAccess, session.CheckNodeReferences, session.CheckMetadataAccess, session.CheckUseOwners} {
			if err := check(); err != cause {
				t.Fatalf("capability check = %v, want %v", err, cause)
			}
		}
		for _, call := range []func() error{
			func() error { _, err := session.OpenAt(ctx, storage.ChildName{}, storage.OpenAtOptions{}); return err },
			func() error { _, err := session.ReadDirNode(ctx, storage.DirectoryTarget{}); return err },
			func() error { _, err := session.LookupAt(ctx, storage.ChildName{}); return err },
			func() error { _, err := session.MutateName(ctx, storage.NameCommand{}); return err },
			func() error { _, err := session.OpenNodeRef(ctx, 1, storage.NodeRefOptions{}); return err },
			func() error {
				_, err := session.OpenChildRef(ctx, storage.ChildName{}, storage.NodeRefOptions{})
				return err
			},
			func() error { _, err := session.SetMetadata(ctx, 1, "key", nil, nil); return err },
			func() error {
				_, err := session.NewUseOwner(ctx, 1, storage.UseScope{}, storage.OwnerOptions{})
				return err
			},
			func() error { return session.RetireUseOwner(ctx, 1) },
		} {
			if err := call(); err != cause {
				t.Fatalf("unsupported call = %v, want %v", err, cause)
			}
		}
	}
	if probe.calls != 0 {
		t.Fatalf("refused capability dispatched %d operations", probe.calls)
	}
}

func TestAtomicOpensPreserveCaptureAndWrapErrorReferences(t *testing.T) {
	probe, session, scope, ctx := probeSession()
	for _, test := range []struct {
		name     string
		options  storage.OpenAtOptions
		mutation bool
	}{
		{"keep", storage.OpenAtOptions{Existing: storage.Keep}, false},
		{"armed keep", storage.OpenAtOptions{Existing: storage.Keep, CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose}}, false},
		{"create", storage.OpenAtOptions{Create: true, Existing: storage.Keep}, true},
		{"reset", storage.OpenAtOptions{Existing: storage.ResetContent}, true},
		{"replace", storage.OpenAtOptions{Existing: storage.ReplaceNode}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 7, Scope: &storage.UseScope{Token: "parent scope"}}, RawLeaf: []byte{0xff, 'A'}}
			test.options.Initial.OnCreate.LinkTarget = []byte{0xff, '/'}
			result, err := session.OpenAt(ctx, name, test.options)
			if !reflect.DeepEqual(probe.openName, name) || !reflect.DeepEqual(probe.openOptions, test.options) {
				t.Fatal("atomic open inputs were changed")
			}
			if err != probe.callErr || !reflect.DeepEqual(result.Attr, probe.attr) || result.Outcome != storage.Replaced || result.File == nil || result.File == probe.file {
				t.Fatalf("atomic result = %+v, %v", result, err)
			}
			want := locking.MutationScope{}
			if test.mutation {
				want = scope
			}
			if !reflect.DeepEqual(probe.got, want) || probe.trace != "request trace" {
				t.Fatalf("open context = %+v, %v", probe.got, probe.trace)
			}
			if _, err := result.File.WriteAt(context.Background(), 0, nil); err != probe.callErr || !reflect.DeepEqual(probe.got, scope) {
				t.Fatalf("error file lost its scoped wrapper: %+v, %v", probe.got, err)
			}
		})
	}
}

func TestOptionalSessionOperationsPreserveScopesAndValues(t *testing.T) {
	probe, session, scope, ctx := probeSession()
	for _, test := range []struct {
		name     string
		mutation bool
		call     func() error
	}{
		{"read directory", false, func() error {
			result, err := session.ReadDirNode(ctx, storage.DirectoryTarget{})
			if result.Observation.ParentID != 7 || len(result.Entries) != 1 || !reflect.DeepEqual(result.Entries[0].RawLeaf, []byte{0xff}) {
				t.Fatalf("directory = %+v", result)
			}
			return err
		}},
		{"lookup", false, func() error {
			result, err := session.LookupAt(ctx, storage.ChildName{})
			if !reflect.DeepEqual(result, probe.attr) {
				t.Fatalf("lookup = %+v", result)
			}
			return err
		}},
		{"name mutation", true, func() error {
			result, err := session.MutateName(ctx, storage.NameCommand{})
			if result.Attr == nil || !reflect.DeepEqual(*result.Attr, probe.attr) {
				t.Fatalf("name result = %+v", result)
			}
			return err
		}},
		{"metadata", true, func() error {
			result, err := session.SetMetadata(ctx, 19, "test.tag", nil, nil)
			if string(result.Version) != "version" || string(result.Data) != "payload" {
				t.Fatalf("metadata = %+v", result)
			}
			return err
		}},
		{"new owner", false, func() error {
			owner, err := session.NewUseOwner(ctx, 19, storage.UseScope{Token: "scope"}, storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Group: 7})
			if probe.ownerOptions != (storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Group: 7}) {
				t.Fatalf("owner options = %+v", probe.ownerOptions)
			}
			if owner != 23 {
				t.Fatalf("owner = %v", owner)
			}
			return err
		}},
		{"retire owner", false, func() error { return session.RetireUseOwner(ctx, 23) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err != probe.callErr {
				t.Fatalf("lost operation failure: %v", err)
			}
			want := locking.MutationScope{}
			if test.mutation {
				want = scope
			}
			if !reflect.DeepEqual(probe.got, want) || probe.trace != "request trace" {
				t.Fatalf("operation context = %+v, %v", probe.got, probe.trace)
			}
		})
	}
}

func TestNodeReferenceOpensPreserveScopesAndLifetime(t *testing.T) {
	probe, session, scope, ctx := probeSession()
	for _, create := range []bool{false, true} {
		for _, child := range []bool{false, true} {
			options := storage.NodeRefOptions{Create: create, CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose}}
			var result storage.NodeOpenResult
			var err error
			if child {
				result, err = session.OpenChildRef(ctx, storage.ChildName{}, options)
			} else {
				result, err = session.OpenNodeRef(ctx, 19, options)
			}
			if err != probe.callErr || !reflect.DeepEqual(result.Attr, probe.attr) || result.Outcome != storage.Created || result.Reference == nil || result.Reference == probe.node {
				t.Fatalf("node open = %+v, %v", result, err)
			}
			want := locking.MutationScope{}
			if create {
				want = scope
			}
			if !reflect.DeepEqual(probe.got, want) {
				t.Fatalf("node open scope = %+v, want %+v", probe.got, want)
			}
			if _, err := result.Reference.Stat(ctx); err != probe.callErr || !reflect.DeepEqual(probe.got, locking.MutationScope{}) {
				t.Fatalf("node stat = %+v, %v", probe.got, err)
			}
			if _, err := result.Reference.SetAttr(context.Background(), storage.AttrChange{}); err != probe.callErr || !reflect.DeepEqual(probe.got, scope) {
				t.Fatalf("node mutation = %+v, %v", probe.got, err)
			}
			if err := result.Reference.Close(ctx); err != probe.callErr || !reflect.DeepEqual(probe.got, locking.MutationScope{}) {
				t.Fatalf("node close retained Strong proof: %+v, %v", probe.got, err)
			}
		}
	}
	if session.storage.wrapFile(nil) != nil || session.storage.wrapReference(nil) != nil {
		t.Fatal("nil reference gained a wrapper")
	}
}

func (p *capabilityProbe) LookupAt(ctx context.Context, _ storage.ChildName) (storage.Attr, error) {
	return p.attr, p.record(ctx)
}

type maintenanceProbe struct {
	storage.BoundedStorage
	checkErr     error
	calls        int
	scope        locking.MutationScope
	chain        storage.PublicationAccountingChain
	initializing bool
}

func (p *maintenanceProbe) LockService() locking.Service      { return nil }
func (p *maintenanceProbe) CheckMaintenanceAccounting() error { return p.checkErr }
func (p *maintenanceProbe) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	p.calls++
	if initialize == nil {
		return syscall.EINVAL
	}
	p.scope = locking.ScopeFromContext(ctx)
	p.chain = chain
	p.initializing = true
	initialize(37)
	p.initializing = false
	return nil
}

func TestMaintenanceAccountingForwardsOnlyTheExplicitChainAndInitialization(t *testing.T) {
	view, _, ctx := capabilityView()
	probe := &maintenanceProbe{}
	view.backend = probe
	var events []string
	hook := func(name string) storage.PublicationAccounting {
		return func(previous, next int64) (storage.PublicationSettlement, error) {
			if previous != 37 || next != 42 {
				t.Fatalf("accounting transition %d -> %d", previous, next)
			}
			events = append(events, name+" prepare")
			return func(result storage.PublicationResult) error {
				if result != storage.PublicationApplied {
					t.Fatalf("settlement = %v", result)
				}
				events = append(events, name+" settle")
				return nil
			}, nil
		}
	}
	ctx = storage.WithPublicationAccounting(ctx, hook("context-only"))
	chain := storage.PublicationAccountingFrom(storage.WithPublicationAccounting(context.Background(), hook("first"))).With(hook("second"))
	initialized := 0
	if err := view.CheckMaintenanceAccounting(); err != nil {
		t.Fatal(err)
	}
	if err := view.BindMaintenanceAccounting(ctx, chain, func(used int64) {
		if !probe.initializing || used != 37 {
			t.Fatalf("initialization escaped backend ordering: %v, %d", probe.initializing, used)
		}
		initialized++
	}); err != nil {
		t.Fatal(err)
	}
	if initialized != 1 || !reflect.DeepEqual(probe.scope, locking.MutationScope{}) {
		t.Fatalf("bind initialization/scope = %d %+v", initialized, probe.scope)
	}
	settle, err := storage.PreparePublication(storage.WithPublicationAccountingChain(ctx, probe.chain), 37, 42)
	if err != nil {
		t.Fatal(err)
	}
	if err := settle(storage.PublicationApplied); err != nil {
		t.Fatal(err)
	}
	want := []string{"first prepare", "second prepare", "second settle", "first settle"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("installed chain = %v, want %v", events, want)
	}
	if err := view.BindMaintenanceAccounting(ctx, chain, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil initialization was changed: %v", err)
	}
}

func TestMaintenanceAccountingRefusesMissingOrDeclinedSupport(t *testing.T) {
	view, _, ctx := capabilityView()
	probe := &maintenanceProbe{checkErr: errors.New("maintenance accounting refused")}
	for _, missing := range []bool{true, false} {
		cause := probe.checkErr
		if missing {
			view.backend = &struct{ Backend }{}
			cause = syscall.EOPNOTSUPP
		} else {
			view.backend = probe
		}
		if err := view.CheckMaintenanceAccounting(); err != cause {
			t.Fatalf("check = %v, want %v", err, cause)
		}
		if err := view.BindMaintenanceAccounting(ctx, storage.PublicationAccountingChain{}, func(int64) { t.Fatal("refused bind initialized") }); err != cause {
			t.Fatalf("bind = %v, want %v", err, cause)
		}
	}
	if probe.calls != 0 {
		t.Fatalf("refused bind dispatched %d calls", probe.calls)
	}
}

func TestBoundedDirectoryForwardingPreservesAdmissionAndInvalidatesFailures(t *testing.T) {
	for _, scenario := range []string{"success", "bound", "backend failure", "missing", "declined"} {
		t.Run(scenario, func(t *testing.T) {
			probe, session, _, ctx := probeSession()
			target := storage.DirectoryTarget{NodeID: 7, Scope: &storage.UseScope{Token: "directory"}}
			observation := storage.DirectoryObservation{ParentID: 7, Revision: []byte("observed")}
			failure := errors.New("directory failed after first entry")
			var loaded, admitted int
			limit := int64(2)
			if scenario == "bound" {
				limit = 1
			}
			result, err := storage.NewListResult(limit, 0, func(_ int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
				if loaded != admitted || nameBytes != 1 || metadataBytes != 6 || attr.Metadata != nil {
					t.Fatal("payload was loaded before caller admission")
				}
				admitted++
				return 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			probe.bounded = func(callCtx context.Context, got storage.DirectoryTarget, supplied *storage.ListResult) (storage.DirectoryObservation, error) {
				probe.record(callCtx)
				if supplied != result || !reflect.DeepEqual(got, target) || !reflect.DeepEqual(probe.got, locking.MutationScope{}) || probe.trace != "request trace" {
					t.Fatal("bounded listing lost target, result identity, or read context")
				}
				for _, name := range []string{"a", "b"} {
					reservation, err := supplied.Reserve(1, 6, storage.Attr{ID: 9, Kind: storage.NodeRegular})
					if err != nil {
						return observation, err
					}
					loaded++
					if err := reservation.Commit(name, nil); err != nil {
						return observation, err
					}
					if scenario == "backend failure" {
						return observation, failure
					}
				}
				return observation, nil
			}
			if scenario == "missing" || scenario == "declined" {
				if err := result.Add(storage.Entry{Name: "a", Attr: storage.Attr{Kind: storage.NodeRegular}}); err != nil {
					t.Fatal(err)
				}
				if scenario == "missing" {
					session.FileSession = &struct{ storage.FileSession }{}
				} else {
					probe.checkErr = failure
				}
			}
			got, err := session.ReadDirNodeBounded(ctx, target, result)
			entries, listErr := result.Entries()
			if scenario == "success" {
				if err != nil || listErr != nil || len(entries) != 2 || loaded != 2 || !reflect.DeepEqual(got, observation) {
					t.Fatalf("bounded result = %+v, %v, %+v, %v", got, err, entries, listErr)
				}
				return
			}
			if err == nil || listErr != err || entries != nil {
				t.Fatalf("failed listing exposed a partial result: %v, %+v, %v", err, entries, listErr)
			}
			if scenario == "bound" && (loaded != 1 || !errors.Is(err, syscall.EIO)) {
				t.Fatalf("bound loaded %d entries: %v", loaded, err)
			}
			if (scenario == "backend failure" || scenario == "declined") && err != failure {
				t.Fatalf("failure identity lost: %v", err)
			}
			if scenario == "missing" && !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("missing capability returned %v", err)
			}
			if (scenario == "missing" || scenario == "declined") && probe.calls != 0 {
				t.Fatal("unsupported listing reached backend")
			}
		})
	}
}

func TestSessionRangeControlPreservesReadScopeAndReceipt(t *testing.T) {
	for _, scenario := range []string{"supported", "missing", "declined"} {
		t.Run(scenario, func(t *testing.T) {
			probe, session, _, ctx := probeSession()
			raw := &referenceProbe{capabilityFile: probe.file}
			session.FileSession = &struct {
				storage.FileSession
				storage.RangeControl
			}{RangeControl: raw}
			checkError := error(nil)
			callError := probe.callErr
			if scenario == "missing" {
				session.FileSession = &struct{ storage.FileSession }{}
				checkError, callError = syscall.EOPNOTSUPP, syscall.EOPNOTSUPP
			} else if scenario == "declined" {
				checkError = errors.New("ranges declined")
				probe.checkErr, callError = checkError, checkError
			}
			if err := session.CheckRangeControl(); err != checkError {
				t.Fatalf("check = %v, want %v", err, checkError)
			}
			id := storage.LockRequestID("action")
			checkAttempt := func(attempt storage.RangeAttempt, err error) error {
				if scenario == "supported" && !reflect.DeepEqual(attempt, raw.attempt(id)) {
					t.Fatalf("lost receipt: %+v", attempt)
				}
				return err
			}
			for _, call := range []func() error{
				func() error {
					conflict, err := session.GetConflict(ctx, 1, storage.RangeCommand{})
					if scenario == "supported" && (!conflict.Found || conflict.Owner != 17) {
						t.Fatalf("lost conflict: %+v", conflict)
					}
					return err
				},
				func() error { return checkAttempt(session.Apply(ctx, 1, nil, id)) },
				func() error { return checkAttempt(session.Query(ctx, 1, id)) },
				func() error { return checkAttempt(session.Cancel(ctx, 1, id)) },
				func() error { return session.Drop(ctx, 1, storage.DomainRecord) },
			} {
				if err := call(); err != callError {
					t.Fatalf("call = %v, want %v", err, callError)
				}
			}
			if scenario == "supported" {
				if probe.calls != 5 || !reflect.DeepEqual(probe.got, locking.MutationScope{}) || probe.trace != "request trace" {
					t.Fatalf("range context = %+v, %v, calls %d", probe.got, probe.trace, probe.calls)
				}
			} else if probe.calls != 0 {
				t.Fatal("declined capability dispatched range operations")
			}
		})
	}
}
