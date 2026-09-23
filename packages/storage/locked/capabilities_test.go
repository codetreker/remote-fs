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

type capabilitySessionProbe struct {
	storage.FileSession
	checkErr  error
	failure   error
	attempt   storage.RangeAttempt
	observed  locking.MutationScope
	open      storage.OpenResult
	node      storage.NodeOpenResult
	name      storage.NameResult
	action    storage.FileActionReceipt
	deleted   storage.DeleteIntentStatus
	selection storage.ChildSelection
}

type directoryReaderOnlyProbe struct{ storage.FileSession }

type namespaceOnlyProbe struct{ storage.FileSession }

func (namespaceOnlyProbe) CheckNamespaceAccess() error { return nil }
func (namespaceOnlyProbe) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
}
func (namespaceOnlyProbe) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	return storage.NameResult{}, nil
}

func (directoryReaderOnlyProbe) CheckDirectoryRead() error { return nil }
func (directoryReaderOnlyProbe) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}}, nil
}
func (directoryReaderOnlyProbe) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, _ *storage.ListResult) (storage.DirectoryObservation, error) {
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}, nil
}

func TestDirectoryReadCapabilityIsIndependentOfNamespaceMutation(t *testing.T) {
	wrapper := &fileSession{FileSession: directoryReaderOnlyProbe{}, storage: &Storage{}}
	if err := wrapper.CheckDirectoryRead(); err != nil {
		t.Fatal(err)
	}
	if err := wrapper.CheckNamespaceAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("directory-only backend exposed namespace mutation: %v", err)
	}
	if observed, err := wrapper.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: 7}); err != nil || observed.Observation.ParentID != 7 {
		t.Fatalf("directory-only read = %+v, %v", observed, err)
	}
}

func TestNamespaceCapabilityIsIndependentOfDirectoryRead(t *testing.T) {
	wrapper := &fileSession{FileSession: namespaceOnlyProbe{}, storage: &Storage{}}
	if err := wrapper.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	if err := wrapper.CheckDirectoryRead(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("namespace-only backend exposed directory read: %v", err)
	}
	if attr, err := wrapper.LookupAt(t.Context(), storage.ChildName{}); err != nil || attr.ID != 11 {
		t.Fatalf("namespace-only lookup = %+v, %v", attr, err)
	}
}

func (p *capabilitySessionProbe) CheckAllocationReporting() error { return p.checkErr }

func TestAllocationReportingFollowsWrappedSession(t *testing.T) {
	if err := (&fileSession{FileSession: namespaceOnlyProbe{}}).CheckAllocationReporting(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing backend allocation capability = %v", err)
	}
	cause := errors.New("allocation ledger unavailable")
	if err := (&fileSession{FileSession: &capabilitySessionProbe{checkErr: cause}}).CheckAllocationReporting(); !errors.Is(err, cause) {
		t.Fatalf("backend allocation failure = %v", err)
	}
	if err := (&fileSession{FileSession: &capabilitySessionProbe{}}).CheckAllocationReporting(); err != nil {
		t.Fatalf("available allocation capability = %v", err)
	}
}

func (p *capabilitySessionProbe) capture(ctx context.Context) {
	p.observed = locking.ScopeFromContext(ctx)
}
func (p *capabilitySessionProbe) CheckMetadataAccess() error { return p.checkErr }
func (p *capabilitySessionProbe) SetMetadata(ctx context.Context, _ uint64, _ string, _, _ []byte) (storage.OpaquePayload, error) {
	p.capture(ctx)
	return storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")}, p.failure
}
func (p *capabilitySessionProbe) CheckUseOwners() error { return p.checkErr }
func (p *capabilitySessionProbe) NewUseOwner(ctx context.Context, _ uint64, _ storage.UseScope, _ storage.OwnerOptions) (storage.UseOwner, error) {
	p.capture(ctx)
	return 7, p.failure
}
func (p *capabilitySessionProbe) RetireUseOwner(ctx context.Context, _ storage.UseOwner) error {
	p.capture(ctx)
	return p.failure
}
func (p *capabilitySessionProbe) CheckRangeControl() error { return p.checkErr }
func (p *capabilitySessionProbe) GetConflict(ctx context.Context, _ storage.UseOwner, _ storage.RangeCommand) (storage.RangeConflict, error) {
	p.capture(ctx)
	return storage.RangeConflict{Found: true, Owner: 8}, p.failure
}
func (p *capabilitySessionProbe) Apply(ctx context.Context, _ storage.UseOwner, _ []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.capture(ctx)
	p.attempt.Request = request
	return p.attempt, p.failure
}
func (p *capabilitySessionProbe) Query(ctx context.Context, _ storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.capture(ctx)
	p.attempt.Request = request
	return p.attempt, p.failure
}
func (p *capabilitySessionProbe) Cancel(ctx context.Context, _ storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	p.capture(ctx)
	p.attempt.Request = request
	return p.attempt, p.failure
}
func (p *capabilitySessionProbe) Drop(ctx context.Context, _ storage.UseOwner, _ storage.ConflictDomain) error {
	p.capture(ctx)
	return p.failure
}

func TestNamespaceWrapperRejectsSubstitutedDirectoryIdentity(t *testing.T) {
	probe := &capabilitySessionProbe{}
	view := &Storage{}
	wrapper := &fileSession{FileSession: probe, storage: view}
	target := storage.DirectoryTarget{NodeID: 9}
	if observed, err := wrapper.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.ObservedDirectory{}) {
		t.Fatalf("substituted directory = %+v, %v", observed, err)
	}
	result, err := storage.NewListResult(4096, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes + 64, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := wrapper.ReadDirNodeBounded(t.Context(), target, result); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.DirectoryObservation{}) {
		t.Fatalf("substituted bounded directory = %+v, %v", observed, err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("substituted bounded directory exposed %+v, %v", entries, err)
	}
}
func (p *capabilitySessionProbe) CheckAtomicFileOpen() error { return p.checkErr }
func (p *capabilitySessionProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, _ storage.OpenAtOptions) (storage.OpenResult, error) {
	p.capture(ctx)
	p.selection = selection
	return p.open, p.failure
}
func (p *capabilitySessionProbe) CheckNamespaceAccess() error { return p.checkErr }
func (p *capabilitySessionProbe) CheckDirectoryRead() error   { return p.checkErr }
func (p *capabilitySessionProbe) LookupAt(ctx context.Context, _ storage.ChildName) (storage.Attr, error) {
	p.capture(ctx)
	return storage.Attr{ID: 3, Kind: storage.NodeRegular}, p.failure
}
func (p *capabilitySessionProbe) ReadDirNode(ctx context.Context, _ storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	p.capture(ctx)
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: 3, Revision: []byte{1}}}, p.failure
}
func (p *capabilitySessionProbe) ReadDirNodeBounded(ctx context.Context, _ storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	p.capture(ctx)
	if p.failure == nil && result != nil {
		_ = result.Add(storage.Entry{Name: "entry", Attr: storage.Attr{ID: 4, Kind: storage.NodeRegular}})
	}
	return storage.DirectoryObservation{ParentID: 3, Revision: []byte{1}}, p.failure
}
func (p *capabilitySessionProbe) MutateName(ctx context.Context, _ storage.NameCommand) (storage.NameResult, error) {
	p.capture(ctx)
	return p.name, p.failure
}
func (p *capabilitySessionProbe) CheckNodeReferences() error { return p.checkErr }
func (p *capabilitySessionProbe) OpenNodeRef(ctx context.Context, _ uint64, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.capture(ctx)
	return p.node, p.failure
}
func (p *capabilitySessionProbe) OpenChildRef(ctx context.Context, selection storage.ChildSelection, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.capture(ctx)
	p.selection = selection
	return p.node, p.failure
}
func (p *capabilitySessionProbe) CheckFileActions() error { return p.checkErr }
func (p *capabilitySessionProbe) QueryFileAction(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	p.capture(ctx)
	return p.action, p.failure
}
func (p *capabilitySessionProbe) QueryDeleteIntent(ctx context.Context, _ storage.DeleteIntentOwner, _ storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	p.capture(ctx)
	return p.deleted, p.failure
}
func (p *capabilitySessionProbe) ListDeleteIntents(ctx context.Context, _ storage.DeleteIntentOwner, _ storage.DeleteIntentCursor, _ int) (storage.DeleteIntentPage, error) {
	p.capture(ctx)
	return storage.DeleteIntentPage{Intents: []storage.DeleteIntentStatus{p.deleted}, Next: 1}, p.failure
}
func (p *capabilitySessionProbe) AcknowledgeDeleteIntent(ctx context.Context, _ storage.AcknowledgeDeleteIntentCommand) error {
	p.capture(ctx)
	return p.failure
}

type capabilityFileProbe struct {
	storage.File
	checkErr error
	failure  error
	observed locking.MutationScope
	state    storage.ReferenceState
}

func (p *capabilityFileProbe) Stat(ctx context.Context) (storage.Attr, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state.Attr, p.failure
}
func (p *capabilityFileProbe) SetAttr(ctx context.Context, _ storage.AttrChange) (storage.Attr, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state.Attr, p.failure
}
func (p *capabilityFileProbe) Close(ctx context.Context) error {
	p.observed = locking.ScopeFromContext(ctx)
	return p.failure
}
func (p *capabilityFileProbe) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return storage.ReferenceCloseResult{Released: p.failure == nil}, p.failure
}

func (p *capabilityFileProbe) CheckScopedReference() error { return p.checkErr }
func (p *capabilityFileProbe) Scope(ctx context.Context) (storage.UseScope, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return storage.UseScope{Token: "scope"}, p.failure
}
func (p *capabilityFileProbe) CheckMetadataAccess() error { return p.checkErr }
func (p *capabilityFileProbe) SetMetadata(ctx context.Context, _ string, _, _ []byte) (storage.OpaquePayload, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")}, p.failure
}
func (p *capabilityFileProbe) CheckReferenceState() error { return p.checkErr }
func (p *capabilityFileProbe) State(ctx context.Context) (storage.ReferenceState, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state, p.failure
}
func (p *capabilityFileProbe) CheckDeleteIntent() error { return p.checkErr }
func (p *capabilityFileProbe) SetPendingUnlink(ctx context.Context, _ storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state, p.failure
}
func (p *capabilityFileProbe) ClearPendingUnlink(ctx context.Context, _ storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state, p.failure
}
func (p *capabilityFileProbe) CheckConditionalFileMutation() error { return p.checkErr }
func (p *capabilityFileProbe) MutateFile(ctx context.Context, _ storage.FileMutation) (storage.Attr, error) {
	p.observed = locking.ScopeFromContext(ctx)
	return p.state.Attr, p.failure
}

func TestSessionCapabilitiesSeparateMutationProofsAndPreserveResults(t *testing.T) {
	failure := errors.New("native response failed")
	probe := &capabilitySessionProbe{failure: failure, attempt: storage.RangeAttempt{State: storage.Granted, EverGranted: true}}
	proof := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}}
	view := &Storage{scope: &proof}
	session := &fileSession{FileSession: probe, storage: view}
	if err := session.CheckUseOwners(); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckRangeControl(); err != nil {
		t.Fatal(err)
	}
	if payload, err := session.SetMetadata(t.Context(), 3, "test.value", nil, nil); !errors.Is(err, failure) || string(payload.Data) != "value" || !reflect.DeepEqual(probe.observed, proof) {
		t.Fatalf("metadata=%+v,%v scope=%+v", payload, err, probe.observed)
	}
	request, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := session.Apply(locking.WithScope(t.Context(), proof), 7, nil, request)
	if !errors.Is(err, failure) || attempt.Request != request || !attempt.EverGranted {
		t.Fatalf("attempt=%+v,%v", attempt, err)
	}
	if !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("range control inherited mutation proof: %+v", probe.observed)
	}
	if err := session.RetireUseOwner(locking.WithScope(t.Context(), proof), 7); !errors.Is(err, failure) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("retire owner=%v scope=%+v", err, probe.observed)
	}

	reference := &capabilityFileProbe{failure: failure}
	file := view.wrapFile(reference)
	if err := file.(storage.ScopedReference).CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := file.(storage.ScopedReference).Scope(locking.WithScope(t.Context(), proof)); !errors.Is(err, failure) || scope.Token != "scope" || !reflect.DeepEqual(reference.observed, locking.MutationScope{}) {
		t.Fatalf("scope=%+v,%v context=%+v", scope, err, reference.observed)
	}
	if err := file.(storage.ReferenceMetadataAccess).CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	if payload, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) || string(payload.Data) != "value" || !reflect.DeepEqual(reference.observed, proof) {
		t.Fatalf("reference metadata=%+v,%v context=%+v", payload, err, reference.observed)
	}
	probe.checkErr = failure
	probe.failure = errors.New("dispatch should not occur")
	for name, call := range map[string]func() error{
		"retire owner": func() error { return session.RetireUseOwner(t.Context(), 7) },
		"query": func() error {
			_, err := session.Query(t.Context(), 7, request)
			return err
		},
		"cancel": func() error {
			_, err := session.Cancel(t.Context(), 7, request)
			return err
		},
		"drop": func() error { return session.Drop(t.Context(), 7, storage.DomainEnforced) },
	} {
		if err := call(); !errors.Is(err, failure) {
			t.Fatalf("refused %s=%v", name, err)
		}
	}
	reference.checkErr = failure
	reference.failure = errors.New("reference dispatch should not occur")
	if _, err := file.(storage.ScopedReference).Scope(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("refused scope=%v", err)
	}
	if _, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) {
		t.Fatalf("refused reference metadata=%v", err)
	}

	missingSession := &fileSession{FileSession: struct{ storage.FileSession }{}, storage: view}
	for name, check := range map[string]func() error{
		"metadata": missingSession.CheckMetadataAccess,
		"owners":   missingSession.CheckUseOwners,
		"ranges":   missingSession.CheckRangeControl,
	} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing %s check=%v", name, err)
		}
	}
	missingFile := view.wrapFile(struct{ storage.File }{})
	if err := missingFile.(storage.ScopedReference).CheckScopedReference(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing scope check=%v", err)
	}
	if err := missingFile.(storage.ReferenceMetadataAccess).CheckMetadataAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing reference metadata check=%v", err)
	}
}

func TestIdentityWrappersSeparateReadAndMutationScopes(t *testing.T) {
	failure := errors.New("native result delivery failed")
	attr := storage.Attr{ID: 3, Kind: storage.NodeRegular}
	reference := &capabilityFileProbe{failure: failure, state: storage.ReferenceState{Attr: attr}}
	probe := &capabilitySessionProbe{
		failure: failure,
		open:    storage.OpenResult{File: reference, Attr: attr, Outcome: storage.Created},
		node:    storage.NodeOpenResult{Reference: reference, Attr: attr, Outcome: storage.Opened},
		name:    storage.NameResult{Attr: &attr},
	}
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	probe.action = storage.FileActionReceipt{Action: action, Operation: storage.OpFileMutateName, Outcome: storage.FileActionCompleted}
	probe.deleted = storage.DeleteIntentStatus{ID: intent, NodeID: attr.ID, Outcome: storage.DeleteIntentCompleted}
	proof := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}}
	view := &Storage{scope: &proof}
	session := &fileSession{FileSession: probe, storage: view}
	for name, check := range map[string]func() error{
		"atomic open": session.CheckAtomicFileOpen,
		"namespace":   session.CheckNamespaceAccess,
		"directory":   session.CheckDirectoryRead,
		"node refs":   session.CheckNodeReferences,
		"actions":     session.CheckFileActions,
	} {
		if err := check(); err != nil {
			t.Fatalf("%s check=%v", name, err)
		}
	}
	openSelection := storage.ChildSelection{Name: storage.ChildName{RawLeaf: []byte("open")}, Guards: &storage.NamespaceGuards{RootID: 7}}
	opened, err := session.OpenAt(t.Context(), openSelection, storage.OpenAtOptions{})
	if !errors.Is(err, failure) || opened.File == nil || !reflect.DeepEqual(probe.observed, proof) {
		t.Fatalf("atomic open=%+v error=%v scope=%+v", opened, err, probe.observed)
	}
	if !reflect.DeepEqual(probe.selection, openSelection) {
		t.Fatalf("atomic open selection=%+v, want %+v", probe.selection, openSelection)
	}
	if _, err := session.LookupAt(locking.WithScope(t.Context(), proof), storage.ChildName{}); !errors.Is(err, failure) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("lookup error=%v scope=%+v", err, probe.observed)
	}
	if observed, err := session.ReadDirNode(locking.WithScope(t.Context(), proof), storage.DirectoryTarget{NodeID: attr.ID}); !errors.Is(err, failure) || !reflect.DeepEqual(observed, storage.ObservedDirectory{}) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("directory=%+v error=%v scope=%+v", observed, err, probe.observed)
	}
	bounded, err := storage.NewListResult(4096, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes + 64, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := session.ReadDirNodeBounded(locking.WithScope(t.Context(), proof), storage.DirectoryTarget{NodeID: attr.ID}, bounded); !errors.Is(err, failure) || !reflect.DeepEqual(observed, storage.DirectoryObservation{}) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("bounded directory=%+v error=%v scope=%+v", observed, err, probe.observed)
	}
	if entries, err := bounded.Entries(); entries != nil || !errors.Is(err, failure) {
		t.Fatalf("failed bounded directory exposed %+v, %v", entries, err)
	}
	if result, err := session.MutateName(t.Context(), storage.NameCommand{}); !errors.Is(err, failure) || result.Attr == nil || result.Attr.ID != attr.ID || !reflect.DeepEqual(probe.observed, proof) {
		t.Fatalf("name mutation=%+v error=%v scope=%+v", result, err, probe.observed)
	}
	result, err := session.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{})
	if !errors.Is(err, failure) || result.Reference == nil || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("read-only reference=%+v error=%v scope=%+v", result, err, probe.observed)
	}
	childSelection := storage.ChildSelection{Name: storage.ChildName{RawLeaf: []byte("child")}, Guards: &storage.NamespaceGuards{RootID: 9}}
	result, err = session.OpenChildRef(t.Context(), childSelection, storage.NodeRefOptions{Create: true})
	if !errors.Is(err, failure) || result.Reference == nil || !reflect.DeepEqual(probe.observed, proof) {
		t.Fatalf("creating reference=%+v error=%v scope=%+v", result, err, probe.observed)
	}
	if !reflect.DeepEqual(probe.selection, childSelection) {
		t.Fatalf("child reference selection=%+v, want %+v", probe.selection, childSelection)
	}
	if state, err := result.Reference.State(t.Context()); !errors.Is(err, failure) || state.Attr.ID != attr.ID || !reflect.DeepEqual(reference.observed, locking.MutationScope{}) {
		t.Fatalf("reference state=%+v error=%v scope=%+v", state, err, reference.observed)
	}
	if attr, err := result.Reference.Stat(locking.WithScope(t.Context(), proof)); !errors.Is(err, failure) || attr.ID != 3 || !reflect.DeepEqual(reference.observed, locking.MutationScope{}) {
		t.Fatalf("reference stat=%+v error=%v scope=%+v", attr, err, reference.observed)
	}
	if attr, err := result.Reference.SetAttr(t.Context(), storage.AttrChange{}); !errors.Is(err, failure) || attr.ID != 3 || !reflect.DeepEqual(reference.observed, proof) {
		t.Fatalf("reference setattr=%+v error=%v scope=%+v", attr, err, reference.observed)
	}
	scoped := result.Reference.(storage.ScopedReference)
	if err := scoped.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := scoped.Scope(locking.WithScope(t.Context(), proof)); !errors.Is(err, failure) || scope.Token != "scope" || !reflect.DeepEqual(reference.observed, locking.MutationScope{}) {
		t.Fatalf("reference scope=%+v error=%v scope context=%+v", scope, err, reference.observed)
	}
	stateAccess := result.Reference.(storage.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	deleteIntent := result.Reference.(storage.DeleteIntent)
	if err := deleteIntent.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	if state, err := result.Reference.(storage.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{}); !errors.Is(err, failure) || state.Attr.ID != attr.ID || !reflect.DeepEqual(reference.observed, proof) {
		t.Fatalf("pending state=%+v error=%v scope=%+v", state, err, reference.observed)
	}
	if state, err := deleteIntent.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{}); !errors.Is(err, failure) || state.Attr.ID != attr.ID || !reflect.DeepEqual(reference.observed, proof) {
		t.Fatalf("clear pending state=%+v error=%v scope=%+v", state, err, reference.observed)
	}
	conditional := result.Reference.(storage.ConditionalFileMutation)
	if err := conditional.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	if changed, err := conditional.MutateFile(t.Context(), storage.FileMutation{}); !errors.Is(err, failure) || changed.ID != attr.ID || !reflect.DeepEqual(reference.observed, proof) {
		t.Fatalf("conditional mutation=%+v error=%v scope=%+v", changed, err, reference.observed)
	}
	if receipt, err := session.QueryFileAction(locking.WithScope(t.Context(), proof), action); !errors.Is(err, failure) || receipt.Action != action || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("action receipt=%+v error=%v scope=%+v", receipt, err, probe.observed)
	}
	if status, err := session.QueryDeleteIntent(locking.WithScope(t.Context(), proof), "owner", intent); !errors.Is(err, failure) || status.ID != intent || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("delete status=%+v error=%v scope=%+v", status, err, probe.observed)
	}
	if page, err := session.ListDeleteIntents(locking.WithScope(t.Context(), proof), "owner", 0, 1); !errors.Is(err, failure) || len(page.Intents) != 1 || page.Intents[0].ID != intent || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("delete intents=%+v error=%v scope=%+v", page, err, probe.observed)
	}
	if err := session.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: action, Owner: "owner", Intent: intent}); !errors.Is(err, failure) || !reflect.DeepEqual(probe.observed, proof) {
		t.Fatalf("delete acknowledgement=%v scope=%+v", err, probe.observed)
	}
	if err := result.Reference.Close(locking.WithScope(t.Context(), proof)); !errors.Is(err, failure) || !reflect.DeepEqual(reference.observed, locking.MutationScope{}) {
		t.Fatalf("reference close=%v scope=%+v", err, reference.observed)
	}
}
