package limited

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type capabilityProbe struct {
	storage.FileSession
	checkErr     error
	callErr      error
	attempt      storage.RangeAttempt
	metadata     storage.OpaquePayload
	open         storage.OpenResult
	node         storage.NodeOpenResult
	name         storage.NameResult
	action       storage.FileActionReceipt
	deleteStatus storage.DeleteIntentStatus
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
	wrapper := &fileSession{FileSession: directoryReaderOnlyProbe{}, storage: &Storage{limit: MinLimit}}
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
	wrapper := &fileSession{FileSession: namespaceOnlyProbe{}, storage: &Storage{limit: MinLimit}}
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

func (p *capabilityProbe) CheckAtomicFileOpen() error { return p.checkErr }
func (p *capabilityProbe) OpenAt(context.Context, storage.ChildSelection, storage.OpenAtOptions) (storage.OpenResult, error) {
	return p.open, p.callErr
}
func (p *capabilityProbe) CheckNamespaceAccess() error { return p.checkErr }
func (p *capabilityProbe) CheckDirectoryRead() error   { return p.checkErr }
func (p *capabilityProbe) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{ID: 3, Kind: storage.NodeRegular}, p.callErr
}
func (p *capabilityProbe) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: 3, Revision: []byte{1}}}, p.callErr
}
func (p *capabilityProbe) ReadDirNodeBounded(_ context.Context, _ storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	if p.callErr == nil && result != nil {
		_ = result.Add(storage.Entry{Name: "entry", Attr: storage.Attr{ID: 4, Kind: storage.NodeRegular}})
	}
	return storage.DirectoryObservation{ParentID: 3, Revision: []byte{1}}, p.callErr
}
func (p *capabilityProbe) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	return p.name, p.callErr
}
func (p *capabilityProbe) CheckNodeReferences() error { return p.checkErr }
func (p *capabilityProbe) OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return p.node, p.callErr
}
func (p *capabilityProbe) OpenChildRef(context.Context, storage.ChildSelection, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return p.node, p.callErr
}
func (p *capabilityProbe) CheckFileActions() error { return p.checkErr }
func (p *capabilityProbe) QueryFileAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
	return p.action, p.callErr
}
func (p *capabilityProbe) QueryDeleteIntent(context.Context, storage.DeleteIntentOwner, storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	return p.deleteStatus, p.callErr
}
func (p *capabilityProbe) ListDeleteIntents(context.Context, storage.DeleteIntentOwner, storage.DeleteIntentCursor, int) (storage.DeleteIntentPage, error) {
	return storage.DeleteIntentPage{Intents: []storage.DeleteIntentStatus{p.deleteStatus}, Next: 1}, p.callErr
}
func (p *capabilityProbe) AcknowledgeDeleteIntent(context.Context, storage.AcknowledgeDeleteIntentCommand) error {
	return p.callErr
}

func (p *capabilityProbe) CheckMetadataAccess() error { return p.checkErr }
func (p *capabilityProbe) SetMetadata(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, error) {
	return p.metadata, p.callErr
}
func (p *capabilityProbe) CheckUseOwners() error { return p.checkErr }
func (p *capabilityProbe) NewUseOwner(context.Context, uint64, storage.UseScope, storage.OwnerOptions) (storage.UseOwner, error) {
	return 17, p.callErr
}
func (p *capabilityProbe) RetireUseOwner(context.Context, storage.UseOwner) error { return p.callErr }
func (p *capabilityProbe) CheckRangeControl() error                               { return p.checkErr }
func (p *capabilityProbe) GetConflict(context.Context, storage.UseOwner, storage.RangeCommand) (storage.RangeConflict, error) {
	return storage.RangeConflict{Found: true, Owner: 9}, p.callErr
}
func (p *capabilityProbe) Apply(context.Context, storage.UseOwner, []storage.RangeCommand, storage.LockRequestID) (storage.RangeAttempt, error) {
	return p.attempt, p.callErr
}
func (p *capabilityProbe) Query(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return p.attempt, p.callErr
}
func (p *capabilityProbe) Cancel(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return p.attempt, p.callErr
}
func (p *capabilityProbe) Drop(context.Context, storage.UseOwner, storage.ConflictDomain) error {
	return p.callErr
}

func TestNamespaceWrapperRejectsSubstitutedDirectoryIdentity(t *testing.T) {
	probe := &capabilityProbe{}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{limit: MinLimit}}
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

type referenceProbe struct {
	storage.File
	checkErr error
	callErr  error
	metadata storage.OpaquePayload
	state    storage.ReferenceState
}

func (p *referenceProbe) Stat(context.Context) (storage.Attr, error) {
	return p.state.Attr, p.callErr
}
func (p *referenceProbe) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return p.state.Attr, p.callErr
}
func (p *referenceProbe) Close(context.Context) error { return p.callErr }
func (p *referenceProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	return storage.ReferenceCloseResult{Released: storage.ReferenceCloseReleased(p.callErr)}, p.callErr
}

func (p *referenceProbe) CheckScopedReference() error { return p.checkErr }
func (p *referenceProbe) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "scope"}, p.callErr
}
func (p *referenceProbe) CheckMetadataAccess() error { return p.checkErr }
func (p *referenceProbe) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	return p.metadata, p.callErr
}
func (p *referenceProbe) CheckReferenceState() error { return p.checkErr }
func (p *referenceProbe) State(context.Context) (storage.ReferenceState, error) {
	return p.state, p.callErr
}
func (p *referenceProbe) CheckDeleteIntent() error { return p.checkErr }
func (p *referenceProbe) SetPendingUnlink(context.Context, storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return p.state, p.callErr
}
func (p *referenceProbe) ClearPendingUnlink(context.Context, storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return p.state, p.callErr
}
func (p *referenceProbe) CheckConditionalFileMutation() error { return p.checkErr }
func (p *referenceProbe) MutateFile(context.Context, storage.FileMutation) (storage.Attr, error) {
	return p.state.Attr, p.callErr
}

func TestCapabilityWrappersPreserveChecksAndPartialResults(t *testing.T) {
	failure := errors.New("native result delivery failed")
	request, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	probe := &capabilityProbe{
		callErr:  failure,
		attempt:  storage.RangeAttempt{Request: request, State: storage.Granted, EverGranted: true},
		metadata: storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")},
	}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{limit: MinLimit}}
	if payload, err := wrapper.SetMetadata(t.Context(), 3, "test.value", nil, []byte("value")); !errors.Is(err, failure) || string(payload.Data) != "value" {
		t.Fatalf("metadata=%+v,%v", payload, err)
	}
	if err := wrapper.CheckUseOwners(); err != nil {
		t.Fatal(err)
	}
	if owner, err := wrapper.NewUseOwner(t.Context(), 3, storage.UseScope{Token: "scope"}, storage.OwnerOptions{Lifetime: storage.OwnerExplicit}); !errors.Is(err, failure) || owner != 17 {
		t.Fatalf("owner=%d,%v", owner, err)
	}
	if err := wrapper.RetireUseOwner(t.Context(), 17); !errors.Is(err, failure) {
		t.Fatalf("retire owner=%v", err)
	}
	if conflict, err := wrapper.GetConflict(t.Context(), 17, storage.RangeCommand{}); !errors.Is(err, failure) || !conflict.Found || conflict.Owner != 9 {
		t.Fatalf("conflict=%+v,%v", conflict, err)
	}
	if attempt, err := wrapper.Apply(t.Context(), 17, nil, request); !errors.Is(err, failure) || attempt.Request != request || !attempt.EverGranted {
		t.Fatalf("attempt=%+v,%v", attempt, err)
	}
	if attempt, err := wrapper.Query(t.Context(), 17, request); !errors.Is(err, failure) || attempt.Request != request || !attempt.EverGranted {
		t.Fatalf("query=%+v,%v", attempt, err)
	}
	if attempt, err := wrapper.Cancel(t.Context(), 17, request); !errors.Is(err, failure) || attempt.Request != request || !attempt.EverGranted {
		t.Fatalf("cancel=%+v,%v", attempt, err)
	}
	if err := wrapper.Drop(t.Context(), 17, storage.DomainEnforced); !errors.Is(err, failure) {
		t.Fatalf("drop=%v", err)
	}
	reference := &referenceProbe{callErr: failure, metadata: probe.metadata}
	file := wrapFile(wrapper.storage, reference)
	if err := file.(storage.ScopedReference).CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := file.(storage.ScopedReference).Scope(t.Context()); !errors.Is(err, failure) || scope.Token != "scope" {
		t.Fatalf("scope=%+v,%v", scope, err)
	}
	if payload, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) || string(payload.Data) != "value" {
		t.Fatalf("reference metadata=%+v,%v", payload, err)
	}
	if err := file.(storage.ReferenceMetadataAccess).CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	probe.checkErr = failure
	probe.callErr = errors.New("dispatch should not occur")
	if err := wrapper.CheckRangeControl(); !errors.Is(err, failure) {
		t.Fatalf("check=%v", err)
	}
	for name, call := range map[string]func() error{
		"retire owner": func() error { return wrapper.RetireUseOwner(t.Context(), 17) },
		"query": func() error {
			_, err := wrapper.Query(t.Context(), 17, request)
			return err
		},
		"cancel": func() error {
			_, err := wrapper.Cancel(t.Context(), 17, request)
			return err
		},
		"drop": func() error { return wrapper.Drop(t.Context(), 17, storage.DomainEnforced) },
	} {
		if err := call(); !errors.Is(err, failure) {
			t.Fatalf("refused %s=%v", name, err)
		}
	}
	reference.checkErr = failure
	reference.callErr = errors.New("reference dispatch should not occur")
	if _, err := file.(storage.ScopedReference).Scope(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("refused scope=%v", err)
	}
	if _, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) {
		t.Fatalf("refused reference metadata=%v", err)
	}

	missingSession := &fileSession{FileSession: struct{ storage.FileSession }{}, storage: wrapper.storage}
	for name, check := range map[string]func() error{
		"metadata": missingSession.CheckMetadataAccess,
		"owners":   missingSession.CheckUseOwners,
		"ranges":   missingSession.CheckRangeControl,
	} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing %s check=%v", name, err)
		}
	}
	missingFile := wrapFile(wrapper.storage, struct{ storage.File }{})
	if err := missingFile.(storage.ScopedReference).CheckScopedReference(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing scope check=%v", err)
	}
	if err := missingFile.(storage.ReferenceMetadataAccess).CheckMetadataAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing reference metadata check=%v", err)
	}
}

func TestIdentityCapabilityWrappersPreservePartialResultsAndReferences(t *testing.T) {
	failure := errors.New("native result delivery failed")
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	attr := storage.Attr{ID: 3, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{
		"test.value": {Version: []byte{1}, Data: []byte("value")},
	}}
	nativeReference := &referenceProbe{callErr: failure, state: storage.ReferenceState{Attr: attr, PendingUnlink: true, PendingGeneration: []byte{2}}}
	probe := &capabilityProbe{
		callErr:      failure,
		open:         storage.OpenResult{File: nativeReference, Attr: attr, Outcome: storage.Created},
		node:         storage.NodeOpenResult{Reference: nativeReference, Attr: attr, Outcome: storage.Opened},
		name:         storage.NameResult{Attr: &attr},
		action:       storage.FileActionReceipt{Action: action, Operation: storage.OpFileMutateName, Outcome: storage.FileActionCompleted},
		deleteStatus: storage.DeleteIntentStatus{ID: intent, NodeID: attr.ID, Outcome: storage.DeleteIntentPending},
	}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{limit: MinLimit}}
	for name, check := range map[string]func() error{
		"atomic open": wrapper.CheckAtomicFileOpen,
		"namespace":   wrapper.CheckNamespaceAccess,
		"directory":   wrapper.CheckDirectoryRead,
		"node refs":   wrapper.CheckNodeReferences,
		"actions":     wrapper.CheckFileActions,
	} {
		if err := check(); err != nil {
			t.Fatalf("%s check=%v", name, err)
		}
	}
	opened, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{}}, storage.OpenAtOptions{})
	if !errors.Is(err, failure) || opened.File == nil || opened.Attr.ID != attr.ID || opened.Outcome != storage.Created {
		t.Fatalf("atomic open=%+v error=%v", opened, err)
	}
	reference, err := wrapper.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{})
	if !errors.Is(err, failure) || reference.Reference == nil || reference.Attr.ID != attr.ID {
		t.Fatalf("node reference=%+v error=%v", reference, err)
	}
	if _, ok := reference.Reference.(*nodeReference); !ok {
		t.Fatalf("node reference was not wrapped: %T", reference.Reference)
	}
	child, err := wrapper.OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{}}, storage.NodeRefOptions{})
	if !errors.Is(err, failure) || child.Reference == nil || child.Attr.ID != attr.ID {
		t.Fatalf("child reference=%+v error=%v", child, err)
	}
	if lookedUp, err := wrapper.LookupAt(t.Context(), storage.ChildName{}); !errors.Is(err, failure) || lookedUp.ID != attr.ID {
		t.Fatalf("lookup=%+v error=%v", lookedUp, err)
	}
	if observed, err := wrapper.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: attr.ID}); !errors.Is(err, failure) || !reflect.DeepEqual(observed, storage.ObservedDirectory{}) {
		t.Fatalf("directory=%+v error=%v", observed, err)
	}
	bounded, err := storage.NewListResult(4096, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes + 64, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := wrapper.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: attr.ID}, bounded); !errors.Is(err, failure) || !reflect.DeepEqual(observed, storage.DirectoryObservation{}) {
		t.Fatalf("bounded directory=%+v error=%v", observed, err)
	}
	if entries, err := bounded.Entries(); entries != nil || !errors.Is(err, failure) {
		t.Fatalf("failed bounded directory exposed %+v, %v", entries, err)
	}
	result, err := wrapper.MutateName(t.Context(), storage.NameCommand{})
	if !errors.Is(err, failure) || result.Attr == nil || result.Attr.ID != attr.ID {
		t.Fatalf("name result=%+v error=%v", result, err)
	}
	if receipt, err := wrapper.QueryFileAction(t.Context(), action); !errors.Is(err, failure) || receipt.Action != action {
		t.Fatalf("action receipt=%+v error=%v", receipt, err)
	}
	owner := storage.DeleteIntentOwner("test-owner")
	if status, err := wrapper.QueryDeleteIntent(t.Context(), owner, intent); !errors.Is(err, failure) || status.ID != intent {
		t.Fatalf("delete status=%+v error=%v", status, err)
	}
	if page, err := wrapper.ListDeleteIntents(t.Context(), owner, 0, 1); !errors.Is(err, failure) || len(page.Intents) != 1 || page.Intents[0].ID != intent {
		t.Fatalf("delete page=%+v error=%v", page, err)
	}
	if err := wrapper.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: action, Owner: owner, Intent: intent}); !errors.Is(err, failure) {
		t.Fatalf("acknowledge delete intent=%v", err)
	}
	state, err := reference.Reference.State(t.Context())
	if !errors.Is(err, failure) || state.Attr.ID != attr.ID || !state.PendingUnlink {
		t.Fatalf("reference state=%+v error=%v", state, err)
	}
	deleteRef := reference.Reference.(storage.DeleteIntent)
	if err := deleteRef.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	if state, err := deleteRef.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{}); !errors.Is(err, failure) || state.Attr.ID != attr.ID {
		t.Fatalf("pending state=%+v error=%v", state, err)
	}
	if state, err := deleteRef.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{}); !errors.Is(err, failure) || state.Attr.ID != attr.ID {
		t.Fatalf("cleared state=%+v error=%v", state, err)
	}
	file := opened.File.(storage.ConditionalFileMutation)
	if err := file.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	if mutated, err := file.MutateFile(t.Context(), storage.FileMutation{}); !errors.Is(err, failure) || mutated.ID != attr.ID {
		t.Fatalf("conditional attr=%+v error=%v", mutated, err)
	}
	wrappedReference := reference.Reference
	if changed, err := wrappedReference.SetAttr(t.Context(), storage.AttrChange{}); !errors.Is(err, failure) || changed.ID != attr.ID {
		t.Fatalf("reference setattr=%+v error=%v", changed, err)
	}
	scoped := wrappedReference.(storage.ScopedReference)
	if err := scoped.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := scoped.Scope(t.Context()); !errors.Is(err, failure) || scope.Token != "scope" {
		t.Fatalf("reference scope=%+v error=%v", scope, err)
	}
	stateAccess := wrappedReference.(storage.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	if err := wrappedReference.Close(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("reference close=%v", err)
	}
}

type maintenanceProbe struct {
	storage.BoundedStorage
	initialized int64
	outerCalls  int
}

func (*maintenanceProbe) CheckPublicationAccounting() error { return nil }
func (*maintenanceProbe) CheckMaintenanceAccounting() error { return nil }
func (p *maintenanceProbe) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	initialize(7)
	p.initialized = 7
	settle, err := storage.PreparePublication(storage.WithPublicationAccountingChain(ctx, chain), 7, 5)
	if err != nil {
		return err
	}
	return settle(storage.PublicationApplied)
}

func TestMaintenanceAccountingComposesTheAllowanceHook(t *testing.T) {
	probe := &maintenanceProbe{}
	wrapper := &Storage{backing: probe, limit: MinLimit, count: 7}
	if err := wrapper.CheckMaintenanceAccounting(); err != nil {
		t.Fatal(err)
	}
	chain := (storage.PublicationAccountingChain{}).With(func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 7 || next != 5 {
			t.Fatalf("outer accounting=%d -> %d", previous, next)
		}
		probe.outerCalls++
		return func(storage.PublicationResult) error { return nil }, nil
	})
	initialized := int64(-1)
	if err := wrapper.BindMaintenanceAccounting(t.Context(), chain, func(used int64) { initialized = used }); err != nil {
		t.Fatal(err)
	}
	if initialized != probe.initialized || probe.outerCalls != 1 {
		t.Fatalf("initialized=%d native=%d outer calls=%d", initialized, probe.initialized, probe.outerCalls)
	}
	wrapper.countMu.Lock()
	count := wrapper.count
	wrapper.countMu.Unlock()
	if count != 5 {
		t.Fatalf("limited maintenance count=%d, want 5", count)
	}
}
