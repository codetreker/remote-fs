package limited

import (
	"context"
	"errors"
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

func (p *capabilityProbe) CheckAtomicFileOpen() error { return p.checkErr }
func (p *capabilityProbe) OpenAt(context.Context, storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error) {
	return p.open, p.callErr
}
func (p *capabilityProbe) CheckNamespaceAccess() error { return p.checkErr }
func (p *capabilityProbe) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{ID: 3, Kind: storage.NodeRegular}, p.callErr
}
func (p *capabilityProbe) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	return p.name, p.callErr
}
func (p *capabilityProbe) CheckNodeReferences() error { return p.checkErr }
func (p *capabilityProbe) OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return p.node, p.callErr
}
func (p *capabilityProbe) OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return p.node, p.callErr
}
func (p *capabilityProbe) CheckFileActions() error { return p.checkErr }
func (p *capabilityProbe) QueryFileAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
	return p.action, p.callErr
}
func (p *capabilityProbe) QueryDeleteIntent(context.Context, storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	return p.deleteStatus, p.callErr
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

type referenceProbe struct {
	storage.File
	checkErr error
	callErr  error
	metadata storage.OpaquePayload
	state    storage.ReferenceState
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
	opened, err := wrapper.OpenAt(t.Context(), storage.ChildName{}, storage.OpenAtOptions{})
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
	result, err := wrapper.MutateName(t.Context(), storage.NameCommand{})
	if !errors.Is(err, failure) || result.Attr == nil || result.Attr.ID != attr.ID {
		t.Fatalf("name result=%+v error=%v", result, err)
	}
	if receipt, err := wrapper.QueryFileAction(t.Context(), action); !errors.Is(err, failure) || receipt.Action != action {
		t.Fatalf("action receipt=%+v error=%v", receipt, err)
	}
	if status, err := wrapper.QueryDeleteIntent(t.Context(), intent); !errors.Is(err, failure) || status.ID != intent {
		t.Fatalf("delete status=%+v error=%v", status, err)
	}
	if err := wrapper.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: action, Intent: intent}); !errors.Is(err, failure) {
		t.Fatalf("acknowledge delete intent=%v", err)
	}
	state, err := reference.Reference.State(t.Context())
	if !errors.Is(err, failure) || state.Attr.ID != attr.ID || !state.PendingUnlink {
		t.Fatalf("reference state=%+v error=%v", state, err)
	}
	deleteRef := reference.Reference.(storage.DeleteIntent)
	if state, err := deleteRef.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{}); !errors.Is(err, failure) || state.Attr.ID != attr.ID {
		t.Fatalf("pending state=%+v error=%v", state, err)
	}
	file := opened.File.(storage.ConditionalFileMutation)
	if mutated, err := file.MutateFile(t.Context(), storage.FileMutation{}); !errors.Is(err, failure) || mutated.ID != attr.ID {
		t.Fatalf("conditional attr=%+v error=%v", mutated, err)
	}
}
