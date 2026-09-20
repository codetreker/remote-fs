package limited

import (
	"context"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type capabilityProbe struct {
	storage.FileSession
	checkErr error
	callErr  error
	attempt  storage.RangeAttempt
	metadata storage.OpaquePayload
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
}

func (p *referenceProbe) CheckScopedReference() error { return p.checkErr }
func (p *referenceProbe) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "scope"}, p.callErr
}
func (p *referenceProbe) CheckMetadataAccess() error { return p.checkErr }
func (p *referenceProbe) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	return p.metadata, p.callErr
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
	if owner, err := wrapper.NewUseOwner(t.Context(), 3, storage.UseScope{Token: "scope"}, storage.OwnerOptions{Lifetime: storage.OwnerExplicit}); !errors.Is(err, failure) || owner != 17 {
		t.Fatalf("owner=%d,%v", owner, err)
	}
	if conflict, err := wrapper.GetConflict(t.Context(), 17, storage.RangeCommand{}); !errors.Is(err, failure) || !conflict.Found || conflict.Owner != 9 {
		t.Fatalf("conflict=%+v,%v", conflict, err)
	}
	if attempt, err := wrapper.Apply(t.Context(), 17, nil, request); !errors.Is(err, failure) || attempt.Request != request || !attempt.EverGranted {
		t.Fatalf("attempt=%+v,%v", attempt, err)
	}
	reference := &referenceProbe{callErr: failure, metadata: probe.metadata}
	file := wrapFile(wrapper.storage, reference)
	if scope, err := file.(storage.ScopedReference).Scope(t.Context()); !errors.Is(err, failure) || scope.Token != "scope" {
		t.Fatalf("scope=%+v,%v", scope, err)
	}
	if payload, err := file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) || string(payload.Data) != "value" {
		t.Fatalf("reference metadata=%+v,%v", payload, err)
	}
	probe.checkErr = failure
	if err := wrapper.CheckRangeControl(); !errors.Is(err, failure) {
		t.Fatalf("check=%v", err)
	}
}
