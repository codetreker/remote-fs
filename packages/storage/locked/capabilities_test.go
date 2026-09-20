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
	checkErr error
	failure  error
	attempt  storage.RangeAttempt
	observed locking.MutationScope
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

type capabilityFileProbe struct {
	storage.File
	checkErr error
	failure  error
	observed locking.MutationScope
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
