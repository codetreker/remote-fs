package locked

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type capabilitySessionProbe struct {
	storage.FileSession
	failure  error
	attempt  storage.RangeAttempt
	observed locking.MutationScope
}

func (p *capabilitySessionProbe) capture(ctx context.Context) {
	p.observed = locking.ScopeFromContext(ctx)
}
func (*capabilitySessionProbe) CheckMetadataAccess() error { return nil }
func (p *capabilitySessionProbe) SetMetadata(ctx context.Context, _ uint64, _ string, _, _ []byte) (storage.OpaquePayload, error) {
	p.capture(ctx)
	return storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")}, p.failure
}
func (*capabilitySessionProbe) CheckUseOwners() error { return nil }
func (p *capabilitySessionProbe) NewUseOwner(ctx context.Context, _ uint64, _ storage.UseScope, _ storage.OwnerOptions) (storage.UseOwner, error) {
	p.capture(ctx)
	return 7, p.failure
}
func (p *capabilitySessionProbe) RetireUseOwner(ctx context.Context, _ storage.UseOwner) error {
	p.capture(ctx)
	return p.failure
}
func (*capabilitySessionProbe) CheckRangeControl() error { return nil }
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

func TestSessionCapabilitiesSeparateMutationProofsAndPreserveResults(t *testing.T) {
	failure := errors.New("native response failed")
	probe := &capabilitySessionProbe{failure: failure, attempt: storage.RangeAttempt{State: storage.Granted, EverGranted: true}}
	proof := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}}
	view := &Storage{scope: &proof}
	session := &fileSession{FileSession: probe, storage: view}
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
}
