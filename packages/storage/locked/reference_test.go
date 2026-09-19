package locked

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"reflect"
	"syscall"
	"testing"
	"time"
)

type referenceProbe struct{ *capabilityFile }

func (r *referenceProbe) CheckScopedReference() error         { return r.probe.checkErr }
func (r *referenceProbe) CheckReferenceState() error          { return r.probe.checkErr }
func (r *referenceProbe) CheckMetadataAccess() error          { return r.probe.checkErr }
func (r *referenceProbe) CheckRangeControl() error            { return r.probe.checkErr }
func (r *referenceProbe) CheckDeleteIntent() error            { return r.probe.checkErr }
func (r *referenceProbe) CheckConditionalFileMutation() error { return r.probe.checkErr }

func (r *referenceProbe) Scope(ctx context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "opaque scope"}, r.probe.record(ctx)
}
func (r *referenceProbe) State(ctx context.Context) (storage.ReferenceState, error) {
	return r.state(), r.probe.record(ctx)
}
func (r *referenceProbe) state() storage.ReferenceState {
	return storage.ReferenceState{Attr: r.probe.attr, Detached: true, PendingUnlink: true, PendingGeneration: []byte("pending"), LinkTarget: []byte{0xff, '/'}}
}
func (r *referenceProbe) SetMetadata(ctx context.Context, _ string, _, _ []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{Version: []byte("new"), Data: []byte("payload")}, r.probe.record(ctx)
}
func (r *referenceProbe) GetConflict(ctx context.Context, _ storage.UseOwner, _ storage.RangeCommand) (storage.RangeConflict, error) {
	return storage.RangeConflict{Found: true, Owner: 17, Range: storage.Range{Kind: storage.Boundary, CutAt: 3}, Mode: storage.RangeShared}, r.probe.record(ctx)
}
func (r *referenceProbe) attempt(request storage.LockRequestID) storage.RangeAttempt {
	return storage.RangeAttempt{Request: request, State: storage.Granted, EverGranted: true, HistoryRemaining: time.Minute, Effects: []storage.RangeEffect{{Claim: "original claim", Released: true}}}
}
func (r *referenceProbe) Apply(ctx context.Context, _ storage.UseOwner, _ []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return r.attempt(request), r.probe.record(ctx)
}
func (r *referenceProbe) Query(ctx context.Context, _ storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return r.attempt(request), r.probe.record(ctx)
}
func (r *referenceProbe) Cancel(ctx context.Context, _ storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return r.attempt(request), r.probe.record(ctx)
}

func (r *referenceProbe) Drop(ctx context.Context, _ storage.UseOwner, _ storage.ConflictDomain) error {
	return r.probe.record(ctx)
}
func (r *referenceProbe) SetPendingUnlink(ctx context.Context, _ storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.state(), r.probe.record(ctx)
}
func (r *referenceProbe) ClearPendingUnlink(ctx context.Context, _ storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.state(), r.probe.record(ctx)
}
func (r *referenceProbe) MutateFile(ctx context.Context, _ storage.FileMutation) (storage.Attr, error) {
	return r.probe.attr, r.probe.record(ctx)
}

func referenceCalls(ctx context.Context, r *referenceCapabilities) []func() error {
	return []func() error{
		func() error { _, err := r.Scope(ctx); return err },
		func() error { _, err := r.State(ctx); return err },
		func() error { _, err := r.SetMetadata(ctx, "key", nil, nil); return err },
		func() error { _, err := r.GetConflict(ctx, 1, storage.RangeCommand{}); return err },
		func() error { _, err := r.Apply(ctx, 1, nil, ""); return err },
		func() error { _, err := r.Query(ctx, 1, ""); return err },
		func() error { _, err := r.Cancel(ctx, 1, ""); return err },
		func() error { return r.Drop(ctx, 1, storage.DomainRecord) },
		func() error { _, err := r.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{}); return err },
		func() error { _, err := r.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{}); return err },
		func() error { _, err := r.MutateFile(ctx, storage.FileMutation{}); return err },
	}
}
func TestOptionalReferenceCapabilitiesRefuseMissingAndDeclinedSupport(t *testing.T) {
	probe, session, _, ctx := probeSession()
	raw := &referenceProbe{capabilityFile: probe.file}
	for _, missing := range []bool{true, false} {
		r := &referenceCapabilities{backend: raw, storage: session.storage}
		cause := error(syscall.EOPNOTSUPP)
		if missing {
			r.backend = &struct{ storage.NodeReference }{}
		} else {
			cause = errors.New("reference capability refused")
			probe.checkErr = cause
		}
		for _, check := range []func() error{r.CheckScopedReference, r.CheckReferenceState, r.CheckMetadataAccess, r.CheckRangeControl, r.CheckDeleteIntent, r.CheckConditionalFileMutation} {
			if err := check(); err != cause {
				t.Fatalf("check = %v, want %v", err, cause)
			}
		}
		for _, call := range referenceCalls(ctx, r) {
			if err := call(); err != cause {
				t.Fatalf("unsupported reference call = %v, want %v", err, cause)
			}
		}
	}
	if probe.calls != 0 {
		t.Fatalf("refused reference dispatched %d operations", probe.calls)
	}
}

func TestReferenceCapabilitiesPreserveStrongScopesAndKnownEffects(t *testing.T) {
	probe, session, scope, ctx := probeSession()
	raw := &referenceProbe{capabilityFile: probe.file}
	id := storage.LockRequestID("original action")
	for _, node := range []bool{false, true} {
		var r *referenceCapabilities
		if node {
			wrapped := session.storage.wrapReference(raw)
			if _, ok := wrapped.(storage.File); ok {
				t.Fatal("metadata reference acquired byte I/O")
			}
			r = &wrapped.(*nodeReference).referenceCapabilities
		} else {
			r = &session.storage.wrapFile(raw).(*file).referenceCapabilities
		}
		for _, check := range []func() error{r.CheckScopedReference, r.CheckReferenceState, r.CheckMetadataAccess, r.CheckRangeControl, r.CheckDeleteIntent, r.CheckConditionalFileMutation} {
			if err := check(); err != nil {
				t.Fatal(err)
			}
		}
		checkAttempt := func(result storage.RangeAttempt, err error) error {
			t.Helper()
			if !reflect.DeepEqual(result, raw.attempt(id)) {
				t.Fatalf("lost range effects: %+v", result)
			}
			return err
		}
		checkState := func(result storage.ReferenceState, err error) error {
			t.Helper()
			if !reflect.DeepEqual(result, raw.state()) {
				t.Fatalf("lost reference state: %+v", result)
			}
			return err
		}
		for _, test := range []struct {
			name     string
			mutation bool
			call     func() error
		}{
			{"scope", false, func() error {
				value, err := r.Scope(ctx)
				if value.Token != "opaque scope" {
					t.Fatalf("scope token = %q", value.Token)
				}
				return err
			}},
			{"state", false, func() error { return checkState(r.State(ctx)) }},
			{"metadata", true, func() error {
				value, err := r.SetMetadata(ctx, "key", nil, nil)
				if string(value.Version) != "new" || string(value.Data) != "payload" {
					t.Fatalf("metadata = %+v", value)
				}
				return err
			}},
			{"conflict", false, func() error {
				value, err := r.GetConflict(ctx, 1, storage.RangeCommand{})
				if !value.Found || value.Owner != 17 || value.Range.Kind != storage.Boundary || value.Range.CutAt != 3 {
					t.Fatalf("conflict = %+v", value)
				}
				return err
			}},
			{"apply", false, func() error { return checkAttempt(r.Apply(ctx, 1, nil, id)) }},
			{"query", false, func() error { return checkAttempt(r.Query(ctx, 1, id)) }},
			{"cancel", false, func() error { return checkAttempt(r.Cancel(ctx, 1, id)) }},
			{"drop", false, func() error { return r.Drop(ctx, 1, storage.DomainWholeFile) }},
			{"pending", true, func() error { return checkState(r.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{})) }},
			{"clear pending", true, func() error { return checkState(r.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{})) }},
			{"conditional", true, func() error {
				value, err := r.MutateFile(ctx, storage.FileMutation{})
				if !reflect.DeepEqual(value, probe.attr) {
					t.Fatalf("mutation attr = %+v", value)
				}
				return err
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				if err := test.call(); err != probe.callErr {
					t.Fatalf("failure = %v", err)
				}
				want := locking.MutationScope{}
				if test.mutation {
					want = scope
				}
				if !reflect.DeepEqual(probe.got, want) || probe.trace != "request trace" {
					t.Fatalf("reference context = %+v, %v", probe.got, probe.trace)
				}
			})
		}
		if _, err := r.MutateFile(locking.WithScope(context.Background(), locking.MutationScope{}), storage.FileMutation{}); err != probe.callErr || !reflect.DeepEqual(probe.got, locking.MutationScope{}) {
			t.Fatalf("explicit anonymous mutation inherited proof: %+v, %v", probe.got, err)
		}
	}
}
