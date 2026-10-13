package limited

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"reflect"
	"syscall"
	"testing"
)

type referenceObservationProbe struct {
	storage.File
	id                             uint64
	state                          storage.ReferenceState
	observation                    storage.MetadataObservation
	checkErr, callErr, identityErr error
	stateCalls, observationCalls   int
	projection                     storage.StateProjection
	request                        storage.MetadataObservationRequest
}

func (p *referenceObservationProbe) ReferenceNodeID() (uint64, error) { return p.id, p.identityErr }
func (p *referenceObservationProbe) CheckReferenceState() error       { return p.checkErr }
func (p *referenceObservationProbe) CheckMetadataAccess() error       { return p.checkErr }
func (p *referenceObservationProbe) State(_ context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	p.stateCalls++
	p.projection = projection
	return p.state, p.callErr
}
func (p *referenceObservationProbe) ObserveMetadata(_ context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	p.observationCalls++
	p.request = request
	return p.observation, p.callErr
}
func (p *referenceObservationProbe) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{}, syscall.EBADF
}
func (p *referenceObservationProbe) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func namespaceRequest() storage.MetadataObservationRequest {
	return storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "app.flags", MaxPayloadBytes: 1}
}

func TestReferenceStateKeepsIdentityAndHidesMetadata(t *testing.T) {
	want := storage.ReferenceState{Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular, Size: 7, AllocationKnown: true, AllocationSize: 4096}, Detached: true, PendingUnlink: true, PendingGeneration: []byte{1}}
	probe := &referenceObservationProbe{id: 19, state: want}
	file := wrapFile(&Storage{limit: MinLimit}, probe).(*file)
	if err := file.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	want.Attr.AllocationKnown = false
	want.Attr.AllocationSize = 0
	got, err := file.State(t.Context(), storage.StateScalars)
	if err != nil || !reflect.DeepEqual(got, want) || probe.stateCalls != 1 || probe.projection != storage.StateScalars {
		t.Fatalf("state=%+v %v calls=%d projection=%d", got, err, probe.stateCalls, probe.projection)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
	probe.state.Attr.Size = 8
	got, err = file.State(t.Context(), storage.StateScalars)
	if err != nil || got.Attr.Size != 8 {
		t.Fatalf("fresh state=%+v %v", got, err)
	}
}

func TestReferenceObservationsReturnNoFactsOnError(t *testing.T) {
	failure := errors.New("authority unavailable")
	for _, scenario := range []string{"check", "identity", "zero identity", "substitution", "negative size", "unknown allocation", "capture", "bound", "malformed", "invalid request", "invalid projection", "canceled", "scalar metadata"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &referenceObservationProbe{id: 19, state: storage.ReferenceState{Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}}, observation: storage.MetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}}
			file := wrapFile(&Storage{limit: MinLimit}, probe).(*file)
			ctx := t.Context()
			request := namespaceRequest()
			projection := storage.StateScalars
			want := failure
			switch scenario {
			case "check":
				probe.checkErr = failure
			case "identity":
				probe.identityErr = failure
			case "zero identity":
				probe.id = 0
				want = syscall.EIO
			case "substitution":
				probe.state.Attr.ID = 20
				probe.observation.NodeID = 20
				want = syscall.EIO
			case "negative size":
				probe.state.Attr.Size = -1
				want = syscall.EIO
			case "unknown allocation":
				probe.state.Attr.AllocationSize = 1
				want = syscall.EIO
			case "capture":
				probe.callErr = failure
			case "bound":
				probe.observation.Value.Data = []byte{1, 2}
				want = syscall.EFBIG
			case "malformed":
				probe.observation.Value.Version = nil
				want = syscall.EIO
			case "invalid request":
				request.Purpose = 0
				want = syscall.EINVAL
			case "invalid projection":
				projection = 0
				want = syscall.EINVAL
			case "scalar metadata":
				probe.state.Attr.Metadata = map[string]storage.OpaquePayload{"app.flags": {Version: []byte{1}}}
				want = syscall.EIO
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			if scenario != "bound" && scenario != "malformed" && scenario != "invalid request" {
				got, err := file.State(ctx, projection)
				if !reflect.DeepEqual(got, storage.ReferenceState{}) || !errors.Is(err, want) {
					t.Fatalf("state=%+v %v want %v", got, err, want)
				}
			}
			if scenario != "negative size" && scenario != "unknown allocation" && scenario != "scalar metadata" && scenario != "invalid projection" {
				got, err := file.ObserveMetadata(ctx, request)
				if !reflect.DeepEqual(got, storage.MetadataObservation{}) || !errors.Is(err, want) {
					t.Fatalf("metadata=%+v %v want %v", got, err, want)
				}
			}
		})
	}
}

func TestMetadataObservationOwnsBoundedValueAndConfirmsAbsence(t *testing.T) {
	probe := &referenceObservationProbe{id: 19, observation: storage.MetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}}
	file := wrapFile(&Storage{limit: MinLimit}, probe).(*file)
	if err := file.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	request := namespaceRequest()
	got, err := file.ObserveMetadata(t.Context(), request)
	if err != nil || probe.request != request || probe.observationCalls != 1 {
		t.Fatalf("observation=%+v %v request=%+v", got, err, probe.request)
	}
	got.Value.Version[0] = 7
	got.Value.Data[0] = 7
	if probe.observation.Value.Version[0] != 1 || probe.observation.Value.Data[0] != 2 {
		t.Fatal("authority bytes aliased")
	}
	probe.observation.Value = nil
	got, err = file.ObserveMetadata(t.Context(), request)
	if err != nil || got.NodeID != 19 || got.Value != nil {
		t.Fatalf("absence=%+v %v", got, err)
	}
	probe.observation.Value = &storage.OpaquePayload{Version: []byte{1}}
	got, err = file.ObserveMetadata(t.Context(), request)
	if err != nil || got.Value == nil {
		t.Fatalf("present empty=%+v %v", got, err)
	}
}
func TestReferenceObservationRejectsMissingBackingCapabilities(t *testing.T) {
	file := wrapFile(&Storage{limit: MinLimit}, &struct{ storage.File }{}).(*file)
	for _, check := range []func() error{file.CheckReferenceState, file.CheckMetadataAccess} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
	if state, err := file.State(t.Context(), storage.StateScalars); !reflect.DeepEqual(state, storage.ReferenceState{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("state=%+v %v", state, err)
	}
	if observed, err := file.ObserveMetadata(t.Context(), namespaceRequest()); !reflect.DeepEqual(observed, storage.MetadataObservation{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("observation=%+v %v", observed, err)
	}
	session := &fileSession{FileSession: &struct{ storage.FileSession }{}, storage: &Storage{}}
	if err := session.CheckAllocationReporting(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
}

func TestReferenceObservationRejectsUncertainAllowance(t *testing.T) {
	failure := errors.New("accounting uncertain")
	probe := &referenceObservationProbe{id: 19, state: storage.ReferenceState{Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}}}
	file := wrapFile(&Storage{limit: MinLimit, fault: failure}, probe).(*file)
	if err := file.CheckMetadataAccess(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if state, err := file.State(t.Context(), storage.StateScalars); !reflect.DeepEqual(state, storage.ReferenceState{}) || !errors.Is(err, failure) {
		t.Fatalf("state=%+v %v", state, err)
	}
	if observed, err := file.ObserveMetadata(t.Context(), namespaceRequest()); !reflect.DeepEqual(observed, storage.MetadataObservation{}) || !errors.Is(err, failure) {
		t.Fatalf("observation=%+v %v", observed, err)
	}
	if probe.observationCalls != 0 || probe.stateCalls != 0 {
		t.Fatal("faulty allowance dispatched observation")
	}
}
