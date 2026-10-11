package locked

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
	id                                 uint64
	information                        storage.ReferenceInformation
	observation                        storage.ContentMetadataObservation
	checkErr, callErr, identityErr     error
	informationCalls, observationCalls int
	index                              uint16
}

func (p *referenceObservationProbe) ReferenceNodeID() (uint64, error) { return p.id, p.identityErr }
func (p *referenceObservationProbe) CheckReferenceInformation() error { return p.checkErr }
func (p *referenceObservationProbe) CheckMetadataObservation() error  { return p.checkErr }
func (p *referenceObservationProbe) Information(context.Context) (storage.ReferenceInformation, error) {
	p.informationCalls++
	return p.information, p.callErr
}
func (p *referenceObservationProbe) ObserveMetadata(_ context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	p.observationCalls++
	p.index = index
	return p.observation, p.callErr
}
func (p *referenceObservationProbe) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func TestReferenceInformationKeepsIdentityAndHidesMetadata(t *testing.T) {
	want := storage.ReferenceInformation{NodeID: 19, Kind: storage.NodeRegular, Size: 7, AllocationKnown: true, AllocationSize: 4096, Detached: true, PendingUnlink: true}
	probe := &referenceObservationProbe{id: 19, information: want}
	file := (&Storage{}).wrapFile(probe).(*file)
	if err := file.CheckReferenceInformation(); err != nil {
		t.Fatal(err)
	}

	got, err := file.Information(t.Context())
	if err != nil || got != want || probe.informationCalls != 1 {
		t.Fatalf("information=%+v %v, calls=%d", got, err, probe.informationCalls)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
	probe.information.Size = 8
	got, err = file.Information(t.Context())
	if err != nil || got.Size != 8 {
		t.Fatalf("fresh information=%+v %v", got, err)
	}
}

func TestReferenceObservationsReturnNoFactsOnError(t *testing.T) {
	failure := errors.New("authority unavailable")
	for _, scenario := range []string{"check", "identity", "zero identity", "substitution", "negative size", "unknown allocation", "capture", "bound", "malformed", "index", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &referenceObservationProbe{id: 19, information: storage.ReferenceInformation{NodeID: 19, Kind: storage.NodeRegular}, observation: storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}}
			file := (&Storage{}).wrapFile(probe).(*file)
			file.metadataObservations = []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 1}}
			ctx := t.Context()
			index := uint16(0)
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
				probe.information.NodeID = 20
				probe.observation.NodeID = 20
				want = syscall.EIO
			case "negative size":
				probe.information.Size = -1
				want = syscall.EIO
			case "unknown allocation":
				probe.information.AllocationSize = 1
				want = syscall.EIO
			case "capture":
				probe.callErr = failure
			case "bound":
				probe.observation.Value.Data = []byte{1, 2}
				want = syscall.EFBIG
			case "malformed":
				probe.observation.Value.Version = nil
				want = syscall.EIO
			case "index":
				index = 1
				want = syscall.EINVAL
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			if scenario != "bound" && scenario != "malformed" && scenario != "index" {
				got, err := file.Information(ctx)
				if got != (storage.ReferenceInformation{}) || !errors.Is(err, want) {
					t.Fatalf("information=%+v %v want %v", got, err, want)
				}
			}
			if scenario != "negative size" && scenario != "unknown allocation" {
				got, err := file.ObserveMetadata(ctx, index)
				if !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) || !errors.Is(err, want) {
					t.Fatalf("metadata=%+v %v want %v", got, err, want)
				}
			}
		})
	}
}

func TestMetadataObservationOwnsBoundedValueAndConfirmsAbsence(t *testing.T) {
	probe := &referenceObservationProbe{id: 19, observation: storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}}
	file := (&Storage{}).wrapFile(probe).(*file)
	file.metadataObservations = []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 1}}
	if err := file.CheckMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	got, err := file.ObserveMetadata(t.Context(), 0)
	if err != nil || probe.index != 0 || probe.observationCalls != 1 {
		t.Fatalf("observation=%+v %v", got, err)
	}
	got.Value.Version[0] = 7
	got.Value.Data[0] = 7
	if probe.observation.Value.Version[0] != 1 || probe.observation.Value.Data[0] != 2 {
		t.Fatal("authority bytes aliased")
	}
	probe.observation.Value = nil
	got, err = file.ObserveMetadata(t.Context(), 0)
	if err != nil || got.NodeID != 19 || got.Value != nil {
		t.Fatalf("absence=%+v %v", got, err)
	}
	probe.observation.Value = &storage.OpaquePayload{Version: []byte{1}}
	got, err = file.ObserveMetadata(t.Context(), 0)
	if err != nil || got.Value == nil {
		t.Fatalf("present empty=%+v %v", got, err)
	}
}

type observationSessionProbe struct {
	*identitySessionProbe
	node                storage.NodeReference
	nodeOptions         storage.NodeRefOptions
	checkObservationErr error
}

func (p *observationSessionProbe) CheckOpenMetadataObservation() error { return p.checkObservationErr }
func (p *observationSessionProbe) CheckNodeReferences() error          { return nil }
func (p *observationSessionProbe) OpenNodeRef(_ context.Context, _ uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.nodeOptions = options
	p.openCalls++
	return storage.NodeOpenResult{Reference: p.node}, nil
}
func (p *observationSessionProbe) OpenChildRef(ctx context.Context, _ storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return p.OpenNodeRef(ctx, 19, options)
}

func TestMetadataObservationEnrollmentAcrossAllReferenceOpenPaths(t *testing.T) {
	for _, path := range []string{"file", "node", "child"} {
		t.Run(path, func(t *testing.T) {
			probe := &observationSessionProbe{identitySessionProbe: &identitySessionProbe{opened: storage.OpenResult{File: &referenceObservationProbe{id: 19}}}, node: &storageNodeObservationProbe{}}
			wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
			scopes := []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 1}}
			open := func() (any, error) {
				switch path {
				case "file":
					r, e := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes})
					return r.File, e
				case "node":
					r, e := wrapper.OpenNodeRef(t.Context(), 19, storage.NodeRefOptions{MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes})
					return r.Reference, e
				default:
					r, e := wrapper.OpenChildRef(t.Context(), storage.ChildSelection{}, storage.NodeRefOptions{MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes})
					return r.Reference, e
				}
			}
			resource, err := open()
			if err != nil {
				t.Fatal(err)
			}
			scopes[0].Namespace = "app.changed"
			scopes[0].MaxPayloadBytes = 8
			var kept []storage.MetadataObservationScope
			switch r := resource.(type) {
			case *file:
				kept = r.metadataObservations
			case *nodeReference:
				kept = r.metadataObservations
			}
			if len(kept) != 1 || kept[0].Namespace != "app.flags" || kept[0].MaxPayloadBytes != 1 {
				t.Fatalf("retained scope=%+v", kept)
			}
			if path == "file" {
				probe.options.MetadataObservations[0].Namespace = "app.backend"
			} else {
				probe.nodeOptions.MetadataObservations[0].Namespace = "app.backend"
			}
			if kept[0].Namespace != "app.flags" {
				t.Fatal("backend options aliased retained enrollment")
			}
			scopes = []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 1}}
			failure := errors.New("enrollment unsupported")
			probe.checkObservationErr = failure
			if _, err := open(); !errors.Is(err, failure) || probe.openCalls != 1 {
				t.Fatalf("preflight=%v calls=%d", err, probe.openCalls)
			}
			if err := wrapper.checkMetadataObservationEnrollment(0, scopes); !errors.Is(err, syscall.EINVAL) {
				t.Fatal(err)
			}
			scopes[0].MaxPayloadBytes = -1
			if err := wrapper.checkMetadataObservationEnrollment(storage.WriteMetadata, scopes); !errors.Is(err, syscall.EINVAL) {
				t.Fatal(err)
			}
		})
	}
}

type storageNodeObservationProbe struct{ storage.NodeReference }

func TestReferenceObservationRejectsMissingBackingCapabilities(t *testing.T) {
	file := (&Storage{}).wrapFile(&struct{ storage.File }{}).(*file)
	file.metadataObservations = []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 1}}
	for _, check := range []func() error{file.CheckReferenceInformation, file.CheckMetadataObservation} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
	if info, err := file.Information(t.Context()); info != (storage.ReferenceInformation{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("information=%+v %v", info, err)
	}
	if observed, err := file.ObserveMetadata(t.Context(), 0); !reflect.DeepEqual(observed, storage.ContentMetadataObservation{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("observation=%+v %v", observed, err)
	}
	session := &fileSession{FileSession: &struct{ storage.FileSession }{}, storage: &Storage{}}
	for _, check := range []func() error{session.CheckOpenMetadataObservation, session.CheckVolumeGeometry} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
}
