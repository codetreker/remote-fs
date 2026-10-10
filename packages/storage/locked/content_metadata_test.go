package locked

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type contentMetadataProbe struct {
	storage.File
	id                             uint64
	identityErr, checkErr, callErr error
	observed                       storage.ContentMetadataObservation
	calls                          int
	index                          uint16
}

func (p *contentMetadataProbe) ReferenceNodeID() (uint64, error) { return p.id, p.identityErr }
func (p *contentMetadataProbe) CheckContentMetadata() error      { return p.checkErr }
func (p *contentMetadataProbe) ObserveContentMetadata(_ context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	p.calls++
	p.index = index
	return p.observed, p.callErr
}
func (p *contentMetadataProbe) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func contentWrapper(native storage.File) storage.File {
	return (&Storage{}).wrapFile(native)
}

func TestContentMetadataObservesExactByteOnlyReferenceThroughCompleteChain(t *testing.T) {
	want := storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{'M', 0xff, 0}}}
	probe := &contentMetadataProbe{id: 19, observed: want}
	wrapper := contentWrapper(contentWrapper(probe))
	observer := wrapper.(storage.ReferenceContentMetadata)
	if err := observer.CheckContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("public metadata access = %v", err)
	}
	got, err := observer.ObserveContentMetadata(t.Context(), 3)
	if err != nil || !reflect.DeepEqual(got, want) || probe.index != 3 || probe.calls != 1 {
		t.Fatalf("observation=%+v, %v; calls=%d index=%d", got, err, probe.calls, probe.index)
	}
	got.Value.Version[0] = 9
	got.Value.Data[0] = 'X'
	if probe.observed.Value.Version[0] != 1 || probe.observed.Value.Data[0] != 'M' {
		t.Fatal("observation aliases authority payload")
	}
	probe.observed = storage.ContentMetadataObservation{NodeID: 19}
	if got, err := observer.ObserveContentMetadata(t.Context(), 3); err != nil || got.Value != nil || got.NodeID != 19 {
		t.Fatalf("confirmed absence=%+v, %v", got, err)
	}
	probe.observed = storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}}}
	if got, err := observer.ObserveContentMetadata(t.Context(), 3); err != nil || got.Value == nil {
		t.Fatalf("present empty value became absent: %+v, %v", got, err)
	}
}

func TestContentMetadataRejectsCapabilityIdentityAndCaptureFailures(t *testing.T) {
	cause := errors.New("content metadata unavailable")
	for _, scenario := range []string{"missing capability", "missing identity", "check failure", "identity failure", "zero identity", "wrong identity", "malformed", "capture failure"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &contentMetadataProbe{id: 19, observed: storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{1}}}}
			var native storage.File = probe
			switch scenario {
			case "missing capability":
				native = &struct{ storage.File }{}
			case "missing identity":
				native = &struct {
					storage.File
					storage.ReferenceContentMetadata
				}{ReferenceContentMetadata: probe}
			case "check failure":
				probe.checkErr = cause
			case "identity failure":
				probe.identityErr = cause
			case "zero identity":
				probe.id = 0
			case "wrong identity":
				probe.observed.NodeID = 20
			case "malformed":
				probe.observed.Value.Version = nil
			case "capture failure":
				probe.callErr = cause
			}
			observer := contentWrapper(contentWrapper(native)).(storage.ReferenceContentMetadata)
			got, err := observer.ObserveContentMetadata(t.Context(), 0)
			if err == nil || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
				t.Fatalf("invalid observation=%+v, %v", got, err)
			}
			preflight := scenario == "missing capability" || scenario == "missing identity" || scenario == "check failure" || scenario == "identity failure" || scenario == "zero identity"
			if preflight && probe.calls != 0 {
				t.Fatalf("failed preflight dispatched %d calls", probe.calls)
			}
			if preflight && observer.CheckContentMetadata() == nil {
				t.Fatal("whole-chain checker accepted unsupported reference")
			}
			if (scenario == "check failure" || scenario == "identity failure" || scenario == "capture failure") && !errors.Is(err, cause) {
				t.Fatalf("lost failure cause: %v", err)
			}
		})
	}
}

type contentEnrollmentSessionProbe struct {
	*identitySessionProbe
	enrollmentErr error
}

func (p *contentEnrollmentSessionProbe) CheckOpenContentMetadata() error { return p.enrollmentErr }

func contentEnrollmentEffects() []storage.ContentMetadataEffect {
	return []storage.ContentMetadataEffect{{Namespace: "app.flags", PayloadBytes: 3, PresentPrefix: []byte{'M'}, AbsentPayload: []byte{'M', 0, 0}, ClearMask: []byte{0, 1, 0}, SetMask: []byte{0, 2, 0}}}
}

func TestOpenAtPreflightsAndOwnsContentEnrollment(t *testing.T) {
	probe := &contentEnrollmentSessionProbe{identitySessionProbe: &identitySessionProbe{opened: storage.OpenResult{File: &openIdentityFileProbe{id: 19}, Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}, Outcome: storage.Opened}}}
	wrapper := &fileSession{FileSession: &fileSession{FileSession: probe, storage: &Storage{}}, storage: &Storage{}}
	options := storage.OpenAtOptions{Write: true, Existing: storage.Keep, ContentMetadataEffects: contentEnrollmentEffects()}
	if err := wrapper.CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); err != nil {
		t.Fatal(err)
	}
	if probe.options.MetadataAccess != 0 || !probe.options.Write || !reflect.DeepEqual(probe.options.ContentMetadataEffects, options.ContentMetadataEffects) {
		t.Fatalf("enrollment changed public rights or descriptor: %+v", probe.options)
	}
	options.ContentMetadataEffects[0].Namespace = "app.other"
	options.ContentMetadataEffects[0].PresentPrefix[0] = 'X'
	options.ContentMetadataEffects[0].AbsentPayload[0] = 'X'
	options.ContentMetadataEffects[0].ClearMask[1] = 0xff
	options.ContentMetadataEffects[0].SetMask[1] = 0xff
	if !reflect.DeepEqual(probe.options.ContentMetadataEffects, contentEnrollmentEffects()) {
		t.Fatal("open retained caller descriptor aliases")
	}
	failure := errors.New("enrollment cannot be enforced")
	probe.enrollmentErr = failure
	options.ContentMetadataEffects = contentEnrollmentEffects()
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, failure) || probe.openCalls != 1 {
		t.Fatalf("failed enrollment=%+v, %v; calls=%d", result, err, probe.openCalls)
	}
	wrapper.FileSession = &fileSession{FileSession: probe.identitySessionProbe, storage: &Storage{}}
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) || probe.openCalls != 1 {
		t.Fatalf("missing enrollment=%+v, %v; calls=%d", result, err, probe.openCalls)
	}
	options.ContentMetadataEffects = nil
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); err != nil || probe.openCalls != 2 {
		t.Fatalf("unenrolled byte-only open=%v; calls=%d", err, probe.openCalls)
	}
	options.ContentMetadataEffects = contentEnrollmentEffects()
	options.ContentMetadataEffects[0].SetMask = nil
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !errors.Is(err, syscall.EINVAL) || probe.openCalls != 2 {
		t.Fatalf("malformed descriptor=%v; calls=%d", err, probe.openCalls)
	}
}
