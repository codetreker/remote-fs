package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPVolumeCreationGetterUsesExplicitCachedFact(t *testing.T) {
	for _, created := range []time.Time{time.Time{}, time.Unix(12345, 678).UTC()} {
		instant := TimeOf(created)
		session := &remoteFileSession{capabilities: fileCapabilities{SessionIdentity: true, VolumeCreatedAt: &instant}}
		if got, err := session.VolumeCreatedAt(t.Context()); err != nil || !got.Equal(created) {
			t.Fatalf("cached creation=%v error=%v", got, err)
		}
	}
	for _, errno := range []syscall.Errno{syscall.EOPNOTSUPP, syscall.EIO, syscall.ENOSPC} {
		session := &remoteFileSession{capabilities: fileCapabilities{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: storage.ErrnoNameOf(errno), Message: "creation capture failed"}}}
		if got, err := session.VolumeCreatedAt(t.Context()); !errors.Is(err, errno) || !got.IsZero() {
			t.Fatalf("failed creation=%v error=%v", got, err)
		}
		if err := session.CheckFileSessionIdentity(); err != nil {
			t.Fatalf("creation failure weakened identity: %v", err)
		}
	}
	if got, err := (&remoteFileSession{}).VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || !got.IsZero() {
		t.Fatalf("missing identity creation=%v error=%v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := (&remoteFileSession{}).VolumeCreatedAt(ctx); !errors.Is(err, context.Canceled) || !got.IsZero() {
		t.Fatalf("cancelled creation=%v error=%v", got, err)
	}
}

func TestHTTPVolumeCreationEnrollmentRejectsInvalidFacts(t *testing.T) {
	instant := TimeOf(time.Unix(12345, 678))
	for _, caps := range []fileCapabilities{
		{SessionIdentity: true},
		{SessionIdentity: true, VolumeCreatedAt: &instant, VolumeCreationError: &ErrorResponse{Errno: "EIO"}},
		{VolumeCreatedAt: &instant},
		{VolumeCreationError: &ErrorResponse{Errno: "EIO"}},
		{SessionIdentity: true, VolumeCreatedAt: &Time{Nanos: -1}},
		{SessionIdentity: true, VolumeCreatedAt: &Time{Nanos: 1_000_000_000}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "unknown"}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "EIO", Recorded: new(false)}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "EIO", FileRecorded: new(false)}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "EIO", FileResult: &fileResponse{}}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "EIO", CloseNotExecutedEpoch: 1}},
		{SessionIdentity: true, VolumeCreationError: &ErrorResponse{Errno: "EIO", Message: strings.Repeat("x", 4097)}},
	} {
		if err := validateVolumeCreationCapability(&caps, true); err == nil {
			t.Fatalf("invalid creation facts accepted: %+v", caps)
		}
	}
	if err := validateVolumeCreationCapability(&fileCapabilities{SessionIdentity: true, VolumeCreatedAt: &instant}, false); err == nil {
		t.Fatal("reference advertised session creation")
	}
	valid := &fileCapabilities{SessionIdentity: true, VolumeCreatedAt: &instant}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, damaged := range []string{
		strings.Replace(string(encoded), `"volumeCreatedAt":{"unix_sec":12345,"nanos":678}`, `"volumeCreatedAt":null`, 1),
		strings.Replace(string(encoded), `"nanos":678`, `"nanos":678,"nanos":0`, 1),
		strings.Replace(string(encoded), `"nanos":678`, `"nanos":null`, 1),
		strings.Replace(string(encoded), `"volumeCreatedAt":`, `"VolumeCreatedAt":`, 1),
		strings.Replace(string(encoded), `"nanos":678`, `"nanos":678,"extra":true`, 1),
	} {
		var decoded fileCapabilities
		if err := decodeFileJSON([]byte(damaged), &decoded); err == nil {
			t.Fatalf("malformed creation fact accepted: %s", damaged)
		}
	}
}

func TestHTTPUnknownVolumeCreationPreservesOrdinarySession(t *testing.T) {
	_, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	client, _ := httpIdentityProbeClient(t, &httpBackendIdentityProbe{Storage: backend, wrapSession: func(native storage.FileSession) storage.FileSession {
		return &httpSessionIdentityProbe{FileSession: native}
	}}, nil)
	session := httpStateSession(t, client)
	identity, err := session.FileSessionIdentity(t.Context())
	if err != nil || identity.Backend.Volume == "" {
		t.Fatalf("identity=%+v error=%v", identity, err)
	}
	if created, err := session.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || !created.IsZero() {
		t.Fatalf("unknown creation=%v error=%v", created, err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPVolumeCreationCaptureDoesNotClaimCurrentLifetime(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	created, err := session.VolumeCreatedAt(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	cached, err := session.VolumeCreatedAt(t.Context())
	if err != nil || !cached.Equal(created) {
		t.Fatalf("cached creation=%v error=%v", cached, err)
	}
	if _, err := file.Stat(t.Context()); err == nil {
		t.Fatal("cached creation admitted a retired file query")
	}
}

func TestHTTPEnrollmentBoundsBothOptionalFactDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name                     string
		unitError, creationError bool
	}{
		{name: "creation only", creationError: true},
		{name: "unit only", unitError: true},
		{name: "both", unitError: true, creationError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			unit, created := uint64(4096), TimeOf(time.Unix(12345, 678))
			caps := &fileCapabilities{Allocation: true, SessionIdentity: true, AllocationUnitBytes: &unit, VolumeCreatedAt: &created}
			if test.unitError {
				caps.AllocationUnitBytes = nil
				caps.AllocationUnitError = &ErrorResponse{Errno: "ENOSPC", Message: strings.Repeat("\x01", 4096)}
			}
			if test.creationError {
				caps.VolumeCreatedAt = nil
				caps.VolumeCreationError = &ErrorResponse{Errno: "EIO", Message: strings.Repeat("\x02", 4096)}
			}
			response := fileResponse{Epoch: 1, Session: strings.Repeat("a", 64), Data: []byte{}, Status: &storage.FileSessionStatus{Epoch: "authority", Revision: 1, ActionEpoch: 1}, SessionIdentity: fileSessionIdentityOf(storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "authority"}), Capabilities: caps}
			original, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			bounded, err := boundedFileSessionResponse(response, 8192)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(bounded)
			if err != nil || len(encoded) > 8192 {
				t.Fatalf("session bytes=%d error=%v", len(encoded), err)
			}
			unchanged, err := json.Marshal(response)
			if err != nil || string(unchanged) != string(original) {
				t.Fatal("diagnostic bounding mutated original capabilities")
			}
			if test.unitError && bounded.Capabilities.AllocationUnitError.Errno != "ENOSPC" || test.creationError && bounded.Capabilities.VolumeCreationError.Errno != "EIO" {
				t.Fatal("error classification changed")
			}
			var decoded fileResponse
			if err := decodeFileJSON(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := validateFileResponse(fileRequest{Op: storage.OpFileSessionOpen}, decoded); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type httpVolumeCreationProbe struct {
	*httpSessionIdentityProbe
	created time.Time
	failure error
	calls   *atomic.Int32
}

func (p *httpVolumeCreationProbe) VolumeCreatedAt(context.Context) (time.Time, error) {
	p.calls.Add(1)
	return p.created, p.failure
}

func TestHTTPVolumeCreationEnrollmentCapturesSourceOnce(t *testing.T) {
	_, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	for _, test := range []struct {
		name    string
		created time.Time
		failure error
	}{
		{name: "known", created: time.Unix(12345, 678).UTC()},
		{name: "known year one", created: time.Time{}},
		{name: "source failure with value", created: time.Unix(12345, 678).UTC(), failure: syscall.ENOSPC},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			client, _ := httpIdentityProbeClient(t, &httpBackendIdentityProbe{Storage: backend, wrapSession: func(native storage.FileSession) storage.FileSession {
				return &httpVolumeCreationProbe{httpSessionIdentityProbe: &httpSessionIdentityProbe{FileSession: native}, created: test.created, failure: test.failure, calls: &calls}
			}}, nil)
			session := httpStateSession(t, client)
			for range 2 {
				created, err := session.VolumeCreatedAt(t.Context())
				if test.failure != nil {
					if !errors.Is(err, test.failure) || !created.IsZero() {
						t.Fatalf("failed creation=%v error=%v", created, err)
					}
				} else if err != nil || !created.Equal(test.created) {
					t.Fatalf("creation=%v error=%v", created, err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("source captures=%d", calls.Load())
			}
		})
	}
}
