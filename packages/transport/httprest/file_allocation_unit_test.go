package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPAllocationUnitUsesImmutableEnrollmentFact(t *testing.T) {
	unit := uint64(4096)
	session := &remoteFileSession{capabilities: fileCapabilities{Allocation: true, AllocationUnitBytes: &unit}}
	for range 2 {
		if got, err := session.AllocationUnitBytes(t.Context()); err != nil || got != unit {
			t.Fatalf("cached unit=%d error=%v", got, err)
		}
	}
	for _, errno := range []syscall.Errno{syscall.EOPNOTSUPP, syscall.EIO, syscall.ENOSPC} {
		session.capabilities = fileCapabilities{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: storage.ErrnoNameOf(errno), Message: "unit capture failed"}}
		if got, err := session.AllocationUnitBytes(t.Context()); !errors.Is(err, errno) || got != 0 {
			t.Fatalf("failed unit=%d error=%v", got, err)
		}
		if err := session.CheckAllocationReporting(); err != nil {
			t.Fatalf("unit failure weakened byte capability: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := session.AllocationUnitBytes(ctx); !errors.Is(err, context.Canceled) || got != 0 {
		t.Fatalf("cancelled unit=%d error=%v", got, err)
	}
}

func TestHTTPAllocationUnitEnrollmentRejectsContradictoryFacts(t *testing.T) {
	unit, zero := uint64(4096), uint64(0)
	for _, caps := range []fileCapabilities{
		{Allocation: true},
		{Allocation: true, AllocationUnitBytes: &zero},
		{Allocation: true, AllocationUnitBytes: &unit, AllocationUnitError: &ErrorResponse{Errno: "EIO"}},
		{AllocationUnitBytes: &unit},
		{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "unknown"}},
		{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "EIO", Recorded: new(false)}},
		{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "EIO", FileResult: &fileResponse{}}},
		{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "EIO", CloseNotExecutedEpoch: 1}},
		{Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "EIO", Message: strings.Repeat("x", 4097)}},
	} {
		if err := validateAllocationUnitCapability(&caps, true); err == nil {
			t.Fatalf("invalid unit facts accepted: %+v", caps)
		}
	}
	if err := validateAllocationUnitCapability(&fileCapabilities{Allocation: true, AllocationUnitBytes: &unit}, false); err == nil {
		t.Fatal("reference advertised session unit")
	}
}

type allocationUnitFixture struct {
	storage.FileStorage
	failure error
}

type allocationUnitFixtureSession struct {
	storage.FileSession
	storage.FileSessionIdentity
	failure error
}

func (s *allocationUnitFixture) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &allocationUnitFixtureSession{FileSession: native, FileSessionIdentity: native.(storage.FileSessionIdentity), failure: s.failure}, nil
}
func (*allocationUnitFixtureSession) CheckAllocationReporting() error { return nil }
func (s *allocationUnitFixtureSession) AllocationUnitBytes(context.Context) (uint64, error) {
	return 0, s.failure
}

func TestHTTPUnitFailureDoesNotBlockSessionOrFileAccess(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	handler.files.backend = &allocationUnitFixture{FileStorage: backend, failure: syscall.EOPNOTSUPP}
	session := httpStateSession(t, client)
	if unit, err := session.AllocationUnitBytes(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || unit != 0 {
		t.Fatalf("unsupported unit=%d error=%v", unit, err)
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

func TestHTTPUnitErrorFitsCompleteSessionEnvelope(t *testing.T) {
	response := fileResponse{Epoch: 1, Session: strings.Repeat("a", 64), Data: []byte{}, Status: &storage.FileSessionStatus{Epoch: "authority", Revision: 1, ActionEpoch: 1}, SessionIdentity: fileSessionIdentityOf(storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "authority"}), Capabilities: &fileCapabilities{SessionIdentity: true, Allocation: true, AllocationUnitError: &ErrorResponse{Errno: "EOPNOTSUPP", Message: strings.Repeat("\x01", 4096)}}}
	bounded, err := boundedFileSessionResponse(response, 8192)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(bounded)
	if err != nil || len(encoded) > 8192 {
		t.Fatalf("session envelope bytes=%d error=%v", len(encoded), err)
	}
	if !bounded.Capabilities.Allocation || bounded.Capabilities.AllocationUnitError.Errno != "EOPNOTSUPP" || bounded.Capabilities.AllocationUnitError.Message != boundedErrorDetail {
		t.Fatalf("unit error classification changed: %+v", bounded.Capabilities)
	}
	var decoded fileResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileSessionOpen}, decoded); err != nil {
		t.Fatal(err)
	}
}
