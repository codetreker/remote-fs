package httprest

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestObservationRequestBoundsRejectBeforeAuthorization(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	handler, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())

	tests := []struct {
		name    string
		request fileRequest
	}{
		{
			name: "revision-one-padding-byte",
			request: observationNameRequest(&storage.NamespaceGuards{
				Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: bytes.Repeat([]byte{'r'}, storage.MaxObservationTokenBytes+1)}},
			}),
		},
		{
			name: "revision-no-padding",
			request: observationNameRequest(&storage.NamespaceGuards{
				Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: bytes.Repeat([]byte{'r'}, storage.MaxObservationTokenBytes+2)}},
			}),
		},
		{
			name: "raw-leaf-one-padding-byte",
			request: observationDirectoryMetadataRequest(&storage.NamespaceGuards{
				Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: bytes.Repeat([]byte{'n'}, storage.MaxLeafBytes+1), ChildID: 2}},
			}),
		},
		{
			name: "raw-leaf-no-padding",
			request: observationDirectoryMetadataRequest(&storage.NamespaceGuards{
				Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: bytes.Repeat([]byte{'n'}, storage.MaxLeafBytes+2), ChildID: 2}},
			}),
		},
		{
			name:    "aggregate",
			request: observationDirectoryMetadataRequest(namespaceGuardsAtRequestByteLimit(1)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy.reset(authz.ErrDenied)
			response := fileAuthorizationRequest(t, handler, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("oversized request returned %d: %s", response.Code, response.Body.String())
			}
			policy.mu.Lock()
			calls := len(policy.requests)
			policy.mu.Unlock()
			if calls != 0 {
				t.Fatalf("oversized request reached authorization %d times", calls)
			}
		})
	}

	handler.files.mu.Lock()
	defer handler.files.mu.Unlock()
	if len(handler.files.sessions) != 0 || handler.files.enrolling != 0 || handler.files.running {
		t.Fatal("oversized observation request reached the file backend registry")
	}
}

func TestObservationRequestBoundsAcceptExactMaxima(t *testing.T) {
	guards := namespaceGuardsAtRequestByteLimit(0)
	if err := guards.Check(); err != nil {
		t.Fatalf("test guards do not occupy the exact valid boundary: %v", err)
	}
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	handler, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	for _, request := range []fileRequest{observationNameRequest(guards), observationDirectoryMetadataRequest(guards)} {
		policy.reset(authz.ErrDenied)
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, handler, request), "EACCES", "access denied")
		policy.mu.Lock()
		calls := append([]authz.AccessRequest(nil), policy.requests...)
		policy.mu.Unlock()
		if len(calls) != 1 || calls[0].Operation != request.Op {
			t.Fatalf("exact-bound %s request did not reach authorization: %+v", request.Op, calls)
		}
	}
}

func observationNameRequest(guards *storage.NamespaceGuards) fileRequest {
	return fileRequest{
		Op:          storage.OpFileObserveName,
		Session:     strings.Repeat("a", 64),
		File:        strings.Repeat("b", 64),
		ResultBytes: DefaultMaxBodyBytes,
		Guards:      namespaceGuardsOf(guards),
	}
}

func observationDirectoryMetadataRequest(guards *storage.NamespaceGuards) fileRequest {
	return fileRequest{
		Op:                storage.OpFileObserveDirectoryMetadata,
		Session:           strings.Repeat("a", 64),
		ResultBytes:       DefaultMaxBodyBytes,
		Directory:         &storage.DirectoryTarget{NodeID: 1},
		DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{Guards: guards}),
	}
}

func namespaceGuardsAtRequestByteLimit(excess int) *storage.NamespaceGuards {
	guards := &storage.NamespaceGuards{
		RootID:      1,
		Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: bytes.Repeat([]byte{'r'}, storage.MaxObservationTokenBytes)}},
	}
	remaining := storage.MaxNamespaceGuardBytes - storage.MaxObservationTokenBytes - 8
	for index := 0; remaining > 0; index++ {
		leafBytes := min(storage.MaxLeafBytes, remaining-16)
		if remaining <= storage.MaxLeafBytes+16 {
			leafBytes += excess
		}
		guards.Edges = append(guards.Edges, storage.ObservedEdge{
			ParentID: 1,
			RawLeaf:  bytes.Repeat([]byte{byte('a' + index)}, leafBytes),
			ChildID:  uint64(index + 2),
		})
		remaining -= leafBytes + 16 - excess
	}
	return guards
}
