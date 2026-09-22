package httprest

import (
	"bytes"
	"encoding/json"
	"fmt"
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
		{
			name:    "guarded-open-aggregate",
			request: guardedOpenRequest(t, namespaceGuardsAtRequestByteLimit(1)),
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
	for _, request := range []fileRequest{observationNameRequest(guards), observationDirectoryMetadataRequest(guards), guardedOpenRequest(t, guards)} {
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

func TestObservationRequestBoundsPreserveEscapedMemberNames(t *testing.T) {
	guards := &storage.NamespaceGuards{
		Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{'r'}}},
		Edges:       []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte("child"), ChildID: 2}},
	}
	for _, test := range []struct {
		request fileRequest
		keys    []string
	}{
		{request: observationNameRequest(guards), keys: []string{"resultBytes", "guards", "directories", "parentId", "revision"}},
		{request: observationDirectoryMetadataRequest(guards), keys: []string{"directoryMetadata", "includeName", "guards", "edges", "rawLeaf"}},
		{request: guardedOpenRequest(t, guards), keys: []string{"child", "guards", "edges", "rawLeaf"}},
	} {
		encoded, err := json.Marshal(test.request)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range test.keys {
			encoded = bytes.ReplaceAll(encoded, []byte(`"`+key+`":`), []byte(`"`+escapedJSONMemberName(key)+`":`))
		}
		var decoded fileRequest
		if err := decodeFileJSON(encoded, &decoded); err != nil {
			t.Fatalf("escaped %s request field was rejected: %v", test.request.Op, err)
		}
		if err := validateFileRequest(decoded); err != nil {
			t.Fatalf("escaped %s request changed shape: %v", test.request.Op, err)
		}
		if err := validateFileArguments(decoded, DefaultFileLimits().Session); err != nil {
			t.Fatalf("escaped %s request changed semantics: %v", test.request.Op, err)
		}
	}
}

func TestGuardedOpenKeepsTheV4ChildAndGuardsShape(t *testing.T) {
	request := guardedOpenRequest(t, &storage.NamespaceGuards{RootID: 1})
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"child":`)) || !bytes.Contains(encoded, []byte(`"guards":`)) || bytes.Contains(encoded, []byte(`"selection":`)) {
		t.Fatalf("guarded open changed the file request shape: %s", encoded)
	}
	damaged := bytes.Replace(encoded, []byte(`"child":`), []byte(`"selection":`), 1)
	var decoded fileRequest
	if err := decodeFileJSON(damaged, &decoded); err == nil {
		t.Fatal("accepted a nested child selection DTO")
	}
}

func escapedJSONMemberName(name string) string {
	var result strings.Builder
	for _, value := range []byte(name) {
		fmt.Fprintf(&result, `\u%04x`, value)
	}
	return result.String()
}

func observationNameRequest(guards *storage.NamespaceGuards) fileRequest {
	return fileRequest{
		Op:          storage.OpFileObserveName,
		Session:     strings.Repeat("a", 64),
		File:        strings.Repeat("b", 64),
		Path:        []byte{},
		Data:        []byte{},
		ResultBytes: DefaultMaxBodyBytes,
		Guards:      namespaceGuardsOf(guards),
	}
}

func observationDirectoryMetadataRequest(guards *storage.NamespaceGuards) fileRequest {
	return fileRequest{
		Op:                storage.OpFileObserveDirectoryMetadata,
		Session:           strings.Repeat("a", 64),
		Path:              []byte{},
		Data:              []byte{},
		ResultBytes:       DefaultMaxBodyBytes,
		Directory:         &storage.DirectoryTarget{NodeID: 1},
		DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{Guards: guards}),
	}
}

func guardedOpenRequest(t *testing.T, guards *storage.NamespaceGuards) fileRequest {
	t.Helper()
	action := storage.FileActionID("1:00000000000000000000000000000000")
	return fileRequest{
		Op:          storage.OpFileOpenAt,
		Session:     strings.Repeat("a", 64),
		Action:      storage.LockRequestID(action),
		Path:        []byte{},
		Data:        []byte{},
		ResultBytes: DefaultMaxBodyBytes,
		Child:       childNameOf(storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}),
		Guards:      namespaceGuardsOf(guards),
		OpenAt: openAtOptionsOf(storage.OpenAtOptions{
			Read: true, Target: storage.ChildCondition{State: storage.Any}, Action: action,
			Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
		}),
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
