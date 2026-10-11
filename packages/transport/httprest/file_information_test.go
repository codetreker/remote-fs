package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func httpInformationSession(t *testing.T, client *Storage) *remoteFileSession {
	t.Helper()
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	t.Cleanup(func() {
		if err := remote.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return remote
}

func httpInformationFile(t *testing.T, session *remoteFileSession, root, node uint64, scopes []storage.MetadataObservationScope) storage.File {
	t.Helper()
	return httpInformationFileNamed(t, session, root, node, scopes, "file")
}
func httpInformationFileNamed(t *testing.T, session *remoteFileSession, root, node uint64, scopes []storage.MetadataObservationScope, name string) storage.File {
	t.Helper()
	options := storage.OpenAtOptions{Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.WriteData}, Action: httpFileAction(t, session), MetadataObservations: scopes}
	if len(scopes) != 0 {
		options.MetadataAccess = storage.WriteMetadata
	}
	opened, err := session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root}, RawLeaf: []byte(name)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	return opened.File
}

func TestHTTPFileInformationUsesFreshExactReferenceWithoutStatGrant(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := httpInformationSession(t, client)
	if err := storage.OpenReferenceInformation(session).CheckOpenReferenceInformation(); err != nil {
		t.Fatal(err)
	}
	file := httpInformationFile(t, session, root.ID, attr.ID, nil)
	information := file.(storage.ReferenceInformationAccess)
	if err := information.CheckReferenceInformation(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write-only Stat=%v", err)
	}
	first, err := information.Information(t.Context())
	if err != nil || first.NodeID != attr.ID || first.Size != 6 || first.Kind != storage.NodeRegular || first.Detached || first.PendingUnlink || !first.AllocationKnown || first.AllocationSize != 4096 {
		t.Fatalf("initial information=%+v err=%v", first, err)
	}
	if _, err := file.Truncate(t.Context(), 8193); err != nil {
		t.Fatal(err)
	}
	fresh, err := information.Information(t.Context())
	if err != nil || fresh.NodeID != attr.ID || fresh.Size != 8193 || !fresh.AllocationKnown || fresh.AllocationSize != 12288 {
		t.Fatalf("fresh information=%+v err=%v", fresh, err)
	}
	if err := backend.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	detached, err := information.Information(t.Context())
	if err != nil || detached.NodeID != attr.ID || detached.Size != 8193 || !detached.Detached {
		t.Fatalf("detached information=%+v err=%v", detached, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := information.Information(t.Context()); err == nil || got != (storage.ReferenceInformation{}) {
		t.Fatalf("closed information=%+v err=%v", got, err)
	}
}

func TestHTTPNodeInformationWorksForMetadataOnlyFileAndDirectory(t *testing.T) {
	_, backend := memoryfixture.New(t, "node-information-http", 1<<20, locking.DefaultOptions())
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		t.Run(map[storage.NodeKind]string{storage.NodeRegular: "file", storage.NodeDirectory: "directory"}[kind], func(t *testing.T) {
			client, _ := retainedHTTPPair(t, DefaultFileLimits(), backend)
			name := map[storage.NodeKind]string{storage.NodeRegular: "file", storage.NodeDirectory: "directory"}[kind]
			var err error
			if kind == storage.NodeRegular {
				err = backend.Write(t.Context(), name, []byte("body"))
			} else {
				err = backend.Mkdir(t.Context(), name)
			}
			if err != nil {
				t.Fatal(err)
			}
			attr, err := backend.Stat(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			session := httpInformationSession(t, client)
			opened, err := session.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Action: httpFileAction(t, session)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("metadata-only Stat=%v", err)
			}
			information := opened.Reference.(storage.ReferenceInformationAccess)
			if err := information.CheckReferenceInformation(); err != nil {
				t.Fatal(err)
			}
			unadvertised := &remoteNodeReference{file: &remoteFile{}}
			if err := unadvertised.CheckReferenceInformation(); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unadvertised node information checker=%v", err)
			}
			result, err := information.Information(t.Context())
			if err != nil || result.NodeID != attr.ID || result.Kind != kind || result.Size != attr.Size || result.Detached {
				t.Fatalf("node information=%+v err=%v", result, err)
			}
			if err := opened.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := information.Information(t.Context()); err == nil || got != (storage.ReferenceInformation{}) {
				t.Fatalf("closed information=%+v err=%v", got, err)
			}
		})
	}
}

type httpInformationPolicy struct {
	mu       sync.Mutex
	denied   storage.Operation
	requests []authz.AccessRequest
}

func (p *httpInformationPolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request.Clone())
	if request.Operation == p.denied {
		return authz.ErrDenied
	}
	return nil
}

func (p *httpInformationPolicy) deny(operation storage.Operation) {
	p.mu.Lock()
	p.denied = operation
	p.mu.Unlock()
}

func TestHTTPInformationAndGeometryRecheckCurrentPolicy(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	policy := &httpInformationPolicy{}
	handler.authorizer = policy
	session := httpInformationSession(t, client)
	file := httpInformationFile(t, session, root.ID, attr.ID, nil)
	info := file.(storage.ReferenceInformationAccess)
	if _, err := info.Information(t.Context()); err != nil {
		t.Fatal(err)
	}
	policy.deny(storage.OpFileInformation)
	if got, err := info.Information(t.Context()); !errors.Is(err, syscall.EACCES) || got != (storage.ReferenceInformation{}) {
		t.Fatalf("revoked information=%+v err=%v", got, err)
	}
	policy.deny("")
	geometryAccess := storage.VolumeGeometryAccess(session)
	if err := geometryAccess.CheckVolumeGeometry(); err != nil {
		t.Fatal(err)
	}
	geometry, err := geometryAccess.VolumeGeometry(t.Context())
	if err != nil || geometry.BytesPerSector != 4096 || geometry.SectorsPerAllocationUnit != 1 {
		t.Fatalf("geometry=%+v err=%v", geometry, err)
	}
	identity, err := session.FileSessionIdentity(t.Context())
	if err != nil || geometry.Volume != identity.Backend.Volume {
		t.Fatalf("geometry=%+v identity=%+v err=%v", geometry, identity, err)
	}
	policy.deny(storage.OpFileVolumeGeometry)
	if got, err := geometryAccess.VolumeGeometry(t.Context()); !errors.Is(err, syscall.EACCES) || got != (storage.VolumeGeometry{}) {
		t.Fatalf("revoked geometry=%+v err=%v", got, err)
	}
	policy.deny("")
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPInformationAndGeometryRejectMalformedWireFacts(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation storage.Operation
		response  fileResponse
		fields    []string
	}{
		{name: "information", operation: storage.OpFileInformation, response: fileResponse{Epoch: 1, Data: []byte{}, Information: referenceInformationOf(storage.ReferenceInformation{NodeID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096})}, fields: []string{"nodeId", "kind", "size", "allocationSize", "allocationKnown", "detached", "pendingUnlink"}},
		{name: "geometry", operation: storage.OpFileVolumeGeometry, response: fileResponse{Epoch: 1, Data: []byte{}, Geometry: volumeGeometryOf(storage.VolumeGeometry{Volume: "volume", BytesPerSector: 4096, SectorsPerAllocationUnit: 1})}, fields: []string{"volume", "bytesPerSector", "sectorsPerAllocationUnit"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.response)
			if err != nil {
				t.Fatal(err)
			}
			var decoded fileResponse
			if err := decodeFileJSON(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := validateFileResponse(fileRequest{Op: test.operation}, decoded); err != nil {
				t.Fatal(err)
			}
			for _, field := range test.fields {
				t.Run(field, func(t *testing.T) {
					body := string(encoded)
					start := strings.Index(body, `"`+field+`":`)
					if start < 0 {
						t.Fatal("required wire field absent from encoder")
					}
					valueAt := start + len(field) + 3
					end := valueAt
					for end < len(body) && body[end] != ',' && body[end] != '}' {
						end++
					}
					member := body[start:end]
					removeAt, removeEnd := start, end
					if body[end] == ',' {
						removeEnd++
					} else if body[start-1] == ',' {
						removeAt--
					}
					for name, malformed := range map[string]string{
						"missing":    body[:removeAt] + body[removeEnd:],
						"null":       body[:valueAt] + "null" + body[end:],
						"duplicate":  body[:start] + member + "," + body[start:],
						"wrong case": strings.Replace(body, `"`+field+`":`, `"`+strings.ToUpper(field[:1])+field[1:]+`":`, 1),
						"unknown":    strings.Replace(body, `"`+field+`":`, `"extra":false,"`+field+`":`, 1),
					} {
						if err := decodeFileJSON([]byte(malformed), &decoded); err == nil {
							t.Fatalf("%s field accepted: %s", name, malformed)
						}
					}
				})
			}
		})
	}
	for _, information := range []storage.ReferenceInformation{
		{},
		{NodeID: 7, Kind: storage.NodeRegular, Size: -1},
		{NodeID: 7, Kind: storage.NodeRegular, AllocationSize: 4096},
		{NodeID: 7, Kind: storage.NodeRegular, AllocationKnown: true, AllocationSize: -1},
	} {
		response := fileResponse{Epoch: 1, Data: []byte{}, Information: referenceInformationOf(information)}
		if err := validateFileResponse(fileRequest{Op: storage.OpFileInformation}, response); err == nil {
			t.Fatalf("contradictory facts accepted=%+v", information)
		}
	}
}
