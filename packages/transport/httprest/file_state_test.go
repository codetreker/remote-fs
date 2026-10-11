package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func httpStateSession(t *testing.T, client *Storage) *remoteFileSession {
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

func httpStateFile(t *testing.T, session *remoteFileSession, root, node uint64, metadata storage.MetadataPermissions) storage.File {
	t.Helper()
	return httpStateFileNamed(t, session, root, node, metadata, "file")
}
func httpStateFileNamed(t *testing.T, session *remoteFileSession, root, node uint64, metadata storage.MetadataPermissions, name string) storage.File {
	t.Helper()
	options := storage.OpenAtOptions{Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.WriteData}, Action: httpFileAction(t, session)}
	if metadata != 0 {
		options.MetadataAccess = metadata
	}
	opened, err := session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root}, RawLeaf: []byte(name)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	return opened.File
}

func TestHTTPStateMissingCapabilityFailsBeforeTransport(t *testing.T) {
	for _, reference := range []storage.ReferenceStateAccess{
		&remoteFile{},
		&remoteNodeReference{file: &remoteFile{}},
	} {
		if err := reference.CheckReferenceState(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing information capability=%v", err)
		}
		if got, err := reference.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EOPNOTSUPP) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
			t.Fatalf("unadvertised information=%+v,%v", got, err)
		}
	}
}

func TestHTTPFileScalarStateUsesFreshExactReferenceWithoutStatGrant(t *testing.T) {
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
	session := httpStateSession(t, client)
	file := httpStateFile(t, session, root.ID, attr.ID, 0)
	information := file.(storage.ReferenceStateAccess)
	if err := information.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write-only Stat=%v", err)
	}
	first, err := information.State(t.Context(), storage.StateScalars)
	if err != nil || first.Attr.ID != attr.ID || first.Attr.Size != 6 || first.Attr.Kind != storage.NodeRegular || first.Detached || first.PendingUnlink || !first.Attr.AllocationKnown || first.Attr.AllocationSize != 4096 {
		t.Fatalf("initial information=%+v err=%v", first, err)
	}
	if _, err := file.Truncate(t.Context(), 8193); err != nil {
		t.Fatal(err)
	}
	fresh, err := information.State(t.Context(), storage.StateScalars)
	if err != nil || fresh.Attr.ID != attr.ID || fresh.Attr.Size != 8193 || !fresh.Attr.AllocationKnown || fresh.Attr.AllocationSize != 12288 {
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
	detached, err := information.State(t.Context(), storage.StateScalars)
	if err != nil || detached.Attr.ID != attr.ID || detached.Attr.Size != 8193 || !detached.Detached {
		t.Fatalf("detached information=%+v err=%v", detached, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := information.State(t.Context(), storage.StateScalars); err == nil || !reflect.DeepEqual(got, storage.ReferenceState{}) {
		t.Fatalf("closed information=%+v err=%v", got, err)
	}
}

func TestHTTPNodeScalarStateWorksForMetadataOnlyFileAndDirectory(t *testing.T) {
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
			session := httpStateSession(t, client)
			opened, err := session.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Action: httpFileAction(t, session)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("metadata-only Stat=%v", err)
			}
			information := opened.Reference.(storage.ReferenceStateAccess)
			if err := information.CheckReferenceState(); err != nil {
				t.Fatal(err)
			}
			unadvertised := &remoteNodeReference{file: &remoteFile{}}
			if err := unadvertised.CheckReferenceState(); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unadvertised node information checker=%v", err)
			}
			result, err := information.State(t.Context(), storage.StateScalars)
			if err != nil || result.Attr.ID != attr.ID || result.Attr.Kind != kind || result.Attr.Size != attr.Size || result.Detached {
				t.Fatalf("node information=%+v err=%v", result, err)
			}
			if err := opened.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := information.State(t.Context(), storage.StateScalars); err == nil || !reflect.DeepEqual(got, storage.ReferenceState{}) {
				t.Fatalf("closed information=%+v err=%v", got, err)
			}
		})
	}
}

type httpStatePolicy struct {
	mu               sync.Mutex
	denied           storage.Operation
	deniedProjection storage.StateProjection
	requests         []authz.AccessRequest
}

func (p *httpStatePolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request.Clone())
	if request.Operation == p.denied || request.Operation == storage.OpFileState && request.StateProjection == p.deniedProjection {
		return authz.ErrDenied
	}
	return nil
}

func (p *httpStatePolicy) deny(operation storage.Operation) {
	p.mu.Lock()
	p.denied = operation
	p.mu.Unlock()
}

func TestHTTPStateAndAllocationUnitRecheckCurrentPolicy(t *testing.T) {
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
	policy := &httpStatePolicy{}
	handler.authorizer = policy
	session := httpStateSession(t, client)
	file := httpStateFile(t, session, root.ID, attr.ID, 0)
	info := file.(storage.ReferenceStateAccess)
	if _, err := info.State(t.Context(), storage.StateScalars); err != nil {
		t.Fatal(err)
	}
	policy.deny(storage.OpFileState)
	if got, err := info.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
		t.Fatalf("revoked information=%+v err=%v", got, err)
	}
	policy.deny("")
	reporter := storage.AllocationReporting(session)
	if err := reporter.CheckAllocationReporting(); err != nil {
		t.Fatal(err)
	}
	unit, err := reporter.AllocationUnitBytes(t.Context())
	if err != nil || unit != 4096 {
		t.Fatalf("allocation unit=%d error=%v", unit, err)
	}
	policy.deny(storage.OpFileAllocationUnit)
	if unit, err := reporter.AllocationUnitBytes(t.Context()); !errors.Is(err, syscall.EACCES) || unit != 0 {
		t.Fatalf("revoked allocation unit=%d error=%v", unit, err)
	}
	policy.deny("")
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPStateProjectionIsExplicitAndCurrentPolicyCanDenyFull(t *testing.T) {
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
	policy := &httpStatePolicy{deniedProjection: storage.StateFull}
	handler.authorizer = policy
	session := httpStateSession(t, client)
	file := httpStateFile(t, session, root.ID, attr.ID, storage.ReadMetadata)
	reference := file.(storage.ReferenceStateAccess)
	if _, err := reference.State(t.Context(), storage.StateScalars); err != nil {
		t.Fatal(err)
	}
	if result, err := reference.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(result, storage.ReferenceState{}) {
		t.Fatalf("full result=%+v error=%v", result, err)
	}
	policy.mu.Lock()
	policy.deniedProjection = storage.StateScalars
	policy.mu.Unlock()
	if result, err := reference.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(result, storage.ReferenceState{}) {
		t.Fatalf("revoked scalar result=%+v error=%v", result, err)
	}
	policy.mu.Lock()
	requests := append([]authz.AccessRequest(nil), policy.requests...)
	policy.deniedProjection = 0
	policy.mu.Unlock()
	var projections []storage.StateProjection
	for _, request := range requests {
		if request.Operation == storage.OpFileState {
			projections = append(projections, request.StateProjection)
		}
	}
	if !reflect.DeepEqual(projections, []storage.StateProjection{storage.StateScalars, storage.StateFull, storage.StateScalars}) {
		t.Fatalf("policy selectors=%v", projections)
	}
	for _, projection := range []storage.StateProjection{0, 3} {
		if _, err := reference.State(t.Context(), projection); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid projection=%d error=%v", projection, err)
		}
		if err := validateCapabilityArguments(fileRequest{Op: storage.OpFileState, StateProjection: projection}); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid wire projection=%d error=%v", projection, err)
		}
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPScalarStateRejectsProtectedFieldsEvenWhenEmpty(t *testing.T) {
	state := storage.ReferenceState{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096}, PendingUnlink: true}
	response := fileResponse{Epoch: 1, Data: []byte{}, State: referenceStateOfProjection(state, storage.StateScalars)}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	request := fileRequest{Op: storage.OpFileState, StateProjection: storage.StateScalars}
	var decoded fileResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(request, decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileState, StateProjection: storage.StateFull}, decoded); err == nil {
		t.Fatal("scalar reply accepted for full query")
	}
	if err := validateReferenceState(decoded.State); err == nil {
		t.Fatal("scalar reply accepted as full mutation receipt")
	}
	for name, member := range map[string]string{
		"metadata":          `"metadata":{}`,
		"birth time":        `"birth_time":{"unix_sec":-62135596800,"nanos":0}`,
		"change time":       `"change_time":{"unix_sec":-62135596800,"nanos":0}`,
		"access time":       `"access_time":{"unix_sec":-62135596800,"nanos":0}`,
		"modification time": `"mod_time":{"unix_sec":-62135596800,"nanos":0}`,
		"generation":        `"pendingGeneration":""`,
		"link target":       `"linkTarget":""`,
	} {
		t.Run(name, func(t *testing.T) {
			body := string(encoded)
			if name == "generation" || name == "link target" {
				body = strings.Replace(body, `"pendingUnlink":true`, `"pendingUnlink":true,`+member, 1)
			} else {
				body = strings.Replace(body, `"allocation_known":true`, `"allocation_known":true,`+member, 1)
			}
			if err := decodeFileJSON([]byte(body), &decoded); err != nil {
				return
			}
			if err := validateFileResponse(request, decoded); err == nil {
				t.Fatalf("protected scalar field accepted: %s", body)
			}
		})
	}
	full := fileResponse{Epoch: 1, Data: []byte{}, State: referenceStateOf(storage.ReferenceState{Attr: state.Attr})}
	encoded, err = json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(request, decoded); err == nil {
		t.Fatal("complete full Attr accepted for scalar query")
	}
}
