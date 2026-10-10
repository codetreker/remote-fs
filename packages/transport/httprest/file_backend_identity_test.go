package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/locked"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func TestHTTPBackendAndSessionIdentityForwardNativeDescriptors(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	expected, err := backend.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CheckBackendIdentity(); err != nil {
		t.Fatal(err)
	}
	actual, err := client.BackendIdentity(t.Context())
	if err != nil || actual != expected {
		t.Fatalf("backend identity = %+v, %v; want %+v", actual, err, expected)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil || expected.RootNodeID != root.ID {
		t.Fatalf("root = %+v, %v; descriptor = %+v", root, err, expected)
	}
	var epochs []string
	for range 2 {
		session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := session.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		remote := session.(*remoteFileSession)
		for _, check := range []func() error{remote.CheckFileSessionIdentity, remote.CheckStableReferenceIdentity, remote.CheckOpenMetadataAccess} {
			if err := check(); err != nil {
				t.Fatal(err)
			}
		}
		identity, err := remote.FileSessionIdentity(t.Context())
		if err != nil || identity.Backend != expected {
			t.Fatalf("session identity = %+v, %v", identity, err)
		}
		status, err := session.Status(t.Context())
		if err != nil || identity.SessionEpoch != status.Epoch {
			t.Fatalf("status = %+v, %v; identity = %+v", status, err, identity)
		}
		handler.files.mu.Lock()
		served := handler.files.sessions[remote.id]
		handler.files.mu.Unlock()
		native, err := served.native.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
		if err != nil || native != identity {
			t.Fatalf("native identity = %+v, %v; remote = %+v", native, err, identity)
		}
		epochs = append(epochs, identity.SessionEpoch)
	}
	if epochs[0] == epochs[1] {
		t.Fatalf("separate sessions share epoch %q", epochs[0])
	}
}

type httpBackendIdentityProbe struct {
	*objectstore.Storage
	checkErr    error
	getErr      error
	result      *storage.BackendIdentityResult
	calls       atomic.Int32
	wrapSession func(storage.FileSession) storage.FileSession
}

func (p *httpBackendIdentityProbe) CheckBackendIdentity() error { return p.checkErr }
func (p *httpBackendIdentityProbe) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	p.calls.Add(1)
	if p.getErr != nil {
		return storage.BackendIdentityResult{}, p.getErr
	}
	if p.result != nil {
		return *p.result, nil
	}
	return p.Storage.BackendIdentity(ctx)
}
func (p *httpBackendIdentityProbe) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := p.Storage.NewFileSession(ctx, options)
	if err != nil || p.wrapSession == nil {
		return session, err
	}
	return p.wrapSession(session), nil
}

type httpSessionIdentityProbe struct {
	storage.FileSession
	checkErr error
	getErr   error
	result   *storage.FileSessionIdentityResult
}

func (p *httpSessionIdentityProbe) CheckFileSessionIdentity() error     { return p.checkErr }
func (p *httpSessionIdentityProbe) CheckStableReferenceIdentity() error { return p.checkErr }
func (p *httpSessionIdentityProbe) CheckOpenMetadataAccess() error      { return p.checkErr }
func (p *httpSessionIdentityProbe) FileSessionIdentity(ctx context.Context) (storage.FileSessionIdentityResult, error) {
	if p.getErr != nil {
		return storage.FileSessionIdentityResult{}, p.getErr
	}
	if p.result != nil {
		return *p.result, nil
	}
	return p.FileSession.(storage.FileSessionIdentity).FileSessionIdentity(ctx)
}

func httpIdentityProbeClient(t *testing.T, backend storage.Storage, policy authz.Authorizer) (*Storage, *Handler) {
	t.Helper()
	options := DefaultHandlerOptions()
	if policy != nil {
		options.Authorizer, options.Volume = policy, "authorized-volume-label"
	}
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client, handler
}

func TestHTTPBackendIdentityFailuresAndAuthorizationHaveNoInventedIdentity(t *testing.T) {
	for _, test := range []struct {
		name             string
		checkErr, getErr error
		result           *storage.BackendIdentityResult
		denied           bool
		want             error
		calls            int32
	}{
		{name: "unsupported preflight", checkErr: syscall.EOPNOTSUPP, want: syscall.EOPNOTSUPP},
		{name: "failed preflight", checkErr: syscall.EIO, want: syscall.EIO},
		{name: "failed getter", getErr: syscall.ESTALE, want: syscall.ESTALE, calls: 1},
		{name: "invalid root", result: &storage.BackendIdentityResult{Volume: "volume", Authority: "authority"}, want: syscall.EINVAL, calls: 1},
		{name: "invalid volume", result: &storage.BackendIdentityResult{Volume: "bad volume", Authority: "authority", RootNodeID: 1}, want: syscall.EINVAL, calls: 1},
		{name: "authorization denied", denied: true, want: syscall.EACCES},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &httpBackendIdentityProbe{Storage: volumeFixture(t), checkErr: test.checkErr, getErr: test.getErr, result: test.result}
			requests := make(chan authz.AccessRequest, 1)
			policy := authz.AuthorizerFunc(func(_ context.Context, access authz.AccessRequest) error {
				requests <- access
				if test.denied {
					return authz.ErrDenied
				}
				return nil
			})
			client, _ := httpIdentityProbeClient(t, probe, policy)
			result, err := client.BackendIdentity(t.Context())
			if result != (storage.BackendIdentityResult{}) || !errors.Is(err, test.want) || probe.calls.Load() != test.calls {
				t.Fatalf("identity = %+v, %v; calls = %d", result, err, probe.calls.Load())
			}
			request := <-requests
			if request.Operation != storage.OpFileBackendIdentity || request.Volume != "authorized-volume-label" || request.Open != (storage.OpenAccess{}) {
				t.Fatalf("authorization = %+v", request)
			}
		})
	}
	t.Run("provider absent", func(t *testing.T) {
		client, _ := httpIdentityProbeClient(t, struct{ locked.Backend }{volumeFixture(t)}, nil)
		if result, err := client.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("absent provider = %+v, %v", result, err)
		}
	})
}

func TestHTTPSessionIdentityCapabilityAvailabilityAndValidation(t *testing.T) {
	for _, test := range []struct {
		name             string
		checkErr, getErr error
		result           *storage.FileSessionIdentityResult
		absent           bool
		want             error
	}{
		{name: "provider absent", absent: true},
		{name: "unsupported preflight", checkErr: syscall.EOPNOTSUPP},
		{name: "failed preflight", checkErr: syscall.EIO, want: syscall.EIO},
		{name: "failed getter", getErr: syscall.ESTALE, want: syscall.ESTALE},
		{name: "invalid descriptor", result: &storage.FileSessionIdentityResult{}, want: syscall.EINVAL},
		{name: "wrong epoch", result: &storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "different-epoch"}, want: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &httpBackendIdentityProbe{Storage: volumeFixture(t), wrapSession: func(session storage.FileSession) storage.FileSession {
				if test.absent {
					return struct{ storage.FileSession }{session}
				}
				return &httpSessionIdentityProbe{FileSession: session, checkErr: test.checkErr, getErr: test.getErr, result: test.result}
			}}
			client, _ := httpIdentityProbeClient(t, probe, nil)
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if test.want != nil {
				if session != nil || !errors.Is(err, test.want) {
					t.Fatalf("session = %v, %v", session, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(context.Background())
			remote := session.(*remoteFileSession)
			for _, check := range []func() error{remote.CheckFileSessionIdentity, remote.CheckStableReferenceIdentity, remote.CheckOpenMetadataAccess} {
				if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
					t.Fatalf("unavailable capability = %v", err)
				}
			}
			if identity, err := remote.FileSessionIdentity(t.Context()); identity != (storage.FileSessionIdentityResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unavailable identity = %+v, %v", identity, err)
			}
		})
	}
}

func httpIdentityResponseClient(t *testing.T, body []byte) *Storage {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderProtocol, Version)
		w.Header().Set("Content-Type", contentJSON)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestHTTPBackendIdentityRejectsMalformedWireDescriptors(t *testing.T) {
	valid := `{"epoch":0,"data":"","backendIdentity":{"volume":"volume","authority":"authority","rootNodeId":7}}`
	for name, body := range map[string]string{
		"session epoch":     strings.Replace(valid, `"epoch":0`, `"epoch":1`, 1),
		"absent":            `{"epoch":0,"data":""}`,
		"null":              strings.Replace(valid, `{"volume":"volume","authority":"authority","rootNodeId":7}`, `null`, 1),
		"missing volume":    strings.Replace(valid, `"volume":"volume",`, ``, 1),
		"missing authority": strings.Replace(valid, `"authority":"authority",`, ``, 1),
		"missing root":      strings.Replace(valid, `,"rootNodeId":7`, ``, 1),
		"null volume":       strings.Replace(valid, `"volume":"volume"`, `"volume":null`, 1),
		"duplicate volume":  strings.Replace(valid, `"volume":"volume"`, `"volume":"volume","volume":"other"`, 1),
		"unknown member":    strings.Replace(valid, `"rootNodeId":7`, `"rootNodeId":7,"extra":true`, 1),
		"wrong case":        strings.Replace(valid, `"rootNodeId"`, `"RootNodeID"`, 1),
		"wrong type":        strings.Replace(valid, `"rootNodeId":7`, `"rootNodeId":"7"`, 1),
		"invalid character": strings.Replace(valid, `"authority":"authority"`, `"authority":"bad authority"`, 1),
		"empty volume":      strings.Replace(valid, `"volume":"volume"`, `"volume":""`, 1),
		"zero root":         strings.Replace(valid, `"rootNodeId":7`, `"rootNodeId":0`, 1),
		"oversized volume":  strings.Replace(valid, `"volume":"volume"`, `"volume":"`+strings.Repeat("v", storage.MaxFileIdentityBytes+1)+`"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			client := httpIdentityResponseClient(t, []byte(body))
			if identity, err := client.BackendIdentity(t.Context()); identity != (storage.BackendIdentityResult{}) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed response = %+v, %v: %s", identity, err, body)
			}
		})
	}
	client := httpIdentityResponseClient(t, []byte(valid))
	if identity, err := client.BackendIdentity(t.Context()); err != nil || identity != (storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 7}) {
		t.Fatalf("valid lower-camel descriptor = %+v, %v", identity, err)
	}
}

func TestHTTPSessionIdentityRejectsMalformedWireAndCapabilityDisagreement(t *testing.T) {
	response := fileResponse{Epoch: 1, Data: []byte{}, Session: strings.Repeat("a", 64),
		Status:          &storage.FileSessionStatus{Epoch: "session-epoch", ActionEpoch: 1, Revision: 1, Remaining: time.Minute, HistoryRemaining: time.Minute},
		Capabilities:    &fileCapabilities{SessionIdentity: true, StableIdentity: true, OpenMetadata: true},
		SessionIdentity: fileSessionIdentityOf(storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 7}, SessionEpoch: "session-epoch"}),
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	valid := string(encoded)
	identity, err := json.Marshal(response.SessionIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"absent descriptor":             strings.Replace(valid, `,"sessionIdentity":`+string(identity), ``, 1),
		"null descriptor":               strings.Replace(valid, string(identity), `null`, 1),
		"missing backend":               strings.Replace(valid, `"backend":{"volume":"volume","authority":"authority","rootNodeId":7},`, ``, 1),
		"null backend":                  strings.Replace(valid, `"backend":{"volume":"volume","authority":"authority","rootNodeId":7}`, `"backend":null`, 1),
		"missing epoch":                 strings.Replace(valid, `,"sessionEpoch":"session-epoch"`, ``, 1),
		"duplicate epoch":               strings.Replace(valid, `"sessionEpoch":"session-epoch"`, `"sessionEpoch":"session-epoch","sessionEpoch":"other"`, 1),
		"unknown member":                strings.Replace(valid, `"sessionEpoch":"session-epoch"`, `"sessionEpoch":"session-epoch","extra":true`, 1),
		"wrong case":                    strings.Replace(valid, `"sessionEpoch"`, `"SessionEpoch"`, 1),
		"wrong epoch":                   strings.Replace(valid, `"sessionEpoch":"session-epoch"`, `"sessionEpoch":"other"`, 1),
		"descriptor without capability": strings.Replace(valid, `"sessionIdentity":true`, `"sessionIdentity":false`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			client := httpIdentityResponseClient(t, []byte(body))
			if session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); session != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed response = %v, %v: %s", session, err, body)
			}
		})
	}
}

func TestHTTPOpenAtRequiresCanonicalMetadataAccessMember(t *testing.T) {
	options := openAtOptionsOf(storage.OpenAtOptions{Read: true, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata})
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var decoded openAtOptions
	if err := decodeFileJSON(encoded, &decoded); err != nil || decoded.storage().MetadataAccess != options.MetadataAccess {
		t.Fatalf("metadata access round trip = %+v, %v", decoded, err)
	}
	for _, changed := range [][]byte{
		bytes.Replace(encoded, []byte(`"metadataAccess":3,`), nil, 1),
		bytes.Replace(encoded, []byte(`"metadataAccess":3`), []byte(`"metadataAccess":null`), 1),
		bytes.Replace(encoded, []byte(`"metadataAccess":3`), []byte(`"MetadataAccess":3`), 1),
		bytes.Replace(encoded, []byte(`"metadataAccess":3`), []byte(`"metadataAccess":3,"metadataAccess":0`), 1),
	} {
		if err := decodeFileJSON(changed, &decoded); err == nil {
			t.Fatalf("noncanonical metadata access accepted: %s", changed)
		}
	}
}

func TestHTTPOpenAtForwardsMetadataRightsWithoutEscalatingByteRights(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	for _, access := range []storage.OpenAccess{{Read: true}, {Write: true}, {Read: true, Write: true}} {
		for _, permissions := range []storage.MetadataPermissions{0, storage.ReadMetadata, storage.WriteMetadata, storage.ReadMetadata | storage.WriteMetadata} {
			t.Run(fmt.Sprintf("read=%t/write=%t/metadata=%d", access.Read, access.Write, permissions), func(t *testing.T) {
				name := fmt.Sprintf("rights-%t-%t-%d", access.Read, access.Write, permissions)
				uses := storage.ReadData
				if access.Write {
					uses |= storage.WriteData
				}
				opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)}}, storage.OpenAtOptions{
					Read: access.Read, Write: access.Write, MetadataAccess: permissions, Create: true, Exclusive: true,
					Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Action: httpFileAction(t, session), Use: storage.UseClaim{Uses: uses},
				})
				if err != nil || opened.File == nil || opened.Attr.ID == 0 {
					t.Fatalf("open = %+v, %v", opened, err)
				}
				defer opened.File.Close(context.Background())
				assertAccess := func(operation string, err error, allowed bool) {
					t.Helper()
					if allowed && err != nil || !allowed && !errors.Is(err, syscall.EBADF) {
						t.Fatalf("%s allowed=%t: %v", operation, allowed, err)
					}
				}
				_, err = opened.File.Stat(t.Context())
				assertAccess("Stat", err, permissions&storage.ReadMetadata != 0)
				_, err = opened.File.(storage.ReferenceStateAccess).State(t.Context())
				assertAccess("State", err, permissions&storage.ReadMetadata != 0)
				at := time.Unix(123, 456)
				_, err = opened.File.SetAttr(t.Context(), storage.AttrChange{ModTime: &at})
				assertAccess("SetAttr", err, permissions&storage.WriteMetadata != 0)
				_, err = opened.File.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test.rights", nil, []byte("value"))
				assertAccess("SetMetadata", err, permissions&storage.WriteMetadata != 0)
				_, err = opened.File.ReadAt(t.Context(), 0, 1)
				assertAccess("ReadAt", err, access.Read)
				_, err = opened.File.WriteAt(t.Context(), 0, []byte("body"))
				assertAccess("WriteAt", err, access.Write)
				_, err = opened.File.Truncate(t.Context(), 2)
				assertAccess("Truncate", err, access.Write)
			})
		}
	}
}

func TestHTTPOpenAtAuthorizesMetadataIndependentlyBeforeCreation(t *testing.T) {
	for _, denied := range []storage.Operation{storage.OpFileStat, storage.OpFileSetAttr, storage.OpFileSetMetadata} {
		t.Run(string(denied), func(t *testing.T) {
			backend := volumeFixture(t)
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			requests := make(chan authz.AccessRequest, 20)
			policy := authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				requests <- request
				if request.Operation == denied {
					return authz.ErrDenied
				}
				return nil
			})
			client, _ := httpIdentityProbeClient(t, backend, policy)
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(context.Background())
			permissions := storage.WriteMetadata
			if denied == storage.OpFileStat {
				permissions = storage.ReadMetadata
			}
			opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("denied")}}, storage.OpenAtOptions{
				Read: true, MetadataAccess: permissions, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Action: httpFileAction(t, session), Use: storage.UseClaim{Uses: storage.ReadData},
			})
			if opened.File != nil || !errors.Is(err, syscall.EACCES) {
				t.Fatalf("denied open = %+v, %v", opened, err)
			}
			if _, err := backend.Stat(t.Context(), "denied"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("denied authorization created file: %v", err)
			}
			var found bool
			for len(requests) > 0 {
				request := <-requests
				if request.Operation == denied {
					found = true
				}
				if request.Operation == storage.OpFileOpenAt && request.Open != (storage.OpenAccess{Read: true, Create: true, Exclusive: true}) {
					t.Fatalf("metadata grants changed byte authorization: %+v", request)
				}
			}
			if !found {
				t.Fatalf("independent %s authorization absent", denied)
			}
		})
	}
}

type httpUnsupportedMetadataSession struct {
	storage.FileSession
	opens *atomic.Int32
}

func (p *httpUnsupportedMetadataSession) CheckAtomicFileOpen() error {
	return p.FileSession.(storage.AtomicFileOpener).CheckAtomicFileOpen()
}
func (p *httpUnsupportedMetadataSession) CheckOpenMetadataAccess() error { return syscall.EOPNOTSUPP }
func (p *httpUnsupportedMetadataSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.opens.Add(1)
	return p.FileSession.(storage.AtomicFileOpener).OpenAt(ctx, selection, options)
}

func TestHTTPOpenAtRejectsUnsupportedMetadataGrantBeforeBackendEffects(t *testing.T) {
	var opens atomic.Int32
	backend := &httpBackendIdentityProbe{Storage: volumeFixture(t), wrapSession: func(session storage.FileSession) storage.FileSession {
		return &httpUnsupportedMetadataSession{FileSession: session, opens: &opens}
	}}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	client, _ := httpIdentityProbeClient(t, backend, nil)
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	if err := session.(storage.OpenMetadataAccess).CheckOpenMetadataAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported metadata check = %v", err)
	}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("unsupported")}}, storage.OpenAtOptions{
		Read: true, MetadataAccess: storage.ReadMetadata, Create: true, Exclusive: true, Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Action: httpFileAction(t, session), Use: storage.UseClaim{Uses: storage.ReadData},
	})
	if opened.File != nil || !errors.Is(err, syscall.EOPNOTSUPP) || opens.Load() != 0 {
		t.Fatalf("unsupported grant = %+v, %v; native opens = %d", opened, err, opens.Load())
	}
	if _, err := backend.Stat(t.Context(), "unsupported"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("unsupported grant created file: %v", err)
	}
}

type httpStableReferenceSession struct {
	storage.FileSession
	wrapFile      func(storage.File) storage.File
	wrapReference func(storage.NodeReference) storage.NodeReference
}

func (s *httpStableReferenceSession) CheckStableReferenceIdentity() error { return nil }
func (s *httpStableReferenceSession) CheckAtomicFileOpen() error {
	return s.FileSession.(storage.AtomicFileOpener).CheckAtomicFileOpen()
}
func (s *httpStableReferenceSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	result, err := s.FileSession.(storage.AtomicFileOpener).OpenAt(ctx, selection, options)
	if result.File != nil {
		result.File = s.wrapFile(result.File)
	}
	return result, err
}
func (s *httpStableReferenceSession) CheckNodeReferences() error {
	return s.FileSession.(storage.NodeReferences).CheckNodeReferences()
}
func (s *httpStableReferenceSession) OpenNodeRef(ctx context.Context, node uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, err := s.FileSession.(storage.NodeReferences).OpenNodeRef(ctx, node, options)
	if result.Reference != nil {
		result.Reference = s.wrapReference(result.Reference)
	}
	return result, err
}
func (s *httpStableReferenceSession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, err := s.FileSession.(storage.NodeReferences).OpenChildRef(ctx, selection, options)
	if result.Reference != nil {
		result.Reference = s.wrapReference(result.Reference)
	}
	return result, err
}

type httpInvalidStableFile struct {
	storage.File
	id  uint64
	err error
}

func (f *httpInvalidStableFile) ReferenceNodeID() (uint64, error) { return f.id, f.err }

type httpInvalidStableNodeReference struct {
	storage.NodeReference
	id  uint64
	err error
}

func (r *httpInvalidStableNodeReference) ReferenceNodeID() (uint64, error) { return r.id, r.err }

func TestHTTPStableIdentityPromiseRejectsMissingAndInvalidReferenceIdentity(t *testing.T) {
	for _, kind := range []string{"file", "node", "child"} {
		for _, failure := range []string{"missing", "zero", "mismatch", "getter error"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				backend := volumeFixture(t)
				if err := backend.Create(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
				attr, err := backend.Stat(t.Context(), "file")
				if err != nil {
					t.Fatal(err)
				}
				root, err := backend.Stat(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				native, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer native.Close(context.Background())
				id := attr.ID + 1
				var identityErr error
				if failure == "zero" {
					id = 0
				}
				if failure == "getter error" {
					identityErr = syscall.ESTALE
				}
				session := &httpStableReferenceSession{FileSession: native,
					wrapFile: func(file storage.File) storage.File {
						if failure == "missing" {
							return struct{ storage.File }{file}
						}
						return &httpInvalidStableFile{File: file, id: id, err: identityErr}
					},
					wrapReference: func(reference storage.NodeReference) storage.NodeReference {
						if failure == "missing" {
							return struct{ storage.NodeReference }{reference}
						}
						return &httpInvalidStableNodeReference{NodeReference: reference, id: id, err: identityErr}
					},
				}
				served := &servedFileSession{native: session, stableIdentity: true, files: make(map[string]*servedFile), options: storage.DefaultFileSessionOptions()}
				handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
				request := fileRequest{Op: storage.OpFileOpenAt, Child: childNameOf(storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}), OpenAt: openAtOptionsOf(storage.OpenAtOptions{
					Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Existing: storage.Keep, Action: httpFileAction(t, native), Use: storage.UseClaim{Uses: storage.ReadData},
				})}
				if kind != "file" {
					request.Op, request.OpenAt, request.NodeRef = storage.OpFileOpenChildRef, nil, nodeRefOptionsOf(storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Action: httpFileAction(t, native), MetadataAccess: storage.ReadMetadata})
				}
				if kind == "node" {
					request.Op, request.Node, request.Child = storage.OpFileOpenNodeRef, attr.ID, nil
				}
				response, err := handler.openReference(t.Context(), served, request)
				if storage.ErrnoOf(err) != syscall.EIO || response.File != "" || response.Capabilities != nil || response.Attr == nil || response.Attr.ID != attr.ID {
					t.Fatalf("invalid stable reference = %+v, %v", response, err)
				}
				if len(served.files) != 1 {
					t.Fatalf("invalid identity lost cleanup owner: %d retained files", len(served.files))
				}
				for _, retained := range served.files {
					caps, err := referenceCapabilitiesOf(retained.native)
					if err != nil || caps.ReferenceName {
						t.Fatalf("identity failure depended on name observation: %+v, %v", caps, err)
					}
					if result, err := retained.native.CloseWithResult(t.Context()); !result.Released || err != nil {
						t.Fatalf("retained owner cleanup = %+v, %v", result, err)
					}
				}
			})
		}
	}
}

type httpNoninlineSession struct {
	storage.FileSession
	opens *atomic.Int32
}

func (s *httpNoninlineSession) CheckRecoverableReferenceClose() error {
	return s.FileSession.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose()
}
func (s *httpNoninlineSession) CheckAtomicFileOpen() error {
	return s.FileSession.(storage.AtomicFileOpener).CheckAtomicFileOpen()
}
func (s *httpNoninlineSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	s.opens.Add(1)
	return s.FileSession.(storage.AtomicFileOpener).OpenAt(ctx, selection, options)
}

type httpInlineCheckSession struct {
	*httpNoninlineSession
	err error
}

func (s *httpInlineCheckSession) CheckInlineCloseSettlement() error { return s.err }

type httpInjectedSessionBackend struct {
	*objectstore.Storage
	source storage.FileStorage
}

func (b *httpInjectedSessionBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	return b.source.NewFileSession(ctx, options)
}

func TestHTTPInlineCloseSettlementChecksCompleteNestedWrapperChain(t *testing.T) {
	for _, kind := range []string{"native", "missing", "unsupported", "failed", "http"} {
		t.Run(kind, func(t *testing.T) {
			backend := volumeFixture(t)
			var opens atomic.Int32
			probe := &httpBackendIdentityProbe{Storage: backend, wrapSession: func(session storage.FileSession) storage.FileSession {
				if kind == "native" {
					return session
				}
				proxy := &httpNoninlineSession{FileSession: session, opens: &opens}
				if kind == "missing" {
					return proxy
				}
				checkErr := error(syscall.EOPNOTSUPP)
				if kind == "failed" {
					checkErr = syscall.EIO
				}
				return &httpInlineCheckSession{httpNoninlineSession: proxy, err: checkErr}
			}}
			var source storage.Storage = probe
			if kind == "http" {
				inner, _ := httpIdentityProbeClient(t, backend, nil)
				source = &httpInjectedSessionBackend{Storage: backend, source: inner}
			}
			quota, err := limited.New(t.Context(), source, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			enforcing, err := locked.New(quota)
			if err != nil {
				t.Fatal(err)
			}
			nested, err := enforcing.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer nested.Close(context.Background())
			if err := nested.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose(); err != nil {
				t.Fatalf("recovery preflight = %v", err)
			}
			caps, err := sessionCapabilitiesOf(nested)
			if kind == "failed" {
				if !errors.Is(err, syscall.EIO) || caps.CloseRecovery {
					t.Fatalf("failed inner preflight = %+v, %v", caps, err)
				}
			} else if err != nil || caps.CloseRecovery != (kind == "native") {
				t.Fatalf("nested capabilities = %+v, %v", caps, err)
			}
			client, _ := httpIdentityProbeClient(t, enforcing, nil)
			remoteSession, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if kind == "failed" {
				if remoteSession != nil || !errors.Is(err, syscall.EIO) {
					t.Fatalf("failed enrollment = %v, %v", remoteSession, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer remoteSession.Close(context.Background())
			remote := remoteSession.(*remoteFileSession)
			if kind == "native" {
				if err := remote.CheckRecoverableReferenceClose(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if remote.capabilities.CloseRecovery || !errors.Is(remote.CheckRecoverableReferenceClose(), syscall.EOPNOTSUPP) {
				t.Fatalf("noninline server advertised recovery: %+v", remote.capabilities)
			}
			original := client.http.Transport
			var outbound atomic.Int32
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				outbound.Add(1)
				return original.RoundTrip(request)
			})
			defer func() { client.http.Transport = original }()
			file := &remoteFile{session: remote, id: strings.Repeat("b", 64)}
			action, err := storage.NewFileActionID(remote.epoch)
			if err != nil {
				t.Fatal(err)
			}
			result, err := file.CloseWithAction(t.Context(), storage.CloseAttempt{Action: action, Generation: 1})
			if result != (storage.ReferenceCloseResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) || outbound.Load() != 0 || opens.Load() != 0 {
				t.Fatalf("unsupported explicit close = %+v, %v; outbound=%d native opens=%d", result, err, outbound.Load(), opens.Load())
			}
		})
	}
}

type httpBoundedCloseReplaySession struct{ *httpSettlementSession }

func (s *httpBoundedCloseReplaySession) CheckRecoverableReferenceClose() error {
	return s.FileSession.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose()
}
func (s *httpBoundedCloseReplaySession) CheckInlineCloseSettlement() error {
	return s.FileSession.(storage.InlineCloseSettlement).CheckInlineCloseSettlement()
}
func (s *httpBoundedCloseReplaySession) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.httpSettlementSession.OpenFile(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return &httpBoundedCloseReplayFile{httpSettlementFile: file.(*httpSettlementFile)}, nil
}

type httpBoundedCloseReplayFile struct{ *httpSettlementFile }

func (f *httpBoundedCloseReplayFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	f.backend.fileCloses.Add(1)
	result, err := f.File.(storage.ReferenceCloseActions).CloseWithAction(ctx, attempt)
	if result.Released {
		err = errors.Join(err, f.backend.fileSemantic)
	}
	return result, err
}
func (f *httpBoundedCloseReplayFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	return f.File.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, attempt)
}
func (f *httpBoundedCloseReplayFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	return f.File.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
}

func httpCloseErrorCount(err error) int {
	if err == nil {
		return 0
	}
	count := 1
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			count += httpCloseErrorCount(child)
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		count += httpCloseErrorCount(wrapped.Unwrap())
	}
	return count
}

func TestHTTPSettledCloseReplaysKeepSemanticErrorBounded(t *testing.T) {
	backend := volumeFixture(t)
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	effects := &httpSettlementBackend{Storage: backend, fileSemantic: syscall.ENOTEMPTY}
	probe := &httpBackendIdentityProbe{Storage: backend, wrapSession: func(session storage.FileSession) storage.FileSession {
		return &httpBoundedCloseReplaySession{httpSettlementSession: &httpSettlementSession{FileSession: session, backend: effects}}
	}}
	client, _ := httpIdentityProbeClient(t, probe, nil)
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	if err := session.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose(); err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	closer := file.(storage.ReferenceCloseActions)
	owner, err := closer.CloseOwnerStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(owner.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: action, Generation: owner.NextGeneration}
	var expectedCount, expectedBytes int
	for replay := range 33 {
		result, err := closer.CloseWithAction(t.Context(), attempt)
		var marker *storage.CloseSettlementError
		if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EIO) || errors.As(err, &marker) {
			t.Fatalf("settled replay %d = %+v, %v", replay, result, err)
		}
		count, bytes := httpCloseErrorCount(err), len(err.Error())
		if replay == 0 {
			expectedCount, expectedBytes = count, bytes
		}
		if count != expectedCount || bytes != expectedBytes {
			t.Fatalf("replay %d grew semantic error: count=%d bytes=%d; initial count=%d bytes=%d", replay, count, bytes, expectedCount, expectedBytes)
		}
	}
	if calls := effects.fileCloses.Load(); calls != 1 {
		t.Fatalf("settled replays repeated native close %d times", calls)
	}
}
