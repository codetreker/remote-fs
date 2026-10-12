package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type wholeServiceAuthFixture struct {
	t               *testing.T
	server          *httptest.Server
	current         atomic.Pointer[Service]
	config          Config
	admin           management.User
	ordinary        management.User
	adminVolume     management.Volume
	userVolume      management.Volume
	adminCredential management.Credential
	adminSecret     string
	userCredential  management.Credential
	userSecret      string
}

func newWholeServiceAuthFixture(t *testing.T) *wholeServiceAuthFixture {
	t.Helper()
	temporaryRoot, err := filepath.Abs("../../.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(temporaryRoot, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(temporaryRoot, "whole-service-auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	f := &wholeServiceAuthFixture{t: t}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := f.current.Load()
		if s == nil {
			http.Error(w, "service not running", http.StatusServiceUnavailable)
			return
		}
		s.ServeHTTP(w, r)
	}))
	t.Cleanup(f.server.Close)
	f.config = DefaultConfig(filepath.Join(dir, "state"), f.server.URL)
	if err := InitState(t.Context(), f.config); err != nil {
		t.Fatalf("initialize service state: %v", err)
	}
	f.admin, err = InitializeAdmin(t.Context(), f.config, "administrator", "Administrator", testAuthPassword)
	if err != nil {
		t.Fatalf("initialize administrator: %v", err)
	}
	ordinaryID, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(f.config.StateRoot, "data", "management.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, insertErr := database.ExecContext(t.Context(), "INSERT INTO users(id,role,display_name,auth_epoch,next_action_namespace,retired_before_namespace,logical_time_high_water,created_at) VALUES(?,'user','Ordinary owner',1,1,0,0,?)", ordinaryID, time.Now().UnixMilli())
	if err := errors.Join(insertErr, database.Close()); err != nil {
		t.Fatal(err)
	}
	f.open()
	t.Cleanup(func() { f.close() })
	f.ordinary, err = f.current.Load().maintenance.store.GetUser(t.Context(), ordinaryID)
	if err != nil {
		t.Fatal(err)
	}
	f.adminCredential, f.adminSecret = f.credential(f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles, ScopeAdminSummaries}, true, nil)
	f.userCredential, f.userSecret = f.credential(f.ordinary, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles}, true, nil)
	f.adminVolume = f.createVolume(f.admin, "Administrator workspace")
	f.userVolume = f.createVolume(f.ordinary, "Private workspace")
	// Reopening leaves every runtime cold, so denied access can be observed before
	// any storage activation instead of being hidden by fixture initialization.
	f.close()
	f.open()
	return f
}

func (f *wholeServiceAuthFixture) open() {
	f.t.Helper()
	service, err := Open(f.t.Context(), f.config)
	if err != nil {
		f.t.Fatalf("open whole service: %v", err)
	}
	f.current.Store(service)
}

func (f *wholeServiceAuthFixture) close() {
	f.t.Helper()
	s := f.current.Load()
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		f.t.Errorf("whole service cleanup: %v", err)
		return
	}
	f.current.Store(nil)
}

func (f *wholeServiceAuthFixture) credential(user management.User, scopes []string, all bool, ids []management.ID) (management.Credential, string) {
	f.t.Helper()
	secret, err := randomAuthSecret()
	if err != nil {
		f.t.Fatal(err)
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		f.t.Fatal(err)
	}
	c, err := f.current.Load().maintenance.store.CreateCredential(f.t.Context(), management.CredentialInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, Label: "Whole-service integration", Scopes: scopes, AllOwnedVolumes: all, VolumeIDs: ids, SecretDigest: digest, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		f.t.Fatal(err)
	}
	return c, secret
}

func (f *wholeServiceAuthFixture) createVolume(user management.User, name string) management.Volume {
	f.t.Helper()
	s := f.current.Load()
	namespace, err := s.maintenance.IssueNamespace(f.t.Context(), user.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	nonce, err := management.NewID()
	if err != nil {
		f.t.Fatal(err)
	}
	op, err := s.maintenance.store.CreateVolume(f.t.Context(), user.ID, management.ActionKey{Namespace: namespace.Sequence, Nonce: string(nonce)}, name, limited.MinLimit)
	if err != nil {
		f.t.Fatalf("accept native volume creation: %v", err)
	}
	s.accepted(op)
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		op, err = s.maintenance.store.Operation(f.t.Context(), user.ID, op.ID)
		if err != nil {
			f.t.Fatal(err)
		}
		if op.State == management.OperationSucceeded {
			break
		}
		if op.State == management.OperationFailed {
			f.t.Fatalf("native volume creation failed: %s", op.ErrorCode)
		}
		select {
		case <-deadline.C:
			s.mu.Lock()
			workerErr := s.workerError
			s.mu.Unlock()
			f.t.Fatalf("native volume creation did not settle: phase=%s worker=%v", op.Phase, workerErr)
		case <-ticker.C:
		}
	}
	volume, err := s.maintenance.store.GetVolume(f.t.Context(), user.ID, op.VolumeID)
	if err != nil {
		f.t.Fatal(err)
	}
	if volume.State != management.VolumeActive {
		f.t.Fatalf("native volume state %s", volume.State)
	}
	return volume
}

func (f *wholeServiceAuthFixture) request(method, path string, credential management.Credential, secret string, body any) *http.Response {
	f.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	request, err := http.NewRequestWithContext(f.t.Context(), method, f.server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		f.t.Fatal(err)
	}
	if credential.ID != "" {
		request.Header.Set("Authorization", "Bearer "+string(credential.ID)+"."+secret)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

func (f *wholeServiceAuthFixture) runtimeCount() int {
	s := f.current.Load()
	s.runtimes.mu.Lock()
	defer s.runtimes.mu.Unlock()
	return len(s.runtimes.entries)
}

func wholeFileRefusal(t *testing.T, response *http.Response, errno string) {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != httprest.StatusStorageError || response.Header.Get(httprest.HeaderProtocol) != httprest.Version {
		t.Fatalf("file refusal broke v5 classification: HTTP %d protocol=%q", response.StatusCode, response.Header.Get(httprest.HeaderProtocol))
	}
	var failure httprest.ErrorResponse
	if err := json.Unmarshal(body, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Errno != errno {
		t.Fatalf("file refusal %q wanted %q", failure.Errno, errno)
	}
}

func TestWholeServiceOwnerDenialPrecedesNativeActivation(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	if f.runtimeCount() != 0 {
		t.Fatal("fixture left native runtimes active")
	}
	readAuthResponse(t, f.request("GET", "/api/service/v1/metrics", f.adminCredential, f.adminSecret, nil), http.StatusNotFound, nil)
	if f.runtimeCount() != 0 {
		t.Fatal("unknown metrics route activated native storage")
	}
	for _, path := range []string{"/v5/list?path=", "/v5/read?path=secret&offset=0&length=1", "/v5/subscribe", "/v5/snapshot"} {
		wholeFileRefusal(t, f.request("GET", "/api/volumes/"+string(f.userVolume.ID)+path, f.adminCredential, f.adminSecret, nil), "EACCES")
		if f.runtimeCount() != 0 {
			t.Fatal("administrator file denial activated another owner's native runtime")
		}
	}
	wholeFileRefusal(t, f.request("GET", "/api/volumes/"+string(f.adminVolume.ID)+"/v5/list?path=", f.userCredential, f.userSecret, nil), "EACCES")
	if f.runtimeCount() != 0 {
		t.Fatal("ordinary user file denial activated administrator runtime")
	}
	limitedCredential, limitedSecret := f.credential(f.admin, []string{ScopeVolumesRead}, true, nil)
	wholeFileRefusal(t, f.request("GET", "/api/volumes/"+string(f.adminVolume.ID)+"/v5/list?path=", limitedCredential, limitedSecret, nil), "EACCES")
	if f.runtimeCount() != 0 {
		t.Fatal("credential lacking files scope activated native runtime")
	}
	readAuthResponse(t, f.request("GET", "/api/service/v1/volumes/"+string(f.userVolume.ID), f.adminCredential, f.adminSecret, nil), http.StatusForbidden, nil)
	readAuthResponse(t, f.request("GET", "/api/service/v1/admin/users", f.userCredential, f.userSecret, nil), http.StatusForbidden, nil)
	if f.runtimeCount() != 0 {
		t.Fatal("management denial activated native runtime")
	}
	var own serviceapi.VolumeList
	readAuthResponse(t, f.request("GET", "/api/service/v1/volumes", f.userCredential, f.userSecret, nil), http.StatusOK, &own)
	if len(own.Volumes) != 1 || own.Volumes[0].ID != string(f.userVolume.ID) || own.Volumes[0].Summary == nil || own.Volumes[0].SummaryError != nil {
		t.Fatal("ordinary user list disclosed another owner's volume")
	}
}

type wholeCredentialTransport struct {
	base   http.RoundTripper
	bearer atomic.Value
}

func (transport *wholeCredentialTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	cloned.Header.Set("Authorization", "Bearer "+transport.bearer.Load().(string))
	return transport.base.RoundTrip(cloned)
}

func (f *wholeServiceAuthFixture) volumeClient(volume management.Volume, credential management.Credential, secret string) (*httprest.Storage, *wholeCredentialTransport) {
	f.t.Helper()
	transport := &wholeCredentialTransport{base: f.server.Client().Transport}
	transport.bearer.Store(string(credential.ID) + "." + secret)
	client := *f.server.Client()
	client.Transport = transport
	client.Jar = nil
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	remote, err := httprest.Dial(f.server.URL+"/api/volumes/"+string(volume.ID), &client)
	if err != nil {
		f.t.Fatal(err)
	}
	return remote, transport
}

func TestWholeServiceAdminSummaryDisclosesOnlyAggregateFields(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	userFiles, _ := f.volumeClient(f.userVolume, f.userCredential, f.userSecret)
	if err := userFiles.Create(t.Context(), "private-file-name"); err != nil {
		t.Fatal(err)
	}
	if err := userFiles.Write(t.Context(), "private-file-name", []byte("private file bytes")); err != nil {
		t.Fatal(err)
	}
	if err := userFiles.Mkdir(t.Context(), "private-directory-name"); err != nil {
		t.Fatal(err)
	}
	ownAdminFiles, _ := f.volumeClient(f.adminVolume, f.adminCredential, f.adminSecret)
	if err := ownAdminFiles.Create(t.Context(), "admin-own-file"); err != nil {
		t.Fatal(err)
	}
	if _, err := ownAdminFiles.List(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	f.close()
	f.open()
	response := f.request("GET", "/api/service/v1/admin/users/"+string(f.ordinary.ID)+"/volumes", f.adminCredential, f.adminSecret, nil)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("admin summary status %d", response.StatusCode)
	}
	if strings.Contains(string(body), "private-file-name") || strings.Contains(string(body), "private-directory-name") || strings.Contains(string(body), "private file bytes") || strings.Contains(string(body), string(f.userVolume.ID)) {
		t.Fatal("admin aggregate response disclosed file data or a volume identifier")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope) != 1 || envelope["volumes"] == nil {
		t.Fatalf("unexpected admin envelope fields: %d", len(envelope))
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(envelope["volumes"], &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 || rows[0]["name"] == nil || rows[0]["summary"] == nil {
		t.Fatal("admin row contains fields beyond name and aggregate statistics")
	}
	var summary map[string]string
	if err := json.Unmarshal(rows[0]["summary"], &summary); err != nil {
		t.Fatal(err)
	}
	if len(summary) != 4 || summary["fileCount"] != "1" || summary["directoryCount"] != "1" || summary["usedBytes"] == "" || summary["quotaBytes"] == "" {
		t.Fatalf("unexpected admin statistics fields or counts: %v", summary)
	}
	for _, key := range []string{"fileCount", "directoryCount", "usedBytes", "quotaBytes"} {
		if _, exists := summary[key]; !exists {
			t.Fatalf("missing aggregate %s", key)
		}
	}
	wholeFileRefusal(t, f.request("GET", "/api/volumes/"+string(f.userVolume.ID)+"/v5/list?path=", f.adminCredential, f.adminSecret, nil), "EACCES")
	readAuthResponse(t, f.request("GET", "/api/service/v1/admin/users/"+string(f.admin.ID)+"/volumes", f.adminCredential, f.adminSecret, nil), http.StatusForbidden, nil)
}

func TestWholeServiceCapabilitiesRejectAnotherAccessAndRevocationEndsStream(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	userFiles, transport := f.volumeClient(f.userVolume, f.userCredential, f.userSecret)
	var capabilities serviceapi.Capabilities
	readAuthResponse(t, f.request("GET", "/api/service/v1/capabilities", f.userCredential, f.userSecret, nil), http.StatusOK, &capabilities)
	options, err := capabilities.FileSession.NativeOptions()
	if err != nil {
		t.Fatal(err)
	}
	session, err := userFiles.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatalf("negotiated file-session enrollment: %v", err)
	}
	second, secondSecret := f.credential(f.ordinary, []string{ScopeFiles}, true, nil)
	transport.bearer.Store(string(second.ID) + "." + secondSecret)
	if _, err := session.Status(t.Context()); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("same-owner capability theft: %v", err)
	}
	transport.bearer.Store(string(f.userCredential.ID) + "." + f.userSecret)
	if _, err := session.Status(t.Context()); err != nil {
		t.Fatalf("denied theft damaged original session: %v", err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	subscription, err := userFiles.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	next := make(chan error, 1)
	go func() { _, err := subscription.Next(); next <- err }()
	// A cursor selects history rather than a credential-owned capability.
	secondFiles, _ := f.volumeClient(f.userVolume, second, secondSecret)
	resumed, err := secondFiles.Resubscribe(t.Context(), subscription.Incarnation(), subscription.Position())
	if err != nil {
		t.Fatalf("same-owner fresh access could not resume cursor: %v", err)
	}
	if err := resumed.Close(); err != nil {
		t.Fatal(err)
	}
	readAuthResponse(t, f.request("DELETE", "/api/service/v1/auth/cli-credentials/"+string(f.userCredential.ID), f.userCredential, f.userSecret, nil), http.StatusNoContent, nil)
	select {
	case err := <-next:
		if err == nil {
			t.Fatal("revoked subscription reported successful change")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("revocation left an authenticated stream producing or waiting indefinitely")
	}
	wholeFileRefusal(t, f.request("GET", "/api/volumes/"+string(f.userVolume.ID)+"/v5/list?path=", f.userCredential, f.userSecret, nil), "EACCES")
	if _, err := secondFiles.List(t.Context(), ""); err != nil {
		t.Fatalf("revocation blocked another live access of same owner: %v", err)
	}
}
