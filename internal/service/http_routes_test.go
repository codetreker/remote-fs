package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func routeUser(t *testing.T, f *authFixture) management.User {
	t.Helper()
	id, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", f.database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("INSERT INTO users(id,role,display_name,auth_epoch,next_action_namespace,retired_before_namespace,logical_time_high_water,created_at) VALUES(?,'user','Ordinary owner',1,1,0,0,0)", id)
	if err != nil {
		t.Fatal(err)
	}
	user, err := f.store.GetUser(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func routeCredential(t *testing.T, f *authFixture, user management.User, scopes []string, all bool, ids []management.ID) (management.Credential, string) {
	t.Helper()
	secret, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		t.Fatal(err)
	}
	credential, err := f.store.CreateCredential(t.Context(), management.CredentialInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, Label: "Route test", SecretDigest: digest, Scopes: scopes, AllOwnedVolumes: all, VolumeIDs: ids, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return credential, string(credential.ID) + "." + secret
}

func routeVolume(t *testing.T, f *authFixture, user management.User, name string) (management.Volume, management.Operation) {
	t.Helper()
	namespace, err := f.store.IssueNamespace(t.Context(), user.ID)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := f.store.CreateVolume(t.Context(), user.ID, management.ActionKey{Namespace: namespace.Sequence, Nonce: "create"}, name, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range []management.OperationTransition{
		{State: management.OperationRunning, Phase: management.PhaseInitializing},
		{State: management.OperationRunning, Phase: management.PhaseBound, VerifiedRootProof: []byte("route-test-proof")},
		{State: management.OperationSucceeded, Phase: management.PhaseCompleted, Settled: true, Result: []byte(`{"initialized":true}`)},
	} {
		operation, err = f.store.TransitionOperation(t.Context(), operation.ID, operation.Revision, transition)
		if err != nil {
			t.Fatal(err)
		}
	}
	volume, err := f.store.GetVolume(t.Context(), user.ID, operation.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	return volume, operation
}

func routeRequest(handler http.Handler, method, path, bearer, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "https://service.invalid"+serviceapi.Prefix+path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func exactRouteSummary(context.Context, management.Volume) (storage.VolumeSummaryResult, error) {
	return storage.VolumeSummaryResult{FileCount: 9007199254740993, DirectoryCount: 7, AllocationUsed: 9007199254740995, QuotaBytes: 9007199254740999, QuotaRevision: 9007199254740997}, nil
}

func routeHandler(t *testing.T, f *authFixture, summary func(context.Context, management.Volume) (storage.VolumeSummaryResult, error), accepted func(management.Operation)) http.Handler {
	t.Helper()
	handler, err := newManagementHandler(f.auth, f.store, summary, accepted, fixtureIssuer(f.store))
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestManagementRoutesOwnerAndAdminIsolationBeforeSummary(t *testing.T) {
	f := newAuthFixture(t, nil)
	ordinary := routeUser(t, f)
	other := routeUser(t, f)
	volume, operation := routeVolume(t, f, ordinary, "Private volume")
	_, adminBearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeAdminSummaries, ScopeFiles}, true, nil)
	_, ownerBearer := routeCredential(t, f, ordinary, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles}, true, nil)
	_, otherBearer := routeCredential(t, f, other, []string{ScopeVolumesRead, ScopeFiles}, true, nil)
	var opened atomic.Int64
	handler := routeHandler(t, f, func(ctx context.Context, volume management.Volume) (storage.VolumeSummaryResult, error) {
		opened.Add(1)
		return exactRouteSummary(ctx, volume)
	}, func(management.Operation) { t.Fatal("unexpected mutation") })
	for _, attack := range []struct{ method, path, bearer string }{
		{"GET", "/volumes/" + string(volume.ID), adminBearer},
		{"PATCH", "/volumes/" + string(volume.ID), adminBearer},
		{"PUT", "/volumes/" + string(volume.ID) + "/quota", adminBearer},
		{"DELETE", "/volumes/" + string(volume.ID), adminBearer},
		{"GET", "/volumes/" + string(volume.ID), otherBearer},
		{"GET", "/operations/" + string(operation.ID), adminBearer},
		{"GET", "/admin/users/" + string(f.admin.ID) + "/volumes", adminBearer},
		{"GET", "/admin/users/" + string(ordinary.ID) + "/volumes", ownerBearer},
	} {
		response := routeRequest(handler, attack.method, attack.path, attack.bearer, "{}")
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s %s: %d %s", attack.method, attack.path, response.Code, response.Body.String())
		}
	}
	if opened.Load() != 0 {
		t.Fatal("denied target activated")
	}
	if response := routeRequest(handler, "GET", "/volumes/"+string(volume.ID), "", ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous: %d", response.Code)
	}
	adminSummary := routeRequest(handler, "GET", "/admin/users/"+string(ordinary.ID)+"/volumes", adminBearer, "")
	if adminSummary.Code != http.StatusOK {
		t.Fatal(adminSummary.Body.String())
	}
	var adminDTO serviceapi.UserVolumeList
	if err := json.Unmarshal(adminSummary.Body.Bytes(), &adminDTO); err != nil {
		t.Fatal(err)
	}
	if len(adminDTO.Volumes) != 1 || adminDTO.Volumes[0].Summary.FileCount != "9007199254740993" || adminDTO.Volumes[0].Summary.UsedBytes != "9007199254740995" {
		t.Fatalf("inexact summary: %+v", adminDTO)
	}
	for _, forbidden := range []string{"id", "ownerUserId", "state", "revision", "quotaRevision", "storageName", "fileUrl", "authEpoch"} {
		if strings.Contains(adminSummary.Body.String(), `"`+forbidden+`"`) {
			t.Fatalf("admin metadata leak: %s", forbidden)
		}
	}
	owner := routeRequest(handler, "GET", "/volumes/"+string(volume.ID), ownerBearer, "")
	if owner.Code != http.StatusOK || opened.Load() != 2 {
		t.Fatalf("owner summary: %d opens=%d", owner.Code, opened.Load())
	}
}

func TestManagementRoutesScopedCredentialDoesNotActivateUnselectedVolume(t *testing.T) {
	f := newAuthFixture(t, nil)
	selected, _ := routeVolume(t, f, f.admin, "Selected")
	unselected, _ := routeVolume(t, f, f.admin, "Unselected")
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, false, []management.ID{selected.ID})
	var opened atomic.Int64
	handler := routeHandler(t, f, func(ctx context.Context, v management.Volume) (storage.VolumeSummaryResult, error) {
		if v.ID != selected.ID {
			t.Error("unselected volume activated")
		}
		opened.Add(1)
		return exactRouteSummary(ctx, v)
	}, func(management.Operation) { t.Fatal("limited credential created volume") })
	list := routeRequest(handler, "GET", "/volumes", bearer, "")
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), string(unselected.ID)) || opened.Load() != 1 {
		t.Fatalf("scoped list: %d %s", list.Code, list.Body.String())
	}
	for _, request := range []struct{ method, path, body string }{{"GET", "/volumes/" + string(unselected.ID), ""}, {"POST", "/volumes", `{"action":{"namespace":"1","nonce":"create"},"name":"No","quotaBytes":"999999999999"}`}} {
		response := routeRequest(handler, request.method, request.path, bearer, request.body)
		if response.Code != http.StatusForbidden {
			t.Fatalf("limited scope: %d %s", response.Code, response.Body.String())
		}
	}
	if opened.Load() != 1 {
		t.Fatal("denied scope activated")
	}
}

func TestManagementRoutesOperationQueriesHonorCurrentCredentialSelection(t *testing.T) {
	f := newAuthFixture(t, nil)
	selected, selectedOperation := routeVolume(t, f, f.admin, "Selected")
	other, otherOperation := routeVolume(t, f, f.admin, "Other")
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead}, false, []management.ID{selected.ID})
	var opened atomic.Int64
	handler := routeHandler(t, f, func(ctx context.Context, volume management.Volume) (storage.VolumeSummaryResult, error) {
		opened.Add(1)
		return exactRouteSummary(ctx, volume)
	}, func(management.Operation) { t.Fatal("query woke mutation") })
	for _, item := range []struct {
		operation management.Operation
		status    int
	}{{selectedOperation, http.StatusOK}, {otherOperation, http.StatusForbidden}} {
		for _, path := range []string{"/operations/" + string(item.operation.ID), "/operations?namespace=" + strconv.FormatInt(item.operation.Action.Namespace, 10) + "&nonce=" + url.QueryEscape(item.operation.Action.Nonce)} {
			response := routeRequest(handler, "GET", path, bearer, "")
			if response.Code != item.status {
				t.Fatalf("selected receipt: %d %s", response.Code, response.Body.String())
			}
			if item.status == http.StatusForbidden && strings.Contains(response.Body.String(), string(other.ID)) {
				t.Fatal("unselected receipt leaked")
			}
		}
	}
	if opened.Load() != 0 {
		t.Fatal("receipt queries activated native volume")
	}
	secret, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		t.Fatal(err)
	}
	csrf, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	session, err := f.store.CreateSession(t.Context(), management.SessionInput{UserID: f.admin.ID, AuthEpoch: f.admin.AuthEpoch, SecretDigest: digest, CSRFToken: csrf})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "https://service.invalid"+serviceapi.Prefix+"/operations/"+string(otherOperation.ID), nil)
	request.AddCookie(&http.Cookie{Name: authCookieName, Value: string(session.ID) + "." + secret})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("browser own receipt: %d %s", response.Code, response.Body.String())
	}
}

func TestManagementRoutesStrictMutationAndStableActionRecovery(t *testing.T) {
	f := newAuthFixture(t, nil)
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
	var accepted atomic.Int64
	handler := routeHandler(t, f, exactRouteSummary, func(management.Operation) { accepted.Add(1) })
	for _, body := range []string{`{"action":{"namespace":"1","namespace":"1","nonce":"x"},"name":"x","quotaBytes":"99999999999"}`, `{"action":{"namespace":"1","nonce":"x"},"name":"x","quotaBytes":"99999999999","extra":false}`, `{"action":{"namespace":1,"nonce":"x"},"name":"x","quotaBytes":"99999999999"}`, `{"action":{"namespace":"01","nonce":"x"},"name":"x","quotaBytes":"99999999999"}`, `{"Action":{"namespace":"1","nonce":"x"},"name":"x","quotaBytes":"99999999999"}`, `{"action":{"namespace":"1","nonce":"x"},"name":"\ud800","quotaBytes":"99999999999"}`, `{} {}`, strings.Repeat(" ", 65537)} {
		response := routeRequest(handler, "POST", "/volumes", bearer, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid body: %d %s", response.Code, response.Body.String())
		}
	}
	response := routeRequest(handler, "POST", "/action-namespaces", bearer, "{}")
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var namespace serviceapi.ActionNamespace
	if err := json.Unmarshal(response.Body.Bytes(), &namespace); err != nil {
		t.Fatal(err)
	}
	request := `{"action":{"namespace":"` + string(namespace.Namespace) + `","nonce":"create"},"name":"Created","quotaBytes":"99999999999"}`
	response = routeRequest(handler, "POST", "/volumes", bearer, request)
	if response.Code != http.StatusAccepted || accepted.Load() != 1 {
		t.Fatalf("accept: %d %s", response.Code, response.Body.String())
	}
	var operation serviceapi.Operation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	_, replacement := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
	for _, path := range []string{"/operations/" + operation.ID, "/operations?namespace=" + string(namespace.Namespace) + "&nonce=create"} {
		response = routeRequest(handler, "GET", path, replacement, "")
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), operation.ID) {
			t.Fatalf("stable actor recovery: %d %s", response.Code, response.Body.String())
		}
	}
	conflict := routeRequest(handler, "POST", "/volumes", replacement, strings.Replace(request, "Created", "Different", 1))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("effect conflict: %d", conflict.Code)
	}
}

func TestManagementRoutesAdminSummaryCursorIsOpaqueAndBoundToTarget(t *testing.T) {
	f := newAuthFixture(t, nil)
	ordinary := routeUser(t, f)
	other := routeUser(t, f)
	first, _ := routeVolume(t, f, ordinary, "First")
	second, _ := routeVolume(t, f, ordinary, "Second")
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeAdminSummaries}, false, nil)
	handler := routeHandler(t, f, exactRouteSummary, func(management.Operation) {})
	base := "/admin/users/" + string(ordinary.ID) + "/volumes"
	response := routeRequest(handler, "GET", base+"?limit=1", bearer, "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var page serviceapi.UserVolumeList
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Volumes) != 1 || page.NextAfter == "" || strings.Contains(response.Body.String(), string(first.ID)) || strings.Contains(response.Body.String(), string(second.ID)) {
		t.Fatalf("opaque page: %s", response.Body.String())
	}
	response = routeRequest(handler, "GET", base+"?limit=1&after="+url.QueryEscape(page.NextAfter), bearer, "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var next serviceapi.UserVolumeList
	if err := json.Unmarshal(response.Body.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if len(next.Volumes) != 1 || next.Volumes[0].Name == page.Volumes[0].Name {
		t.Fatalf("cursor did not advance: %+v", next)
	}
	for _, path := range []string{base + "?after=" + string(first.ID), "/admin/users/" + string(other.ID) + "/volumes?after=" + url.QueryEscape(page.NextAfter), base + "?after=" + url.QueryEscape(page.NextAfter+"A")} {
		if response := routeRequest(handler, "GET", path, bearer, ""); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid cursor: %d", response.Code)
		}
	}
	restarted := routeHandler(t, f, exactRouteSummary, func(management.Operation) {})
	if response := routeRequest(restarted, "GET", base+"?after="+url.QueryEscape(page.NextAfter), bearer, ""); response.Code != http.StatusBadRequest {
		t.Fatalf("retired cursor: %d", response.Code)
	}
	users := routeRequest(handler, "GET", "/admin/users?limit=1", bearer, "")
	if users.Code != http.StatusOK || !strings.Contains(users.Body.String(), `"role":"user"`) {
		t.Fatalf("user directory: %d %s", users.Code, users.Body.String())
	}
}

func TestManagementRoutesSummaryErrorsDoNotInventDataAndRecheckRevocation(t *testing.T) {
	f := newAuthFixture(t, nil)
	volume, _ := routeVolume(t, f, f.admin, "Unavailable")
	credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead}, true, nil)
	handler := routeHandler(t, f, func(context.Context, management.Volume) (storage.VolumeSummaryResult, error) {
		return storage.VolumeSummaryResult{}, errors.New("/private/root/metadata.sqlite: SELECT secret")
	}, func(management.Operation) {})
	response := routeRequest(handler, "GET", "/volumes/"+string(volume.ID), bearer, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"summaryError":{"code":"metadata_unavailable"}`) || strings.Contains(response.Body.String(), `"summary":`) || strings.Contains(response.Body.String(), "private") {
		t.Fatalf("summary failure: %d %s", response.Code, response.Body.String())
	}
	handler = routeHandler(t, f, func(ctx context.Context, v management.Volume) (storage.VolumeSummaryResult, error) {
		if err := f.store.RevokeCredential(ctx, f.admin.ID, credential.ID); err != nil {
			t.Fatal(err)
		}
		return exactRouteSummary(ctx, v)
	}, func(management.Operation) {})
	response = routeRequest(handler, "GET", "/volumes/"+string(volume.ID), bearer, "")
	if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), "Unavailable") {
		t.Fatalf("revoked output leaked: %d %s", response.Code, response.Body.String())
	}
}

func TestManagementRoutesBrowserMutationRequiresCSRFAndExactOrigin(t *testing.T) {
	f := newAuthFixture(t, nil)
	secret, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		t.Fatal(err)
	}
	csrf, err := randomAuthSecret()
	if err != nil {
		t.Fatal(err)
	}
	session, err := f.store.CreateSession(t.Context(), management.SessionInput{UserID: f.admin.ID, AuthEpoch: f.admin.AuthEpoch, SecretDigest: digest, CSRFToken: csrf})
	if err != nil {
		t.Fatal(err)
	}
	handler := routeHandler(t, f, exactRouteSummary, func(management.Operation) {})
	for _, headers := range []map[string]string{{}, {"Origin": f.config.Origin}, {"Origin": "https://foreign.invalid", "X-CSRF-Token": csrf}} {
		request := httptest.NewRequest("POST", "https://service.invalid"+serviceapi.Prefix+"/action-namespaces", strings.NewReader("{}"))
		request.Header.Set("Content-Type", "application/json")
		request.AddCookie(&http.Cookie{Name: authCookieName, Value: string(session.ID) + "." + secret})
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("csrf gate: %d %s", response.Code, response.Body.String())
		}
	}
}

type revokingRouteBody struct {
	reader io.Reader
	revoke func()
}

func (b *revokingRouteBody) Read(data []byte) (int, error) {
	if b.revoke != nil {
		revoke := b.revoke
		b.revoke = nil
		revoke()
	}
	return b.reader.Read(data)
}
func (*revokingRouteBody) Close() error { return nil }

func TestManagementRoutesRecheckAuthenticationAfterReceivingMutationBody(t *testing.T) {
	f := newAuthFixture(t, nil)
	volume, _ := routeVolume(t, f, f.admin, "Target")
	var accepted atomic.Int64
	handler := routeHandler(t, f, exactRouteSummary, func(management.Operation) { accepted.Add(1) })
	for _, kind := range []string{"create", "rename", "quota", "delete", "namespace"} {
		t.Run(kind, func(t *testing.T) {
			credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
			namespace, err := f.store.IssueNamespace(t.Context(), f.admin.ID)
			if err != nil {
				t.Fatal(err)
			}
			action := `"action":{"namespace":"` + strconv.FormatInt(namespace.Sequence, 10) + `","nonce":"body-revoke"}`
			method, path, body := "POST", "/volumes", `{`+action+`,"name":"New","quotaBytes":"99999999999"}`
			switch kind {
			case "rename":
				method, path, body = "PATCH", "/volumes/"+string(volume.ID), `{`+action+`,"expectedRevision":"1","name":"Changed"}`
			case "quota":
				method, path, body = "PUT", "/volumes/"+string(volume.ID)+"/quota", `{`+action+`,"expectedRevision":"1","quotaBytes":"99999999999"}`
			case "delete":
				method, path, body = "DELETE", "/volumes/"+string(volume.ID), `{`+action+`,"expectedRevision":"1"}`
			case "namespace":
				path, body = "/action-namespaces", "{}"
			}
			request := httptest.NewRequest(method, "https://service.invalid"+serviceapi.Prefix+path, nil)
			request.Header.Set("Authorization", "Bearer "+bearer)
			request.Header.Set("Content-Type", "application/json")
			request.Body = &revokingRouteBody{reader: strings.NewReader(body), revoke: func() {
				if err := f.store.RevokeCredential(t.Context(), f.admin.ID, credential.ID); err != nil {
					t.Fatal(err)
				}
			}}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("revoked mutation: %d %s", response.Code, response.Body.String())
			}
			if accepted.Load() != 0 {
				t.Fatal("revoked mutation scheduled work")
			}
			if kind != "namespace" {
				if _, err := f.store.FindOperation(t.Context(), f.admin.ID, management.ActionKey{Namespace: namespace.Sequence, Nonce: "body-revoke"}); !errors.Is(err, management.ErrNotFound) {
					t.Fatalf("revoked action persisted: %v", err)
				}
			} else {
				next, err := f.store.IssueNamespace(t.Context(), f.admin.ID)
				if err != nil || next.Sequence != namespace.Sequence+1 {
					t.Fatalf("revoked namespace persisted: %+v %v", next, err)
				}
			}
		})
	}
}

func TestManagementRoutesOwnMutationReceiptsAndBoundedPagination(t *testing.T) {
	f := newAuthFixture(t, nil)
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
	var accepted atomic.Int64
	handler := routeHandler(t, f, exactRouteSummary, func(management.Operation) { accepted.Add(1) })
	for _, kind := range []string{"rename", "quota", "delete"} {
		volume, _ := routeVolume(t, f, f.admin, kind)
		namespace, err := f.store.IssueNamespace(t.Context(), f.admin.ID)
		if err != nil {
			t.Fatal(err)
		}
		action := `"action":{"namespace":"` + strconv.FormatInt(namespace.Sequence, 10) + `","nonce":"mutate"},"expectedRevision":"1"`
		method, path, body, status := "PATCH", "/volumes/"+string(volume.ID), `{`+action+`,"name":"Renamed"}`, http.StatusOK
		switch kind {
		case "quota":
			method, path, body, status = "PUT", path+"/quota", `{`+action+`,"quotaBytes":"99999999999"}`, http.StatusAccepted
		case "delete":
			method, body, status = "DELETE", `{`+action+`}`, http.StatusAccepted
		}
		response := routeRequest(handler, method, path, bearer, body)
		if response.Code != status || response.Header().Get("Location") == "" {
			t.Fatalf("own %s: %d %s", kind, response.Code, response.Body.String())
		}
		var operation serviceapi.Operation
		if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
			t.Fatal(err)
		}
		if operation.Kind != kind || operation.VolumeID != string(volume.ID) {
			t.Fatalf("wrong operation: %+v", operation)
		}
	}
	if accepted.Load() != 2 {
		t.Fatalf("accepted pending callbacks: %d", accepted.Load())
	}
	namespace, err := f.store.IssueNamespace(t.Context(), f.admin.ID)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := f.store.CreateVolume(t.Context(), f.admin.ID, management.ActionKey{Namespace: namespace.Sequence, Nonce: "failed"}, "Failed", limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	failed, err = f.store.TransitionOperation(t.Context(), failed.ID, failed.Revision, management.OperationTransition{State: management.OperationFailed, Phase: management.PhaseAccepted, Settled: true, ErrorCode: "volume-busy"})
	if err != nil {
		t.Fatal(err)
	}
	failure := routeRequest(handler, "GET", "/operations/"+string(failed.ID), bearer, "")
	if failure.Code != http.StatusOK || !strings.Contains(failure.Body.String(), `"error":{"code":"condition_conflict"}`) {
		t.Fatalf("failure receipt: %d %s", failure.Code, failure.Body.String())
	}
	me := routeRequest(handler, "GET", "/me", bearer, "")
	if me.Code != http.StatusOK || !strings.Contains(me.Body.String(), string(f.admin.ID)) {
		t.Fatalf("identity: %d %s", me.Code, me.Body.String())
	}
	page := routeRequest(handler, "GET", "/volumes?limit=1", bearer, "")
	var volumes serviceapi.VolumeList
	if page.Code != http.StatusOK {
		t.Fatal(page.Body.String())
	}
	if err := json.Unmarshal(page.Body.Bytes(), &volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Volumes) != 1 || volumes.NextAfter == "" {
		t.Fatalf("page: %+v", volumes)
	}
	page = routeRequest(handler, "GET", "/volumes?limit=1&after="+volumes.NextAfter, bearer, "")
	if page.Code != http.StatusOK {
		t.Fatal(page.Body.String())
	}
	for _, path := range []string{"/me?extra=true", "/volumes?limit=51", "/volumes?limit=01", "/volumes?limit=1&limit=2", "/volumes?after=invalid", "/operations?namespace=01&nonce=x", "/operations?namespace=1&nonce=x&extra=y"} {
		if response := routeRequest(handler, "GET", path, bearer, ""); response.Code != http.StatusBadRequest {
			t.Fatalf("query %s: %d", path, response.Code)
		}
	}
}

func TestManagementRoutesNamespaceIssuerRequiresFinalAuthorizationAndValidResult(t *testing.T) {
	f := newAuthFixture(t, nil)
	credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesWrite}, true, nil)
	var called atomic.Int64
	issuer := func(ctx context.Context, actor management.ID) (management.ActionNamespace, error) {
		called.Add(1)
		return management.ActionNamespace{Actor: actor, Sequence: 77, AcceptUntil: time.Now().Add(time.Hour)}, nil
	}
	for _, issuers := range [][]func(context.Context, management.ID) (management.ActionNamespace, error){nil, {nil}, {issuer, issuer}} {
		if _, err := newManagementHandler(f.auth, f.store, exactRouteSummary, func(management.Operation) {}, issuers...); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid namespace issuer: %v", err)
		}
	}
	handler, err := newManagementHandler(f.auth, f.store, exactRouteSummary, func(management.Operation) {}, issuer)
	if err != nil {
		t.Fatal(err)
	}
	response := routeRequest(handler, "POST", "/action-namespaces", bearer, "{}")
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"namespace":"77"`) || called.Load() != 1 {
		t.Fatalf("custom issuer: %d %s calls=%d", response.Code, response.Body.String(), called.Load())
	}
	if response := routeRequest(handler, "POST", "/action-namespaces", "", "{}"); response.Code != http.StatusUnauthorized || called.Load() != 1 {
		t.Fatalf("anonymous reached issuer: %d", response.Code)
	}
	request := httptest.NewRequest("POST", "https://service.invalid"+serviceapi.Prefix+"/action-namespaces", nil)
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	request.Body = &revokingRouteBody{reader: strings.NewReader("{}"), revoke: func() {
		if err := f.store.RevokeCredential(t.Context(), f.admin.ID, credential.ID); err != nil {
			t.Fatal(err)
		}
	}}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || called.Load() != 1 {
		t.Fatalf("revoked body reached issuer: %d calls=%d", response.Code, called.Load())
	}
	_, bearer = routeCredential(t, f, f.admin, []string{ScopeVolumesWrite}, true, nil)
	for _, result := range []management.ActionNamespace{{}, {Actor: f.admin.ID, Sequence: 1}, {Actor: management.ID("00000000000000000000000000000000"), Sequence: 1, AcceptUntil: time.Now().Add(time.Hour)}} {
		handler, err = newManagementHandler(f.auth, f.store, exactRouteSummary, func(management.Operation) {}, func(context.Context, management.ID) (management.ActionNamespace, error) { return result, nil })
		if err != nil {
			t.Fatal(err)
		}
		response = routeRequest(handler, "POST", "/action-namespaces", bearer, "{}")
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("invalid namespace result: %d %s", response.Code, response.Body.String())
		}
	}
	handler, err = newManagementHandler(f.auth, f.store, exactRouteSummary, func(management.Operation) {}, func(context.Context, management.ID) (management.ActionNamespace, error) {
		return management.ActionNamespace{}, errors.New("issuer storage failure")
	})
	if err != nil {
		t.Fatal(err)
	}
	response = routeRequest(handler, "POST", "/action-namespaces", bearer, "{}")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "issuer storage failure") {
		t.Fatalf("issuer failure: %d %s", response.Code, response.Body.String())
	}
}
