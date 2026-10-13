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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
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
	_, err = db.Exec("INSERT INTO users(id,role,display_name,auth_epoch,created_at) VALUES(?,'user','Ordinary owner',1,0)", id)
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

func routeVolume(t *testing.T, f *authFixture, user management.User, name string) management.Volume {
	t.Helper()
	volume, err := f.store.CreateVolume(t.Context(), user.ID, name, 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err = f.store.SetVolumeState(t.Context(), user.ID, volume.ID, volume.ManagementRevision, management.VolumeActive, "")
	if err != nil {
		t.Fatal(err)
	}
	return volume
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

func exactRouteSummary(context.Context, management.Volume) (VolumeSummary, error) {
	return VolumeSummary{FileCount: 9007199254740993, DirectoryCount: 7, UsedBytes: 9007199254740995, QuotaBytes: 9007199254740999}, nil
}

func routeHandler(t *testing.T, f *authFixture, summary func(context.Context, management.Volume) (VolumeSummary, error), process func(context.Context, management.Volume) (management.Volume, error)) http.Handler {
	t.Helper()
	if process == nil {
		process = func(ctx context.Context, v management.Volume) (management.Volume, error) {
			state := management.VolumeActive
			if v.State == management.VolumeDeleting {
				state = management.VolumeDeleted
			}
			return f.store.SetVolumeState(ctx, v.OwnerUserID, v.ID, v.ManagementRevision, state, "")
		}
	}
	handler, err := newManagementHandler(f.auth, f.store, summary, process)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestManagementRoutesOwnerAndAdminIsolationBeforeSummary(t *testing.T) {
	f := newAuthFixture(t, nil)
	ordinary, other := routeUser(t, f), routeUser(t, f)
	volume := routeVolume(t, f, ordinary, "Private volume")
	_, adminBearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeAdminSummaries, ScopeFiles}, true, nil)
	_, ownerBearer := routeCredential(t, f, ordinary, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles}, true, nil)
	_, otherBearer := routeCredential(t, f, other, []string{ScopeVolumesRead, ScopeFiles}, true, nil)
	var opened atomic.Int64
	handler := routeHandler(t, f, func(ctx context.Context, v management.Volume) (VolumeSummary, error) {
		opened.Add(1)
		return exactRouteSummary(ctx, v)
	}, nil)
	for _, attack := range []struct{ method, path, bearer string }{
		{"GET", "/volumes/" + string(volume.ID), adminBearer},
		{"PATCH", "/volumes/" + string(volume.ID), adminBearer},
		{"DELETE", "/volumes/" + string(volume.ID), adminBearer},
		{"GET", "/volumes/" + string(volume.ID), otherBearer},
		{"GET", "/admin/users/" + string(f.admin.ID) + "/volumes", adminBearer},
		{"GET", "/admin/users/" + string(ordinary.ID) + "/volumes", ownerBearer},
	} {
		response := routeRequest(handler, attack.method, attack.path, attack.bearer, "")
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
	response := routeRequest(handler, "GET", "/admin/users/"+string(ordinary.ID)+"/volumes", adminBearer, "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var values serviceapi.UserVolumeList
	if err := json.Unmarshal(response.Body.Bytes(), &values); err != nil {
		t.Fatal(err)
	}
	if len(values.Volumes) != 1 || values.Volumes[0].Summary.FileCount != "9007199254740993" || values.Volumes[0].Summary.UsedBytes != "9007199254740995" {
		t.Fatalf("inexact summary: %+v", values)
	}
	for _, field := range []string{"id", "ownerUserId", "state", "revision", "quotaRevision", "storageName", "fileUrl", "authEpoch"} {
		if strings.Contains(response.Body.String(), `"`+field+`"`) {
			t.Fatalf("admin metadata leak: %s", field)
		}
	}
	response = routeRequest(handler, "GET", "/volumes/"+string(volume.ID), ownerBearer, "")
	if response.Code != http.StatusOK || opened.Load() != 2 {
		t.Fatalf("owner summary: %d opens=%d", response.Code, opened.Load())
	}
}

func TestManagementRoutesScopedCredentialDoesNotActivateUnselectedVolume(t *testing.T) {
	f := newAuthFixture(t, nil)
	selected := routeVolume(t, f, f.admin, "Selected")
	unselected := routeVolume(t, f, f.admin, "Unselected")
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, false, []management.ID{selected.ID})
	var opened atomic.Int64
	handler := routeHandler(t, f, func(ctx context.Context, v management.Volume) (VolumeSummary, error) {
		if v.ID != selected.ID {
			t.Error("unselected volume activated")
		}
		opened.Add(1)
		return exactRouteSummary(ctx, v)
	}, nil)
	list := routeRequest(handler, "GET", "/volumes", bearer, "")
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), string(unselected.ID)) || opened.Load() != 1 {
		t.Fatalf("scoped list: %d %s", list.Code, list.Body.String())
	}
	for _, request := range []struct{ method, path, body string }{{"GET", "/volumes/" + string(unselected.ID), ""}, {"PATCH", "/volumes/" + string(unselected.ID), `{"name":"Changed"}`}, {"DELETE", "/volumes/" + string(unselected.ID), ""}, {"POST", "/volumes", `{"name":"New","quotaBytes":"4096"}`}} {
		response := routeRequest(handler, request.method, request.path, bearer, request.body)
		if response.Code != http.StatusForbidden {
			t.Fatalf("limited scope: %d %s", response.Code, response.Body.String())
		}
	}
	if opened.Load() != 1 {
		t.Fatal("denied scope activated")
	}
}

func TestManagementRoutesStrictMutationAndSynchronousCRUD(t *testing.T) {
	f := newAuthFixture(t, nil)
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
	handler := routeHandler(t, f, exactRouteSummary, nil)
	for _, body := range []string{`{"name":"x","name":"x","quotaBytes":"4096"}`, `{"name":"x","quotaBytes":"4096","extra":false}`, `{"name":"x","quotaBytes":4096}`, `{"name":"x","quotaBytes":"04096"}`, `{"Name":"x","quotaBytes":"4096"}`, `{"name":"\ud800","quotaBytes":"4096"}`, `{"name":"x","quotaBytes":"4095"}`, `{} {}`, strings.Repeat(" ", 65537)} {
		response := routeRequest(handler, "POST", "/volumes", bearer, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid body: %d %s", response.Code, response.Body.String())
		}
	}
	response := routeRequest(handler, "POST", "/volumes", bearer, `{"name":"Created","quotaBytes":"4096"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, response.Body.String())
	}
	var volume serviceapi.Volume
	if err := json.Unmarshal(response.Body.Bytes(), &volume); err != nil {
		t.Fatal(err)
	}
	if volume.State != "active" || response.Header().Get("Location") != serviceapi.Prefix+"/volumes/"+volume.ID {
		t.Fatalf("created volume: %+v", volume)
	}
	response = routeRequest(handler, "PATCH", "/volumes/"+volume.ID, bearer, `{"name":"Renamed"}`)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"Renamed"`) {
		t.Fatalf("rename: %d %s", response.Code, response.Body.String())
	}
	if response := routeRequest(handler, "DELETE", "/volumes/"+volume.ID, bearer, "{}"); response.Code != http.StatusBadRequest {
		t.Fatalf("delete body: %d", response.Code)
	}
	response = routeRequest(handler, "DELETE", "/volumes/"+volume.ID, bearer, "")
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
		t.Fatalf("delete: %d %s", response.Code, response.Body.String())
	}
	if _, err := f.store.GetVolume(t.Context(), f.admin.ID, management.ID(volume.ID)); !errors.Is(err, management.ErrNotFound) {
		t.Fatalf("completed deletion remains in catalog: %v", err)
	}
	for _, path := range []string{"/me?extra=true", "/volumes?limit=51", "/volumes?limit=01", "/volumes?limit=1&limit=2", "/volumes?after=invalid"} {
		if response := routeRequest(handler, "GET", path, bearer, ""); response.Code != http.StatusBadRequest {
			t.Fatalf("query %s: %d", path, response.Code)
		}
	}
}

func TestManagementRoutesAdminSummaryCursorIsOpaqueAndBoundToTarget(t *testing.T) {
	f := newAuthFixture(t, nil)
	ordinary, other := routeUser(t, f), routeUser(t, f)
	first, second := routeVolume(t, f, ordinary, "First"), routeVolume(t, f, ordinary, "Second")
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeAdminSummaries}, false, nil)
	handler := routeHandler(t, f, exactRouteSummary, nil)
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
	restarted := routeHandler(t, f, exactRouteSummary, nil)
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
	volume := routeVolume(t, f, f.admin, "Unavailable")
	credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead}, true, nil)
	handler := routeHandler(t, f, func(context.Context, management.Volume) (VolumeSummary, error) {
		return VolumeSummary{}, errors.New("/private/root/metadata.sqlite: SELECT secret")
	}, nil)
	response := routeRequest(handler, "GET", "/volumes/"+string(volume.ID), bearer, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"summaryError":{"code":"metadata_unavailable"}`) || strings.Contains(response.Body.String(), `"summary":`) || strings.Contains(response.Body.String(), "private") {
		t.Fatalf("summary failure: %d %s", response.Code, response.Body.String())
	}
	handler = routeHandler(t, f, func(ctx context.Context, v management.Volume) (VolumeSummary, error) {
		if err := f.store.RevokeCredential(ctx, f.admin.ID, credential.ID); err != nil {
			t.Fatal(err)
		}
		return exactRouteSummary(ctx, v)
	}, nil)
	response = routeRequest(handler, "GET", "/volumes/"+string(volume.ID), bearer, "")
	if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), "Unavailable") {
		t.Fatalf("revoked output leaked: %d %s", response.Code, response.Body.String())
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
	volume := routeVolume(t, f, f.admin, "Target")
	var processed atomic.Int64
	handler := routeHandler(t, f, exactRouteSummary, func(context.Context, management.Volume) (management.Volume, error) {
		processed.Add(1)
		t.Fatal("revoked mutation reached processing")
		return management.Volume{}, nil
	})
	for _, kind := range []string{"create", "rename", "delete"} {
		t.Run(kind, func(t *testing.T) {
			credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
			method, path, body := "POST", "/volumes", `{"name":"New","quotaBytes":"4096"}`
			if kind == "rename" {
				method, path, body = "PATCH", "/volumes/"+string(volume.ID), `{"name":"Changed"}`
			}
			if kind == "delete" {
				method, path, body = "DELETE", "/volumes/"+string(volume.ID), ""
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
			volumes, err := f.store.ListVolumes(t.Context(), f.admin.ID, "", 50)
			if err != nil || len(volumes) != 1 || volumes[0].ID != volume.ID || volumes[0].DisplayName != "Target" || volumes[0].State != management.VolumeActive || processed.Load() != 0 {
				t.Fatalf("revoked mutation persisted: %+v %v", volumes, err)
			}
		})
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
	handler := routeHandler(t, f, exactRouteSummary, nil)
	for _, headers := range []map[string]string{{}, {"Origin": f.config.Origin}, {"Origin": "https://foreign.invalid", "X-CSRF-Token": csrf}} {
		request := httptest.NewRequest("POST", "https://service.invalid"+serviceapi.Prefix+"/volumes", strings.NewReader(`{"name":"Created","quotaBytes":"4096"}`))
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

func TestManagementRoutesProcessingFailureCannotReportSuccess(t *testing.T) {
	f := newAuthFixture(t, nil)
	_, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, true, nil)
	handler := routeHandler(t, f, exactRouteSummary, func(ctx context.Context, v management.Volume) (management.Volume, error) {
		if _, err := f.store.SetVolumeState(ctx, v.OwnerUserID, v.ID, v.ManagementRevision, management.VolumeFailed, "storage_unavailable"); err != nil {
			t.Fatal(err)
		}
		return management.Volume{}, errors.New("private backing path failed")
	})
	response := routeRequest(handler, "POST", "/volumes", bearer, `{"name":"Failed","quotaBytes":"4096"}`)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private") {
		t.Fatalf("false create success: %d %s", response.Code, response.Body.String())
	}
	volumes, err := f.store.ListVolumes(t.Context(), f.admin.ID, "", 50)
	if err != nil || len(volumes) != 1 || volumes[0].State != management.VolumeFailed {
		t.Fatalf("failure status: %+v %v", volumes, err)
	}
}
