package serviceapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testID = "0123456789abcdef0123456789abcdef"
const secondID = "1123456789abcdef0123456789abcdef"

func testCredential() Credential {
	return Credential{ID: testID, Secret: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
}
func testVolume() Volume {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	return Volume{ID: testID, OwnerUserID: secondID, Name: "files", State: "active", Revision: "9007199254740993", CreatedAt: now, UpdatedAt: now, Summary: &VolumeSummary{FileCount: "9007199254740993", DirectoryCount: "0", UsedBytes: "4096", QuotaBytes: "9223372036854775807", QuotaRevision: "1"}}
}
func testOperation() Operation {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	return Operation{ID: testID, VolumeID: secondID, Kind: "create", State: "succeeded", Phase: "completed", Revision: "1", Settled: true, CreatedAt: now, UpdatedAt: now}
}
func testCredentialInfo() CredentialInfo {
	return CredentialInfo{ID: testID, Label: "cli", Scopes: []string{"files"}, AllOwnedVolumes: true, Generation: "1", ExpiresAt: time.Now().Add(time.Hour)}
}

func TestDecimalExactAndRejectsCoercion(t *testing.T) {
	for _, value := range []string{"0", "9007199254740993", "9223372036854775807", "18446744073709551615"} {
		encoded, err := json.Marshal(Decimal(value))
		if err != nil {
			t.Fatal(err)
		}
		var decoded Decimal
		if err := json.Unmarshal(encoded, &decoded); err != nil || string(decoded) != value {
			t.Fatalf("%s: %s %v", value, decoded, err)
		}
	}
	for _, input := range []string{`0`, `null`, `""`, `"00"`, `"01"`, `"+1"`, `"-1"`, `"1.0"`, `"1e2"`, `"18446744073709551616"`} {
		var value Decimal
		if json.Unmarshal([]byte(input), &value) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	if _, err := Decimal("9223372036854775808").Int64(); err == nil {
		t.Fatal("accepted signed overflow")
	}
	if _, err := json.Marshal(DecimalFromInt64(-1)); err == nil {
		t.Fatal("marshaled negative")
	}
	if DecimalFromUint64(1) != "1" || DecimalFromInt64(1) != "1" {
		t.Fatal("format")
	}
}

func TestDecodeRejectsAmbiguityAndUnknownFields(t *testing.T) {
	for _, input := range []string{`{"code":"x","code":"x"}`, `{"code":"x","CODE":"y"}`, `{"CODE":"x"}`, `{}`, `{"code":null}`, `{"code":"\ud800"}`, `{"code":"\udfff"}`, `{"code":"\ud800\u0041"}`, `{"code":"x"} {}`, `{"code":"x","unexpected":1}`, `null`, `{"code":}`, string([]byte{'{', '"', 'c', 'o', 'd', 'e', '"', ':', '"', 0xff, '"', '}'}), strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66)} {
		var output ErrorDetail
		if DecodeJSON([]byte(input), &output) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	var output CredentialInfo
	valid, err := json.Marshal(testCredentialInfo())
	if err != nil {
		t.Fatal(err)
	}
	if err := DecodeJSON(valid, &output); err != nil {
		t.Fatal(err)
	}
	var detail ErrorDetail
	for _, valid := range []string{`{"code":"\ud83d\ude00"}`, `{"code":"\\ud800"}`} {
		if err := DecodeJSON([]byte(valid), &detail); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientRejectsFalseTerminalOperations(t *testing.T) {
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Phase = "accepted" },
		func(o *Operation) { o.Error = &ErrorDetail{Code: "storage_unavailable"} },
		func(o *Operation) { o.State = "failed" },
		func(o *Operation) { o.State = "pending"; o.Phase = "accepted" },
		func(o *Operation) { o.Kind = "quota"; o.Phase = "bound"; o.State = "running"; o.Settled = false },
	} {
		operation := testOperation()
		mutate(&operation)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(operation) }))
		client, _ := NewClient(server.URL, server.Client(), Credential{})
		if _, err := client.WaitOperation(context.Background(), testID, time.Millisecond); err == nil {
			t.Fatalf("false terminal %+v accepted", operation)
		}
		server.Close()
	}
	operation := testOperation()
	operation.State = "failed"
	operation.Phase = "initializing"
	operation.Error = &ErrorDetail{Code: "storage_unavailable"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(operation) }))
	defer server.Close()
	client, _ := NewClient(server.URL, server.Client(), Credential{})
	got, err := client.WaitOperation(context.Background(), testID, time.Millisecond)
	if err != nil || got.State != "failed" {
		t.Fatalf("settled failure rejected: %+v %v", got, err)
	}
	if _, err := client.WaitOperation(context.Background(), testID, 0); err == nil {
		t.Fatal("zero interval accepted")
	}
}

func TestClientRequiresExplicitListTruthAndCredentialRotationMetadata(t *testing.T) {
	for _, body := range []string{`{}`, `{"volumes":null}`, `{"volumes":[],"nextAfter":"invalid"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
		client, _ := NewClient(server.URL, server.Client(), Credential{})
		if _, err := client.ListVolumes(context.Background()); err == nil {
			t.Fatalf("accepted %s", body)
		}
		server.Close()
	}
	credential := testCredentialInfo()
	expires := time.Now().Add(time.Minute)
	credential.PendingActionID = secondID
	credential.PendingExpiresAt = &expires
	credential.RotationActionID = secondID
	credential.RotationGeneration = "1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(credential) }))
	defer server.Close()
	client, _ := NewClient(server.URL, server.Client(), Credential{})
	if _, err := client.Credential(context.Background(), testID); err != nil {
		t.Fatal(err)
	}
}

func TestClientPinsOriginAndNeverFollowsRedirects(t *testing.T) {
	var leaked bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked = true; w.WriteHeader(http.StatusNoContent) }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testCredential().ID+"."+testCredential().Secret {
			t.Error("missing credential")
		}
		if strings.Contains(r.URL.String(), testCredential().Secret) || r.Header.Get("Cookie") != "" {
			t.Error("secret in URL or ambiguous cookie")
		}
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), testCredential())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetVolume(context.Background(), testID); err == nil {
		t.Fatal("redirect accepted")
	}
	if leaked {
		t.Fatal("redirect forwarded credential")
	}
	request, _ := http.NewRequest(http.MethodGet, other.URL, nil)
	if _, err := client.HTTPClient().Do(request); err == nil {
		t.Fatal("external origin allowed")
	}
	if leaked {
		t.Fatal("external request sent")
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL, nil)
	request.Host = "another.example"
	if _, err := client.HTTPClient().Do(request); err == nil {
		t.Fatal("virtual host override accepted")
	}
	for _, base := range []string{"http://example.com", "https://a.example/path", "https://user@a.example", "https://a.example?x=1", "https://a.example#x", "://bad"} {
		if _, err := NewClient(base, http.DefaultClient, Credential{}); err == nil {
			t.Errorf("allowed %s", base)
		}
	}
	if _, err := NewClient(server.URL, nil, Credential{}); err == nil {
		t.Fatal("nil HTTP client")
	}
	if _, err := client.WithCredential(Credential{ID: testID, Secret: "bad"}); err == nil {
		t.Fatal("bad credential")
	}
	if _, err := client.VolumeURL("../escape"); err == nil {
		t.Fatal("bad volume ID")
	}
	volumeURL, err := client.VolumeURL(testID)
	if err != nil || volumeURL != server.URL+"/api/volumes/"+testID {
		t.Fatalf("%s %v", volumeURL, err)
	}
}

func TestClientManagementMethodsAndLosslessDTO(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		var result any
		switch {
		case r.URL.Path == Prefix+"/auth/cli-credentials":
			if r.Header.Get("Authorization") != "" {
				t.Error("login sent existing bearer")
			}
			result = CredentialLogin{Credential: testCredentialInfo(), Secret: testCredential().Secret, User: User{ID: secondID, DisplayName: "Admin", Role: "admin"}}
		case strings.HasPrefix(r.URL.Path, Prefix+"/auth/cli-credentials/"):
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			result = testCredentialInfo()
		case r.URL.Path == Prefix+"/action-namespaces":
			result = ActionNamespace{SubjectID: secondID, Namespace: "9007199254740993", AcceptUntil: time.Now().Add(time.Hour)}
		case r.URL.Path == Prefix+"/volumes" && r.Method == http.MethodGet:
			if r.URL.Query().Get("after") == "" {
				result = VolumeList{Volumes: []Volume{testVolume()}, NextAfter: testID}
			} else {
				v := testVolume()
				v.ID = secondID
				result = VolumeList{Volumes: []Volume{v}}
			}
		case r.URL.Path == Prefix+"/admin/users":
			result = UserList{Users: []User{{ID: testID, DisplayName: "Ordinary", Role: "user"}}}
		case strings.HasPrefix(r.URL.Path, Prefix+"/admin/users/"):
			result = UserVolumeList{Volumes: []UserVolumeSummary{{Name: "Private", Summary: &UserVolumeStats{FileCount: "1", DirectoryCount: "0", UsedBytes: "4096", QuotaBytes: "8192"}}}}
		case strings.HasPrefix(r.URL.Path, Prefix+"/volumes/") && r.Method == http.MethodGet:
			result = testVolume()
		default:
			result = testOperation()
			if strings.HasPrefix(r.URL.Path, Prefix+"/volumes/") {
				operation := testOperation()
				operation.VolumeID = testID
				result = operation
			}
		}
		if err := json.NewEncoder(w).Encode(result); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), testCredential())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.Login(ctx, CreateCredentialRequest{LoginName: "admin", Password: "private password", Label: "cli"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Credential(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PrepareRotation(ctx, testID, PrepareRotationRequest{ActionID: secondID, ExpectedGeneration: "1", Secret: testCredential().Secret}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ActivateRotation(ctx, testID, ActivateRotationRequest{ActionID: secondID, ExpectedGeneration: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RevokeCredential(ctx, testID); err != nil {
		t.Fatal(err)
	}
	namespace, err := client.IssueNamespace(ctx)
	if err != nil || namespace.Namespace != "9007199254740993" {
		t.Fatalf("%+v %v", namespace, err)
	}
	volumes, err := client.ListVolumes(ctx)
	if err != nil || len(volumes) != 2 || volumes[0].Revision != "9007199254740993" {
		t.Fatalf("%+v %v", volumes, err)
	}
	if _, err := client.GetVolume(ctx, testID); err != nil {
		t.Fatal(err)
	}
	action := Action{Namespace: "1", Nonce: "nonce"}
	if _, err := client.CreateVolume(ctx, CreateVolumeRequest{Action: action, Name: "private", QuotaBytes: "4096"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RenameVolume(ctx, testID, RenameVolumeRequest{Action: action, ExpectedRevision: "1", Name: "new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQuota(ctx, testID, SetQuotaRequest{Action: action, ExpectedRevision: "1", QuotaBytes: "8192"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteVolume(ctx, testID, DeleteVolumeRequest{Action: action, ExpectedRevision: "1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.FindOperation(ctx, action); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitOperation(ctx, testID, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListUserVolumes(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 17 {
		t.Fatalf("exchanges %d %+v", len(methods), methods)
	}
}

func TestClientRejectsUnknownTruthAndReturnsClosedErrors(t *testing.T) {
	for _, fixture := range []struct {
		status int
		body   string
		code   string
	}{
		{503, `{"error":{"code":"storage_unavailable"}}`, "storage_unavailable"},
		{503, `{"error":{"code":"raw backend trace and password"}}`, ""},
		{200, `null`, ""},
		{200, `{"id":"` + testID + `"}`, ""},
		{200, strings.Repeat("x", maxResponseBytes+1), ""},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(fixture.status)
			_, _ = io.WriteString(w, fixture.body)
		}))
		client, err := NewClient(server.URL, server.Client(), Credential{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.GetVolume(context.Background(), testID)
		if err == nil {
			t.Fatal("invalid successful truth accepted")
		}
		var problem *APIError
		if fixture.code != "" && (!errors.As(err, &problem) || problem.Code != fixture.code || problem.Status != fixture.status) {
			t.Fatalf("%v", err)
		}
		if strings.Contains(err.Error(), "password") {
			t.Fatal("raw diagnostics exposed")
		}
		server.Close()
	}
}
