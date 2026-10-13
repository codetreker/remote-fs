package serviceapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	return Volume{ID: testID, OwnerUserID: secondID, Name: "files", State: "active", Revision: "9007199254740993", CreatedAt: now, UpdatedAt: now, Summary: &VolumeSummary{FileCount: "9007199254740993", DirectoryCount: "0", UsedBytes: "4096", QuotaBytes: "9223372036854775807"}}
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

func TestClientRequiresExplicitListTruth(t *testing.T) {
	for _, body := range []string{`{}`, `{"volumes":null}`, `{"volumes":[],"nextAfter":"invalid"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
		client, _ := NewClient(server.URL, server.Client(), Credential{})
		if _, err := client.ListVolumes(context.Background()); err == nil {
			t.Fatalf("accepted %s", body)
		}
		server.Close()
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
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			result = testVolume()
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
	if err := client.RevokeCredential(ctx, testID); err != nil {
		t.Fatal(err)
	}
	volumes, err := client.ListVolumes(ctx)
	if err != nil || len(volumes) != 2 || volumes[0].Revision != "9007199254740993" {
		t.Fatalf("%+v %v", volumes, err)
	}
	if _, err := client.GetVolume(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateVolume(ctx, CreateVolumeRequest{Name: "private", QuotaBytes: "4096"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RenameVolume(ctx, testID, RenameVolumeRequest{Name: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteVolume(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListUsers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListUserVolumes(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 11 {
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

func TestClientReadOnlyDirectoryPreservesOpaqueBytePaths(t *testing.T) {
	token := base64.StdEncoding.EncodeToString([]byte("dir\xff"))
	directory := FileDirectory{PathBase64: token, Entries: []FileEntry{{Name: "file", NameBase64: "ZmlsZQ==", PathBase64: base64.StdEncoding.EncodeToString([]byte("dir\xff/file")), NodeID: "9007199254740993", Kind: "file", SizeBytes: "1", ModifiedAt: time.Now()}}}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != Prefix+"/volumes/"+testID+"/files" || r.URL.Query().Get("path") != token {
			t.Error("directory identity or readonly method changed")
		}
		json.NewEncoder(w).Encode(directory)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), testCredential())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ListFiles(t.Context(), testID, token)
	if err != nil || result.PathBase64 != token || result.Entries[0].NodeID != "9007199254740993" || result.Entries[0].PathBase64 != directory.Entries[0].PathBase64 {
		t.Fatalf("directory identity changed: %+v %v", result, err)
	}
	for _, input := range []struct{ id, path string }{{"../escape", token}, {testID, "not base64"}, {testID, token + "\n"}} {
		if _, err := client.ListFiles(t.Context(), input.id, input.path); err == nil {
			t.Fatal("invalid directory request accepted")
		}
	}
	if calls != 1 {
		t.Fatal("invalid request reached server")
	}
	directory.PathBase64 = ""
	if _, err := client.ListFiles(t.Context(), testID, token); err == nil {
		t.Fatal("another directory was substituted")
	}
	directory.PathBase64 = token
	directory.Entries = nil
	if _, err := client.ListFiles(t.Context(), testID, token); err == nil {
		t.Fatal("unknown directory reported as empty")
	}
}

func TestDirectoryResponseExceedsManagementLimitWithoutTruncation(t *testing.T) {
	const count = 25000
	const firstNode = uint64(9007199254740993)
	parent := strings.Repeat(strings.Repeat("d", 200)+"/", 10) + "raw\xff"
	directory := FileDirectory{PathBase64: base64.StdEncoding.EncodeToString([]byte(parent)), Entries: make([]FileEntry, count)}
	modified := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for index := range directory.Entries {
		name := strings.Repeat("f", 220) + strconv.Itoa(index)
		path := base64.StdEncoding.EncodeToString([]byte(name))
		directory.Entries[index] = FileEntry{Name: name, NameBase64: path, PathBase64: base64.StdEncoding.EncodeToString([]byte(parent + "/" + name)), NodeID: DecimalFromUint64(firstNode + uint64(index)), Kind: "file", SizeBytes: "1", ModifiedAt: modified}
	}
	body, err := json.Marshal(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= maxResponseBytes || len(body) >= maxDirectoryResponseBytes {
		t.Fatalf("directory fixture has inappropriate size: %d", len(body))
	}
	if bytes.Count(body, []byte(`"pathBase64"`)) != 1 || bytes.Contains(body, []byte(directory.Entries[count-1].PathBase64)) {
		t.Fatal("directory wire repeated full child paths")
	}
	if len(body)+count*len(directory.PathBase64) <= maxDirectoryResponseBytes {
		t.Fatal("fixture did not exercise a deep directory whose repeated paths exceed the response limit")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != Prefix+"/volumes/"+testID+"/files" || r.URL.Query().Get("path") != directory.PathBase64 {
			t.Error("unexpected directory request")
		}
		if _, err := w.Write(body); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), testCredential())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ListFiles(t.Context(), testID, directory.PathBase64)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != count {
		t.Fatalf("directory truncated: %d", len(result.Entries))
	}
	expected := directory.Entries[count-1]
	actual := result.Entries[count-1]
	if actual != expected || result.PathBase64 != directory.PathBase64 {
		t.Fatalf("directory identity or metadata changed: %+v", actual)
	}
}

func TestCredentialStatusValidatesCurrentAndPendingMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*CredentialInfo)
		valid  bool
	}{
		{name: "current", valid: true},
		{name: "pending metadata", valid: true, mutate: func(info *CredentialInfo) {
			expiry := time.Now().Add(time.Minute)
			info.PendingActionID = secondID
			info.PendingExpiresAt = &expiry
			info.RotationActionID = secondID
			info.RotationGeneration = "1"
		}},
		{name: "missing pending expiry", mutate: func(info *CredentialInfo) { info.PendingActionID = secondID }},
		{name: "zero generation", mutate: func(info *CredentialInfo) { info.Generation = "0" }},
		{name: "overflow generation", mutate: func(info *CredentialInfo) { info.Generation = "9223372036854775808" }},
		{name: "missing current expiry", mutate: func(info *CredentialInfo) { info.ExpiresAt = time.Time{} }},
		{name: "duplicate scope", mutate: func(info *CredentialInfo) { info.Scopes = []string{"files", "files"} }},
		{name: "unknown scope", mutate: func(info *CredentialInfo) { info.Scopes = []string{"unknown"} }},
		{name: "different credential", mutate: func(info *CredentialInfo) { info.ID = secondID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := testCredentialInfo()
			if test.mutate != nil {
				test.mutate(&status)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != Prefix+"/auth/cli-credentials/"+testID {
					t.Error("status lookup changed credential identity or method")
				}
				if err := json.NewEncoder(w).Encode(status); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, server.Client(), testCredential())
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Credential(t.Context(), testID)
			if !test.valid {
				if err == nil {
					t.Fatal("untrustworthy credential status reported success")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.ID != status.ID || result.Generation != status.Generation || !result.ExpiresAt.Equal(status.ExpiresAt) || result.PendingActionID != status.PendingActionID || result.RotationActionID != status.RotationActionID || result.RotationGeneration != status.RotationGeneration {
				t.Fatalf("credential status metadata changed: %+v", result)
			}
			if status.PendingExpiresAt != nil && (result.PendingExpiresAt == nil || !result.PendingExpiresAt.Equal(*status.PendingExpiresAt)) {
				t.Fatal("pending expiry metadata was lost")
			}
		})
	}
}
