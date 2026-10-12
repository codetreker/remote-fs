package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	passwordinput "github.com/codetreker/remote-fs/cmd/internal/password"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"golang.org/x/sys/unix"
)

func credentialFixture(t *testing.T, server string) (string, credentialRecord) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	secret, err := randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	record := credentialRecord{Version: 1, Server: server, ID: strings.Repeat("a", 32), Secret: secret, SubjectID: strings.Repeat("b", 32), Generation: "1"}
	name := filepath.Join(dir, "credentials.json")
	if err := writePrivate(name, record); err != nil {
		t.Fatal(err)
	}
	return name, record
}
func TestPrivateRecordsRejectLinksPermissionsAndConcurrentWriters(t *testing.T) {
	name, record := credentialFixture(t, "https://example.invalid")
	unlock, err := lockPrivate(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockPrivate(name); err == nil {
		t.Fatal("concurrent writer acquired private lock")
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(name), "link.json")
	if err := os.Symlink(name, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCredential(link); err == nil {
		t.Fatal("credential symlink accepted")
	}
	if err := writePrivate(link, record); err == nil {
		t.Fatal("credential symlink replaced")
	}
	if err := os.Chmod(name, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCredential(name); err == nil {
		t.Fatal("read publicly accessible credentials")
	}
	if err := writePrivate(name, record); err == nil {
		t.Fatal("replaced publicly accessible credentials")
	}
}
func TestMountCredentialTransportReloadsAndRejectsForeignOrigin(t *testing.T) {
	var headers []string
	var mutex sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		headers = append(headers, r.Header.Get("Authorization"))
		mutex.Unlock()
		w.WriteHeader(204)
	}))
	defer server.Close()
	file, record := credentialFixture(t, server.URL)
	client, err := credentialHTTPClient(file, server.URL+"/api/volumes/"+strings.Repeat("c", 32), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(server.URL + "/v5/stat")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	old := record.Secret
	record.Secret, err = randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err = writePrivate(file, record); err != nil {
		t.Fatal(err)
	}
	response, err = client.Get(server.URL + "/v5/stat")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if _, err = client.Get("https://example.invalid/"); err == nil {
		t.Fatal("sent authenticated request to foreign origin")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(headers) != 2 || headers[0] != "Bearer "+record.ID+"."+old || headers[1] != "Bearer "+record.ID+"."+record.Secret {
		t.Fatal("mount did not reload credentials")
	}
}
func TestLoginPersistsPrivateSecretWithoutPrintingIt(t *testing.T) {
	secret, err := randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/service/v1/auth/cli-credentials" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var input serviceapi.CreateCredentialRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if input.Password != "password from stdin" || r.Header.Get("Authorization") != "" {
			t.Error("invalid login request")
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(serviceapi.CredentialLogin{Credential: serviceapi.CredentialInfo{ID: strings.Repeat("a", 32), Generation: "1", Scopes: []string{"files"}, VolumeIDs: []string{}, ExpiresAt: time.Now().Add(time.Hour)}, Secret: secret, User: serviceapi.User{ID: strings.Repeat("b", 32), DisplayName: "Root", Role: "admin"}})
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "private", "credentials.json")
	var output bytes.Buffer
	err = runServiceCommand([]string{"login", "-server", server.URL, "-allow-insecure-http", "-login", "root", "-password-stdin", "-credential-file", file, "-json"}, strings.NewReader("password from stdin\n"), &output, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), "password from stdin") {
		t.Fatal("secret was printed")
	}
	record, err := loadCredential(file)
	if err != nil {
		t.Fatal(err)
	}
	if record.Secret != secret {
		t.Fatal("credential not persisted")
	}
}
func TestServiceURLRequiresExplicitHTTPAndRejectsEmbeddedCredentials(t *testing.T) {
	for _, raw := range []string{"http://localhost:8080", "https://user:secret@example.org", "https://example.org/path", "https://example.org?secret=x", "https://example.org#x"} {
		if _, err := validateServiceURL(raw, false); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	if _, err := validateServiceURL("http://localhost:8080", true); err != nil {
		t.Fatal(err)
	}
}
func TestPendingRotationRetriesActivationAfterPrepareAuthenticationFails(t *testing.T) {
	pendingSecret, err := randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/prepare") {
			w.WriteHeader(401)
			json.NewEncoder(w).Encode(serviceapi.ErrorResponse{Error: serviceapi.ErrorDetail{Code: "authentication_required"}})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/activate") || r.Header.Get("Authorization") != "Bearer "+strings.Repeat("a", 32)+"."+pendingSecret {
			t.Error("invalid recovery activation request")
		}
		json.NewEncoder(w).Encode(serviceapi.CredentialInfo{ID: strings.Repeat("a", 32), Generation: "2", RotationActionID: strings.Repeat("d", 32), RotationGeneration: "2", Scopes: []string{"admin:summaries"}, VolumeIDs: []string{}, ExpiresAt: time.Now().Add(time.Hour)})
	}))
	defer server.Close()
	file, record := credentialFixture(t, server.URL)
	record.Pending = &pendingRotation{Secret: pendingSecret, ActionID: strings.Repeat("d", 32), ExpectedGeneration: "1"}
	if err = writePrivate(file, record); err != nil {
		t.Fatal(err)
	}
	client, err := serviceapi.NewClient(server.URL, server.Client(), serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = rotateCredential(context.Background(), client, record, cliOptions{credentials: file, json: true}, &out); err != nil {
		t.Fatal(err)
	}
	result, err := loadCredential(file)
	if err != nil {
		t.Fatal(err)
	}
	if result.Secret != pendingSecret || result.Pending != nil || result.Generation != "2" {
		t.Fatal("recovery did not commit pending credential")
	}
	if len(calls) != 2 {
		t.Fatal("recovery did not attempt activation")
	}
	if strings.Contains(out.String(), pendingSecret) {
		t.Fatal("rotation printed secret")
	}
}
func TestPrivateReadRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	file, _ := credentialFixture(t, "https://example.org")
	for _, data := range []string{"{} {}", `{"unexpected":true}`} {
		if err := os.WriteFile(file, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		var result credentialRecord
		if err := readPrivate(file, &result); err == nil {
			t.Fatal("accepted invalid record")
		}
	}
	if _, err := loadCredential(filepath.Join(filepath.Dir(file), "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file error %v", err)
	}
}

func TestExpiredPendingRotationUsesProvenCurrentGeneration(t *testing.T) {
	pending, err := randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	prepares := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := serviceapi.CredentialInfo{ID: strings.Repeat("a", 32), Generation: "2", Scopes: []string{"admin:summaries"}, VolumeIDs: []string{}, ExpiresAt: time.Now().Add(time.Hour)}
		if strings.HasSuffix(r.URL.Path, "/prepare") {
			var input serviceapi.PrepareRotationRequest
			json.NewDecoder(r.Body).Decode(&input)
			prepares++
			if prepares == 1 {
				if input.ExpectedGeneration != "1" {
					t.Error("initial generation changed")
				}
				w.WriteHeader(409)
				json.NewEncoder(w).Encode(serviceapi.ErrorResponse{Error: serviceapi.ErrorDetail{Code: "condition_conflict"}})
				return
			}
			if input.ExpectedGeneration != "2" || input.Secret == pending {
				t.Error("expired secret/generation was reused")
			}
			pending = input.Secret
			expiry := time.Now().Add(time.Minute)
			result.PendingExpiresAt = &expiry
			result.PendingActionID = input.ActionID
		} else if strings.HasSuffix(r.URL.Path, "/activate") {
			var input serviceapi.ActivateRotationRequest
			json.NewDecoder(r.Body).Decode(&input)
			if prepares == 1 {
				w.WriteHeader(401)
				json.NewEncoder(w).Encode(serviceapi.ErrorResponse{Error: serviceapi.ErrorDetail{Code: "authentication_required"}})
				return
			}
			result.Generation = "3"
			result.RotationActionID = input.ActionID
			result.RotationGeneration = "3"
		}
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	file, record := credentialFixture(t, server.URL)
	record.Pending = &pendingRotation{Secret: pending, ActionID: strings.Repeat("d", 32), ExpectedGeneration: "1"}
	if err = writePrivate(file, record); err != nil {
		t.Fatal(err)
	}
	client, err := serviceapi.NewClient(server.URL, server.Client(), serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	if err != nil {
		t.Fatal(err)
	}
	if err = rotateCredential(context.Background(), client, record, cliOptions{credentials: file, json: true}, io.Discard); err != nil {
		t.Fatal(err)
	}
	current, err := loadCredential(file)
	if err != nil {
		t.Fatal(err)
	}
	if current.Pending != nil || current.Secret != pending || current.Generation != "3" {
		t.Fatal("fresh rotation was not persisted")
	}
}

func TestJournalRejectsChangedQuotaWithoutSubmitting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("changed action reached service")
		w.WriteHeader(500)
	}))
	defer server.Close()
	file, record := credentialFixture(t, server.URL)
	actionFile := filepath.Join(filepath.Dir(file), "action.json")
	input, err := json.Marshal(serviceapi.SetQuotaRequest{Action: serviceapi.Action{Namespace: "1", Nonce: "same-action"}, ExpectedRevision: "1", QuotaBytes: "1024"})
	if err != nil {
		t.Fatal(err)
	}
	journal := actionRecord{Version: 1, Server: record.Server, SubjectID: record.SubjectID, Kind: "quota", VolumeID: strings.Repeat("c", 32), Namespace: "1", Nonce: "same-action", Input: input}
	if err = writePrivate(actionFile, journal); err != nil {
		t.Fatal(err)
	}
	client, err := serviceapi.NewClient(server.URL, server.Client(), serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	if err != nil {
		t.Fatal(err)
	}
	err = volumeMutation(t.Context(), client, record, cliOptions{credentials: file, actionFile: actionFile, volume: journal.VolumeID}, "volume quota", "", "2048", nil, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("changed journal effect accepted: %v", err)
	}
}
func TestMountCredentialTransportRejectsHostOverrideAndStripsCookies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" {
			t.Error("cookie leaked")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	file, _ := credentialFixture(t, server.URL)
	client, err := credentialHTTPClient(file, server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "other.example"
	if _, err = client.Do(request); err == nil {
		t.Fatal("host override was accepted")
	}
	request.Host = ""
	request.Header.Set("Cookie", "other-session=secret")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

func TestAdminUserCommandsExposeOnlySummaryDTO(t *testing.T) {
	userID := strings.Repeat("b", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/service/v1/admin/users":
			json.NewEncoder(w).Encode(serviceapi.UserList{Users: []serviceapi.User{{ID: userID, DisplayName: "ordinary user", Role: "user"}}})
		case "/api/service/v1/admin/users/" + userID + "/volumes":
			json.NewEncoder(w).Encode(serviceapi.UserVolumeList{Volumes: []serviceapi.UserVolumeSummary{{Name: "summary only", Summary: &serviceapi.UserVolumeStats{FileCount: "9007199254740993", DirectoryCount: "2", UsedBytes: "123", QuotaBytes: "1024"}}}})
		default:
			t.Errorf("unexpected file request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	file, _ := credentialFixture(t, server.URL)
	for _, args := range [][]string{{"user", "list", "-credential-file", file, "-json"}, {"user", "volumes", userID, "-credential-file", file, "-json"}} {
		var out bytes.Buffer
		if err := runServiceCommand(args, strings.NewReader(""), &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		if args[1] == "volumes" && !strings.Contains(out.String(), `"fileCount":"9007199254740993"`) {
			t.Fatalf("summary integer changed: %s", out.String())
		}
		if strings.Contains(out.String(), "entries") || strings.Contains(out.String(), "rawLeaf") {
			t.Fatal("file details leaked")
		}
	}
}

func TestTerminalPasswordCancellationAndSuccessRestoreEcho(t *testing.T) {
	for _, input := range []bool{false, true} {
		t.Run(fmt.Sprintf("input=%v", input), func(t *testing.T) {
			fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			if err = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
				t.Fatal(err)
			}
			number, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
			if err != nil {
				t.Fatal(err)
			}
			tty, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer tty.Close()
			original, err := unix.IoctlGetTermios(int(tty.Fd()), unix.TCGETS)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type response struct {
				password string
				err      error
			}
			finished := make(chan response, 1)
			go func() {
				password, err := passwordinput.ReadTTY(ctx, tty, io.Discard, "Administrator password: ", 1024)
				finished <- response{password, err}
			}()
			deadline := time.Now().Add(2 * time.Second)
			for {
				current, err := unix.IoctlGetTermios(int(tty.Fd()), unix.TCGETS)
				if err != nil {
					t.Fatal(err)
				}
				if current.Lflag&unix.ECHO == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("password input did not disable echo")
				}
				time.Sleep(time.Millisecond)
			}
			if input {
				if _, err = unix.Write(fd, []byte("private password\n")); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case got := <-finished:
				if input && (got.err != nil || got.password != "private password") {
					t.Fatalf("password read error %v", got.err)
				}
				if !input && !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancellation error %v", got.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("terminal password remained blocked")
			}
			restored, err := unix.IoctlGetTermios(int(tty.Fd()), unix.TCGETS)
			if err != nil || restored.Lflag != original.Lflag {
				t.Fatalf("terminal state was not restored: %v", err)
			}
		})
	}
}
