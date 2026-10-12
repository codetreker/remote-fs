package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	record := credentialRecord{Version: 1, Server: server, ID: strings.Repeat("a", 32), Secret: secret, SubjectID: strings.Repeat("b", 32)}
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
func TestLoginPersistsPrivateSecretWithoutPrintingIt(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
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
	err := runServiceCommand([]string{"login", "-server", server.URL, "-allow-insecure-http", "-login", "root", "-password-stdin", "-credential-file", file, "-json"}, strings.NewReader("password from stdin\n"), &output, io.Discard)
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
