package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/serviceapi"
)

func TestManagementCLIUsesDirectCRUDAndReadOnlyListing(t *testing.T) {
	volume := serviceapi.Volume{ID: strings.Repeat("c", 32), OwnerUserID: strings.Repeat("b", 32), Name: "mine", State: "active", Revision: "1", CreatedAt: time.Now(), UpdatedAt: time.Now(), Summary: &serviceapi.VolumeSummary{FileCount: "1", DirectoryCount: "0", UsedBytes: "4096", QuotaBytes: "8192"}}
	token := base64.StdEncoding.EncodeToString([]byte("directory\xff"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST " + serviceapi.Prefix + "/volumes":
			var input serviceapi.CreateVolumeRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.Name != "mine" || input.QuotaBytes != "8192" {
				t.Errorf("wrong create input: %+v", input)
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(volume)
		case "PATCH " + serviceapi.Prefix + "/volumes/" + volume.ID:
			var input serviceapi.RenameVolumeRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.Name != "renamed" {
				t.Errorf("wrong rename input: %+v", input)
			}
			volume.Name = input.Name
			json.NewEncoder(w).Encode(volume)
		case "GET " + serviceapi.Prefix + "/volumes":
			json.NewEncoder(w).Encode(serviceapi.VolumeList{Volumes: []serviceapi.Volume{volume}})
		case "GET " + serviceapi.Prefix + "/volumes/" + volume.ID:
			json.NewEncoder(w).Encode(volume)
		case "GET " + serviceapi.Prefix + "/volumes/" + volume.ID + "/files":
			if r.URL.Query().Get("path") != token {
				t.Error("directory token changed")
			}
			json.NewEncoder(w).Encode(serviceapi.FileDirectory{PathBase64: token, Entries: []serviceapi.FileEntry{{Name: "file", NameBase64: "ZmlsZQ==", PathBase64: base64.StdEncoding.EncodeToString([]byte("directory\xff/file")), NodeID: "9007199254740993", Kind: "file", SizeBytes: "1", ModifiedAt: time.Now()}}})
		case "DELETE " + serviceapi.Prefix + "/volumes/" + volume.ID:
			w.WriteHeader(http.StatusNoContent)
		case "GET " + serviceapi.Prefix + "/auth/cli-credentials/" + strings.Repeat("a", 32):
			json.NewEncoder(w).Encode(serviceapi.CredentialInfo{ID: strings.Repeat("a", 32), Generation: "1", Scopes: []string{"volumes:read"}, VolumeIDs: []string{}, ExpiresAt: time.Now().Add(time.Hour)})
		case "DELETE " + serviceapi.Prefix + "/auth/cli-credentials/" + strings.Repeat("a", 32):
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected management request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	file, _ := credentialFixture(t, server.URL)
	for _, args := range [][]string{
		{"status"},
		{"volume", "create", "-name", "mine", "-quota", "8KiB"},
		{"volume", "rename", volume.ID, "-name", "renamed"},
		{"volume", "list"},
		{"volume", "show", volume.ID},
		{"files", "ls", "-volume", volume.ID, "-path-base64", token},
		{"volume", "delete", volume.ID},
	} {
		var out bytes.Buffer
		if err := runServiceCommand(append(args, "-credential-file", file, "-json"), strings.NewReader(""), &out, io.Discard); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if out.Len() == 0 {
			t.Fatalf("%v omitted its confirmed result", args)
		}
		if args[0] == "files" && !strings.Contains(out.String(), `"nodeId":"9007199254740993"`) {
			t.Fatal("node identity was rounded")
		}
		if args[0] == "files" && !strings.Contains(out.String(), `"pathBase64":"`+base64.StdEncoding.EncodeToString([]byte("directory\xff/file"))+`"`) {
			t.Fatal("listing omitted the exact derived navigation token")
		}

	}
	for _, args := range [][]string{
		{"volume", "create", "-name", "bad", "-quota", "0"},
		{"volume", "rename", volume.ID},
		{"volume", "show", volume.ID, "-volume", volume.ID},
		{"volume", "list", "extra"},
		{"status", "extra"},
		{"logout", "extra"},
		{"files", "ls"},
		{"files", "upload"},
		{"operation", "show"},
		{"mount"},
		{"rotate"},
	} {
		if err := runServiceCommand(append(args, "-credential-file", file), strings.NewReader(""), io.Discard, io.Discard); err == nil {
			t.Fatalf("invalid/removed command accepted: %v", args)
		}
	}
	var out bytes.Buffer
	if err := runServiceCommand([]string{"logout", "-credential-file", file}, strings.NewReader(""), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("logout kept revoked credential: %v", err)
	}
}

func TestTTYPasswordCancellationOrMissingTerminalIsActionable(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := readTTYPassword(ctx, io.Discard)
	if err == nil {
		t.Fatal("password read succeeded after cancellation without input")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "-password-stdin or -password-file") {
		t.Fatalf("missing actionable password error: %v", err)
	}
}

func TestManagementHelpAndInvalidFlagsDoNotLoadCredentials(t *testing.T) {
	for _, test := range []struct {
		args       []string
		usageError bool
	}{
		{args: []string{"help"}},
		{args: []string{"status", "-h"}},
		{args: []string{"status", "-unknown-flag"}, usageError: true},
		{args: []string{"status", "-timeout=invalid"}, usageError: true},
	} {
		var output bytes.Buffer
		var diagnostics bytes.Buffer
		err := runServiceCommand(test.args, strings.NewReader(""), &output, &diagnostics)
		if test.usageError {
			if !errors.Is(err, errUsage) || diagnostics.Len() == 0 {
				t.Fatalf("invalid flags lost their usage failure: %v %v", test.args, err)
			}
		} else if err != nil || output.Len()+diagnostics.Len() == 0 {
			t.Fatalf("help required credentials or omitted usage: %v %v", test.args, err)
		}
	}
	for _, command := range []string{"volume", "files", "user"} {
		err := runServiceCommand([]string{command}, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "requires a subcommand") {
			t.Fatalf("missing subcommand misreported: %s %v", command, err)
		}
	}
}
