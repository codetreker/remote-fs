package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/service"
)

func TestServiceCommandPrebindDoesNotInitializeUnavailableAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	state := filepath.Join(t.TempDir(), "not-created")
	config := writeServiceCommandConfiguration(t, state, listener.Addr().String())
	if err := runServiceCommand(context.Background(), []string{"serve", "--config", config}, nil, io.Discard); err == nil {
		t.Fatal("occupied address was accepted")
	}
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed listener initialized storage: %v", err)
	}
}

func TestServiceOfflineAdministratorResetRetainsIdentityAndAnchor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	state := filepath.Join(t.TempDir(), "state")
	path := writeServiceCommandConfiguration(t, state, "127.0.0.1:0")
	password := filepath.Join(t.TempDir(), "password")
	const originalPassword = "initial administrator password"
	if err := os.WriteFile(password, []byte(originalPassword+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	command := func(arguments ...string) error {
		return run(append(arguments, "--config", path), &output)
	}
	if err := command("init"); err != nil {
		t.Fatal(err)
	}
	if err := command("admin", "create", "--login", "admin", "--display-name", "Administrator", "--password-file", password); err != nil {
		t.Fatal(err)
	}
	config, err := service.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	application, err := service.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	originalID := loginServiceUser(t, application, config, originalPassword)
	if err := command("admin", "reset", "--login", "admin", "--password-file", password); !errors.Is(err, service.ErrRootBusy) {
		t.Fatalf("online administrator reset was admitted: %v", err)
	}
	if err := application.Close(ctx); err != nil {
		t.Fatal(err)
	}
	anchorPath := filepath.Join(state, "service.lock")
	anchor, err := os.Stat(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(password, []byte("changed administrator password\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := command("admin", "reset", "--login", "admin", "--password-file", password); err != nil {
		t.Fatal(err)
	}
	resetAnchor, err := os.Stat(anchorPath)
	if err != nil || !os.SameFile(anchor, resetAnchor) {
		t.Fatalf("administrator reset replaced the stable lifetime anchor: %v", err)
	}
	application, err = service.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := application.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if id := loginServiceUser(t, application, config, "changed administrator password"); id != originalID {
		t.Fatalf("password reset changed administrator identity: %s != %s", id, originalID)
	}
	if bytes.Contains(output.Bytes(), []byte(originalPassword)) {
		t.Fatal("offline command leaked the administrator password")
	}
}

func loginServiceUser(t *testing.T, application *service.Service, config service.Config, password string) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]string{"loginName": "admin", "password": password})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(application)
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/service/v1/auth/login", bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", config.ExternalOrigin)
	request.Header.Set("X-RFS-Request", "1")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, err := io.ReadAll(response.Body)
		t.Fatalf("administrator login status = %d, body %s, read error %v", response.StatusCode, body, err)
	}
	var result struct {
		User struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || result.User.ID == "" {
		t.Fatalf("administrator identity response is invalid: %v", err)
	}
	return result.User.ID
}

func writeServiceCommandConfiguration(t *testing.T, state, listen string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	encoded := "state_root: " + state + "\nexternal_origin: https://files.example.com\nlisten: " + listen + "\n"
	if err := os.WriteFile(path, []byte(encoded), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestServiceRunRejectsInvalidCommandsWithoutCreatingState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "not-created")
	config := writeServiceCommandConfiguration(t, state, "127.0.0.1:0")
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"missing admin action", []string{"admin"}, "admin requires"},
		{"unknown command", []string{"unknown", "--config", config}, "unknown service command"},
		{"missing configuration", []string{"serve"}, "--config is required"},
		{"positional argument", []string{"init", "--config", config, "unexpected"}, "positional arguments"},
		{"admin flags on serve", []string{"serve", "--config", config, "--login", "admin"}, "administrator flags"},
		{"missing display name", []string{"admin", "create", "--config", config, "--login", "admin"}, "--display-name"},
		{"absent password file", []string{"admin", "create", "--config", config, "--login", "admin", "--display-name", "Administrator", "--password-file", filepath.Join(t.TempDir(), "absent")}, "no such file"},
		{"absent configuration", []string{"init", "--config", filepath.Join(t.TempDir(), "absent")}, "no such file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := run(test.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("command error = %v, expected %q", err, test.want)
			}
			if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid command created state: %v", err)
			}
		})
	}
	for _, args := range [][]string{{"serve", "--help"}, {"reference", "--help"}} {
		var output bytes.Buffer
		if err := run(args, &output); err != nil || !strings.Contains(strings.ToLower(output.String()), "usage") {
			t.Fatalf("help result = %v, output = %q", err, output.String())
		}
	}
	if err := run([]string{"serve", "--unknown"}, io.Discard); !errors.Is(err, errUsage) {
		t.Fatalf("invalid flags did not report usage: %v", err)
	}
}
