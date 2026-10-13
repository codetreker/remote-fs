package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/service"
)

func TestServiceServesAuthenticatedRequestsBehindTLSProxyAndStops(t *testing.T) {
	address := unusedServiceAddress(t)
	backend, err := url.Parse("http://" + address)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(backend))
	defer proxy.Close()
	config := service.DefaultConfig(filepath.Join(t.TempDir(), "state"), proxy.URL)
	config.Listen = address
	config.TrustedProxyCIDRs = []string{"127.0.0.1/32"}
	if err := service.InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := service.InitializeAdmin(t.Context(), config, "admin", "Administrator", "administrator test password"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		stopped <- serveService(ctx, config, io.Discard)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("service did not stop after cancellation")
		}
	})
	client := proxy.Client()
	client.Timeout = time.Second
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(proxy.URL + "/health/ready")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				break
			}
		}
		select {
		case err := <-stopped:
			t.Fatalf("service stopped before readiness: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("service did not become ready through its TLS proxy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Get(proxy.URL + "/api/service/v1/volumes")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous volume listing status = %d", response.StatusCode)
	}
	client.Jar, err = cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, proxy.URL+"/api/service/v1/auth/login", bytes.NewBufferString(`{"loginName":"admin","password":"administrator test password"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", proxy.URL)
	request.Header.Set("X-RFS-Request", "1")
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator login through proxy status = %d", response.StatusCode)
	}
	response, err = client.Get(proxy.URL + "/api/service/v1/volumes")
	if err != nil {
		t.Fatal(err)
	}
	var listing struct {
		Volumes []json.RawMessage `json:"volumes"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&listing)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || len(listing.Volumes) != 0 {
		t.Fatalf("own-volume listing status = %d, decode = %v, volumes = %d", response.StatusCode, decodeErr, len(listing.Volumes))
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("service shutdown failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("service shutdown did not return")
	}
	assertServiceResourcesReleased(t, config)
}

func unusedServiceAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func assertServiceResourcesReleased(t *testing.T, config service.Config) {
	t.Helper()
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		t.Fatalf("service retained its listening address: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	maintenance, err := service.OpenMaintenance(t.Context(), config)
	if err != nil {
		t.Fatalf("service retained its state anchor: %v", err)
	}
	if err := maintenance.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRunServeHandlesTerminationAndReleasesState(t *testing.T) {
	path := writeServiceCommandConfiguration(t, filepath.Join(t.TempDir(), "state"), unusedServiceAddress(t))
	config, err := service.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := service.InitializeAdmin(t.Context(), config, "admin", "Administrator", "administrator test password"); err != nil {
		t.Fatal(err)
	}
	output := &signalOnFirstWrite{signal: syscall.SIGTERM}
	if err := run([]string{"serve", "--config", path}, output); err != nil {
		t.Fatal(err)
	}
	if output.err != nil || !bytes.Contains(output.Bytes(), []byte(`"event":"serving"`)) || !bytes.Contains(output.Bytes(), []byte(`"event":"stopped"`)) {
		t.Fatalf("termination status output = %q, signal error = %v", output.String(), output.err)
	}
	assertServiceResourcesReleased(t, config)
}

func TestServiceServeStartupFailuresReleaseAddressAndState(t *testing.T) {
	for _, initializedAdmin := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing administrator", true: "startup output unavailable"}[initializedAdmin], func(t *testing.T) {
			config := service.DefaultConfig(filepath.Join(t.TempDir(), "state"), "https://files.example.com")
			config.Listen = unusedServiceAddress(t)
			if err := service.InitState(t.Context(), config); err != nil {
				t.Fatal(err)
			}
			if initializedAdmin {
				if _, err := service.InitializeAdmin(t.Context(), config, "admin", "Administrator", "administrator test password"); err != nil {
					t.Fatal(err)
				}
			}
			failure := errors.New("startup output unavailable")
			var output io.Writer = io.Discard
			if initializedAdmin {
				reader, writer := io.Pipe()
				reader.CloseWithError(failure)
				defer writer.Close()
				output = writer
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := serveService(ctx, config, output)
			if initializedAdmin {
				if !errors.Is(err, failure) {
					t.Fatalf("startup output failure was lost: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "service requires an initialized administrator") {
				t.Fatalf("missing administrator rejection = %v", err)
			}
			assertServiceResourcesReleased(t, config)
		})
	}
}
