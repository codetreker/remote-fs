package service

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const configurationFixture = "state_root: /var/lib/remote-fs\nexternal_origin: https://files.example.com\n"

func TestParseConfigKeepsExplicitPolicyAndDefaults(t *testing.T) {
	config, err := ParseConfig(strings.NewReader(configurationFixture + "listen: 127.0.0.1:9191\nshutdown_timeout: 17s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Listen != "127.0.0.1:9191" || config.ShutdownTimeout != 17*time.Second {
		t.Fatalf("explicit configuration was changed: %+v", config)
	}
}

func TestParseConfigRejectsAmbiguousAndInvalidPolicy(t *testing.T) {
	for name, input := range map[string]string{
		"empty":            "",
		"missing origin":   "state_root: /var/lib/remote-fs\n",
		"unknown field":    configurationFixture + "shutdown_timout: 30s\n",
		"duplicate root":   configurationFixture + "state_root: /var/lib/other\n",
		"alias":            configurationFixture + "listen: &address 127.0.0.1:8080\ntrusted_proxy_cidrs: [*address]\n",
		"null":             configurationFixture + "listen: null\n",
		"duration integer": configurationFixture + "shutdown_timeout: 30\n",
		"duration zero":    configurationFixture + "shutdown_timeout: 0s\n",
		"extra document":   configurationFixture + "---\nlisten: 127.0.0.1:1234\n",
		"insecure origin":  "state_root: /var/lib/remote-fs\nexternal_origin: http://files.example.com\n",
		"oversized":        configurationFixture + strings.Repeat("# oversized\n", maxConfigurationBytes),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig(strings.NewReader(input)); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestLoadConfigExampleAndReadFailures(t *testing.T) {
	config, err := LoadConfig(filepath.Join("..", "..", "deployments", "service", "config.example.yml"))
	if err != nil || config.StateRoot != "/var/lib/remote-fs" || config.Listen == "" {
		t.Fatalf("deployment configuration = %+v, %v", config, err)
	}
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing configuration error = %v", err)
	}
	readErr := errors.New("configuration input failed")
	if _, err := ParseConfig(configurationFailureReader{readErr}); !errors.Is(err, readErr) {
		t.Fatalf("input error was lost: %v", err)
	}
}

type configurationFailureReader struct{ err error }

func (r configurationFailureReader) Read([]byte) (int, error) { return 0, r.err }
