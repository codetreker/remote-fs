package service

import (
	"path/filepath"
	"testing"
	"time"
)

func TestConfigDeploymentAndLifecyclePolicies(t *testing.T) {
	base := DefaultConfig(filepath.Join(t.TempDir(), "state"), "https://files.example.test")
	if err := base.Validate(); err != nil {
		t.Fatalf("default policy: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"relative-root", func(c *Config) { c.StateRoot = "state" }},
		{"nul-root", func(c *Config) { c.StateRoot += "\x00" }},
		{"non-utf8-root", func(c *Config) { c.StateRoot += "\xff" }},
		{"unclean-root", func(c *Config) { c.StateRoot += "/../state" }},
		{"http-origin", func(c *Config) { c.ExternalOrigin = "http://files.example.test" }},
		{"origin-path", func(c *Config) { c.ExternalOrigin += "/console" }},
		{"different-auth-origin", func(c *Config) { c.Auth.Origin = "https://other.example.test" }},
		{"missing-listen-port", func(c *Config) { c.Listen = "localhost" }},
		{"invalid-proxy", func(c *Config) { c.TrustedProxyCIDRs = []string{"loopback"} }},
		{"zero-workers", func(c *Config) { c.MaxLifecycleWorkers = 0 }},
		{"oversized-workers", func(c *Config) { c.MaxLifecycleWorkers = 65537 }},
		{"zero-authority-actions", func(c *Config) { c.MaxAuthorityActions = 0 }},
		{"oversized-authority-actions", func(c *Config) { c.MaxAuthorityActions = 1<<20 + 1 }},
		{"invalid-idle-timeout", func(c *Config) { c.IdleRuntimeTimeout = time.Nanosecond }},
		{"invalid-shutdown-timeout", func(c *Config) { c.ShutdownTimeout = 0 }},
		{"missing-auth-policy", func(c *Config) { c.Auth.LoginBuckets = 0 }},
		{"management-no-reader", func(c *Config) { c.Management.MaxReaders = 0 }},
		{"management-create", func(c *Config) { c.Management.Create = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.change(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("invalid policy was accepted")
			}
		})
	}
}
