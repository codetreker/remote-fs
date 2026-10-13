package service

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/internal/management"
)

type Config struct {
	Listen            string
	ExternalOrigin    string
	StateRoot         string
	TrustedProxyCIDRs []string
	ShutdownTimeout   time.Duration
	Auth              AuthConfig
	Management        management.Options
}

func DefaultConfig(stateRoot, origin string) Config {
	return Config{
		Listen: "127.0.0.1:8080", ExternalOrigin: origin, StateRoot: stateRoot,
		TrustedProxyCIDRs: []string{"127.0.0.1/32", "::1/128"},
		ShutdownTimeout:   30 * time.Second,
		Auth:              DefaultAuthConfig(origin), Management: management.DefaultOptions(),
	}
}

func (c Config) Validate() error {
	if err := c.Management.Validate(); err != nil {
		return fmt.Errorf("management configuration: %w", err)
	}
	if c.Management.Create || c.Management.ExpectedDatabaseID != "" {
		return errors.New("management creation and identity are owned by state-root initialization")
	}
	if c.StateRoot == "" || !utf8.ValidString(c.StateRoot) || strings.ContainsRune(c.StateRoot, 0) || !filepath.IsAbs(c.StateRoot) || filepath.Clean(c.StateRoot) != c.StateRoot || c.StateRoot == string(filepath.Separator) {
		return errors.New("state root must be a clean absolute directory path")
	}
	u, err := url.Parse(c.ExternalOrigin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return errors.New("external origin must be an exact HTTPS origin without a path")
	}
	if c.Auth.Origin != c.ExternalOrigin {
		return errors.New("authentication origin differs from external origin")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen requires a host and port: %w", err)
	}
	for _, cidr := range c.TrustedProxyCIDRs {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return fmt.Errorf("invalid trusted proxy CIDR: %w", err)
		}
	}
	if c.ShutdownTimeout < time.Millisecond {
		return errors.New("service durations must be at least one millisecond")
	}
	if c.Auth.VerificationWait <= 0 || c.Auth.LoginRateWindow <= 0 || c.Auth.LoginAttempts < 1 || c.Auth.LoginBuckets < 2 || c.Auth.OutputCheckInterval <= 0 || c.Auth.OutputWriteTimeout <= 0 || c.Auth.MaxOutputContexts < 1 || c.Auth.MaxOutputsPerAccess < 1 || c.Auth.MaxOutputsPerAccess > c.Auth.MaxOutputContexts {
		return errors.New("authentication limits must be positive")
	}
	return nil
}
