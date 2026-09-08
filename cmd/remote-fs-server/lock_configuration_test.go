package main

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestLockDefaultsAndExplicitInitializationReachConfiguration(t *testing.T) {
	config, help, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-local-store", "/store", "-workspace", "workspace", "-quota", "8M", "-initialize-lock-state",
	}, io.Discard)
	if err != nil || help {
		t.Fatalf("parse local-store locks: help=%t err=%v", help, err)
	}
	if config.locks != locking.DefaultOptions() || !config.initializeLockState {
		t.Fatalf("lock configuration is %+v, initialize=%t", config.locks, config.initializeLockState)
	}
	openedConfig, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-local-store", "/store", "-workspace", "workspace", "-quota", "8M",
	}, io.Discard)
	if err != nil || openedConfig.initializeLockState {
		t.Fatalf("ordinary startup permits initialization: config=%+v err=%v", openedConfig, err)
	}
}

func TestLockCapacityAndLifetimeFlagsReachAuthorityOptions(t *testing.T) {
	config, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-local-store", "/store", "-workspace", "workspace", "-quota", "8M",
		"-lock-max-sessions", "11", "-lock-max-tickets", "13", "-lock-max-owners", "17",
		"-lock-max-resources", "19", "-lock-max-actions", "23", "-lock-max-grants", "29", "-lock-max-queued", "31",
		"-lock-owners-per-session", "3", "-lock-owner-actions-per-session", "5", "-lock-actions-per-owner", "7",
		"-lock-grants-per-owner", "2", "-lock-queued-per-owner", "4", "-lock-queued-per-resource", "6",
		"-lock-max-proofs", "8", "-lock-max-request-bytes", "37",
		"-lock-max-lease", "2s", "-lock-max-wait", "3s", "-lock-ticket-ttl", "4s",
		"-lock-session-idle", "5s", "-lock-resource-ttl", "6s",
		"-http-max-concurrent-lock-controls", "17", "-http-max-waiting-lock-controls", "19",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := locking.Options{
		MaxSessions: 11, MaxTickets: 13, MaxOwners: 17, MaxResources: 19, MaxActions: 23, MaxGrants: 29, MaxQueued: 31,
		OwnersPerSession: 3, OwnerActionsPerSession: 5, ActionsPerOwner: 7, GrantsPerOwner: 2, QueuedPerOwner: 4,
		QueuedPerResource: 6, MaxProofs: 8, MaxRequestBytes: 37,
		MaxLease: 2 * time.Second, MaxWait: 3 * time.Second, TicketTTL: 4 * time.Second,
		SessionIdle: 5 * time.Second, ResourceTTL: 6 * time.Second,
	}
	if config.locks != want {
		t.Fatalf("authority options %+v, want %+v", config.locks, want)
	}
	if config.http.MaxConcurrentLockControls != 17 || config.http.MaxWaitingLockControls != 19 {
		t.Fatalf("HTTP lock admission is %d active/%d waiting", config.http.MaxConcurrentLockControls, config.http.MaxWaitingLockControls)
	}
}

func TestInvalidLockConfigurationDoesNotAcquireListenerOrInitializeState(t *testing.T) {
	for _, test := range []struct {
		name, value string
	}{
		{"lock-max-sessions", "0"}, {"lock-max-tickets", "-1"}, {"lock-max-owners", "0"},
		{"lock-max-resources", "0"}, {"lock-max-actions", "0"}, {"lock-max-grants", "0"}, {"lock-max-queued", "0"},
		{"lock-owners-per-session", "0"}, {"lock-owner-actions-per-session", "0"}, {"lock-actions-per-owner", "0"},
		{"lock-grants-per-owner", "0"}, {"lock-queued-per-owner", "0"}, {"lock-queued-per-resource", "0"},
		{"lock-max-proofs", "0"}, {"lock-max-request-bytes", "0"},
		{"lock-max-resources", strconv.Itoa(math.MaxInt)},
		{"lock-max-lease", "0s"}, {"lock-max-wait", "1ns"}, {"lock-ticket-ttl", "-1s"},
		{"lock-resource-ttl", "0s"}, {"lock-session-idle", "1s"},
		{"http-max-concurrent-lock-controls", "0"}, {"http-max-waiting-lock-controls", "-1"},
	} {
		t.Run(test.name+"="+test.value, func(t *testing.T) {
			root := t.TempDir()
			err := run([]string{
				"-listen", "invalid-address", "-local-store", root, "-workspace", "workspace", "-quota", "8M", "-initialize-lock-state",
				"-" + test.name, test.value,
			}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "-"+test.name) {
				t.Fatalf("invalid lock option reached startup: %v", err)
			}
			for _, directory := range []string{root} {
				entries, readErr := os.ReadDir(directory)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if len(entries) != 0 {
					t.Fatalf("invalid configuration created %d entries", len(entries))
				}
			}
		})
	}
}

func TestUnsupportedNamespaceFlagsFailBeforeInitialization(t *testing.T) {
	for _, name := range []string{
		"dir", "lock-state-root", "dir-max-operations", "dir-max-waiters", "dir-max-pinned-targets",
		"dir-max-snapshot-entries", "dir-max-recovery-entries", "dir-max-path-bytes",
		"dir-max-staging-bytes", "dir-max-snapshot-bytes", "quota-max-directory-bytes", "quota-max-frontier-bytes",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			var output bytes.Buffer
			err := run([]string{
				"-listen", "invalid-address", "-local-store", root, "-workspace", "workspace", "-quota", "8M",
				"-initialize-lock-state", "-" + name, "1",
			}, &output)
			if !errors.Is(err, errUsage) || !strings.Contains(output.String(), "flag provided but not defined: -"+name) {
				t.Fatalf("unsupported %s returned %v: %s", name, err, output.String())
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("unsupported option changed storage: entries=%d err=%v", len(entries), readErr)
			}
		})
	}
}

func TestLockHelpExplainsExplicitInitializationAndRecovery(t *testing.T) {
	var output bytes.Buffer
	_, help, err := parseConfig([]string{"-help"}, &output)
	if err != nil || !help {
		t.Fatalf("help=%t err=%v", help, err)
	}
	for _, phrase := range []string{"-initialize-lock-state", "-lock-max-lease", "restart recovery", "existing state", "enrollment"} {
		if !strings.Contains(strings.ToLower(output.String()), strings.ToLower(phrase)) {
			t.Fatalf("help omits %q: %s", phrase, output.String())
		}
	}
}
