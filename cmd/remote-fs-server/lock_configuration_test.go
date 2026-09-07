package main

import (
	"bytes"
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
		"-listen", "127.0.0.1:0", "-dir", "/namespace", "-lock-state-root", "/state", "-initialize-lock-state",
	}, io.Discard)
	if err != nil || help {
		t.Fatalf("parse directory locks: help=%t err=%v", help, err)
	}
	if config.locks != locking.DefaultOptions() || config.lockStateRoot != "/state" || !config.initializeLockState {
		t.Fatalf("lock configuration is %+v, state=%q initialize=%t", config.locks, config.lockStateRoot, config.initializeLockState)
	}
	openedConfig, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-dir", "/namespace", "-lock-state-root", "/state",
	}, io.Discard)
	if err != nil || openedConfig.initializeLockState {
		t.Fatalf("ordinary startup permits initialization: config=%+v err=%v", openedConfig, err)
	}
}

func TestLockCapacityAndLifetimeFlagsReachAuthorityOptions(t *testing.T) {
	config, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-dir", "/namespace", "-lock-state-root", "/state",
		"-lock-max-sessions", "11", "-lock-max-tickets", "13", "-lock-max-owners", "17",
		"-lock-max-resources", "19", "-lock-max-actions", "23", "-lock-max-grants", "29", "-lock-max-queued", "31",
		"-lock-owners-per-session", "3", "-lock-owner-actions-per-session", "5", "-lock-actions-per-owner", "7",
		"-lock-grants-per-owner", "2", "-lock-queued-per-owner", "4", "-lock-queued-per-resource", "6",
		"-lock-max-proofs", "8", "-lock-max-request-bytes", "37",
		"-lock-max-lease", "2s", "-lock-max-wait", "3s", "-lock-ticket-ttl", "4s",
		"-lock-session-idle", "5s", "-lock-resource-ttl", "6s",
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
	} {
		t.Run(test.name+"="+test.value, func(t *testing.T) {
			root, state := t.TempDir(), t.TempDir()
			err := run([]string{
				"-listen", "invalid-address", "-dir", root, "-lock-state-root", state, "-initialize-lock-state",
				"-" + test.name, test.value,
			}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "-"+test.name) {
				t.Fatalf("invalid lock option reached startup: %v", err)
			}
			for _, directory := range []string{root, state} {
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

func TestLockStateRootRequiresDirectorySource(t *testing.T) {
	for _, args := range [][]string{
		{"-dir", "/namespace"},
		{"-local-store", "/store", "-workspace", "workspace", "-quota", "8M", "-lock-state-root", "/state"},
		{"-blob-container", "container", "-metastore", "/meta", "-workspace", "workspace", "-lock-state-root", "/state"},
	} {
		_, _, err := parseConfig(append([]string{"-listen", "127.0.0.1:0"}, args...), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "-lock-state-root") {
			t.Fatalf("invalid state configuration returned %v", err)
		}
	}
}

func TestLockHelpExplainsExplicitInitializationAndRecovery(t *testing.T) {
	var output bytes.Buffer
	_, help, err := parseConfig([]string{"-help"}, &output)
	if err != nil || !help {
		t.Fatalf("help=%t err=%v", help, err)
	}
	for _, phrase := range []string{"-lock-state-root", "-initialize-lock-state", "-lock-max-lease", "same mount", "restart recovery", "existing state", "enrollment"} {
		if !strings.Contains(strings.ToLower(output.String()), strings.ToLower(phrase)) {
			t.Fatalf("help omits %q: %s", phrase, output.String())
		}
	}
}
