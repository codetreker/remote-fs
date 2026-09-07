package main

import (
	"io"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestDirectoryNativeLimitsAreConfigurable(t *testing.T) {
	config, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-dir", "/namespace", "-lock-state-root", "/state",
		"-dir-max-operations", "2", "-dir-max-waiters", "3", "-dir-max-pinned-targets", "5",
		"-dir-max-snapshot-entries", "7", "-dir-max-recovery-entries", "11", "-dir-max-path-bytes", "13",
		"-dir-max-staging-bytes", "2M", "-dir-max-snapshot-bytes", "3M", "-http-max-write-bytes", "2M",
		"-http-max-concurrent-lock-controls", "17", "-http-max-waiting-lock-controls", "19",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := localdir.Limits{
		MaxOperations: 2, MaxWaiters: 3, MaxPinnedTargets: 5, MaxSnapshotEntries: 7,
		MaxRecoveryEntries: 11, MaxPathBytes: 13, MaxStagingBytes: 2 << 20, MaxSnapshotBytes: 3 << 20,
	}
	if config.directoryLimits != want || config.http.MaxConcurrentLockControls != 17 || config.http.MaxWaitingLockControls != 19 {
		t.Fatalf("directory=%+v HTTP controls=%d/%d", config.directoryLimits, config.http.MaxConcurrentLockControls, config.http.MaxWaitingLockControls)
	}
}

func TestDirectoryNativeLimitsRejectInvalidConfiguration(t *testing.T) {
	for _, test := range []struct{ flag, value string }{
		{"dir-max-operations", "0"}, {"dir-max-waiters", "-1"}, {"dir-max-pinned-targets", "0"},
		{"dir-max-snapshot-entries", "0"}, {"dir-max-recovery-entries", "0"}, {"dir-max-path-bytes", "0"},
		{"dir-max-pinned-targets", strconv.Itoa(math.MaxInt)},
		{"dir-max-staging-bytes", "1M"}, {"dir-max-snapshot-bytes", "9223372036854775807"},
		{"http-max-concurrent-lock-controls", "0"}, {"http-max-waiting-lock-controls", "-1"},
	} {
		t.Run(test.flag+"="+test.value, func(t *testing.T) {
			_, _, err := parseConfig([]string{
				"-listen", "invalid", "-dir", "/namespace", "-lock-state-root", "/state",
				"-http-max-write-bytes", "2M",
				"-" + test.flag, test.value,
			}, io.Discard)
			if err == nil {
				t.Fatal("invalid native directory bound was accepted")
			}
		})
	}
}

func TestDirectoryInheritedWriteLimitFitsNativeStaging(t *testing.T) {
	base := []string{"-listen", "127.0.0.1:0", "-dir", "/namespace", "-lock-state-root", "/state"}
	for _, test := range []struct {
		name  string
		flags []string
		want  int64
	}{
		{"package defaults preserve the HTTP write ceiling", nil, httprest.DefaultMaxBodyBytes},
		{"body below staging", []string{"-http-max-body-bytes", "2M"}, 2 << 20},
		{"staging below body", []string{"-dir-max-staging-bytes", "3M"}, 3 << 20},
		{"explicit bound", []string{"-http-max-write-bytes", "4M"}, 4 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, _, err := parseConfig(append(append([]string{}, base...), test.flags...), io.Discard)
			if err != nil || config.http.MaxWriteBytes != test.want {
				t.Fatalf("directory write limit=%d err=%v, want %d", config.http.MaxWriteBytes, err, test.want)
			}
		})
	}
	_, _, err := parseConfig(append(base, "-dir-max-staging-bytes", "3M", "-http-max-write-bytes", "4M"), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-http-max-write-bytes") || !strings.Contains(err.Error(), "-dir-max-staging-bytes") {
		t.Fatalf("explicit write above staging returned %v", err)
	}
}

func TestDirectoryNativeLimitsRequireDirectoryMode(t *testing.T) {
	for _, name := range []string{
		"dir-max-operations", "dir-max-waiters", "dir-max-pinned-targets", "dir-max-snapshot-entries",
		"dir-max-recovery-entries", "dir-max-path-bytes", "dir-max-staging-bytes", "dir-max-snapshot-bytes",
	} {
		_, _, err := parseConfig([]string{
			"-listen", "127.0.0.1:0", "-local-store", "/store", "-workspace", "workspace", "-quota", "8M", "-" + name, "1",
		}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "-"+name+" requires -dir") {
			t.Fatalf("%s returned %v", name, err)
		}
	}
}
