package sqlite

import (
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestFileOperationLimitsPreserveConfiguredBoundaries(t *testing.T) {
	for _, test := range []struct {
		name     string
		bytes    int64
		attempts int
		timeout  time.Duration
	}{
		{"defaults", 1 << 30, 8, 30 * time.Second}, {"minimum", 1, 1, time.Nanosecond}, {"custom", 17, 3, 150 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := lockingTestConfig(t)
			config.SQLite.Files.MaxFileBytes = test.bytes
			config.SQLite.Files.MaxMaterializedBytes = 2 * test.bytes
			config.SQLite.Files.MaxFileAttempts = test.attempts
			config.SQLite.Files.FileOperationTimeout = test.timeout
			s, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			bytes, attempts, timeout := s.Store.FileOperationLimits()
			if bytes != test.bytes || attempts != test.attempts || timeout != test.timeout {
				t.Fatalf("operation limits=%d/%d/%s want%d/%d/%s", bytes, attempts, timeout, test.bytes, test.attempts, test.timeout)
			}
		})
	}
}

func TestFileOperationLimitsRejectUnservableConfiguration(t *testing.T) {
	for _, scenario := range []string{"two copies", "no attempts", "no timeout"} {
		t.Run(scenario, func(t *testing.T) {
			config := lockingTestConfig(t)
			switch scenario {
			case "two copies":
				config.SQLite.Files.MaxMaterializedBytes = 3
				config.SQLite.Files.MaxFileBytes = 2
			case "no attempts":
				config.SQLite.Files.MaxFileAttempts = 0
			case "no timeout":
				config.SQLite.Files.FileOperationTimeout = 0
			}
			s, err := OpenLocking(t.Context(), config)
			if s != nil {
				_ = s.Close()
			}
			if s != nil || !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("unservable operation configuration returnedstore=%v err=%v", s, err)
			}
		})
	}
}
