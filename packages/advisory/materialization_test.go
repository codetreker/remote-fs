package advisory

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
)

func TestMaterializationBoundsAndRelease(t *testing.T) {
	for _, test := range []struct {
		name          string
		operations    int
		first, second int64
	}{{"bytes", 2, 7, 4}, {"operations", 1, 4, 4}} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.MaxMaterializedBytes = 10
			config.MaxFileBytes = 5
			config.MaxMaterializations = test.operations
			c := fixture(t, config)
			release, err := c.AcquireMaterialization(background, test.first)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.AcquireMaterialization(background, test.second); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("admission = %v", err)
			}
			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				go func() { defer wg.Done(); release() }()
			}
			wg.Wait()
			release, err = c.AcquireMaterialization(background, 10)
			if err != nil {
				t.Fatal(err)
			}
			release()
			if c.materializedBytes != 0 || c.materializations != 0 {
				t.Fatal("materialization charge survived release")
			}
			bytes, attempts, timeout := c.FileOperationLimits()
			if bytes != config.MaxFileBytes || attempts != config.MaxFileAttempts || timeout != config.FileOperationTimeout {
				t.Fatal("file operation limits differ from configured limits")
			}
		})
	}
}

func TestMaterializationRejectsInvalidAndCancelledRequests(t *testing.T) {
	config := DefaultConfig()
	config.MaxMaterializedBytes = 10
	config.MaxFileBytes = 5
	c := fixture(t, config)
	for _, test := range []struct {
		bytes int64
		errno syscall.Errno
	}{{-1, syscall.EINVAL}, {11, syscall.EFBIG}} {
		if _, err := c.AcquireMaterialization(background, test.bytes); !errors.Is(err, test.errno) {
			t.Fatalf("admission = %v; want %v", err, test.errno)
		}
	}
	ctx, cancel := context.WithCancel(background)
	cancel()
	if _, err := c.AcquireMaterialization(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission = %v", err)
	}
	if c.materializations != 0 || c.materializedBytes != 0 {
		t.Fatal("rejected admission retained a charge")
	}
}

func TestFileAndMaterializationLimitsAgree(t *testing.T) {
	defaults := DefaultConfig()
	if defaults.MaxFileBytes != 1<<30 || defaults.MaxMaterializedBytes != 2<<30 {
		t.Fatalf("file defaults = %d/%d; want 1 GiB file and 2 GiB materialization", defaults.MaxFileBytes, defaults.MaxMaterializedBytes)
	}
	for _, test := range []struct {
		name        string
		file, total int64
		valid       bool
	}{
		{"exact two buffers", 5, 10, true},
		{"one byte short", 5, 9, false},
		{"larger aggregate", 5, 11, true},
		{"overflowing pair", 1 << 62, 1<<63 - 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := defaults
			config.MaxFileBytes, config.MaxMaterializedBytes = test.file, test.total
			err := config.Check()
			if test.valid && err != nil || !test.valid && !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("limits %d/%d: %v; valid=%v", test.file, test.total, err, test.valid)
			}
		})
	}
}
