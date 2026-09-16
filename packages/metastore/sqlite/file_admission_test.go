package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestFileBudgetAndLeaseReadCancelBeforeHealthWriter(t *testing.T) {
	opened, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := opened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, test := range []struct {
		name string
		call func(context.Context) error
	}{
		{"materialization", func(ctx context.Context) error {
			release, err := opened.AcquireMaterialization(ctx, 1)
			if release != nil {
				release()
			}
			return err
		}},
		{"lease read", func(ctx context.Context) error { _, err := opened.MaxLease(ctx); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := opened.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			opened.coordinator.health.Lock()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			done := make(chan error, 1)
			go func() { done <- test.call(ctx) }()
			finished := false
			select {
			case err := <-done:
				finished = true
				if !errors.Is(err, context.Canceled) {
					t.Errorf("canceled admission=%v", err)
				}
			case <-time.After(time.Second):
				t.Error("canceled admission waited on health writer")
			}
			opened.coordinator.health.Unlock()
			opened.coordinator.commit.release()
			if !finished {
				<-done
			}
		})
	}
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < opened.fileDomain.contentConfig.MaxMaterializations; i++ {
		release, err := opened.AcquireMaterialization(t.Context(), 0)
		if err != nil {
			t.Fatalf("canceled request consumed capacity at %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	if release, err := opened.AcquireMaterialization(t.Context(), 0); !errors.Is(err, syscall.EAGAIN) {
		if release != nil {
			release()
		}
		t.Fatalf("capacity=%v", err)
	}
}
