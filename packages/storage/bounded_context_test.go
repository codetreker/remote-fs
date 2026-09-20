package storage_test

import (
	"context"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestBoundedListResultCarriesTheSmallestOuterTransportLimit(t *testing.T) {
	ctx := storage.WithBoundedListResult(context.Background(), 4096)
	ctx = storage.WithBoundedListResult(ctx, 1024)
	ctx = storage.WithBoundedListResult(ctx, 2048)
	limit, ok := storage.ListResultByteLimit(ctx)
	if !ok || limit != 1024 {
		t.Fatalf("list result limit = %d, %v", limit, ok)
	}
}

func TestBoundedListResultRejectsNonPositiveLimits(t *testing.T) {
	for name, limit := range map[string]int64{"zero": 0, "negative": -1} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("limit %d did not panic", limit)
				}
			}()
			_ = storage.WithBoundedListResult(context.Background(), limit)
		})
	}
}
