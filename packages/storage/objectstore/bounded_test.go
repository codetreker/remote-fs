package objectstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestBoundedReadRefusesFromMetadataBeforeFetchingTheObject(t *testing.T) {
	objects := &observedBoundedObjects{Objects: memory.New()}
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 1024, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if err := namespace.Write(t.Context(), "f", []byte("four")); err != nil {
		t.Fatal(err)
	}
	objects.gets = 0
	if got, err := namespace.ReadBounded(t.Context(), "f", 3); got != nil || !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("ReadBounded returned %q, %v; want EFBIG", got, err)
	}
	if objects.gets != 0 {
		t.Fatalf("the oversized read fetched %d objects before refusal", objects.gets)
	}
	if got, err := namespace.ReadBounded(t.Context(), "f", 4); err != nil || string(got) != "four" {
		t.Fatalf("ReadBounded at the boundary returned %q, %v", got, err)
	}
}

func TestBoundedStorageCheckRefusesAnUnboundedDependency(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 1024, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	namespace := objectstore.New(struct{ objectstore.Objects }{Objects: memory.New()}, meta)
	t.Cleanup(func() { _ = namespace.Close() })
	if err := namespace.CheckBounded(); err == nil {
		t.Fatal("CheckBounded accepted an object backend without BoundedObjects")
	}

	meta2, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 1024, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	namespace2 := objectstore.New(memory.New(), struct{ metastore.Store }{Store: meta2})
	t.Cleanup(func() { _ = namespace2.Close() })
	if err := namespace2.CheckBounded(); err == nil {
		t.Fatal("CheckBounded accepted a metastore without BoundedLister")
	}
}

func TestBoundedListDoesNotRetainTheEntryThatCrossesItsBudget(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 1024, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	namespace := objectstore.New(memory.New(), meta)
	t.Cleanup(func() { _ = namespace.Close() })
	for _, name := range []string{"a", "bb", "ccc"} {
		if err := namespace.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	result, err := storage.NewListResult(3, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded returned %v, want EIO", err)
	}
	if entries, resultErr := result.Entries(); resultErr == nil || entries != nil {
		t.Fatalf("the refused entry left a usable bounded listing: %+v, %v", entries, resultErr)
	}
}

type observedBoundedObjects struct {
	*memory.Objects
	gets int
}

func (o *observedBoundedObjects) GetBounded(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	o.gets++
	return o.Objects.GetBounded(ctx, key, maxBytes)
}
