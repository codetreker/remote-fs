package metastore_test

import (
	"context"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileIOContextDistinguishesMetadataFromEmptyDataIO(t *testing.T) {
	if operation, present := metastore.FileIOFromContext(t.Context()); present || operation != (storage.FileIO{}) {
		t.Fatalf("metadata context carries data admission: %+v %t", operation, present)
	}
	ctx := metastore.WithFileIO(t.Context(), storage.FileIO{})
	if operation, present := metastore.FileIOFromContext(ctx); !present || operation != (storage.FileIO{}) {
		t.Fatalf("explicit zero I/O was lost: %+v %t", operation, present)
	}
}

func TestFileIOContextPreservesRangeLifetimeAndCallerValues(t *testing.T) {
	type actorKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(t.Context(), actorKey{}, "native-reference-owner"))
	operation := storage.FileIO{Offset: 19, Length: 23, Write: true, Truncate: true, Size: 7}
	ctx := metastore.WithFileIO(parent, operation)
	if stored, present := metastore.FileIOFromContext(ctx); !present || stored != operation {
		t.Fatalf("logical range changed: %+v %t", stored, present)
	}
	if ctx.Value(actorKey{}) != "native-reference-owner" {
		t.Fatal("logical I/O replaced the caller's context")
	}
	cancel()
	if ctx.Err() != context.Canceled {
		t.Fatal("logical I/O detached request cancellation")
	}
}

func TestFileIOContextOwnsItsValueAndChildOverrides(t *testing.T) {
	input := storage.FileIO{Offset: 11, Length: 13, Write: true}
	parent := metastore.WithFileIO(t.Context(), input)
	expected := input
	input.Offset = 999
	input.Write = false
	childValue := storage.FileIO{Truncate: true, Write: true, Size: 3}
	child := metastore.WithFileIO(parent, childValue)
	if stored, present := metastore.FileIOFromContext(parent); !present || stored != expected {
		t.Fatalf("input or child changed parent I/O: %+v %t", stored, present)
	}
	if stored, present := metastore.FileIOFromContext(child); !present || stored != childValue {
		t.Fatalf("child override was lost: %+v %t", stored, present)
	}
}

func TestFileIOContextCopiesOptionalConditions(t *testing.T) {
	owner, size := storage.RangeOwnerID(7), int64(13)
	ctx := metastore.WithFileIO(t.Context(), storage.FileIO{Owner: &owner, ExpectedSize: &size, Write: true})
	owner, size = 99, 100
	first, _ := metastore.FileIOFromContext(ctx)
	if *first.Owner != 7 || *first.ExpectedSize != 13 {
		t.Fatalf("input changed captured conditions: %+v", first)
	}
	*first.Owner, *first.ExpectedSize = 3, 4
	second, _ := metastore.FileIOFromContext(ctx)
	if *second.Owner != 7 || *second.ExpectedSize != 13 {
		t.Fatalf("returned pointers changed context: %+v", second)
	}
}
