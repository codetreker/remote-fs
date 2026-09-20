package metastore_test

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileAccessValidationAndContextRoundTrip(t *testing.T) {
	valid := []metastore.FileAccess{
		{Uses: storage.ReadData, Offset: 1, Length: 2},
		{Uses: storage.WriteData, Offset: 1, Length: 2},
		{Uses: storage.WriteData, Append: true},
		{Uses: storage.WriteData, Truncate: true, Size: 3},
	}
	for _, access := range valid {
		if err := access.Check(); err != nil {
			t.Fatalf("valid access %+v: %v", access, err)
		}
		got, ok := metastore.FileAccessFrom(metastore.WithFileAccess(t.Context(), access))
		if !ok || got != access {
			t.Fatalf("context access = %+v, %v; want %+v", got, ok, access)
		}
	}
	for _, access := range []metastore.FileAccess{
		{},
		{Uses: storage.ReadEntries},
		{Uses: storage.ReadData, Offset: -1},
		{Uses: storage.ReadData, Offset: math.MaxInt64, Length: 1},
		{Uses: storage.ReadData, Append: true},
		{Uses: storage.WriteData, Truncate: true, Offset: 1},
		{Uses: storage.WriteData, Size: 1},
	} {
		if err := access.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("invalid access %+v = %v", access, err)
		}
	}
	if _, ok := metastore.FileAccessFrom(t.Context()); ok {
		t.Fatal("plain context unexpectedly carries file access")
	}
}
