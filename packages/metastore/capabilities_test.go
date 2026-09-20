package metastore_test

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/advisory"
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

func TestReferenceSessionRoundTripsAndDefaultsToNil(t *testing.T) {
	if session := metastore.ReferenceSession(t.Context()); session != nil {
		t.Fatalf("unbound reference session = %p", session)
	}
	want := &advisory.Session{}
	ctx := metastore.WithReferenceSession(t.Context(), want)
	if session := metastore.ReferenceSession(ctx); session != want {
		t.Fatalf("reference session = %p, want %p", session, want)
	}
	if session := metastore.ReferenceSession(context.WithValue(ctx, struct{}{}, "unrelated")); session != want {
		t.Fatalf("unrelated context value changed reference session = %p", session)
	}
}
