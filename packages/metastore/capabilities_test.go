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

func TestReferenceSessionAndAccessContextPreserveExplicitBindings(t *testing.T) {
	type key struct{}
	source := context.WithValue(t.Context(), key{}, "host")
	if metastore.ReferenceSession(source) != nil {
		t.Fatal("missing session became bound")
	}
	session := new(advisory.Session)
	bound := metastore.WithReferenceSession(source, session)
	if metastore.ReferenceSession(bound) != session || metastore.ReferenceSession(source) != nil || bound.Value(key{}) != "host" {
		t.Fatal("session binding altered context ancestry")
	}
	if _, present := metastore.FileAccessFrom(bound); present {
		t.Fatal("metadata capture invented data access")
	}
	access := metastore.FileAccess{Uses: storage.WriteData, Offset: 2, Length: 4}
	scoped := metastore.WithFileAccess(bound, access)
	if got, present := metastore.FileAccessFrom(scoped); !present || got != access || metastore.ReferenceSession(scoped) != session {
		t.Fatalf("lost access/session binding %+v", got)
	}
	if _, present := metastore.FileAccessFrom(bound); present {
		t.Fatal("access modified preceding context")
	}
}

func TestFileAccessBoundsDescribeLogicalUserExtent(t *testing.T) {
	for _, a := range []metastore.FileAccess{{Uses: storage.ReadData}, {Uses: storage.WriteData, Offset: math.MaxInt64}, {Uses: storage.WriteData, Truncate: true, Size: 9}, {Uses: storage.ReadData, Offset: 3, Length: 7}, {Uses: storage.WriteData, Append: true, Length: 4}} {
		if err := a.Check(); err != nil {
			t.Fatalf("validlogicalaccess%+v=%v", a, err)
		}
	}
	for _, a := range []metastore.FileAccess{{}, {Uses: storage.ReadEntries}, {Uses: storage.ReadData | storage.WriteData}, {Uses: storage.WriteData, Offset: -1}, {Uses: storage.ReadData, Length: -1}, {Uses: storage.ReadData, Offset: math.MaxInt64, Length: 1}, {Uses: storage.WriteData, Size: 1}, {Uses: storage.WriteData, Truncate: true, Size: -1}, {Uses: storage.ReadData, Truncate: true}, {Uses: storage.WriteData, Truncate: true, Offset: 1}, {Uses: storage.WriteData, Truncate: true, Length: 1}, {Uses: storage.ReadData, Append: true}, {Uses: storage.WriteData, Append: true, Truncate: true}, {Uses: storage.WriteData, Append: true, Offset: 1}, {Uses: storage.WriteData, Append: true, Size: 1}} {
		if !errors.Is(a.Check(), syscall.EINVAL) {
			t.Fatalf("invalidlogicalextentaccepted:%+v", a)
		}
	}
}
