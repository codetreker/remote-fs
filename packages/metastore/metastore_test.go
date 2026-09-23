package metastore_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNodeAttrPreservesIdentityTypeAndTimes(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		allocation := int64(0)
		if kind == storage.NodeRegular {
			allocation = 4096
		}
		node := metastore.Node{ID: 42, Kind: kind, Size: 123, AllocationSize: allocation, AllocationKnown: true, Content: "object-key", AccessTime: time.Unix(-1, 123).UTC(), ModTime: time.Unix(4, 567).UTC()}
		want := storage.Attr{ID: 42, Kind: kind, Size: 123, AllocationSize: allocation, AllocationKnown: true, AccessTime: node.AccessTime, ModTime: node.ModTime}
		if got := node.Attr(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Attr = %+v; want %+v", got, want)
		}
		if got := node.IsDir(); got != (kind == storage.NodeDirectory) {
			t.Fatalf("IsDir(%v) = %v", kind, got)
		}
	}
}

func TestNodeAttrPreservesUnknownThirdPartyAllocation(t *testing.T) {
	node := metastore.Node{ID: 7, Kind: storage.NodeRegular, Size: 9}
	attr := node.Attr()
	if attr.AllocationKnown || attr.AllocationSize != 0 {
		t.Fatalf("unknown allocation was reported as measured: %+v", attr)
	}
}

func TestNodeCloneOwnsDirectoryRevision(t *testing.T) {
	revision := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	node := metastore.Node{ID: 1, Kind: storage.NodeDirectory, DirectoryRevision: revision}
	clone := node.Clone()
	revision[7] = 2
	if !reflect.DeepEqual(clone.DirectoryRevision, []byte{0, 0, 0, 0, 0, 0, 0, 1}) {
		t.Fatalf("clone aliases directory revision: %x", clone.DirectoryRevision)
	}
}
