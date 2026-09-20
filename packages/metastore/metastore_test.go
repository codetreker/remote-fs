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
		node := metastore.Node{ID: 42, Kind: kind, Size: 123, Content: "object-key", AccessTime: time.Unix(-1, 123).UTC(), ModTime: time.Unix(4, 567).UTC()}
		want := storage.Attr{ID: 42, Kind: kind, Size: 123, AccessTime: node.AccessTime, ModTime: node.ModTime}
		if got := node.Attr(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Attr = %+v; want %+v", got, want)
		}
		if got := node.IsDir(); got != (kind == storage.NodeDirectory) {
			t.Fatalf("IsDir(%v) = %v", kind, got)
		}
	}
}
