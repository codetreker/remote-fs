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
		node := metastore.Node{ID: 42, Kind: kind, Size: 123, Content: "object-key", AccessTime: time.Unix(-1, 123), ModTime: time.Unix(4, 567)}
		want := storage.Attr{ID: 42, Kind: kind, Size: 123, AccessTime: node.AccessTime, ModTime: node.ModTime}
		if got := node.Attr(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Attr = %+v; want %+v", got, want)
		}
		if got := node.IsDir(); got != (kind == storage.NodeDirectory) {
			t.Fatalf("IsDir(%v) = %v", kind, got)
		}
	}
}

func TestNodeSnapshotsOwnTheirMetadataAndTargets(t *testing.T) {
	created, changed := time.Unix(1, 2), time.Unix(3, 4)
	node := metastore.Node{ID: 42, Kind: storage.NodeSymlink,
		CreationTime: &created, ChangeTime: &changed,
		MetadataRevision: 7, Metadata: storage.Metadata{{Key: "application", Version: 1, Data: []byte("a")}},
		LinkTarget: []byte("target")}
	attr, cloned := node.Attr(), node.Clone()
	attr.Metadata[0].Data[0] = 'b'
	*attr.CreationTime = time.Time{}
	cloned.Metadata[0].Data[0] = 'c'
	cloned.LinkTarget[0] = 'X'
	*cloned.ChangeTime = time.Time{}
	if string(node.Metadata[0].Data) != "a" || string(node.LinkTarget) != "target" ||
		!node.CreationTime.Equal(created) || !node.ChangeTime.Equal(changed) || attr.MetadataRevision != 7 {
		t.Fatalf("snapshot changed source metadata: %+v", node)
	}
	if empty := (metastore.Node{}).Clone(); empty.CreationTime != nil || empty.ChangeTime != nil || empty.Metadata != nil || empty.LinkTarget != nil {
		t.Fatalf("unknown facts were populated: %+v", empty)
	}
}
