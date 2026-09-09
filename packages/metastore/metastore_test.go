package metastore_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNodeAttrPreservesIdentityTypeAndTimes(t *testing.T) {
	for _, mode := range []fs.FileMode{0o640, fs.ModeDir | 0o750, fs.ModeSymlink | 0o777} {
		node := metastore.Node{ID: 42, Mode: mode, Size: 123, Content: "object-key", AccessTime: time.Unix(-1, 123), ModTime: time.Unix(4, 567)}
		want := storage.Attr{ID: 42, Mode: mode, Size: 123, AccessTime: node.AccessTime, ModTime: node.ModTime}
		if got := node.Attr(); got != want {
			t.Fatalf("Attr = %+v; want %+v", got, want)
		}
		if got := node.IsDir(); got != (mode&fs.ModeDir != 0) {
			t.Fatalf("IsDir(%v) = %v", mode, got)
		}
	}
}
