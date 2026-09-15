package smb

import (
	"context"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestUnknownMutationRemainsObservableAfterReferencesRetire(t *testing.T) {
	c, _, tr, f, ws, _ := testConnection(t)
	f.writeErr = syscall.EIO
	ws.queryErr = syscall.EIO
	if _, status := tr.files.handle(context.Background(), writeCommand(wire.FileID{1}, []byte("uncertain"))); status != statusIO {
		t.Fatal(status)
	}
	status := c.server.Status()
	if status.UnconfirmedMutations != 1 || status.FencedTrees != 1 {
		t.Fatalf("failure hidden %+v", status)
	}
	if err := c.closeTree(tr); err != nil {
		t.Fatal(err)
	}
	status = c.server.Status()
	if status.UnconfirmedMutations != 1 || status.FencedTrees != 0 {
		t.Fatalf("retirement erased unknown result %+v", status)
	}
}
