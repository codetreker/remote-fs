package smb

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

type rejectedCloseFile struct{ windowsFile }

func (rejectedCloseFile) Close(context.Context, windowsActionID) (windowsActionResult, error) {
	return windowsActionResult{}, syscall.EACCES
}

func TestTreeCleanupRetainsUnconfirmedReferences(t *testing.T) {
	d, f, _, id := commandDispatcher()
	if err := d.closeAll(context.Background()); err != nil || d.handleCount() != 0 || f.closed != 1 {
		t.Fatalf("closed %v count%d", err, d.handleCount())
	}
	d.handles[id] = &fileHandle{file: rejectedCloseFile{}}
	if err := d.closeAll(context.Background()); !errors.Is(err, syscall.EIO) || d.handleCount() != 1 {
		t.Fatalf("unconfirmed %v count%d", err, d.handleCount())
	}
}
