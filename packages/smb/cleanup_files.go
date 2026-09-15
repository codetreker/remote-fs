package smb

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func (d *fileDispatcher) closeAll(ctx context.Context) error {
	d.mu.Lock()
	handles := make(map[wire.FileID]*fileHandle, len(d.handles))
	for id, h := range d.handles {
		handles[id] = h
	}
	d.mu.Unlock()
	for id, h := range handles {
		action, err := d.actionID()
		if err != nil {
			return err
		}
		result, err := h.file.Close(ctx, action)
		if d.mutationResult(action, result, err) != 0 {
			return syscall.EIO
		}
		d.mu.Lock()
		_, existed := d.handles[id]
		delete(d.handles, id)
		d.mu.Unlock()
		d.leases.release(h.lease)
		if existed && d.onOpen != nil {
			d.onOpen(-1)
		}
	}
	return nil
}
