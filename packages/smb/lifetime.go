package smb

import (
	"context"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) renew(t *tree, principal Principal) {
	defer c.wg.Done()
	interval := c.server.config.Limits.FileSession.Lease / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.done:
			return
		case <-ticker.C:
			t.closeMu.Lock()
			closed := t.closed
			t.closeMu.Unlock()
			if closed {
				return
			}
			ctx, cancel := context.WithTimeout(WithPrincipal(c.ctx, principal), interval)
			err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileRenew})
			if err == nil {
				state, renewErr := t.session.Renew(ctx)
				err = renewErr
				if err == nil && (state.Fenced || state.Retired || state.Remaining <= 0) {
					err = ErrStopped
				}
			}
			cancel()
			if err != nil {
				c.closeTree(t)
				return
			}
		}
	}
}
