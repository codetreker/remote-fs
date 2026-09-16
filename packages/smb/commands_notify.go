package smb

import (
	"context"
	"errors"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) notify(ctx context.Context, t *tree, r wire.Request, key *signing.Session) ([]byte, uint32) {
	n, err := r.Notify()
	if err != nil || n.Flags&^uint16(1) != 0 || n.OutputLength == 0 || n.OutputLength > uint32(c.server.config.Limits.MaxIOBytes) {
		return nil, statusInvalid
	}
	h := t.files.get(n.FileID)
	if h == nil {
		return nil, fileClosed
	}
	if h.access&1 == 0 {
		return nil, statusDenied
	}
	if err := h.file.Sync(ctx); err != nil {
		return nil, statusError(err)
	}
	registered := func() error {
		if pending, ok := ctx.Value(pendingKey{}).(func(*signing.Session) error); ok {
			return pending(key)
		}
		return nil
	}
	events, err := t.export.changes.watchRegistered(ctx, c.notifyKey(n.FileID), h.identity, n.Filter, n.Flags&1 != 0, n.OutputLength, registered, h.file.ValidateNotificationLocation)
	if errors.Is(err, ErrNotifyRescan) {
		return nil, 0x10c
	}
	if err != nil {
		return nil, statusError(err)
	}
	if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpReplicationSubscribe}); err != nil {
		t.export.changes.remove(c.notifyKey(n.FileID))
		return nil, statusError(err)
	}
	if err := h.file.Sync(ctx); err != nil {
		t.export.changes.remove(c.notifyKey(n.FileID))
		return nil, statusError(err)
	}
	return wire.BufferResponseBody(wire.NotifyInformation(events)), 0
}
