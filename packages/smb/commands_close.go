package smb

import (
	"context"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (c *connection) closeFile(ctx context.Context, s *session, t *tree, request wire.Request, inherited wire.FileID) ([]byte, uint32) {
	parsed, err := request.Close()
	if err != nil || parsed.Flags&^uint16(1) != 0 {
		return nil, statusInvalid
	}
	if parsed.FileID == wire.InvalidFileID && request.Header.Flags&wire.FlagRelated != 0 {
		parsed.FileID = inherited
	}
	if parsed.FileID == (wire.FileID{}) || parsed.FileID.IsRelatedPlaceholder() || parsed.FileID.HasPartialRelatedPlaceholder() {
		return nil, closeStatusError(syscall.EBADF)
	}
	if !t.beginFileWork(s) {
		return nil, statusNetworkDeleted
	}
	defer t.endFileWork()
	handle := t.findFileHandle(parsed.FileID)
	if handle == nil {
		return nil, closeStatusError(syscall.EBADF)
	}
	if err := handle.requestCloseMu.lock(ctx); err != nil {
		return nil, closeStatusError(err)
	}
	defer handle.requestCloseMu.unlock()
	if t.findFileHandle(parsed.FileID) != handle {
		return nil, closeStatusError(syscall.EBADF)
	}
	if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileClose}); err != nil {
		return nil, closeStatusError(err)
	}
	response := wire.CloseResponse{}
	if parsed.Flags&1 != 0 {
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileStat}); err == nil {
			var attr storage.Attr
			if handle.file != nil {
				attr, err = handle.file.Stat(ctx)
			} else if handle.node != nil {
				attr, err = handle.node.Stat(ctx)
			}
			if err == nil {
				var metadata createMetadata
				metadata, err = projectCreateMetadata(attr)
				if err == nil {
					response.Flags = 1
					response.CreationTime, response.LastAccessTime, response.LastWriteTime, response.ChangeTime = metadata.CreationTime, metadata.LastAccessTime, metadata.LastWriteTime, metadata.ChangeTime
					response.AllocationSize, response.EndOfFile, response.Attributes = metadata.AllocationSize, metadata.EndOfFile, metadata.Attributes
				}
			}
		}
	}
	if err := t.closeFileHandle(ctx, handle); err != nil {
		return nil, closeStatusError(err)
	}
	return wire.CloseResponseBody(response), statusOK
}
