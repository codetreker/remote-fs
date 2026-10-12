package smb

import (
	"context"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) queryDirectory(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.QueryDirectory()
	if err != nil {
		return nil, statusInvalid
	}
	head, supported := wire.DirectoryInformationHeadSize(parsed.FileInformationClass)
	if !supported {
		if wire.IsDirectoryInformationClass(parsed.FileInformationClass) {
			return nil, statusUnsupported
		}
		return nil, statusInvalidInfoClass
	}
	if parsed.OutputBufferLength > uint32(c.server.config.Limits.MaxIOBytes) {
		return nil, statusInvalid
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h := t.findFileHandle(id)
	if h == nil {
		return nil, statusInvalidHandle
	}
	if h.nodeKind != storage.NodeDirectory || h.node == nil {
		return nil, statusInvalid
	}
	authorize := func(call context.Context, handle *fileHandle) error {
		if err := c.authorizeReference(call, s, t, handle, storage.OpFileReadDirNode, nil); err != nil {
			return err
		}
		return c.authorizeReference(call, s, t, handle, storage.OpFileState, nil)
	}
	ticket, err := c.admitInformation(ctx, s, t, id, accessReadData, authorize)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h = ticket.handle
	if err := h.node.CheckReferenceState(); err != nil {
		return nil, ioStatusError(err)
	}
	observed, err := h.node.State(ctx, storage.StateScalars)
	if err != nil {
		return nil, ioStatusError(err)
	}
	if observed.CheckProjection(h.nodeID, storage.StateScalars) != nil || observed.Attr.Kind != storage.NodeDirectory {
		return nil, statusIO
	}
	if parsed.OutputBufferLength < uint32(head) {
		if err := authorize(ctx, h); err != nil {
			return nil, ioStatusError(err)
		}
		return wire.InfoLengthMismatchErrorBody(), statusInfoLengthMismatch
	}
	restart := parsed.Flags&(wire.QueryDirectoryRestartScans|wire.QueryDirectoryReopen) != 0
	if h.directory == nil || h.directory.snapshot == nil || restart {
		pattern := []uint16{'*'}
		if h.directory != nil && len(h.directory.pattern) != 0 {
			pattern = h.directory.pattern
		}
		if parsed.Pattern != "" && (h.directory == nil || restart) {
			pattern = nameUnits(parsed.Pattern)
		}
		if _, err := compileDirectoryPattern(pattern); err != nil {
			return nil, statusInvalid
		}
		if err := reserveDirectoryCursor(t, h); err != nil {
			return nil, ioStatusError(err)
		}
		releaseDirectorySnapshot(t, h)
		cursor := h.directory
		cursor.pattern, cursor.next, cursor.returned, cursor.settled = pattern, 0, false, false
		if err := c.authorizeReference(ctx, s, t, h, storage.OpFileScope, nil); err != nil {
			return nil, ioStatusError(err)
		}
		if err := h.node.CheckScopedReference(); err != nil {
			return nil, ioStatusError(err)
		}
		scope, err := h.node.Scope(ctx)
		if err != nil {
			return nil, ioStatusError(err)
		}
		if scope.Check() != nil {
			return nil, statusIO
		}
		snapshot, err := captureDirectorySnapshot(ctx, t, h, scope, pattern)
		if err != nil {
			return nil, ioStatusError(err)
		}
		cursor.snapshot = snapshot
	}
	cursor := h.directory
	if parsed.FileInformationClass != wire.FileNamesInformation {
		if status := cursor.snapshot.richStatus(); status != statusOK {
			return nil, status
		}
	}
	candidate, err := cursor.page(parsed.FileInformationClass, parsed.OutputBufferLength, parsed.Flags&wire.QueryDirectoryReturnSingleEntry != 0)
	if err != nil {
		return nil, statusIO
	}
	if err := authorize(ctx, h); err != nil {
		return nil, ioStatusError(err)
	}
	cursor.next, cursor.returned, cursor.settled = candidate.next, candidate.returned, candidate.settled
	return candidate.body, candidate.status
}
