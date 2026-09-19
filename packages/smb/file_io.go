package smb

import (
	"context"
	"math"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) readHandle(ctx context.Context, t *tree, request wire.Request) ([]byte, uint32) {
	r, err := request.Read()
	if err != nil {
		return nil, statusInvalid
	}
	if r.Flags&^byte(3) != 0 {
		return nil, statusInvalid
	}
	if r.Flags != 0 || r.Channel != 0 || len(r.ChannelInfo) != 0 {
		return nil, statusUnsupported
	}
	limits := c.server.config.Limits
	if uint64(r.Length) > uint64(limits.MaxIOBytes) || r.Offset > math.MaxInt64 || uint64(r.Length) > math.MaxInt64-r.Offset {
		return nil, statusInvalid
	}
	h := t.files.get(r.FileID)
	if h == nil {
		return nil, statusFileClosed
	}
	release, err := h.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer release()
	if h.grantedAccess&fileReadData == 0 {
		return nil, statusDenied
	}
	if h.file == nil {
		return nil, statusInvalidDeviceRequest
	}
	if err := c.authorizeFileOperation(ctx, t, storage.OpFileRead); err != nil {
		return nil, fileCommandStatus(err)
	}
	releaseResult, err := reserveFileCommandResult(ctx, t, false)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseResult()
	result, err := h.file.ReadAt(ctx, int64(r.Offset), int(r.Length))
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if result.Attr.ID != h.nodeID || result.Attr.Kind != storage.NodeRegular || result.Attr.Size < 0 || len(result.Data) > int(r.Length) {
		return nil, statusIO
	}
	remaining := max(result.Attr.Size-int64(r.Offset), 0)
	if int64(len(result.Data)) > remaining || r.Length > 0 && remaining > 0 && len(result.Data) == 0 {
		return nil, statusIO
	}
	if r.Length > 0 && remaining == 0 || uint64(len(result.Data)) < uint64(r.MinimumCount) {
		return nil, statusEndOfFile
	}
	return wire.ReadResponseBody(result.Data, 0), statusOK
}

func (c *connection) writeHandle(ctx context.Context, t *tree, request wire.Request) ([]byte, uint32) {
	r, err := request.Write()
	if err != nil {
		return nil, statusInvalid
	}
	if r.Flags&^uint32(3) != 0 || r.Flags == 1 {
		return nil, statusInvalid
	}
	if r.Flags != 0 || r.Channel != 0 || len(r.ChannelInfo) != 0 {
		return nil, statusUnsupported
	}
	limits := c.server.config.Limits
	if len(r.Data) > limits.MaxIOBytes {
		return nil, statusInvalid
	}
	h := t.files.get(r.FileID)
	if h == nil {
		return nil, statusFileClosed
	}
	release, err := h.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer release()
	if h.grantedAccess&(fileWriteData|fileAppendData) == 0 {
		return nil, statusDenied
	}
	if h.file == nil {
		return nil, statusInvalidDeviceRequest
	}
	// FSA uses negative offsets for append, except -2 for current position.
	// Append-only access ignores the supplied offset.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/fbf656c3-b897-4b9c-abfd-7c8d876d77a1
	appendOnly := h.grantedAccess&fileWriteData == 0
	appendWrite := appendOnly || r.Offset > math.MaxInt64
	if !appendOnly && r.Offset == math.MaxUint64-1 {
		return nil, statusUnsupported
	}
	if !appendWrite && uint64(len(r.Data)) > math.MaxInt64-r.Offset {
		return nil, statusInvalid
	}
	if err := c.authorizeFileOperation(ctx, t, storage.OpFileWrite); err != nil {
		return nil, fileCommandStatus(err)
	}
	releaseResult, err := reserveFileCommandResult(ctx, t, false)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseResult()
	var attr storage.Attr
	if appendWrite {
		mutation, ok := h.file.(storage.ConditionalFileMutation)
		if !ok {
			return nil, statusUnsupported
		}
		if err := mutation.CheckConditionalFileMutation(); err != nil {
			return nil, fileCommandStatus(err)
		}
		if err := c.authorizeFileOperation(ctx, t, storage.OpFileMutate); err != nil {
			return nil, fileCommandStatus(err)
		}
		attr, err = mutation.MutateFile(ctx, storage.FileMutation{Kind: storage.MutateAppend, Data: r.Data})
	} else {
		attr, err = h.file.WriteAt(ctx, int64(r.Offset), r.Data)
	}
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if attr.ID != h.nodeID || attr.Kind != storage.NodeRegular || attr.Size < 0 {
		return nil, statusIO
	}
	if len(r.Data) > 0 {
		minimum := int64(len(r.Data))
		if !appendWrite {
			minimum += int64(r.Offset)
		}
		if attr.Size < minimum {
			return nil, statusIO
		}
	}
	return wire.WriteResponseBody(uint32(len(r.Data)), 0), statusOK
}

func (c *connection) flushHandle(ctx context.Context, t *tree, request wire.Request) ([]byte, uint32) {
	id, err := request.FileID()
	if err != nil || request.Header.Command != wire.Flush {
		return nil, statusInvalid
	}
	h := t.files.get(id)
	if h == nil {
		return nil, statusFileClosed
	}
	release, err := h.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer release()
	if h.grantedAccess&(fileWriteData|fileAppendData) == 0 {
		return nil, statusDenied
	}
	if h.file == nil {
		return nil, statusUnsupported
	}
	if err := c.authorizeFileOperation(ctx, t, storage.OpFileSync); err != nil {
		return nil, fileCommandStatus(err)
	}
	releaseResult, err := reserveFileCommandResult(ctx, t, false)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseResult()
	if err := h.file.Sync(ctx); err != nil {
		return nil, fileCommandStatus(err)
	}
	return wire.EmptyResponseBody(), statusOK
}
