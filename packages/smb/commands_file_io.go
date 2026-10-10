package smb

import (
	"context"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusEndOfFile     uint32 = 0xc0000011
	statusQuotaExceeded uint32 = 0xc0000044
)

func relatedIOFileID(ctx context.Context, request wire.Request, id wire.FileID) (wire.FileID, error) {
	if id == wire.InvalidFileID && request.Header.Flags&wire.FlagRelated != 0 {
		id, _ = ctx.Value(relatedFileKey{}).(wire.FileID)
	}
	if id == (wire.FileID{}) || id.IsRelatedPlaceholder() || id.HasPartialRelatedPlaceholder() {
		return wire.FileID{}, syscall.EBADF
	}
	return id, nil
}

func checkFileIOIdentity(ctx context.Context, s *session, t *tree, h *fileHandle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h == nil || h.file == nil || h.ownerSession != s || h.treeID != t.id || h.sessionID != s.id {
		return syscall.EBADF
	}
	if s.expiredIdentity() {
		return syscall.ESTALE
	}
	a := t.authority
	if a == nil {
		return syscall.EIO
	}
	a.installMu.RLock()
	t.fileMu.Lock()
	s.mu.Lock()
	valid := !a.stopping && !a.closed && time.Now().Before(a.deadline) && a.actionEpoch != 0 &&
		!t.fileStopping && !s.retired && h.identity == a.identity && t.handles[h.id] == h &&
		handleState(h.state.Load()) == handleLive && h.published
	s.mu.Unlock()
	t.fileMu.Unlock()
	a.installMu.RUnlock()
	if !valid {
		return syscall.ESTALE
	}
	identity, ok := h.file.(storage.ReferenceIdentity)
	if !ok {
		return syscall.EIO
	}
	node, err := identity.ReferenceNodeID()
	if err != nil {
		return err
	}
	if node == 0 || node != h.nodeID {
		return syscall.EIO
	}
	return nil
}

func (c *connection) admitCommandIO(ctx context.Context, s *session, t *tree, id wire.FileID, operation storage.Operation, effects []storage.ContentMetadataEffect) (*fileIOTicket, error) {
	h := t.findFileHandle(id)
	if h == nil {
		return nil, syscall.EBADF
	}
	if h.file == nil {
		return nil, syscall.EOPNOTSUPP
	}
	if operation == storage.OpFileRead && h.access&accessReadData == 0 ||
		operation != storage.OpFileRead && h.access&(accessWriteData|accessAppend) == 0 {
		return nil, syscall.EACCES
	}
	if err := c.authorizeFileIO(ctx, s, t, h, operation, effects); err != nil {
		return nil, err
	}
	ticket, err := t.admitFileIO(ctx, s, id, c.server.config.Limits.MaxHandleIORequests)
	if err != nil {
		return nil, err
	}
	if err := ticket.wait(ctx); err != nil {
		ticket.finish()
		return nil, err
	}
	if err := c.authorizeFileIO(ctx, s, t, h, operation, effects); err != nil {
		ticket.finish()
		return nil, err
	}
	if err := settlePendingFileIO(ctx, h); err != nil {
		ticket.finish()
		return nil, errors.Join(syscall.EIO, err)
	}
	return ticket, nil
}

func ioStatusError(err error) uint32 {
	if errors.Is(err, syscall.EIO) {
		return statusIO
	}
	if storage.ErrnoOf(err) == syscall.EDQUOT {
		return statusQuotaExceeded
	}
	if errors.Is(err, syscall.EBADF) {
		return statusInvalidHandle
	}
	if errors.Is(err, storage.ErrConditionConflict) {
		return statusRetry
	}
	return statusError(err)
}

func checkReadResult(h *fileHandle, request wire.ReadRequest, result storage.FileRead) error {
	attr := result.Attr
	if attr.ID != h.nodeID || attr.Kind != storage.NodeRegular || attr.Size < 0 || len(result.Data) > int(request.Length) {
		return syscall.EIO
	}
	if err := checkCreateAllocation(attr); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	offset := int64(request.Offset)
	remaining := max(int64(0), attr.Size-offset)
	if int64(len(result.Data)) > remaining || request.Length > 0 && remaining > 0 && len(result.Data) == 0 {
		return syscall.EIO
	}
	return nil
}

func (c *connection) readFile(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.Read()
	if err != nil {
		return nil, statusInvalid
	}
	if parsed.Channel != wire.ChannelNone {
		return nil, statusInvalid
	}
	if parsed.Flags&^byte(wire.ReadFlagRequestCompressed) != 0 {
		return nil, statusUnsupported
	}
	if parsed.Length > uint32(c.server.config.Limits.MaxIOBytes) || parsed.Offset > math.MaxInt64 || uint64(parsed.Length) > math.MaxInt64-parsed.Offset {
		return nil, statusInvalid
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	ticket, err := c.admitCommandIO(ctx, s, t, id, storage.OpFileRead, nil)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h := ticket.handle
	call := storage.WithBoundedAttrResult(ctx, int64(c.server.config.Limits.MaxFrameBytes), createAttrBudget(int64(c.server.config.Limits.MaxFrameBytes)))
	result, err := h.file.ReadAt(call, int64(parsed.Offset), int(parsed.Length))
	if err != nil {
		return nil, ioStatusError(err)
	}
	if err := checkReadResult(h, parsed, result); err != nil {
		return nil, statusIO
	}
	if len(result.Data) == 0 || uint32(len(result.Data)) < parsed.MinimumCount {
		return nil, statusEndOfFile
	}
	body, err := wire.ReadResponseBody(result.Data)
	if err != nil {
		return nil, statusIO
	}
	return body, statusOK
}

func (c *connection) writeFile(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.Write()
	if err != nil {
		return nil, statusInvalid
	}
	if parsed.Channel != wire.ChannelNone {
		return nil, statusInvalid
	}
	if parsed.Flags&wire.WriteFlagWriteThrough != 0 && parsed.Flags&wire.WriteFlagUnbuffered == 0 {
		return nil, statusInvalid
	}
	if parsed.Flags != 0 {
		return nil, statusUnsupported
	}
	if parsed.Length > uint32(c.server.config.Limits.MaxIOBytes) || parsed.Offset != math.MaxUint64 &&
		(parsed.Offset > math.MaxInt64 || uint64(parsed.Length) > math.MaxInt64-parsed.Offset) {
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
	if h.file == nil {
		return nil, statusUnsupported
	}
	if h.access&(accessWriteData|accessAppend) == 0 {
		return nil, statusDenied
	}
	var effects []storage.ContentMetadataEffect
	if len(parsed.Data) != 0 {
		effects = h.contentEffects
		if len(effects) != 1 {
			return nil, statusIO
		}
	}
	ticket, err := c.admitCommandIO(ctx, s, t, id, storage.OpFileWrite, effects)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h = ticket.handle
	command := storage.FileMutation{Kind: storage.MutateWriteAt}
	if len(parsed.Data) != 0 {
		command.ExpectedMetadata, err = c.observeWriteMetadata(ctx, s, t, h)
		if err != nil {
			return nil, ioStatusError(err)
		}
		command.ContentEffects = []uint16{0}
		if h.access&accessWriteData == 0 || parsed.Offset == math.MaxUint64 {
			command.Kind = storage.MutateAppend
		} else {
			command.Offset = int64(parsed.Offset)
		}
	} else if parsed.Offset != math.MaxUint64 {
		command.Offset = int64(parsed.Offset)
	}
	t.authority.installMu.RLock()
	epoch := t.authority.actionEpoch
	t.authority.installMu.RUnlock()
	command.Action, err = storage.NewFileActionID(epoch)
	if err != nil {
		return nil, ioStatusError(err)
	}
	command.Data = parsed.Data
	owner, err := c.server.reserveWriteOwner(t, h, ticket.sequence, command, effects)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h.pendingWrite = owner
	registerIOResponse(ctx, owner.recordResponse)
	authorize := func(call context.Context) error {
		return c.authorizeFileIO(call, s, t, h, storage.OpFileWrite, effects)
	}
	reobserve := func(call context.Context) (map[string][]byte, error) {
		return c.observeWriteMetadata(call, s, t, h)
	}
	currentEpoch := func() uint64 {
		t.authority.installMu.RLock()
		defer t.authority.installMu.RUnlock()
		return t.authority.actionEpoch
	}
	call := storage.WithBoundedAttrResult(ctx, int64(c.server.config.Limits.MaxFrameBytes), createAttrBudget(int64(c.server.config.Limits.MaxFrameBytes)))
	_, err = owner.execute(call, authorize, reobserve, currentEpoch)
	if err != nil {
		return nil, ioStatusError(err)
	}
	return wire.WriteResponseBody(parsed.Length), statusOK
}

func (c *connection) flushFile(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.Flush()
	if err != nil {
		return nil, statusInvalid
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	ticket, err := c.admitCommandIO(ctx, s, t, id, storage.OpFileSync, nil)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h := ticket.handle
	owner, err := c.server.reserveFlushOwner(t, h, ticket.sequence)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h.pendingFlush = owner
	registerIOResponse(ctx, owner.recordResponse)
	err = owner.confirm(ctx, func(call context.Context) error {
		return c.authorizeFileIO(call, s, t, h, storage.OpFileSync, nil)
	})
	if err != nil {
		return nil, ioStatusError(err)
	}
	return wire.FlushResponseBody(), statusOK
}

type ioResponseKey struct{}

func registerIOResponse(ctx context.Context, record func(ResponseDisposition)) {
	if records, ok := ctx.Value(ioResponseKey{}).(*[]func(ResponseDisposition)); ok {
		*records = append(*records, record)
	}
}
