package smb

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const windowsMutationAttempts = 4

func reserveFileCommandResult(ctx context.Context, tree *tree, state bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	charge := maxOpenResultCharge()
	if state {
		charge += storage.MaxLinkTargetBytes + storage.MaxObservationTokenBytes
	}
	registry := tree.files
	registry.mu.Lock()
	server := tree.export.server
	server.mu.Lock()
	if charge > registry.limits.MaxOpenResultBytes-registry.openResultBytes ||
		charge > registry.limits.MaxOpenResultBytes-tree.export.openResultBytes {
		server.mu.Unlock()
		registry.mu.Unlock()
		return nil, syscall.ENOMEM
	}
	registry.openResultBytes += charge
	tree.export.openResultBytes += charge
	server.mu.Unlock()
	registry.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			registry.mu.Lock()
			server.mu.Lock()
			registry.openResultBytes -= charge
			tree.export.openResultBytes -= charge
			server.mu.Unlock()
			registry.mu.Unlock()
		})
	}, nil
}

func checkRetainedRegularAttr(attr storage.Attr, nodeID uint64) error {
	if nodeID == 0 || attr.ID != nodeID || attr.Kind != storage.NodeRegular || attr.Size < 0 {
		return syscall.EIO
	}
	if err := storage.CheckMetadata(attr.Metadata); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	return nil
}

func (c *connection) readHandle(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	read, err := request.Read()
	if err != nil {
		return nil, statusInvalid
	}
	if read.Flags&^byte(3) != 0 || read.MinimumCount > read.Length {
		return nil, statusInvalid
	}
	if read.Flags != 0 || read.Channel != 0 || len(read.ChannelInfo) != 0 {
		return nil, statusUnsupported
	}
	if uint64(read.Length) > uint64(c.server.config.Limits.MaxIOBytes) || read.Offset > math.MaxInt64 ||
		uint64(read.Length) > math.MaxInt64-read.Offset {
		return nil, statusInvalid
	}

	handle := tree.files.get(read.FileID)
	if handle == nil {
		return nil, statusFileClosed
	}
	releaseHandle, err := handle.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseHandle()
	if handle.grantedAccess&fileReadData == 0 {
		return nil, statusDenied
	}
	if handle.file == nil {
		return nil, statusInvalidDeviceRequest
	}
	if err := c.authorizeFileOperation(ctx, tree, storage.OpFileRead); err != nil {
		return nil, fileCommandStatus(err)
	}
	releaseResult, err := reserveFileCommandResult(ctx, tree, false)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseResult()

	result, err := handle.file.ReadAt(ctx, int64(read.Offset), int(read.Length))
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := checkRetainedRegularAttr(result.Attr, handle.nodeID); err != nil || len(result.Data) > int(read.Length) {
		return nil, statusIO
	}
	remaining := max(result.Attr.Size-int64(read.Offset), 0)
	if int64(len(result.Data)) > remaining || read.Length > 0 && remaining > 0 && len(result.Data) == 0 {
		return nil, statusIO
	}
	if read.Length > 0 && remaining == 0 || uint64(len(result.Data)) < uint64(read.MinimumCount) {
		return nil, statusEndOfFile
	}
	return wire.ReadResponseBody(result.Data, 0), statusOK
}

func contentMutationOperation(kind storage.FileMutationKind) (storage.Operation, error) {
	switch kind {
	case storage.MutateWriteAt, storage.MutateAppend:
		return storage.OpFileWrite, nil
	case storage.MutateTruncate:
		return storage.OpFileTruncate, nil
	default:
		return "", syscall.EINVAL
	}
}

func windowsArchiveMutation(attr storage.Attr, command storage.FileMutation) (storage.FileMutation, error) {
	if attr.ID == 0 || attr.Kind != storage.NodeRegular || attr.Size < 0 {
		return storage.FileMutation{}, syscall.EIO
	}
	if (command.Kind == storage.MutateWriteAt || command.Kind == storage.MutateAppend) && len(command.Data) == 0 {
		return command, nil
	}
	metadata, err := decodeWindowsMetadata(attr.Metadata)
	if err != nil {
		return storage.FileMutation{}, err
	}
	if metadata.Attributes&dosReadOnly != 0 {
		return storage.FileMutation{}, syscall.EROFS
	}
	metadata.Attributes |= dosArchive
	payload, err := encodeWindowsMetadata(metadata)
	if err != nil {
		return storage.FileMutation{}, err
	}
	version := bytes.Clone(attr.Metadata[windowsMetadataKey].Version)
	command.ExpectedMetadata = map[string][]byte{windowsMetadataKey: bytes.Clone(version)}
	command.Metadata = map[string]storage.OpaquePayload{
		windowsMetadataKey: {Version: version, Data: payload},
	}
	return command, nil
}

// mutateWindowsFile publishes the byte or length change, timestamps, and
// ARCHIVE metadata in one authority mutation. A metadata race restarts from a
// fresh capture; an ambiguous result is returned without redispatch.
func (c *connection) mutateWindowsFile(ctx context.Context, tree *tree, handle *fileHandle, command storage.FileMutation) (storage.Attr, error) {
	operation, err := contentMutationOperation(command.Kind)
	if err != nil {
		return storage.Attr{}, err
	}
	if handle.file == nil {
		return storage.Attr{}, syscall.EISDIR
	}
	mutation, ok := handle.reference.(storage.ConditionalFileMutation)
	if !ok {
		return storage.Attr{}, syscall.EOPNOTSUPP
	}
	if err := mutation.CheckConditionalFileMutation(); err != nil {
		return storage.Attr{}, err
	}
	required := []storage.Operation{storage.OpFileStat, storage.OpFileMutate, operation}
	if command.Kind == storage.MutateTruncate || len(command.Data) != 0 {
		required = append(required, storage.OpFileSetMetadata)
	}
	for _, required := range required {
		if err := c.authorizeFileOperation(ctx, tree, required); err != nil {
			return storage.Attr{}, err
		}
	}

	for range windowsMutationAttempts {
		if err := ctx.Err(); err != nil {
			return storage.Attr{}, err
		}
		before, err := handle.file.Stat(ctx)
		if err != nil {
			return storage.Attr{}, err
		}
		if err := checkRetainedRegularAttr(before, handle.nodeID); err != nil {
			return storage.Attr{}, err
		}
		attempt, err := windowsArchiveMutation(before, command)
		if err != nil {
			return storage.Attr{}, err
		}
		attempt.Action, err = tree.authority.newFileAction()
		if err != nil {
			return storage.Attr{}, err
		}
		if err := attempt.CheckDataLimit(int64(tree.files.limits.MaxIOBytes)); err != nil {
			return storage.Attr{}, err
		}
		result, err := mutation.MutateFile(ctx, attempt)
		if errors.Is(err, storage.ErrConditionConflict) && storage.ErrnoOf(err) == syscall.EAGAIN && result.ID == 0 {
			continue
		}
		if err != nil {
			return storage.Attr{}, err
		}
		if err := checkRetainedRegularAttr(result, handle.nodeID); err != nil {
			return storage.Attr{}, err
		}
		if command.Kind == storage.MutateTruncate && result.Size != command.Size ||
			command.Kind == storage.MutateWriteAt && len(command.Data) != 0 && result.Size < command.Offset+int64(len(command.Data)) {
			return storage.Attr{}, syscall.EIO
		}
		if command.Kind == storage.MutateTruncate || len(command.Data) != 0 {
			stored, err := decodeWindowsMetadata(result.Metadata)
			if err != nil || stored.Attributes&dosArchive == 0 || result.ModTime.IsZero() || result.ChangeTime == nil {
				return storage.Attr{}, errors.Join(err, syscall.EIO)
			}
		}
		return result, nil
	}
	return storage.Attr{}, storage.ErrConditionConflict
}

func (c *connection) writeHandle(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	write, err := request.Write()
	if err != nil {
		return nil, statusInvalid
	}
	if write.Flags&^uint32(3) != 0 {
		return nil, statusInvalid
	}
	if write.Flags&2 != 0 || write.Channel != 0 || len(write.ChannelInfo) != 0 {
		return nil, statusUnsupported
	}
	if len(write.Data) > c.server.config.Limits.MaxIOBytes {
		return nil, statusInvalid
	}

	handle := tree.files.get(write.FileID)
	if handle == nil {
		return nil, statusFileClosed
	}
	releaseHandle, err := handle.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseHandle()
	if handle.grantedAccess&(fileWriteData|fileAppendData) == 0 {
		return nil, statusDenied
	}
	if handle.file == nil {
		return nil, statusInvalidDeviceRequest
	}

	// FSA defines every negative byte offset as append after handling -2 as the
	// current-position sentinel. Append-only access ignores the supplied offset.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/fbf656c3-b897-4b9c-abfd-7c8d876d77a1
	appendOnly := handle.grantedAccess&fileWriteData == 0
	appendWrite := appendOnly || write.Offset > math.MaxInt64
	if !appendOnly && write.Offset == math.MaxUint64-1 {
		return nil, statusUnsupported
	}
	if !appendWrite && uint64(len(write.Data)) > math.MaxInt64-write.Offset {
		return nil, statusInvalid
	}
	releaseResult, err := reserveFileCommandResult(ctx, tree, false)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseResult()

	command := storage.FileMutation{Kind: storage.MutateWriteAt, Offset: int64(write.Offset), Data: write.Data}
	if appendWrite {
		command.Kind = storage.MutateAppend
		command.Offset = 0
	}
	attr, err := c.mutateWindowsFile(ctx, tree, handle, command)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if len(write.Data) != 0 {
		minimum := int64(len(write.Data))
		if !appendWrite {
			minimum += int64(write.Offset)
		}
		if attr.Size < minimum {
			return nil, statusIO
		}
	}
	return wire.WriteResponseBody(uint32(len(write.Data)), 0), statusOK
}

func (c *connection) flushHandle(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	id, err := request.FileID()
	if err != nil || request.Header.Command != wire.Flush {
		return nil, statusInvalid
	}
	handle := tree.files.get(id)
	if handle == nil {
		return nil, statusFileClosed
	}
	releaseHandle, err := handle.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseHandle()
	if handle.grantedAccess&(fileWriteData|fileAppendData) == 0 {
		return nil, statusDenied
	}
	if handle.file == nil {
		return nil, statusInvalidDeviceRequest
	}
	if err := c.authorizeFileOperation(ctx, tree, storage.OpFileSync); err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := handle.file.Sync(ctx); err != nil {
		return nil, fileCommandStatus(err)
	}
	return wire.EmptyResponseBody(), statusOK
}
