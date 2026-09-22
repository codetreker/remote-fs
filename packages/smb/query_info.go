package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusInfoLengthMismatch uint32 = 0xc0000004
	statusBufferOverflow     uint32 = 0x80000005
)

func queryInformationSize(kind, class byte) (uint32, error) {
	switch kind {
	case 1:
		return fileInformationSize(class)
	case 2:
		return filesystemInformationSize(class)
	default:
		return 0, syscall.EOPNOTSUPP
	}
}

// SMB 3.1.1 fixed-size query failures carry one empty error context.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d4da8b67-c180-47e3-ba7a-d24214ac4aaa
func queryLengthFailure() ([]byte, uint32) {
	body := make([]byte, 16)
	binary.LittleEndian.PutUint16(body, 9)
	body[2] = 1
	binary.LittleEndian.PutUint32(body[4:], 8)
	return body, statusInfoLengthMismatch
}

func (c *connection) queryInfo(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	query, err := request.QueryInfo()
	if err != nil || uint64(query.OutputLength) > uint64(c.server.config.Limits.MaxIOBytes) {
		return nil, statusInvalid
	}
	fixed, err := queryInformationSize(query.Type, query.Class)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if query.OutputLength < fixed {
		return queryLengthFailure()
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}

	handle := tree.files.get(query.FileID)
	if handle == nil {
		return nil, statusFileClosed
	}
	releaseHandle, err := handle.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseHandle()
	if handle.nodeID == 0 {
		return nil, statusIO
	}

	operation := storage.OpVolumeSpace
	if query.Type == 1 {
		if err := checkFileInformationAccess(query.Class, handle.grantedAccess); err != nil {
			return nil, fileCommandStatus(err)
		}
		operation = storage.OpFileStat
		if query.Class == 5 {
			operation = storage.OpFileState
		}
	}
	if err := c.authorizeFileOperation(ctx, tree, operation); err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}

	if query.Type == 1 && (query.Class == 4 || query.Class == 5 || query.Class == 34 || query.Class == 35) {
		releaseResult, err := reserveFileCommandResult(ctx, tree, query.Class == 5)
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		defer releaseResult()
	}

	var data []byte
	overflow := false
	if query.Type == 1 {
		data, err = queryFileInformation(ctx, tree, handle, query.Class)
	} else {
		switch query.Class {
		case 3, 7:
			var space storage.Space
			space, err = tree.export.share.Backend.Space(ctx)
			if err == nil {
				data, err = encodeFilesystemSizeInformation(space, query.Class == 7)
			}
		case 4:
			data = encodeFilesystemDeviceInformation()
		case 5:
			data, overflow, err = encodeFilesystemAttributeInformation(query.OutputLength)
		}
	}
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if len(data) > c.server.config.Limits.MaxFrameBytes-wire.HeaderSize-8 {
		return nil, statusResources
	}
	status := uint32(statusOK)
	if overflow {
		status = statusBufferOverflow
	}
	return wire.BufferResponseBody(data), status
}

func checkQueryCapture(attr storage.Attr, nodeID uint64) error {
	if attr.ID != nodeID || nodeID == 0 || attr.Kind.Check() != nil || attr.Kind != storage.NodeDirectory && attr.Size < 0 {
		return fmt.Errorf("query result does not identify the retained node: %w", syscall.EIO)
	}
	if err := storage.CheckMetadata(attr.Metadata); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	return nil
}

func queryFileInformation(ctx context.Context, tree *tree, handle *fileHandle, class byte) ([]byte, error) {
	switch class {
	case 6:
		return encodeInternalInformation(handle.nodeID)
	case 7:
		// Internal metadata namespaces are not exposed as Windows extended attributes.
		return make([]byte, 4), nil
	case 8:
		return encodeAccessInformation(handle.grantedAccess), nil
	case 59:
		return encodeFileIDInformation(handle.nodeID, virtualVolumeSerial(tree.export.share.Volume))
	}
	if class == 5 {
		stateAccess, ok := handle.reference.(storage.ReferenceStateAccess)
		if !ok {
			return nil, syscall.EOPNOTSUPP
		}
		if err := stateAccess.CheckReferenceState(); err != nil {
			return nil, err
		}
		state, err := stateAccess.State(ctx)
		if err != nil {
			return nil, err
		}
		if err := checkQueryCapture(state.Attr, handle.nodeID); err != nil {
			return nil, err
		}
		size, err := virtualFileSize(state.Attr)
		if err != nil {
			return nil, err
		}
		links := uint32(1)
		pending := state.Detached || state.PendingUnlink
		if pending {
			links = 0
		}
		return encodeStandardInformation(size, links, pending, state.Attr.Kind == storage.NodeDirectory)
	}

	attr, err := handle.reference.Stat(ctx)
	if err != nil {
		return nil, err
	}
	if err := checkQueryCapture(attr, handle.nodeID); err != nil {
		return nil, err
	}
	switch class {
	case 4:
		return encodeBasicInformation(attr)
	case 34:
		size, err := virtualFileSize(attr)
		if err != nil {
			return nil, err
		}
		return encodeNetworkOpenInformation(attr, size)
	case 35:
		return encodeAttributeTagInformation(attr)
	default:
		return nil, syscall.EOPNOTSUPP
	}
}
