package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const statusInfoLengthMismatch uint32 = 0xc0000004

func windowsMetadataObservations() []storage.MetadataObservationScope {
	return []storage.MetadataObservationScope{{Namespace: windowsMetadataKey, MaxPayloadBytes: 8}}
}

func (c *connection) authorizeReference(ctx context.Context, s *session, t *tree, h *fileHandle, operation storage.Operation, scopes []storage.MetadataObservationScope) error {
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return err
	}
	s.identityMu.RLock()
	principal := s.principal
	s.identityMu.RUnlock()
	request := authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation, MetadataObservations: storage.CloneMetadataObservationScopes(scopes)}
	if err := c.server.config.Authorize.Authorize(WithPrincipal(ctx, principal), request.Clone()); err != nil {
		return err
	}
	return checkFileIOIdentity(ctx, s, t, h)
}

func (c *connection) admitInformation(ctx context.Context, s *session, t *tree, id wire.FileID, rights uint32, authorize func(context.Context, *fileHandle) error) (*fileIOTicket, error) {
	h := t.findFileHandle(id)
	if h == nil {
		return nil, syscall.EBADF
	}
	if h.access&rights != rights {
		return nil, syscall.EACCES
	}
	if err := authorize(ctx, h); err != nil {
		return nil, err
	}
	ticket, err := t.admitFileIO(ctx, s, id, c.server.config.Limits.MaxHandleIORequests)
	if err != nil {
		return nil, err
	}
	if err = ticket.wait(ctx); err == nil {
		err = authorize(ctx, h)
	}
	if err == nil {
		err = settlePendingFileIO(ctx, h)
	}
	if err != nil {
		ticket.finish()
		return nil, err
	}
	return ticket, nil
}

func fileInfoSize(class byte) int {
	switch class {
	case wire.FileBasicInformation:
		return 40
	case wire.FileStandardInformation, wire.FileIdInformation:
		return 24
	case wire.FileInternalInformation:
		return 8
	case wire.FileNetworkOpenInformation:
		return 56
	}
	return 0
}

func (c *connection) queryFileInfo(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.QueryInfo()
	if err != nil {
		return nil, statusInvalid
	}
	if parsed.InfoType != wire.InfoFile {
		return nil, statusUnsupported
	}
	size := fileInfoSize(parsed.FileInfoClass)
	if size == 0 {
		return nil, statusUnsupported
	}
	if parsed.OutputBufferLength > uint32(c.server.config.Limits.MaxFrameBytes-72) {
		return nil, statusResources
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	operation, rights := storage.OpFileInformation, uint32(0)
	if parsed.FileInfoClass == wire.FileBasicInformation || parsed.FileInfoClass == wire.FileNetworkOpenInformation {
		operation, rights = storage.OpFileStat, accessReadAttr
	}
	authorize := func(call context.Context, h *fileHandle) error {
		return c.authorizeReference(call, s, t, h, operation, nil)
	}
	ticket, err := c.admitInformation(ctx, s, t, id, rights, authorize)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h := ticket.handle
	if parsed.OutputBufferLength < uint32(size) {
		return wire.InfoLengthMismatchErrorBody(), statusInfoLengthMismatch
	}
	var data []byte
	if rights != 0 {
		attr, err := c.statInformation(ctx, h)
		if err != nil {
			return nil, ioStatusError(err)
		}
		basic, err := basicInformation(attr)
		if err != nil {
			return nil, ioStatusError(err)
		}
		if parsed.FileInfoClass == wire.FileBasicInformation {
			data = wire.FileBasicInformationBody(basic)
		} else {
			if err = c.checkInformationAllocation(ctx, s, t, h, attr.AllocationKnown, attr.AllocationSize); err != nil {
				return nil, ioStatusError(err)
			}
			eof := int64(0)
			if attr.Kind == storage.NodeRegular {
				eof = attr.Size
			}
			data = wire.FileNetworkOpenInformationBody(wire.FileNetworkOpenInfo{FileBasicInfo: basic, AllocationSize: attr.AllocationSize, EndOfFile: eof})
		}
	} else {
		information, ok := h.reference().(storage.ReferenceInformationAccess)
		if !ok {
			return nil, statusUnsupported
		}
		if err = information.CheckReferenceInformation(); err != nil {
			return nil, ioStatusError(err)
		}
		result, err := information.Information(ctx)
		if err != nil {
			return nil, ioStatusError(err)
		}
		if err = result.Check(h.nodeID); err != nil || result.Kind != h.nodeKind {
			return nil, statusIO
		}
		switch parsed.FileInfoClass {
		case wire.FileInternalInformation:
			data = wire.FileInternalInformationBody(result.NodeID)
		case wire.FileIdInformation:
			serial, err := stableVolumeSerial(h.identity.Backend.Volume)
			if err != nil {
				return nil, statusIO
			}
			var id [16]byte
			binary.LittleEndian.PutUint64(id[:8], result.NodeID)
			data = wire.FileIdInformationBody(serial, id)
		case wire.FileStandardInformation:
			if err = c.checkInformationAllocation(ctx, s, t, h, result.AllocationKnown, result.AllocationSize); err != nil {
				return nil, ioStatusError(err)
			}
			links := uint32(1)
			if result.Detached {
				links = 0
			}
			eof := int64(0)
			if result.Kind == storage.NodeRegular {
				eof = result.Size
			}
			data = wire.FileStandardInformationBody(wire.FileStandardInfo{AllocationSize: result.AllocationSize, EndOfFile: eof, NumberOfLinks: links, DeletePending: result.PendingUnlink, Directory: result.Kind == storage.NodeDirectory})
		}
	}
	if err = authorize(ctx, h); err != nil {
		return nil, ioStatusError(err)
	}
	body, err := wire.QueryInfoResponseBody(data)
	if err != nil {
		return nil, statusIO
	}
	return body, statusOK
}

func (c *connection) statInformation(ctx context.Context, h *fileHandle) (storage.Attr, error) {
	limit := int64(c.server.config.Limits.MaxFrameBytes)
	call := storage.WithBoundedAttrResult(ctx, limit, func(attr storage.Attr, n int64) error {
		if attr.ID != h.nodeID || attr.Kind != h.nodeKind || attr.Kind == storage.NodeRegular && attr.Size < 0 {
			return syscall.EIO
		}
		retained, err := storage.MetadataRetentionBytes(n)
		if err != nil {
			return err
		}
		if retained+512 > limit {
			return syscall.EFBIG
		}
		return nil
	})
	var attr storage.Attr
	var err error
	if h.node != nil {
		attr, err = h.node.Stat(call)
	} else {
		attr, err = h.file.Stat(call)
	}
	if err != nil {
		return storage.Attr{}, err
	}
	if attr.ID != h.nodeID || attr.Kind != h.nodeKind || attr.Kind == storage.NodeRegular && attr.Size < 0 {
		return storage.Attr{}, syscall.EIO
	}
	return attr, nil
}

func basicInformation(attr storage.Attr) (wire.FileBasicInfo, error) {
	attributes, err := projectWindowsAttributes(attr)
	if err != nil {
		return wire.FileBasicInfo{}, err
	}
	birth, err := optionalFiletime(attr.BirthTime)
	if err != nil {
		return wire.FileBasicInfo{}, err
	}
	access, err := toFiletime(attr.AccessTime)
	if err != nil {
		return wire.FileBasicInfo{}, err
	}
	mod, err := toFiletime(attr.ModTime)
	if err != nil {
		return wire.FileBasicInfo{}, err
	}
	change, err := optionalFiletime(attr.ChangeTime)
	if err != nil {
		return wire.FileBasicInfo{}, err
	}
	if birth > math.MaxInt64 || access > math.MaxInt64 || mod > math.MaxInt64 || change > math.MaxInt64 {
		return wire.FileBasicInfo{}, syscall.EIO
	}
	return wire.FileBasicInfo{CreationTime: int64(birth), LastAccessTime: int64(access), LastWriteTime: int64(mod), ChangeTime: int64(change), Attributes: attributes}, nil
}

func (c *connection) checkInformationAllocation(ctx context.Context, s *session, t *tree, h *fileHandle, known bool, allocated int64) error {
	if !known || allocated < 0 {
		return syscall.EIO
	}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileVolumeGeometry, nil); err != nil {
		return err
	}
	geometry, ok := t.authority.raw.(storage.VolumeGeometryAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := geometry.CheckVolumeGeometry(); err != nil {
		return err
	}
	result, err := geometry.VolumeGeometry(ctx)
	if err != nil {
		return err
	}
	if err = result.CheckVolume(h.identity.Backend.Volume); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	unit, err := result.AllocationUnitBytes()
	if err != nil {
		return err
	}
	if uint64(allocated)%unit != 0 {
		return syscall.EIO
	}
	return nil
}

func setFiletime(value int64) (*time.Time, error) {
	if value == 0 {
		return nil, nil
	}
	if value == -1 || value == -2 {
		return nil, syscall.EOPNOTSUPP
	}
	if value < 0 {
		return nil, syscall.EINVAL
	}
	seconds := value/10_000_000 - filetimeUnixOffset
	instant := time.Unix(seconds, (value%10_000_000)*100).UTC()
	return &instant, nil
}

func basicMutation(info wire.FileBasicInfo, kind storage.NodeKind, _ bool) (storage.FileMutation, error) {
	command := storage.FileMutation{Kind: storage.MutateAttributes}
	var err error
	command.Attr.BirthTime, err = setFiletime(info.CreationTime)
	if err != nil {
		return storage.FileMutation{}, err
	}
	command.Attr.AccessTime, err = setFiletime(info.LastAccessTime)
	if err != nil {
		return storage.FileMutation{}, err
	}
	command.Attr.ModTime, err = setFiletime(info.LastWriteTime)
	if err != nil {
		return storage.FileMutation{}, err
	}
	if info.ChangeTime < -2 {
		return storage.FileMutation{}, syscall.EINVAL
	}
	if info.ChangeTime != 0 {
		return storage.FileMutation{}, syscall.EOPNOTSUPP
	}
	if info.Attributes == 0 {
		return command, nil
	}
	if info.Attributes&dosDirectory != 0 && kind != storage.NodeDirectory {
		return storage.FileMutation{}, syscall.EINVAL
	}
	attributes := info.Attributes &^ dosDirectory
	if attributes&^dosSettableAttributes != 0 {
		return storage.FileMutation{}, syscall.EOPNOTSUPP
	}
	if attributes&dosNormal != 0 && attributes != dosNormal {
		return storage.FileMutation{}, syscall.EINVAL
	}
	attributes &^= dosNormal
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes})
	if err != nil {
		return storage.FileMutation{}, err
	}
	command.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Data: data}}
	return command, nil
}

func (c *connection) observeBasicMetadata(ctx context.Context, s *session, t *tree, h *fileHandle, command storage.FileMutation) (storage.FileMutation, error) {
	if len(command.Metadata) == 0 {
		return command, nil
	}
	if len(h.metadataObservations) != 1 {
		return storage.FileMutation{}, syscall.EIO
	}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileObserveMetadata, h.metadataObservations); err != nil {
		return storage.FileMutation{}, err
	}
	observer, ok := h.reference().(storage.ReferenceMetadataObserver)
	if !ok {
		return storage.FileMutation{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckMetadataObservation(); err != nil {
		return storage.FileMutation{}, err
	}
	observed, err := observer.ObserveMetadata(ctx, 0)
	if err != nil {
		return storage.FileMutation{}, err
	}
	if err = observed.CheckScope(h.nodeID, h.metadataObservations[0]); err != nil {
		return storage.FileMutation{}, err
	}
	values := map[string]storage.OpaquePayload{}
	var token []byte
	if observed.Value != nil {
		values[windowsMetadataKey] = *observed.Value
		token = bytes.Clone(observed.Value.Version)
	}
	old, err := decodeWindowsMetadata(values)
	if err != nil {
		return storage.FileMutation{}, err
	}
	update := command.Metadata[windowsMetadataKey]
	update.Data = bytes.Clone(update.Data)
	if h.nodeID == h.identity.Backend.RootNodeID {
		attributes := binary.LittleEndian.Uint32(update.Data[4:])
		attributes = attributes&^(dosHidden|dosSystem) | old.Attributes&(dosHidden|dosSystem)
		binary.LittleEndian.PutUint32(update.Data[4:], attributes)
	}
	update.Version = bytes.Clone(token)
	command.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: update}
	command.ExpectedMetadata = map[string][]byte{windowsMetadataKey: token}
	return command, nil
}

func (c *connection) setFileInfo(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.SetInfo()
	if err != nil {
		return nil, statusInvalid
	}
	if parsed.InfoType != wire.InfoFile {
		return nil, statusUnsupported
	}
	if parsed.AdditionalInformation != 0 {
		return nil, statusInvalid
	}
	if parsed.FileInfoClass != wire.FileBasicInformation && parsed.FileInfoClass != wire.FileEndOfFileInformation {
		return nil, statusUnsupported
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h := t.findFileHandle(id)
	if h == nil {
		return nil, statusInvalidHandle
	}
	var command storage.FileMutation
	var effects []storage.ContentMetadataEffect
	rights := accessWriteAttr
	if parsed.FileInfoClass == wire.FileBasicInformation {
		info, err := wire.ParseFileBasicInformation(parsed.Buffer)
		if err != nil {
			return nil, statusInvalid
		}
		command, err = basicMutation(info, h.nodeKind, h.nodeID == h.identity.Backend.RootNodeID)
		if err != nil {
			return nil, ioStatusError(err)
		}
	} else {
		size, err := wire.ParseFileEndOfFileInformation(parsed.Buffer)
		if err != nil || size < 0 {
			return nil, statusInvalid
		}
		if h.access&accessWriteData == 0 {
			return nil, statusDenied
		}
		if h.nodeKind != storage.NodeRegular || h.file == nil {
			return nil, statusUnsupported
		}
		rights = accessWriteData
		effects = h.contentEffects
		if len(effects) != 1 {
			return nil, statusIO
		}
		command = storage.FileMutation{Kind: storage.MutateTruncate, Size: size, ContentEffects: []uint16{0}}
	}
	operationKind, hasMetadata := command.Kind, len(command.Metadata) != 0
	authorize := func(call context.Context, handle *fileHandle) error {
		if operationKind == storage.MutateTruncate {
			return c.authorizeFileIO(call, s, t, handle, storage.OpFileTruncate, effects)
		}
		if err := c.authorizeReference(call, s, t, handle, storage.OpFileSetAttr, nil); err != nil {
			return err
		}
		if hasMetadata {
			return c.authorizeReference(call, s, t, handle, storage.OpFileSetMetadata, nil)
		}
		return nil
	}
	ticket, err := c.admitInformation(ctx, s, t, id, rights, authorize)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	reobserve := func(call context.Context) (storage.FileMutation, error) {
		if command.Kind == storage.MutateTruncate {
			refreshed := command
			refreshed.ExpectedMetadata, err = c.observeWriteMetadata(call, s, t, h)
			return refreshed, err
		}
		return c.observeBasicMetadata(call, s, t, h, command)
	}
	command, err = reobserve(ctx)
	if err != nil {
		return nil, ioStatusError(err)
	}
	epoch := func() uint64 {
		t.authority.installMu.RLock()
		defer t.authority.installMu.RUnlock()
		return t.authority.actionEpoch
	}
	command.Action, err = storage.NewFileActionID(epoch())
	if err != nil {
		return nil, ioStatusError(err)
	}
	var preservation []mutationMetadataPreservation
	if command.Kind == storage.MutateAttributes && len(command.Metadata) != 0 && h.nodeID == h.identity.Backend.RootNodeID {
		mask := make([]byte, 8)
		binary.LittleEndian.PutUint32(mask[4:], dosHidden|dosSystem)
		preservation = []mutationMetadataPreservation{{Namespace: windowsMetadataKey, Mask: mask}}
	}
	owner, err := c.server.reserveMutationOwner(t, h, ticket.sequence, command, effects, preservation...)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h.pendingWrite = owner
	registerIOResponse(ctx, owner.recordResponse)
	authorizeOwner := func(call context.Context) error { return authorize(call, h) }
	limit := int64(c.server.config.Limits.MaxFrameBytes)
	call := storage.WithBoundedAttrResult(ctx, limit, func(attr storage.Attr, n int64) error {
		if attr.ID != h.nodeID || attr.Kind != h.nodeKind || attr.Kind == storage.NodeRegular && attr.Size < 0 {
			return syscall.EIO
		}
		retained, err := storage.MetadataRetentionBytes(n)
		if err != nil {
			return err
		}
		if retained+512 > limit {
			return syscall.EFBIG
		}
		return nil
	})
	_, err = owner.executeMutation(call, authorizeOwner, reobserve, epoch)
	if err != nil {
		return nil, ioStatusError(err)
	}
	return wire.SetInfoResponseBody(), statusOK
}
