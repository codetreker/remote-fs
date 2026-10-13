package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const statusInfoLengthMismatch uint32 = 0xc0000004

func (c *connection) authorizeReference(ctx context.Context, s *session, t *tree, h *fileHandle, operation storage.Operation, observation *storage.MetadataObservationRequest) error {
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return err
	}
	s.identityMu.RLock()
	principal := s.principal
	s.identityMu.RUnlock()
	request := authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation, MetadataObservation: observation}
	if operation == storage.OpFileState {
		request.StateProjection = storage.StateScalars
	}
	if observation != nil && observation.Purpose == storage.MetadataObservationContentEffect {
		selected, err := storage.ResolveContentMetadataEffects(h.contentEffects, []uint16{observation.EffectIndex})
		if err != nil {
			return err
		}
		request.ContentMetadataEffects = selected
	}
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
	if size == 0 && parsed.FileInfoClass != wire.FileNormalizedNameInformation {
		return nil, statusUnsupported
	}
	if parsed.OutputBufferLength > uint32(c.server.config.Limits.MaxFrameBytes-72) {
		return nil, statusResources
	}
	if parsed.FileInfoClass == wire.FileNormalizedNameInformation {
		return c.queryNormalizedName(ctx, s, t, request, parsed)
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	operation, rights := storage.OpFileState, uint32(0)
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
		information, ok := h.reference().(storage.ReferenceStateAccess)
		if !ok {
			return nil, statusUnsupported
		}
		if err = information.CheckReferenceState(); err != nil {
			return nil, ioStatusError(err)
		}
		result, err := information.State(ctx, storage.StateScalars)
		if err != nil {
			return nil, ioStatusError(err)
		}
		if err = result.CheckProjection(h.nodeID, storage.StateScalars); err != nil || result.Attr.Kind != h.nodeKind {
			return nil, statusIO
		}
		switch parsed.FileInfoClass {
		case wire.FileInternalInformation:
			data = wire.FileInternalInformationBody(result.Attr.ID)
		case wire.FileIdInformation:
			serial, err := stableVolumeSerial(h.identity.Backend.Volume)
			if err != nil {
				return nil, statusIO
			}
			var id [16]byte
			binary.LittleEndian.PutUint64(id[:8], result.Attr.ID)
			data = wire.FileIdInformationBody(serial, id)
		case wire.FileStandardInformation:
			if err = c.checkInformationAllocation(ctx, s, t, h, result.Attr.AllocationKnown, result.Attr.AllocationSize); err != nil {
				return nil, ioStatusError(err)
			}
			links := uint32(1)
			if result.Detached || result.PendingUnlink {
				links = 0
			}
			eof := int64(0)
			if result.Attr.Kind == storage.NodeRegular {
				eof = result.Attr.Size
			}
			data = wire.FileStandardInformationBody(wire.FileStandardInfo{AllocationSize: result.Attr.AllocationSize, EndOfFile: eof, NumberOfLinks: links, DeletePending: result.PendingUnlink, Directory: result.Attr.Kind == storage.NodeDirectory})
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
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return err
	}
	reporter, ok := t.authority.raw.(storage.AllocationReporting)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := reporter.CheckAllocationReporting(); err != nil {
		return err
	}
	unit, err := reporter.AllocationUnitBytes(ctx)
	if err != nil {
		return err
	}
	if unit == 0 || uint64(allocated)%unit != 0 {
		return syscall.EIO
	}
	return checkFileIOIdentity(ctx, s, t, h)
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

func basicMutation(info wire.FileBasicInfo, kind storage.NodeKind) (storage.FileMutation, error) {
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

func (c *connection) observeBasicMetadata(ctx context.Context, s *session, t *tree, h *fileHandle) (windowsMetadataCondition, error) {
	request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace,
		Namespace: windowsMetadataKey, MaxPayloadBytes: 8}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileObserveMetadata, &request); err != nil {
		return windowsMetadataCondition{}, err
	}
	observer, ok := h.reference().(storage.ReferenceMetadataAccess)
	if !ok {
		return windowsMetadataCondition{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckMetadataAccess(); err != nil {
		return windowsMetadataCondition{}, err
	}
	observed, err := observer.ObserveMetadata(ctx, request)
	if err != nil {
		return windowsMetadataCondition{}, err
	}
	if err = observed.CheckNamespace(h.nodeID, request.Namespace, request.MaxPayloadBytes); err != nil {
		return windowsMetadataCondition{}, err
	}
	values := map[string]storage.OpaquePayload{}
	var token []byte
	if observed.Value != nil {
		values[windowsMetadataKey] = *observed.Value
		token = bytes.Clone(observed.Value.Version)
	}
	old, err := decodeWindowsMetadata(values)
	if err != nil {
		return windowsMetadataCondition{}, err
	}
	condition := windowsMetadataCondition{Version: token}
	if h.nodeID == h.identity.Backend.RootNodeID {
		condition.RootAttributes = old.Attributes & (dosHidden | dosSystem)
	}
	return condition, nil
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
	if parsed.FileInfoClass == wire.FileRenameInformation || parsed.FileInfoClass == wire.FileDispositionInformation {
		return c.setNameInformation(ctx, s, t, request, parsed)
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
		command, err = basicMutation(info, h.nodeKind)
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
	var reobserve func(context.Context) (windowsMetadataCondition, error)
	if command.Kind == storage.MutateTruncate {
		command.ExpectedMetadata = map[string][]byte{windowsMetadataKey: nil}
		reobserve = func(call context.Context) (windowsMetadataCondition, error) {
			return c.observeWriteMetadata(call, s, t, h)
		}
	} else if hasMetadata {
		reobserve = func(call context.Context) (windowsMetadataCondition, error) {
			return c.observeBasicMetadata(call, s, t, h)
		}
	}
	if reobserve != nil {
		condition, observeErr := reobserve(ctx)
		if observeErr != nil {
			return nil, ioStatusError(observeErr)
		}
		command, err = applyWindowsMetadataCondition(command, condition, h.nodeID == h.identity.Backend.RootNodeID)
		if err != nil {
			return nil, ioStatusError(err)
		}
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
	owner, err := c.server.reserveMutationOwner(t, h, ticket.sequence, command, effects)
	if err != nil {
		return nil, ioStatusError(err)
	}
	h.pendingWrite = owner
	registerIOResponse(ctx, owner.recordResponse)
	authorizeOwner := func(call context.Context) error { return authorize(call, h) }
	owner.resultByteLimit = int64(c.server.config.Limits.MaxFrameBytes)
	_, err = owner.executeMutation(ctx, authorizeOwner, reobserve, epoch)
	if err != nil {
		return nil, ioStatusError(err)
	}
	return wire.SetInfoResponseBody(), statusOK
}
