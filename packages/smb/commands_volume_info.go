package smb

import (
	"context"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusInvalidInfoClass uint32 = 0xc0000003
	statusBufferOverflow   uint32 = 0x80000005
	maxVolumeLabelUnits           = 32
	volumeFileSystemName          = "remote-fs"
)

func volumeInfoLengths(class byte) (minimum, maximum int) {
	switch class {
	case wire.FileFsVolumeInformation:
		return 24, 18 + 2*maxVolumeLabelUnits
	case wire.FileFsSizeInformation:
		return 24, 24
	case wire.FileFsDeviceInformation:
		return 8, 8
	case wire.FileFsAttributeInformation:
		return 12, 12 + 2*len(volumeFileSystemName)
	case wire.FileFsFullSizeInformation:
		return 32, 32
	default:
		return 0, 0
	}
}

func (c *connection) queryInfo(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32) {
	parsed, err := request.QueryInfo()
	if err != nil {
		return nil, statusInvalid
	}
	if parsed.InfoType == wire.InfoFilesystem {
		return c.queryVolumeInfo(ctx, s, t, request, parsed)
	}
	return c.queryFileInfo(ctx, s, t, request)
}

func (c *connection) queryVolumeInfo(ctx context.Context, s *session, t *tree, request wire.Request, parsed wire.QueryInfoRequest) ([]byte, uint32) {
	minimum, _ := volumeInfoLengths(parsed.FileInfoClass)
	if minimum == 0 {
		if parsed.FileInfoClass >= 1 && parsed.FileInfoClass <= 11 {
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
	operation := storage.OpFileBackendIdentity
	capacity := parsed.FileInfoClass == wire.FileFsSizeInformation || parsed.FileInfoClass == wire.FileFsFullSizeInformation
	if capacity {
		operation = storage.OpVolumeSpace
	}
	authorize := func(call context.Context, h *fileHandle) error {
		if err := c.authorizeReference(call, s, t, h, storage.OpFileState, nil); err != nil {
			return err
		}
		return c.authorizeReference(call, s, t, h, operation, nil)
	}
	if parsed.OutputBufferLength < uint32(minimum) {
		if err := authorize(ctx, t.findFileHandle(id)); err != nil {
			return nil, ioStatusError(err)
		}
		return wire.InfoLengthMismatchErrorBody(), statusInfoLengthMismatch
	}
	ticket, err := c.admitInformation(ctx, s, t, id, 0, authorize)
	if err != nil {
		return nil, ioStatusError(err)
	}
	defer ticket.finish()
	h := ticket.handle
	state, ok := h.reference().(storage.ReferenceStateAccess)
	if !ok {
		return nil, statusUnsupported
	}
	if err := state.CheckReferenceState(); err != nil {
		return nil, ioStatusError(err)
	}
	observed, err := state.State(ctx, storage.StateScalars)
	if err != nil {
		return nil, ioStatusError(err)
	}
	if observed.CheckProjection(h.nodeID, storage.StateScalars) != nil || observed.Attr.Kind != h.nodeKind {
		return nil, statusIO
	}
	var data []byte
	switch parsed.FileInfoClass {
	case wire.FileFsVolumeInformation:
		if t.export.share.VolumeLabel == nil {
			return nil, statusUnsupported
		}
		created, err := storage.VolumeCreatedAt(ctx, t.authority.raw)
		if err != nil {
			return nil, ioStatusError(err)
		}
		instant, err := toFiletime(created)
		if err != nil || instant > math.MaxInt64 {
			return nil, statusIO
		}
		data, err = wire.FileFsVolumeInformationBody(wire.FileFsVolumeInfo{
			CreationTime: int64(instant), Serial: uint32(t.export.volumeSerial), Label: *t.export.share.VolumeLabel,
		})
	case wire.FileFsSizeInformation, wire.FileFsFullSizeInformation:
		space, unit, sourceErr := volumeCapacity(ctx, s, t, parsed.FileInfoClass == wire.FileFsFullSizeInformation)
		if sourceErr != nil {
			return nil, volumeCapacityStatus(sourceErr)
		}
		if parsed.FileInfoClass == wire.FileFsSizeInformation {
			data, err = wire.FileFsSizeInformationBody(wire.FileFsSizeInfo{
				TotalAllocationUnits: space.Total / int64(unit), AvailableAllocationUnits: space.Avail / int64(unit),
				SectorsPerAllocationUnit: 1, BytesPerSector: unit,
			})
		} else {
			data, err = wire.FileFsFullSizeInformationBody(wire.FileFsFullSizeInfo{
				TotalAllocationUnits: space.Total / int64(unit), CallerAvailableAllocationUnits: space.Avail / int64(unit),
				ActualAvailableAllocationUnits: (space.Total - space.Used) / int64(unit),
				SectorsPerAllocationUnit:       1, BytesPerSector: unit,
			})
		}
	case wire.FileFsDeviceInformation:
		data = wire.FileFsDeviceInformationBody(wire.FileFsDeviceInfo{DeviceType: 7, Characteristics: 0x10})
	case wire.FileFsAttributeInformation:
		data, err = wire.FileFsAttributeInformationBody(wire.FileFsAttributeInfo{
			Attributes: 6, MaximumComponentNameLength: maxNameUnits, FileSystemName: volumeFileSystemName,
		})
	}
	if err != nil {
		return nil, statusIO
	}
	if err = authorize(ctx, h); err != nil {
		return nil, ioStatusError(err)
	}
	status := statusOK
	if uint32(len(data)) > parsed.OutputBufferLength {
		data = data[:parsed.OutputBufferLength]
		status = statusBufferOverflow
	}
	body, err := wire.QueryInfoResponseBody(data)
	if err != nil {
		return nil, statusIO
	}
	return body, status
}

func volumeCapacity(ctx context.Context, s *session, t *tree, full bool) (storage.Space, uint32, error) {
	reporter, ok := t.authority.raw.(storage.AllocationReporting)
	if !ok {
		return storage.Space{}, 0, syscall.EOPNOTSUPP
	}
	if err := reporter.CheckAllocationReporting(); err != nil {
		return storage.Space{}, 0, err
	}
	unit, err := reporter.AllocationUnitBytes(ctx)
	if err != nil {
		return storage.Space{}, 0, err
	}
	if unit == 0 || unit > math.MaxUint32 {
		return storage.Space{}, 0, syscall.EIO
	}
	s.identityMu.RLock()
	principal := s.principal
	s.identityMu.RUnlock()
	space, err := t.export.share.Backend.Space(WithPrincipal(ctx, principal))
	if err != nil {
		return storage.Space{}, 0, err
	}
	if !space.Coherent() || space.Used > space.Total || space.Total%int64(unit) != 0 || space.Avail%int64(unit) != 0 ||
		full && (space.Total-space.Used)%int64(unit) != 0 {
		return storage.Space{}, 0, syscall.EIO
	}
	return space, uint32(unit), nil
}

func volumeCapacityStatus(err error) uint32 {
	status := ioStatusError(err)
	if status == statusUnsupported {
		return statusIO
	}
	return status
}
