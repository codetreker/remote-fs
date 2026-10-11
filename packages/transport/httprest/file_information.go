package httprest

import (
	"context"
	"encoding/json"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceInformation struct {
	NodeID          uint64           `json:"nodeId"`
	Kind            storage.NodeKind `json:"kind"`
	Size            int64            `json:"size"`
	AllocationSize  int64            `json:"allocationSize"`
	AllocationKnown bool             `json:"allocationKnown"`
	Detached        bool             `json:"detached"`
	PendingUnlink   bool             `json:"pendingUnlink"`
}

func referenceInformationOf(v storage.ReferenceInformation) *referenceInformation {
	return &referenceInformation{v.NodeID, v.Kind, v.Size, v.AllocationSize, v.AllocationKnown, v.Detached, v.PendingUnlink}
}
func (v referenceInformation) storage() storage.ReferenceInformation {
	return storage.ReferenceInformation{NodeID: v.NodeID, Kind: v.Kind, Size: v.Size, AllocationSize: v.AllocationSize, AllocationKnown: v.AllocationKnown, Detached: v.Detached, PendingUnlink: v.PendingUnlink}
}

type volumeGeometry struct {
	Volume                   storage.VolumeID `json:"volume"`
	BytesPerSector           uint32           `json:"bytesPerSector"`
	SectorsPerAllocationUnit uint32           `json:"sectorsPerAllocationUnit"`
}

func volumeGeometryOf(v storage.VolumeGeometry) *volumeGeometry {
	return &volumeGeometry{v.Volume, v.BytesPerSector, v.SectorsPerAllocationUnit}
}
func (v volumeGeometry) storage() storage.VolumeGeometry {
	return storage.VolumeGeometry{Volume: v.Volume, BytesPerSector: v.BytesPerSector, SectorsPerAllocationUnit: v.SectorsPerAllocationUnit}
}

func (s *remoteFileSession) CheckVolumeGeometry() error {
	return checkFileCapability(s.capabilities.Geometry)
}
func (s *remoteFileSession) VolumeGeometry(ctx context.Context) (storage.VolumeGeometry, error) {
	if err := s.CheckVolumeGeometry(); err != nil {
		return storage.VolumeGeometry{}, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileVolumeGeometry})
	if err != nil {
		return storage.VolumeGeometry{}, err
	}
	value := response.Geometry.storage()
	if err := value.CheckVolume(s.identity.Backend.Volume); err != nil {
		return storage.VolumeGeometry{}, unreachable(Request{Op: OpFileControl}, err)
	}
	return value, nil
}
func (f *remoteFile) CheckReferenceInformation() error {
	return checkFileCapability(f.capabilities.Information)
}
func (f *remoteFile) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	if err := f.CheckReferenceInformation(); err != nil {
		return storage.ReferenceInformation{}, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileInformation})
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	value := response.Information.storage()
	if err := value.Check(f.node); err != nil {
		return storage.ReferenceInformation{}, unreachable(Request{Op: OpFileControl}, err)
	}
	return value, nil
}
func (r *remoteNodeReference) CheckReferenceInformation() error {
	return r.file.CheckReferenceInformation()
}
func (r *remoteNodeReference) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	return r.file.Information(ctx)
}

func performVolumeGeometry(ctx context.Context, session storage.FileSession) (fileResponse, error) {
	capability, ok := session.(storage.VolumeGeometryAccess)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := capability.CheckVolumeGeometry(); err != nil {
		return fileResponse{}, err
	}
	value, err := capability.VolumeGeometry(ctx)
	if err != nil {
		return fileResponse{}, err
	}
	identity, ok := session.(storage.FileSessionIdentity)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := identity.CheckFileSessionIdentity(); err != nil {
		return fileResponse{}, err
	}
	owner, err := identity.FileSessionIdentity(ctx)
	if err != nil {
		return fileResponse{}, err
	}
	if err := owner.Check(); err != nil {
		return fileResponse{}, err
	}
	if err := value.CheckVolume(owner.Backend.Volume); err != nil {
		return fileResponse{}, err
	}
	return fileResponse{Geometry: volumeGeometryOf(value)}, nil
}
func performReferenceInformation(ctx context.Context, reference retainedReference) (fileResponse, error) {
	capability, ok := reference.(storage.ReferenceInformationAccess)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := capability.CheckReferenceInformation(); err != nil {
		return fileResponse{}, err
	}
	value, err := capability.Information(ctx)
	if err != nil {
		return fileResponse{}, err
	}
	node, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return fileResponse{}, err
	}
	if err := value.Check(node); err != nil {
		return fileResponse{}, err
	}
	return fileResponse{Information: referenceInformationOf(value)}, nil
}
func boundedObservationResponse(response fileResponse, budget int64) (fileResponse, error) {
	encoded, err := json.Marshal(response)
	if err != nil {
		return fileResponse{}, err
	}
	if int64(len(encoded))+32 > budget {
		return fileResponse{}, syscall.EFBIG
	}
	return response, nil
}

var (
	_ storage.VolumeGeometryAccess       = (*remoteFileSession)(nil)
	_ storage.ReferenceInformationAccess = (*remoteFile)(nil)
	_ storage.ReferenceInformationAccess = (*remoteNodeReference)(nil)
)
