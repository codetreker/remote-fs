package management

import "time"

type VolumeState string

const (
	VolumeCreating VolumeState = "creating"
	VolumeActive   VolumeState = "active"
	VolumeDeleting VolumeState = "deleting"
	VolumeDeleted  VolumeState = "deleted"
	VolumeFailed   VolumeState = "failed"
)

type Volume struct {
	ID                 ID
	OwnerUserID        ID
	DisplayName        string
	StorageName        string
	InitialQuotaBytes  int64
	State              VolumeState
	ManagementRevision int64
	ErrorCode          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
