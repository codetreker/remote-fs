package management

import "time"

type OperationKind string

const (
	OperationCreate OperationKind = "create"
	OperationRename OperationKind = "rename"
	OperationQuota  OperationKind = "quota"
	OperationDelete OperationKind = "delete"
)

type OperationState string

const (
	OperationPending   OperationState = "pending"
	OperationRunning   OperationState = "running"
	OperationSucceeded OperationState = "succeeded"
	OperationFailed    OperationState = "failed"
)

type OperationPhase string

const (
	PhaseAccepted      OperationPhase = "accepted"
	PhaseInitializing  OperationPhase = "initializing"
	PhaseBound         OperationPhase = "bound"
	PhaseApplying      OperationPhase = "applying"
	PhaseFrozen        OperationPhase = "frozen"
	PhaseVerifiedClose OperationPhase = "verified-close"
	PhaseQuarantined   OperationPhase = "quarantined"
	PhaseReclaiming    OperationPhase = "reclaiming"
	PhaseCompleted     OperationPhase = "completed"
)

type Operation struct {
	ID                  ID
	ActorUserID         ID
	VolumeID            ID
	Kind                OperationKind
	Action              ActionKey
	Fingerprint         []byte
	ExpectedRevision    int64
	Payload             []byte
	State               OperationState
	Phase               OperationPhase
	Revision            int64
	VerifiedRootProof   []byte
	PlannedReclaimProof []byte
	QuarantineLeaf      string
	ReclaimingProof     []byte
	Result              []byte
	ErrorCode           string
	Settled             bool
	SettlementProof     []byte
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type OperationTransition struct {
	State               OperationState
	Phase               OperationPhase
	VerifiedRootProof   []byte
	PlannedReclaimProof []byte
	QuarantineLeaf      string
	ReclaimingProof     []byte
	Result              []byte
	ErrorCode           string
	Settled             bool
}

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
	State              VolumeState
	ManagementRevision int64
	LastOperationID    ID
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type VolumeMutation struct {
	Action           ActionKey
	VolumeID         ID
	ExpectedRevision int64
	Name             string
	QuotaBytes       int64
}
