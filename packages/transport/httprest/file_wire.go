package httprest

import (
	"context"
	"encoding/json"
	"github.com/codetreker/remote-fs/packages/storage"
)

const OpFile Op = "file"
const OpFileControl Op = "file-control"

// MaxFileControlBytes bounds complete neutral control commands and receipts.
const MaxFileControlBytes int64 = 256 << 10

func fileControl(op storage.Operation) bool {
	switch op {
	case storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose, storage.OpFileQueryAction, storage.OpFileQueryDeleteIntent, storage.OpFileAcknowledgeDeleteIntent, storage.OpFileClose, storage.OpFileAck, storage.OpFileState, storage.OpFileScope, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		return true
	}
	return false
}

type fileRequest struct {
	Op           storage.Operation               `json:"op"`
	Session      string                          `json:"session"`
	File         string                          `json:"file"`
	Action       storage.LockRequestID           `json:"action"`
	Path         []byte                          `json:"path"`
	Node         uint64                          `json:"node"`
	Options      storage.FileSessionOptions      `json:"options"`
	Open         storage.FileOpenOptions         `json:"open"`
	Child        *childName                      `json:"child,omitempty"`
	OpenAt       *openAtOptions                  `json:"openAt,omitempty"`
	NodeRef      *nodeRefOptions                 `json:"nodeRef,omitempty"`
	Name         *nameCommand                    `json:"name,omitempty"`
	Offset       int64                           `json:"offset"`
	ResultBytes  int64                           `json:"resultBytes"`
	Length       int                             `json:"length"`
	Data         []byte                          `json:"data"`
	Change       *AttrChange                     `json:"change,omitempty"`
	Scope        *storage.UseScope               `json:"scope,omitempty"`
	OwnerOptions storage.OwnerOptions            `json:"ownerOptions"`
	Owner        storage.UseOwner                `json:"owner"`
	LockID       storage.LockRequestID           `json:"lockId"`
	Domain       storage.ConflictDomain          `json:"domain"`
	Commands     []storage.RangeCommand          `json:"commands,omitempty"`
	Namespace    string                          `json:"namespace"`
	Version      metadataVersion                 `json:"version"`
	Payload      metadataPayload                 `json:"payload"`
	Pending      *pendingUnlinkCommand           `json:"pending,omitempty"`
	ClearPending *clearPendingUnlinkCommand      `json:"clearPending,omitempty"`
	Mutation     *fileMutationOptions            `json:"mutation,omitempty"`
	FileAction   storage.FileActionID            `json:"fileAction"`
	DeleteIntent storage.DeleteIntentID          `json:"deleteIntent"`
	Acknowledge  *acknowledgeDeleteIntentCommand `json:"acknowledge,omitempty"`
}

func (r fileRequest) MarshalJSON() ([]byte, error) {
	type request fileRequest
	encoded, err := json.Marshal(request(r))
	if err != nil || len(r.Open.InitialMetadata) == 0 {
		return encoded, err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return nil, err
	}
	var open map[string]json.RawMessage
	if err := json.Unmarshal(envelope["open"], &open); err != nil {
		return nil, err
	}
	metadata := make(map[string]metadataPayload, len(r.Open.InitialMetadata))
	for namespace, value := range r.Open.InitialMetadata {
		metadata[namespace] = metadataPayload(value)
	}
	open["InitialMetadata"], err = json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	envelope["open"], err = json.Marshal(open)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

type fileResponse struct {
	Node          uint64                     `json:"node,omitempty"`
	Session       string                     `json:"session,omitempty"`
	File          string                     `json:"file,omitempty"`
	Retry         bool                       `json:"retry,omitempty"`
	Epoch         uint64                     `json:"epoch"`
	Status        *storage.FileSessionStatus `json:"status,omitempty"`
	Attr          *Attr                      `json:"attr,omitempty"`
	Data          []byte                     `json:"data"`
	Conflict      *storage.RangeConflict     `json:"conflict,omitempty"`
	Attempt       *storage.RangeAttempt      `json:"attempt,omitempty"`
	Barrier       *MutationBarrier           `json:"barrier,omitempty"`
	Capabilities  *fileCapabilities          `json:"capabilities,omitempty"`
	Scope         *storage.UseScope          `json:"scope,omitempty"`
	Owner         storage.UseOwner           `json:"owner,omitempty"`
	Metadata      *OpaquePayload             `json:"metadata,omitempty"`
	Outcome       storage.OpenOutcome        `json:"outcome,omitempty"`
	State         *referenceState            `json:"state,omitempty"`
	ActionReceipt *storage.FileActionReceipt `json:"actionReceipt,omitempty"`
	DeleteStatus  *deleteIntentStatus        `json:"deleteStatus,omitempty"`
}

func fileMutation(op storage.Operation) bool {
	switch op {
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileSetNodeAttr, storage.OpFileMutateName, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSync, storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate, storage.OpFileClose, storage.OpFileSessionClose:
		return true
	}
	return false
}

func fileActionRequired(op storage.Operation) bool {
	switch op {
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef,
		storage.OpFileSetNodeAttr, storage.OpFileMutateName, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr,
		storage.OpFileSync, storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink,
		storage.OpFileClearPendingUnlink, storage.OpFileMutate, storage.OpFileAcknowledgeDeleteIntent,
		storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeDrop:
		return true
	}
	return false
}

func semanticFileAction(request fileRequest) storage.LockRequestID {
	switch request.Op {
	case storage.OpFileOpenAt:
		if request.OpenAt != nil {
			return storage.LockRequestID(request.OpenAt.Action)
		}
	case storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		if request.NodeRef != nil {
			return storage.LockRequestID(request.NodeRef.Action)
		}
	case storage.OpFileMutateName:
		if request.Name != nil {
			return storage.LockRequestID(request.Name.Action)
		}
	case storage.OpFileSetPendingUnlink:
		if request.Pending != nil {
			return storage.LockRequestID(request.Pending.Action)
		}
	case storage.OpFileClearPendingUnlink:
		if request.ClearPending != nil {
			return storage.LockRequestID(request.ClearPending.Action)
		}
	case storage.OpFileMutate:
		if request.Mutation != nil {
			return storage.LockRequestID(request.Mutation.Action)
		}
	case storage.OpFileAcknowledgeDeleteIntent:
		if request.Acknowledge != nil {
			return storage.LockRequestID(request.Acknowledge.Action)
		}
	}
	return ""
}

// FileWithBarrier exposes authority progress without requiring a directory entry
// for detached files. A nil barrier means the volume has no change log.
type FileWithBarrier interface {
	storage.File
	WriteAtWithBarrier(context.Context, int64, []byte) (storage.Attr, *MutationBarrier, error)
	TruncateWithBarrier(context.Context, int64) (storage.Attr, *MutationBarrier, error)
	SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
	CloseWithBarrier(context.Context) (*MutationBarrier, error)
}

type FileSessionWithBarrier interface {
	storage.FileSession
	OpenFileWithBarrier(context.Context, string, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	OpenNodeWithBarrier(context.Context, uint64, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	SetNodeAttrWithBarrier(context.Context, uint64, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
	CloseWithBarrier(context.Context) (*MutationBarrier, error)
}

type fileCapabilities struct {
	DirectoryMetadata bool `json:"directoryMetadata"`
	ReferenceName     bool `json:"referenceName"`
	AtomicOpen        bool `json:"atomicOpen"`
	Namespace         bool `json:"namespace"`
	References        bool `json:"references"`
	Actions           bool `json:"actions"`
	Metadata          bool `json:"metadata"`
	Owners            bool `json:"owners"`
	Ranges            bool `json:"ranges"`
	State             bool `json:"state"`
	Scope             bool `json:"scope"`
	Delete            bool `json:"delete"`
	Conditional       bool `json:"conditional"`
}
