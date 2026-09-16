package httprest

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
)

const OpFile Op = "file"
const OpFileControl Op = "file-control"

// MaxFileControlBytes bounds complete neutral command and result envelopes.
const MaxFileControlBytes int64 = 256 << 10

func fileControl(op storage.Operation) bool {
	switch op {
	case storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose, storage.OpFileClose, storage.OpFileAck, storage.OpFileScope, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
		return true
	}
	return false
}

type fileRequest struct {
	Op           storage.Operation                  `json:"op"`
	Session      string                             `json:"session"`
	File         string                             `json:"file"`
	Action       storage.LockRequestID              `json:"action"`
	Path         []byte                             `json:"path"`
	Node         uint64                             `json:"node"`
	Options      storage.FileSessionOptions         `json:"options"`
	Open         storage.FileOpenOptions            `json:"open"`
	Offset       int64                              `json:"offset"`
	ResultBytes  int64                              `json:"resultBytes"`
	Length       int                                `json:"length"`
	Data         []byte                             `json:"data"`
	Change       *AttrChange                        `json:"change,omitempty"`
	Owner        storage.UseOwner                   `json:"owner"`
	LockID       storage.LockRequestID              `json:"lockId"`
	Domain       storage.ConflictDomain             `json:"domain"`
	Commands     []storage.RangeCommand             `json:"commands,omitempty"`
	Child        *storage.ChildName                 `json:"child,omitempty"`
	Directory    *storage.DirectoryTarget           `json:"directory,omitempty"`
	OpenAt       *openAtOptions                     `json:"openAt,omitempty"`
	NodeRef      *nodeRefOptions                    `json:"nodeRef,omitempty"`
	Name         *nameCommand                       `json:"name,omitempty"`
	Scope        *storage.UseScope                  `json:"scope,omitempty"`
	OwnerOptions storage.OwnerOptions               `json:"ownerOptions"`
	Namespace    string                             `json:"namespace"`
	Version      []byte                             `json:"version,omitempty"`
	Payload      []byte                             `json:"payload,omitempty"`
	Pending      *storage.PendingUnlinkCommand      `json:"pending,omitempty"`
	ClearPending *storage.ClearPendingUnlinkCommand `json:"clearPending,omitempty"`
	Mutation     *fileMutationOptions               `json:"mutation,omitempty"`
}

type fileResponse struct {
	Session      string                     `json:"session,omitempty"`
	File         string                     `json:"file,omitempty"`
	Retry        bool                       `json:"retry,omitempty"`
	Epoch        uint64                     `json:"epoch"`
	Status       *storage.FileSessionStatus `json:"status,omitempty"`
	Attr         *Attr                      `json:"attr,omitempty"`
	Data         []byte                     `json:"data"`
	Conflict     *storage.RangeConflict     `json:"conflict,omitempty"`
	Attempt      *storage.RangeAttempt      `json:"attempt,omitempty"`
	Barrier      *MutationBarrier           `json:"barrier,omitempty"`
	Capabilities *fileCapabilities          `json:"capabilities,omitempty"`
	Outcome      storage.OpenOutcome        `json:"outcome,omitempty"`
	Directory    *observedDirectory         `json:"directory,omitempty"`
	State        *referenceState            `json:"state,omitempty"`
	Scope        *storage.UseScope          `json:"scope,omitempty"`
	Owner        storage.UseOwner           `json:"owner,omitempty"`
	Metadata     *OpaquePayload             `json:"metadata,omitempty"`
}

func fileMutation(op storage.Operation) bool {
	switch op {
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileSetNodeAttr, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSync, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileMutateName, storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
		return true
	}
	return false
}

func fileActionRequired(op storage.Operation) bool {
	switch op {
	case storage.OpFileOpen, storage.OpFileOpenNode, storage.OpFileSetNodeAttr, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSync, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileMutateName, storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeDrop, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
		return true
	}
	return false
}

// FileWithBarrier exposes authority progress without requiring a directory entry
// for detached files. A nil barrier means the volume has no change log.
type FileWithBarrier interface {
	storage.File
	CloseWithBarrier(context.Context) (*MutationBarrier, error)
	WriteAtWithBarrier(context.Context, int64, []byte) (storage.Attr, *MutationBarrier, error)
	TruncateWithBarrier(context.Context, int64) (storage.Attr, *MutationBarrier, error)
	SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
}

type FileSessionWithBarrier interface {
	storage.FileSession
	CloseWithBarrier(context.Context) (*MutationBarrier, error)
	OpenFileWithBarrier(context.Context, string, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	OpenNodeWithBarrier(context.Context, uint64, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	SetNodeAttrWithBarrier(context.Context, uint64, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
}
