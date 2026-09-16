package httprest

import (
	"context"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const OpFile Op = "file"
const OpFileControl Op = "file-control"

// The registry owns sessions; reference identities belong to those native sessions.
type fileRequest struct {
	Name        []byte                         `json:"name,omitempty"`
	Op          storage.Operation              `json:"op"`
	Session     string                         `json:"session,omitempty"`
	Reference   storage.FileReferenceID        `json:"reference,omitempty"`
	Action      storage.FileActionID           `json:"action,omitempty"`
	Options     *storage.FileSessionOptions    `json:"options,omitempty"`
	Node        uint64                         `json:"node,omitempty"`
	Observation *storage.ObservationOptions    `json:"observation,omitempty"`
	Check       *storage.ObservationCondition  `json:"check,omitempty"`
	Retain      *fileRetainRequest             `json:"retain,omitempty"`
	RetainAt    *fileRetainAtRequest           `json:"retainAt,omitempty"`
	Create      *fileCreateRequest             `json:"create,omitempty"`
	Reset       *fileResetRequest              `json:"reset,omitempty"`
	Change      *AttrChange                    `json:"change,omitempty"`
	Kind        *fileKindRequest               `json:"kind,omitempty"`
	Read        *fileReadRequest               `json:"read,omitempty"`
	Write       *fileWriteRequest              `json:"write,omitempty"`
	Truncate    *fileTruncateRequest           `json:"truncate,omitempty"`
	List        *storage.DirectoryPageRequest  `json:"list,omitempty"`
	Rename      *fileRenameRequest             `json:"rename,omitempty"`
	Claim       *storage.AccessClaim           `json:"claim,omitempty"`
	Prepare     *storage.PrepareRemovalRequest `json:"prepare,omitempty"`
	Intent      *storage.RemovalIntentID       `json:"intent,omitempty"`
	Drain       *storage.DrainEntryRequest     `json:"drain,omitempty"`
	CancelDrain *storage.CancelDrainRequest    `json:"cancelDrain,omitempty"`
	Owner       *storage.RangeOwnerID          `json:"owner,omitempty"`
	Scope       *storage.RangeScope            `json:"scope,omitempty"`
	Ranges      *storage.RangeReplaceRequest   `json:"ranges,omitempty"`
	Wait        *storage.RangeWaitRequest      `json:"wait,omitempty"`
}

type fileRetainRequest struct {
	NodeID                   uint64                       `json:"nodeId"`
	ExpectedMetadataRevision storage.NodeMetadataRevision `json:"expectedMetadataRevision"`
	Claim                    storage.AccessClaim          `json:"claim"`
	Witness                  *storage.EntryLocation       `json:"witness,omitempty"`
	Prepared                 *storage.RemovalCondition    `json:"prepared,omitempty"`
}
type fileRetainAtRequest struct {
	Target   fileEntryTarget           `json:"target"`
	Claim    storage.AccessClaim       `json:"claim"`
	Prepared *storage.RemovalCondition `json:"prepared,omitempty"`
}
type fileReadRequest struct {
	Offset int64                 `json:"offset"`
	Length int                   `json:"length"`
	Owner  *storage.RangeOwnerID `json:"owner,omitempty"`
}
type fileWriteRequest struct {
	ExpectedSize *int64                `json:"expectedSize,omitempty"`
	Offset       int64                 `json:"offset"`
	Data         []byte                `json:"data"`
	Owner        *storage.RangeOwnerID `json:"owner,omitempty"`
}
type fileTruncateRequest struct {
	Size  int64                 `json:"size"`
	Owner *storage.RangeOwnerID `json:"owner,omitempty"`
}
type fileConflict struct {
	Kind     storage.FileConflictKind `json:"kind"`
	NodeID   uint64                   `json:"nodeId"`
	EntryID  storage.EntryID          `json:"entryId"`
	Revision uint64                   `json:"revision"`
	Claim    storage.AccessClaim      `json:"claim"`
	Range    *storage.HeldRange       `json:"range,omitempty"`
}
type fileEntryTarget struct {
	Parent                   storage.FileReferenceID      `json:"parent"`
	ParentID                 uint64                       `json:"parentId"`
	Name                     []byte                       `json:"name"`
	DirectoryRevision        storage.DirectoryRevision    `json:"directoryRevision"`
	ExpectedEntryID          storage.EntryID              `json:"expectedEntryId"`
	ExpectedNodeID           uint64                       `json:"expectedNodeId"`
	ExpectedMetadataRevision storage.NodeMetadataRevision `json:"expectedMetadataRevision"`
	Witness                  *storage.EntryLocation       `json:"witness,omitempty"`
}
type fileRenameRequest struct {
	NewName     []byte          `json:"newName,omitempty"`
	Source      fileEntryTarget `json:"source"`
	Destination fileEntryTarget `json:"destination"`
}
type fileEntryLookup struct {
	ParentID          uint64                    `json:"parentId"`
	DirectoryRevision storage.DirectoryRevision `json:"directoryRevision"`
	Name              []byte                    `json:"name"`
	Found             bool                      `json:"found"`
	EntryID           storage.EntryID           `json:"entryId"`
	Attr              *Attr                     `json:"attr,omitempty"`
}
type fileInitial struct {
	Kind         storage.NodeKind `json:"kind"`
	Metadata     []byte           `json:"metadata"`
	LinkTarget   []byte           `json:"linkTarget"`
	AccessTime   *Time            `json:"accessTime,omitempty"`
	ModTime      *Time            `json:"modTime,omitempty"`
	CreationTime *Time            `json:"creationTime,omitempty"`
	ChangeTime   *Time            `json:"changeTime,omitempty"`
}
type fileCreateRequest struct {
	Target   fileEntryTarget           `json:"target"`
	Initial  fileInitial               `json:"initial"`
	Claim    storage.AccessClaim       `json:"claim"`
	Prepared *storage.RemovalCondition `json:"prepared,omitempty"`
}
type fileResetRequest struct {
	Target           fileEntryTarget              `json:"target"`
	ExpectedRevision storage.NodeMetadataRevision `json:"expectedRevision"`
	Change           AttrChange                   `json:"change"`
	Claim            storage.AccessClaim          `json:"claim"`
	Prepared         *storage.RemovalCondition    `json:"prepared,omitempty"`
}
type fileKindRequest struct {
	Owner            *storage.RangeOwnerID        `json:"owner,omitempty"`
	Witness          *storage.EntryLocation       `json:"witness,omitempty"`
	ExpectedRevision storage.NodeMetadataRevision `json:"expectedRevision"`
	Kind             storage.NodeKind             `json:"kind"`
	LinkTarget       []byte                       `json:"linkTarget"`
	Metadata         []byte                       `json:"metadata"`
}
type fileObservation struct {
	Removal    storage.RemovalStatus  `json:"removal"`
	Attr       *Attr                  `json:"attr"`
	Location   *storage.EntryLocation `json:"location,omitempty"`
	LinkTarget []byte                 `json:"linkTarget"`
}
type fileReceipt struct {
	Action           storage.FileActionID    `json:"action"`
	Operation        storage.Operation       `json:"operation"`
	State            storage.FileActionState `json:"state"`
	Effects          storage.FileEffects     `json:"effects"`
	Reference        storage.FileReferenceID `json:"reference"`
	Observation      *fileObservation        `json:"observation,omitempty"`
	RangeRevision    uint64                  `json:"rangeRevision"`
	Removal          storage.RemovalStatus   `json:"removal"`
	Errno            string                  `json:"errno"`
	Conflict         *fileConflict           `json:"conflict,omitempty"`
	HistoryRemaining int64                   `json:"historyRemaining"`
}
type fileDirectoryEntry struct {
	EntryID storage.EntryID `json:"entryId"`
	Name    []byte          `json:"name"`
	Attr    *Attr           `json:"attr"`
}
type fileDirectoryPage struct {
	ParentID uint64                    `json:"parentId"`
	Revision storage.DirectoryRevision `json:"revision"`
	Entries  []fileDirectoryEntry      `json:"entries"`
	Next     storage.DirectoryCursor   `json:"next"`
	Done     bool                      `json:"done"`
}
type fileResponse struct {
	Lookup      *fileEntryLookup           `json:"lookup,omitempty"`
	Node        uint64                     `json:"node,omitempty"`
	Reference   storage.FileReferenceID    `json:"reference,omitempty"`
	State       *storage.FileVolumeState   `json:"state,omitempty"`
	Session     string                     `json:"session,omitempty"`
	Status      *storage.FileSessionStatus `json:"status,omitempty"`
	Observation *fileObservation           `json:"observation,omitempty"`
	Receipt     *fileReceipt               `json:"receipt,omitempty"`
	Data        []byte                     `json:"data,omitempty"`
	Page        *fileDirectoryPage         `json:"page,omitempty"`
	Ranges      *storage.RangeSnapshot     `json:"ranges,omitempty"`
	Barrier     *MutationBarrier           `json:"barrier,omitempty"`
}
type fileErrorResponse struct {
	NotAdmitted bool             `json:"notAdmitted,omitempty"`
	LockCode    *locking.Code    `json:"lockCode,omitempty"`
	Recorded    *bool            `json:"recorded,omitempty"`
	Errno       string           `json:"errno"`
	Message     string           `json:"message"`
	Conflict    *fileConflict    `json:"conflict,omitempty"`
	Receipt     *fileReceipt     `json:"receipt,omitempty"`
	Barrier     *MutationBarrier `json:"barrier,omitempty"`
}

func fileControl(op storage.Operation) bool {
	switch op {
	case storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose,
		storage.OpFileClose, storage.OpFileQueryAction, storage.OpFileCancelAction,
		storage.OpFileRetireRangeOwner, storage.OpFileRetireRanges,
		storage.OpFileReplaceClaim, storage.OpFileCancelPrepared, storage.OpFileCancelDrain:
		return true
	}
	return false
}
func fileActionRequired(op storage.Operation) bool {
	switch op {
	case storage.OpFileRetain, storage.OpFileRetainAt, storage.OpFileCreateAndRetainAt,
		storage.OpFileResetAndRetainAt, storage.OpFileReplaceAndRetainAt, storage.OpFileSetNodeAttr,
		storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetKind,
		storage.OpFileRename, storage.OpFileReplaceClaim, storage.OpFilePrepareRemoval,
		storage.OpFileCancelPrepared, storage.OpFileDrainEntry, storage.OpFileCancelDrain,
		storage.OpFileReplaceRanges, storage.OpFileWaitRanges, storage.OpFileRetireRangeOwner,
		storage.OpFileRetireRanges, storage.OpFileSessionClose, storage.OpFileClose,
		storage.OpFileQueryAction, storage.OpFileCancelAction:
		return true
	}
	return false
}
func fileMutation(op storage.Operation) bool {
	return fileActionRequired(op) && op != storage.OpFileQueryAction && op != storage.OpFileCancelAction
}

// Inline barriers order confirmed effects against a metadata replica. Receipts
// remain available when observation of the barrier fails.
type FileWithBarrier interface {
	storage.File
	WriteAtWithBarrier(context.Context, storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	TruncateWithBarrier(context.Context, storage.FileTruncateRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	SetAttrWithBarrier(context.Context, storage.AttrChange, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	SetKindWithBarrier(context.Context, storage.SetKindRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	RenameWithBarrier(context.Context, storage.RenameRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	PrepareRemovalWithBarrier(context.Context, storage.PrepareRemovalRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CancelPreparedWithBarrier(context.Context, storage.RemovalIntentID, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	DrainEntryWithBarrier(context.Context, storage.DrainEntryRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CancelDrainWithBarrier(context.Context, storage.CancelDrainRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CloseWithBarrier(context.Context, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
}
type FileSessionWithBarrier interface {
	storage.FileSession
	RetainWithBarrier(context.Context, storage.RetainRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	RetainAtWithBarrier(context.Context, storage.RetainAtRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CreateAndRetainAtWithBarrier(context.Context, storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	ResetAndRetainAtWithBarrier(context.Context, storage.ResetAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	ReplaceAndRetainAtWithBarrier(context.Context, storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	SetNodeAttrWithBarrier(context.Context, uint64, storage.AttrChange, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	QueryActionWithBarrier(context.Context, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CancelActionWithBarrier(context.Context, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
	CloseWithBarrier(context.Context, storage.FileActionID) (storage.FileActionReceipt, *MutationBarrier, error)
}

func fileWait(op storage.Operation) bool { return op == storage.OpFileWaitRanges }
