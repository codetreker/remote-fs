package metastore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

// FileSession owns native references. Receipts contain identifiers and values;
// resolving a reference never creates another pin or substitutes a path.
type FileSession interface {
	Retain(context.Context, storage.RetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	RetainAt(context.Context, storage.RetainAtRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	CreateAndRetainAt(context.Context, storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	ResetAndRetainAt(context.Context, storage.ResetAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	ReplaceAndRetainAt(context.Context, storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	Reference(context.Context, storage.FileReferenceID) (File, bool, error)
	StatNode(context.Context, uint64, storage.ObservationOptions) (storage.FileObservation, error)
	SetNodeAttr(context.Context, uint64, storage.AttrChange, storage.FileActionID) (storage.FileActionReceipt, error)
	QueryAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
	CancelAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
	RetireRangeOwner(context.Context, storage.RangeOwnerID, storage.FileActionID) (storage.FileActionReceipt, error)
	Renew(context.Context) (storage.FileSessionStatus, error)
	Status(context.Context) (storage.FileSessionStatus, error)
	Retire(context.Context) error
	Dispose(context.Context) error
	BeginClose(context.Context, storage.FileActionID) (storage.FileActionReceipt, bool, error)
	Close(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

// File separates native publication from object-byte materialization. Retire
// fences publication before the byte-serving layer drains admitted work; Close
// releases claims and physical retention only after that drain.
type File interface {
	Reference() storage.FileReferenceID
	NodeID() uint64
	AcquireIO(context.Context) (IOMembership, error)
	Stat(context.Context, storage.ObservationOptions) (storage.FileObservation, error)
	CheckObservation(context.Context, storage.ObservationCondition) (storage.FileObservation, error)
	Capture(context.Context, storage.FileIO) (FileState, error)
	Reserve(context.Context, int64) (Key, error)
	BeginContent(context.Context, storage.FileActionID, [32]byte, storage.FileIO) (storage.FileActionReceipt, bool, error)
	CommitContent(context.Context, storage.FileActionID, uint64, Object) (storage.FileActionReceipt, error)
	RejectContent(context.Context, storage.FileActionID, error) (storage.FileActionReceipt, error)
	SetAttr(context.Context, storage.AttrChange, storage.FileActionID) (storage.FileActionReceipt, error)
	SetKind(context.Context, storage.SetKindRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	ListAt(context.Context, storage.DirectoryPageRequest) (storage.DirectoryPage, error)
	LookupAt(context.Context, []byte) (storage.EntryLookup, error)
	Rename(context.Context, storage.RenameRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	ReplaceClaim(context.Context, storage.AccessClaim, storage.FileActionID) (storage.FileActionReceipt, error)
	PrepareRemoval(context.Context, storage.PrepareRemovalRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	CancelPrepared(context.Context, storage.RemovalIntentID, storage.FileActionID) (storage.FileActionReceipt, error)
	DrainEntry(context.Context, storage.DrainEntryRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	CancelDrain(context.Context, storage.CancelDrainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	RangeSnapshot(context.Context, storage.RangeOwnerID, storage.RangeScope) (storage.RangeSnapshot, error)
	ReplaceRanges(context.Context, storage.RangeReplaceRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	WaitRanges(context.Context, storage.RangeWaitRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	RetireRangeOwner(context.Context, storage.RangeOwnerID, storage.RangeScope, storage.FileActionID) (storage.FileActionReceipt, error)
	Sync(context.Context) (FileState, error)
	Retire(context.Context) error
	BeginClose(context.Context, storage.FileActionID) (storage.FileActionReceipt, bool, error)
	Close(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

// IOMembership retains native storage until the byte-serving operation has
// stopped using it. It does not extend session or publication permission.
type IOMembership interface{ Close(context.Context) error }

type fileIOKey struct{}

// WithFileIO describes the caller's logical operation at revision capture and
// final publication. It supplies no reference or ownership authority.
func WithFileIO(ctx context.Context, operation storage.FileIO) context.Context {
	if operation.ExpectedSize != nil {
		size := *operation.ExpectedSize
		operation.ExpectedSize = &size
	}
	if operation.Owner != nil {
		owner := *operation.Owner
		operation.Owner = &owner
	}
	return context.WithValue(ctx, fileIOKey{}, operation)
}

func FileIOFromContext(ctx context.Context) (storage.FileIO, bool) {
	operation, ok := ctx.Value(fileIOKey{}).(storage.FileIO)
	if operation.ExpectedSize != nil {
		size := *operation.ExpectedSize
		operation.ExpectedSize = &size
	}
	if operation.Owner != nil {
		owner := *operation.Owner
		operation.Owner = &owner
	}
	return operation, ok
}
