package metastore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

// WindowsStore is the native authority beneath object materialization. Every
// reference and action belongs to the same ordering domain as ordinary writes.
type WindowsStore interface {
	CheckWindowsStore() error
	WindowsState(context.Context) (storage.WindowsState, error)
	EnableWindows(context.Context, storage.WindowsActionID) (storage.WindowsActivation, error)
	QueryWindowsActivation(context.Context, storage.WindowsActionID) (storage.WindowsActivation, error)
	NewWindowsSession(context.Context, storage.FileSessionOptions) (WindowsSession, error)
}

type WindowsSession interface {
	Open(context.Context, storage.WindowsOpenRequest, storage.WindowsActionID) (WindowsResult, error)
	QueryAction(context.Context, storage.WindowsActionID) (WindowsResult, error)
	CancelAction(context.Context, storage.WindowsActionID) (WindowsResult, error)
	Renew(context.Context) (storage.FileSessionStatus, error)
	Status(context.Context) (storage.FileSessionStatus, error)
	Close(context.Context) error
}

// WindowsResult keeps the native reference separate from the byte-serving
// reference returned by storage. A receipt never substitutes a path for either.
type WindowsResult struct {
	storage.WindowsActionResult
	Reference WindowsFile
}

// WindowsIO describes the logical operation, not the whole object uploaded by
// an implementation of a range write. Truncate checks the changed EOF interval.
type WindowsIO struct {
	Offset   int64
	Length   int64
	Write    bool
	Truncate bool
	Size     int64
}

type fileIOKey struct{}

// WithFileIO carries the logical byte interval to native revision capture and
// publication. It supplies no authority: the native reference determines actor.
func WithFileIO(ctx context.Context, operation WindowsIO) context.Context {
	return context.WithValue(ctx, fileIOKey{}, operation)
}

func FileIOFromContext(ctx context.Context) (WindowsIO, bool) {
	operation, ok := ctx.Value(fileIOKey{}).(WindowsIO)
	return operation, ok
}

type WindowsFile interface {
	Reference() string
	Stat(context.Context) (storage.WindowsAttr, error)
	Capture(context.Context, WindowsIO) (FileState, error)
	Reserve(context.Context, int64) (Key, error)
	// BeginContent admits one fingerprinted action before staging any bytes.
	// Fresh false means the returned receipt already owns this action identity.
	BeginContent(context.Context, storage.WindowsActionID, [32]byte, WindowsIO) (result WindowsResult, fresh bool, err error)
	CommitContent(context.Context, storage.WindowsActionID, uint64, Object) (WindowsResult, error)
	RejectContent(context.Context, storage.WindowsActionID, error) (WindowsResult, error)
	SetAttr(context.Context, storage.WindowsAttrChange, storage.WindowsActionID) (WindowsResult, error)
	ReadLink(context.Context) (storage.WindowsSymlinkInfo, error)
	SetLink(context.Context, string, storage.WindowsActionID) (WindowsResult, error)
	ListBounded(context.Context, *storage.WindowsListResult) error
	Rename(context.Context, storage.WindowsRenameRequest, storage.WindowsActionID) (WindowsResult, error)
	SetDeletePending(context.Context, bool, storage.WindowsActionID) (WindowsResult, error)
	LockBatch(context.Context, storage.WindowsLockBatch, storage.WindowsActionID) (WindowsResult, error)
	Sync(context.Context) error
	Close(context.Context, storage.WindowsActionID) (WindowsResult, error)
}
