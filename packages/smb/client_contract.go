package smb

import (
	"context"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// These private interfaces describe local Windows interpretation. The backing
// implementation uses one generic FileSession and never sends these values over HTTP.
type windowsBackend interface {
	Check() error
	Space(context.Context) (storage.Space, error)
	State(context.Context) (windowsState, error)
	NewSession(context.Context, storage.FileSessionOptions) (windowsSession, error)
}
type windowsState struct {
	VolumeIdentity string
	VolumeSerial   uint64
	RootID         uint64
	MaxEventBytes  int64
}
type windowsSession interface {
	Open(context.Context, windowsOpenRequest, windowsActionID) (windowsOpenResult, error)
	QueryAction(context.Context, windowsActionID) (windowsActionResult, error)
	CancelAction(context.Context, windowsActionID) (windowsActionResult, error)
	Status(context.Context) (storage.FileSessionStatus, error)
	Renew(context.Context) (storage.FileSessionStatus, error)
	Close(context.Context) error
}
type windowsFile interface {
	Reference() storage.FileReferenceID
	Stat(context.Context) (windowsAttr, error)
	ObserveName(context.Context) (windowsAttr, error)
	ValidateNotificationLocation(context.Context, storage.EntryLocation) error
	ReadAt(context.Context, int64, int) (storage.FileRead, error)
	WriteAt(context.Context, int64, []byte, windowsActionID) (windowsActionResult, error)
	Truncate(context.Context, int64, windowsActionID) (windowsActionResult, error)
	SetAttr(context.Context, windowsAttrChange, windowsActionID) (windowsActionResult, error)
	ListBounded(context.Context, *windowsListResult) error
	Rename(context.Context, windowsRenameRequest, windowsActionID) (windowsActionResult, error)
	SetDeletePending(context.Context, bool, windowsActionID) (windowsActionResult, error)
	ReadLink(context.Context) (windowsSymlinkInfo, error)
	SetLink(context.Context, string, windowsActionID) (windowsActionResult, error)
	LockBatch(context.Context, windowsLockBatch, windowsActionID) (windowsActionResult, error)
	Sync(context.Context) error
	Close(context.Context, windowsActionID) (windowsActionResult, error)
}
type windowsActionID = storage.FileActionID
type windowsActionState uint8

const (
	windowsActionPending windowsActionState = iota + 1
	windowsActionCompleted
	windowsActionRejected
	windowsActionCancelled
)

type windowsCreateAction uint8

const (
	windowsOpened windowsCreateAction = iota + 1
	windowsCreated
	windowsOverwritten
	windowsSuperseded
)

type windowsActionResult struct {
	notAdmitted      bool
	GrantedAccess    windowsAccess
	proof            *nameProof
	Action           windowsActionID
	State            windowsActionState
	Attr             windowsAttr
	File             windowsFile
	CreateAction     windowsCreateAction
	Errno            syscall.Errno
	Failure          windowsFailure
	Symlink          *windowsSymlinkInfo
	Applied          int
	HistoryRemaining time.Duration
	Receipt          storage.FileActionReceipt
}
type windowsOpenResult struct {
	notAdmitted   bool
	GrantedAccess windowsAccess
	proof         *nameProof
	File          windowsFile
	Attr          windowsAttr
	CreateAction  windowsCreateAction
}

// nameProof owns the directory versions used for local comparison. A supplied
// proof is validated by the final generic operation rather than silently recaptured.
type nameProof struct {
	Target      storage.EntryTarget
	Location    storage.EntryLocation
	Observation storage.FileObservation
}
