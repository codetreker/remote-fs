package storage

import (
	"context"
	"syscall"
	"time"
)

// WindowsStorage supplies authority-wide Windows access rules. Capability checks
// include retained directories, persistent naming policy and complete bounded
// change observation; path-based substitutes cannot satisfy this contract.
type WindowsStorage interface {
	BoundedStorage
	CheckWindowsStorage() error
	WindowsState(context.Context) (WindowsState, error)
	EnableWindows(context.Context, WindowsActionID) (WindowsActivation, error)
	QueryWindowsActivation(context.Context, WindowsActionID) (WindowsActivation, error)
	NewWindowsSession(context.Context, FileSessionOptions) (WindowsSession, error)
}

// WindowsState is an authoritative observation, not a cached publication hint.
// ActionEpoch scopes activation receipts. Enabled persists across share removal.
type WindowsState struct {
	Enabled     bool
	ActionEpoch uint64
	// MaxEventBytes bounds the sum of raw variable-length fields in one change:
	// current/from names, content key and canonical notification payload. The
	// authority enforces it before publication; it is not an encoded frame bound.
	MaxEventBytes int64
	// VolumeIdentity is the durable authority identity and is stable across
	// sessions, reopen and endpoint changes. It is not a display/share name.
	VolumeIdentity string
	// VolumeSerial is a stable protocol label derived by the authority. It is
	// not an identity or authorization key; protocols may report its low 32 bits.
	VolumeSerial uint64
}

type WindowsActivation struct {
	Action           WindowsActionID
	State            WindowsActionState
	Enabled          bool
	Errno            syscall.Errno
	HistoryRemaining time.Duration
}

// WindowsSession fences every reference, access reservation and range lock as
// one lifetime. Methods support concurrent calls. Unknown admission or mutation
// outcomes are reconciled under the original action identity, never replayed as
// a new action. Expired action epochs cannot admit unknown requests.
type WindowsSession interface {
	Open(context.Context, WindowsOpenRequest, WindowsActionID) (WindowsOpenResult, error)
	// QueryAction returns a known receipt together with the original action error
	// when that action failed. Success and pending receipts have nil errors.
	// Failure of the query itself must not fabricate a historical receipt.
	QueryAction(context.Context, WindowsActionID) (WindowsActionResult, error)
	// CancelAction preserves an already determined result and its original error;
	// cancellation cannot erase partial effects or a stopped-on-symlink result.
	CancelAction(context.Context, WindowsActionID) (WindowsActionResult, error)
	Renew(context.Context) (FileSessionStatus, error)
	Status(context.Context) (FileSessionStatus, error)
	Close(context.Context) error
}

// WindowsFile retains one object independently of its names. Directories and
// metadata-only references are valid. Every operation checks the granted access,
// current sharing rules, range locks and session fence at its ordered effect.
// Mutations retain exact action receipts, including partially applied lock batches.
type WindowsFile interface {
	Reference() string
	Stat(context.Context) (WindowsAttr, error)
	ReadAt(context.Context, int64, int) (FileRead, error)
	WriteAt(context.Context, int64, []byte, WindowsActionID) (WindowsActionResult, error)
	Truncate(context.Context, int64, WindowsActionID) (WindowsActionResult, error)
	SetAttr(context.Context, WindowsAttrChange, WindowsActionID) (WindowsActionResult, error)
	ListBounded(context.Context, *WindowsListResult) error
	// ReadLink reports the retained symbolic link's authoritative target without
	// following it. Non-links return WindowsNotReparsePoint with EINVAL.
	ReadLink(context.Context) (WindowsSymlinkInfo, error)
	// SetLink atomically converts an empty, exclusively referenced regular file
	// or directory to a symbolic link while retaining its identity. A directory
	// link retains the WindowsDOSDirectory attribute. It requires write-data
	// access, native volume confinement and the usual final publication checks.
	// Conflicting references return WindowsSharingViolation. Other reparse types
	// are not represented by this interface.
	SetLink(context.Context, string, WindowsActionID) (WindowsActionResult, error)
	Rename(context.Context, WindowsRenameRequest, WindowsActionID) (WindowsActionResult, error)
	SetDeletePending(context.Context, bool, WindowsActionID) (WindowsActionResult, error)
	LockBatch(context.Context, WindowsLockBatch, WindowsActionID) (WindowsActionResult, error)
	Sync(context.Context) error
	Close(context.Context, WindowsActionID) (WindowsActionResult, error)
}

// WindowsActionID reuses the bounded epoch-and-nonce identity format used by
// retained file actions. The owning Windows session supplies the epoch.
type WindowsActionID = LockRequestID

type WindowsActionState uint8

const (
	WindowsActionPending WindowsActionState = iota + 1
	WindowsActionCompleted
	WindowsActionRejected
	WindowsActionCancelled
)

// WindowsActionResult preserves known effects even when Errno is nonzero.
// Applied counts completed batch elements; State alone does not mean rollback.
// File is returned for a successfully opened reference, including on replay.
// A nonnil returned error does not invalidate a known receipt: callers retain
// its Applied count and Symlink observation while handling the original failure.
type WindowsActionResult struct {
	Action           WindowsActionID
	State            WindowsActionState
	Attr             WindowsAttr
	File             WindowsFile
	CreateAction     WindowsCreateAction
	Errno            syscall.Errno
	Failure          WindowsFailure
	Symlink          *WindowsSymlinkInfo
	Applied          int
	HistoryRemaining time.Duration
}

type WindowsOpenResult struct {
	File         WindowsFile
	Attr         WindowsAttr
	CreateAction WindowsCreateAction
}

type WindowsCreateAction uint8

const (
	WindowsOpened WindowsCreateAction = iota + 1
	WindowsCreated
	WindowsOverwritten
	WindowsSuperseded
)

// WindowsFailure distinguishes protocol-relevant failures which share an errno.
// The error returned by an operation remains the authoritative failure result.
type WindowsFailure string

const (
	WindowsSharingViolation WindowsFailure = "sharing-violation"
	WindowsLockConflict     WindowsFailure = "lock-conflict"
	WindowsDeletePending    WindowsFailure = "delete-pending"
	WindowsRangeNotLocked   WindowsFailure = "range-not-locked"
	WindowsNotReparsePoint  WindowsFailure = "not-reparse-point"
)
