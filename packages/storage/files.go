package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// FileStorage owns one authority for references, claims and finite action history.
type FileStorage interface {
	BoundedStorage
	CheckFileStorage() error
	FileState(context.Context) (FileVolumeState, error)
	NewFileSession(context.Context, FileSessionOptions) (FileSession, FileSessionStatus, error)
}

type FileVolumeState struct {
	VolumeIdentity string
	RootID         uint64
	MaxEventBytes  int64
}

// NewFileSession confirms initial status with ownership; no first Status call is
// required to learn the cleanup action epoch.
//
// Reference resolves an existing session-owned ID without opening a path or
// adding a pin. QueryAction preserves the recorded receipt and original error.
// A terminal reference cannot be resurrected by replaying its retain action.
// Retention receipts include a same-commit location only when the request or
// its target supplies Witness. They include a symbolic link's bounded target.
// NodeID remains immutable after retirement.
type FileSession interface {
	Retain(context.Context, RetainRequest, FileActionID) (FileActionReceipt, error)
	RetainAt(context.Context, RetainAtRequest, FileActionID) (FileActionReceipt, error)
	CreateAndRetainAt(context.Context, CreateAndRetainRequest, FileActionID) (FileActionReceipt, error)
	ResetAndRetainAt(context.Context, ResetAndRetainRequest, FileActionID) (FileActionReceipt, error)
	ReplaceAndRetainAt(context.Context, CreateAndRetainRequest, FileActionID) (FileActionReceipt, error)
	Reference(context.Context, FileReferenceID) (File, error)
	StatNode(context.Context, uint64, ObservationOptions) (FileObservation, error)
	SetNodeAttr(context.Context, uint64, AttrChange, FileActionID) (FileActionReceipt, error)
	QueryAction(context.Context, FileActionID) (FileActionReceipt, error)
	CancelAction(context.Context, FileActionID) (FileActionReceipt, error)
	RetireRangeOwner(context.Context, RangeOwnerID, FileActionID) (FileActionReceipt, error)
	Renew(context.Context) (FileSessionStatus, error)
	Status(context.Context) (FileSessionStatus, error)
	Close(context.Context, FileActionID) (FileActionReceipt, error)
}

// File retains a regular file, directory or symbolic link. Every read and final
// publication checks the same authority's claims, ranges and lifetime fence.
type File interface {
	Reference() FileReferenceID
	NodeID() uint64
	Stat(context.Context, ObservationOptions) (FileObservation, error)
	CheckObservation(context.Context, ObservationCondition) (FileObservation, error)
	ReadAt(context.Context, FileReadRequest) (FileRead, error)
	WriteAt(context.Context, FileWriteRequest, FileActionID) (FileActionReceipt, error)
	Truncate(context.Context, FileTruncateRequest, FileActionID) (FileActionReceipt, error)
	SetAttr(context.Context, AttrChange, FileActionID) (FileActionReceipt, error)
	SetKind(context.Context, SetKindRequest, FileActionID) (FileActionReceipt, error)
	LookupAt(context.Context, []byte) (EntryLookup, error)
	ListAt(context.Context, DirectoryPageRequest) (DirectoryPage, error)
	Rename(context.Context, RenameRequest, FileActionID) (FileActionReceipt, error)
	ReplaceClaim(context.Context, AccessClaim, FileActionID) (FileActionReceipt, error)
	PrepareRemoval(context.Context, PrepareRemovalRequest, FileActionID) (FileActionReceipt, error)
	CancelPrepared(context.Context, RemovalIntentID, FileActionID) (FileActionReceipt, error)
	DrainEntry(context.Context, DrainEntryRequest, FileActionID) (FileActionReceipt, error)
	CancelDrain(context.Context, CancelDrainRequest, FileActionID) (FileActionReceipt, error)
	RangeSnapshot(context.Context, RangeOwnerID, RangeScope) (RangeSnapshot, error)
	RetireRangeOwner(context.Context, RangeOwnerID, RangeScope, FileActionID) (FileActionReceipt, error)
	ReplaceRanges(context.Context, RangeReplaceRequest, FileActionID) (FileActionReceipt, error)
	// A fresh WaitRanges blocks until its guard changes; it never grants ranges.
	// Replaying an admitted pending action may return Pending immediately. Recovery
	// must settle that action before planning a replacement, without a polling loop.
	WaitRanges(context.Context, RangeWaitRequest, FileActionID) (FileActionReceipt, error)
	Sync(context.Context) error
	Close(context.Context, FileActionID) (FileActionReceipt, error)
}

type FileReferenceID uint64

type ObservationOptions struct{ IncludeLocation, IncludeLinkTarget bool }

type FileObservation struct {
	Removal    RemovalStatus
	Attr       Attr
	Location   *EntryLocation
	LinkTarget []byte
}

type FileRead struct {
	Attr Attr
	Data []byte
}

type FileReadRequest struct {
	Offset int64
	Length int
	Owner  *RangeOwnerID
}
type FileWriteRequest struct {
	ExpectedSize *int64
	Offset       int64
	Data         []byte
	Owner        *RangeOwnerID
}
type FileTruncateRequest struct {
	Size  int64
	Owner *RangeOwnerID
}

// ExpectedSize, when supplied, is checked against the captured and final
// publication size; a mismatch refuses the operation without changing bytes.
//
// FileIO attributes native admission to an optional range owner. A nil Owner
// is anonymous; zero is a valid owner identity when explicitly present.
type FileIO struct {
	ExpectedSize   *int64
	Truncate       bool
	Size           int64
	Offset, Length int64
	Write          bool
	Owner          *RangeOwnerID
}

const DefaultFileMaxSize int64 = 1 << 30

// FileSessionOptions bounds one session. The zero value is invalid; callers
// can explicitly select DefaultFileSessionOptions and replace individual limits.
// Volume-wide limits additionally bound the aggregate across all sessions.
type FileSessionOptions struct {
	Lease   time.Duration
	History time.Duration
	// MaxFileSize is checked against each captured content revision before
	// materialization or staging. Truncate to zero discards the old body
	// without reading it and is permitted even when that body exceeds the bound.
	MaxFileSize       int64
	MaxFiles          int
	MaxOperations     int
	MaxWaiters        int
	MaxRangeOwners    int
	MaxRanges         int
	MaxPendingActions int
	MaxActions        int
}

func DefaultFileSessionOptions() FileSessionOptions {
	return FileSessionOptions{
		Lease: 30 * time.Second, History: time.Minute,
		MaxFileSize: DefaultFileMaxSize,
		MaxFiles:    4096, MaxOperations: 64, MaxWaiters: 256,
		MaxRangeOwners: 4096, MaxRanges: 65536,
		MaxPendingActions: 1024, MaxActions: 16384,
	}
}

// Check rejects invalid limits before any session-owned resource is admitted.
func (o FileSessionOptions) Check() error {
	if o.Lease <= 0 || o.History <= 0 {
		return fmt.Errorf("file session lease and history must be positive: %w", syscall.EINVAL)
	}
	if o.MaxFileSize <= 0 || o.MaxFiles <= 0 || o.MaxOperations <= 0 || o.MaxWaiters < 0 ||
		o.MaxRangeOwners <= 0 || o.MaxRanges <= 0 ||
		o.MaxPendingActions <= 0 || o.MaxActions <= 0 {
		return fmt.Errorf("file session resource limits must be positive, except zero waiters: %w", syscall.EINVAL)
	}
	return nil
}

// FileSessionStatus reports confirmed authority state. Remaining and
// HistoryRemaining are conservative durations at return: transports subtract
// elapsed request time from the authority's answer before returning them.
// Receiving an old reply must never restart either lifetime. Revision orders
// renewals; Fenced reports lost continuity while cleanup remains possible.
//
// ActionEpoch identifies the current admission window for actions.
// A request from a retired epoch is queried only from retained history and must
// never be executed again. Status observes the epoch; Renew may advance it.
type FileSessionStatus struct {
	Epoch            string
	Remaining        time.Duration
	Revision         uint64
	ActionEpoch      uint64
	HistoryRemaining time.Duration
	Retired          bool
	Fenced           bool
}

// FileActionID combines the server's current action epoch with a random nonce.
// An unknown request in a retired epoch is ESTALE, never a new operation.
// Current-epoch receipts cannot be evicted to make admission room; terminal
// receipts remain for at least History after completion and until epoch retirement.
type FileActionID string

// NewFileActionID creates one action identity for the current server-issued
// epoch. Retries must retain the returned identity and the original request.
func NewFileActionID(epoch uint64) (FileActionID, error) {
	if epoch == 0 {
		return "", fmt.Errorf("file action requires a nonzero action epoch: %w", syscall.EINVAL)
	}
	var nonce [16]byte
	rand.Read(nonce[:])
	return FileActionID(strconv.FormatUint(epoch, 10) + ":" + hex.EncodeToString(nonce[:])), nil
}

// Epoch validates the bounded canonical action identity and returns its epoch.
func (r FileActionID) Epoch() (uint64, error) {
	if len(r) > 53 {
		return 0, fmt.Errorf("file action identity exceeds its length bound: %w", syscall.EINVAL)
	}
	epochText, nonce, ok := strings.Cut(string(r), ":")
	if !ok || len(epochText) == 0 || len(epochText) > 20 || len(nonce) != 32 || epochText[0] == '0' {
		return 0, fmt.Errorf("invalid file action identity: %w", syscall.EINVAL)
	}
	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != epochText {
		return 0, fmt.Errorf("invalid file action epoch: %w", syscall.EINVAL)
	}
	for _, c := range nonce {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return 0, fmt.Errorf("invalid file action nonce: %w", syscall.EINVAL)
		}
	}
	return epoch, nil
}

func (r FileReadRequest) Check() error {
	if r.Offset < 0 || r.Length < 0 || int64(r.Length) > math.MaxInt64-r.Offset {
		return syscall.EINVAL
	}
	return nil
}

func (r FileWriteRequest) Check() error {
	if r.ExpectedSize != nil && *r.ExpectedSize < 0 {
		return syscall.EINVAL
	}
	if r.Offset < 0 || int64(len(r.Data)) > math.MaxInt64-r.Offset {
		return syscall.EINVAL
	}
	return nil
}

func (r FileTruncateRequest) Check() error {
	if r.Size < 0 {
		return syscall.EINVAL
	}
	return nil
}

func (r FileIO) Check() error {
	if r.ExpectedSize != nil && *r.ExpectedSize < 0 {
		return syscall.EINVAL
	}
	if r.Truncate {
		if !r.Write || r.Size < 0 || r.Offset != 0 || r.Length != 0 {
			return syscall.EINVAL
		}
		return nil
	}
	if r.Size != 0 || r.Offset < 0 || r.Length < 0 || r.Length > math.MaxInt64-r.Offset {
		return syscall.EINVAL
	}
	return nil
}

func (o FileObservation) Check(options ObservationOptions) error {
	if err := o.Removal.Check(); err != nil {
		return err
	}
	if err := o.Attr.Check(); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	if options.IncludeLocation {
		if o.Location == nil {
			return syscall.EIO
		}
		if err := o.Location.Check(); err != nil {
			return errors.Join(syscall.EIO, err)
		}
		if o.Location.NodeID != o.Attr.ID {
			return syscall.EIO
		}
	} else if o.Location != nil {
		return syscall.EIO
	}
	if options.IncludeLinkTarget {
		if err := checkLinkData(o.Attr.Kind, o.LinkTarget); err != nil {
			return errors.Join(syscall.EIO, err)
		}
		if o.Attr.Kind == NodeSymlink && int64(len(o.LinkTarget)) != o.Attr.Size {
			return syscall.EIO
		}
	} else if len(o.LinkTarget) != 0 {
		return syscall.EIO
	}
	return nil
}

func (o FileObservation) Clone() FileObservation {
	o.Attr = o.Attr.Clone()
	if o.Location != nil {
		value := o.Location.Clone()
		o.Location = &value
	}
	o.LinkTarget = bytes.Clone(o.LinkTarget)
	return o
}
