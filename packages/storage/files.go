package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// FileStorage retains live file objects independently of their names. A missing
// implementation is EOPNOTSUPP; callers must not emulate it by reopening paths.
// Opening files acquires neither advisory locks nor strong S/X grants.
type FileStorage interface {
	BoundedStorage
	// CheckFileStorage verifies every dependency can retain identity, enforce
	// lifetime fences, and bound resources before serving file operations.
	CheckFileStorage() error
	NewFileSession(context.Context, FileSessionOptions) (FileSession, error)
}

// FileSession owns file references, advisory owners, and action history until
// explicit close or its confirmed finite lifetime expires. Retirement fences
// new operations and final publication before admitted operations are drained.
// Sessions and their files must support concurrent calls.
type FileSession interface {
	// OpenFile resolves the name, checks ExpectedID, creates or truncates when
	// requested, and retains the resulting object as one ordered operation.
	// Identity mismatch is ESTALE. Existing directories and links are EISDIR
	// and ELOOP respectively. Exclusive creation of any existing node is EEXIST.
	// Nonexclusive creation opens an existing regular file, including a
	// concurrent creator's winner, and applies requested truncation while
	// preserving its mode. Creation mode applies only to a newly created file.
	OpenFile(context.Context, string, FileOpenOptions) (File, error)

	// OpenNode opens the existing regular file with id without resolving a
	// path. Create and Exclusive are invalid; a missing identity is ESTALE.
	OpenNode(context.Context, uint64, FileOpenOptions) (File, error)

	// StatNode and SetNodeAttr address an identity, including directory and
	// symbolic-link identities. They never substitute a node at a former name.
	StatNode(context.Context, uint64) (Attr, error)
	SetNodeAttr(context.Context, uint64, AttrChange) (Attr, error)

	// Renew confirms a new finite lifetime. A failed call does not extend the
	// last confirmed lifetime. Status observes it without extending it.
	Renew(context.Context) (FileSessionStatus, error)
	Status(context.Context) (FileSessionStatus, error)

	// Close is idempotent, retires all owned references and advisory state,
	// and drains admitted operations. Unknown cleanup retains its charge and
	// ownership and returns an error; it must not report reclaimed resources.
	Close(context.Context) error
}

// File addresses the same live regular-file object through rename, replacement,
// and unlink. Each successful read captures one complete current revision;
// multiple calls may observe different completed revisions. Expired sessions
// and retired authority identities are ESTALE. Closed references and operations
// lacking requested read/write access are EBADF. Unknown outcomes are EIO.
type File interface {
	Stat(context.Context) (Attr, error)

	// ReadAt returns up to length bytes at offset with attributes from the
	// same content revision. At or beyond EOF it returns empty Data and the
	// captured Attr without io.EOF. For positive length before captured EOF,
	// success returns at least one byte; a positive short read is permitted.
	// Negative offset or length is EINVAL.
	ReadAt(ctx context.Context, offset int64, length int) (FileRead, error)

	// WriteAt patches only the requested range against the object's current
	// contents. Truncate changes only its length. Both require write access
	// and acknowledge the server's publication before returning success.
	WriteAt(ctx context.Context, offset int64, data []byte) (Attr, error)
	Truncate(context.Context, int64) (Attr, error)

	// SetAttr permits mode and time changes on read-only descriptors, subject
	// to namespace permission policy. Sync confirms reference health and any
	// durability barrier the backend requires; no dirty content awaits Close.
	SetAttr(context.Context, AttrChange) (Attr, error)
	Sync(context.Context) error

	// GetLock reports one actual conflicting holder. A false Found is a
	// confirmed absence of conflicts, never a substitute for an unknown result.
	GetLock(context.Context, LockOwner, FileLock) (LockConflict, error)

	// SetLock is replayable by request identity and returns immediately with a
	// grant, a pending attempt, or a known rejection. Reusing an identity for
	// another request is EINVAL. POSIX Shared/Exclusive requires matching open
	// access; Flock Exclusive is permitted on read-only files. Conflicts are
	// EAGAIN, lock admission exhaustion is ENOLCK, detected deadlock is EDEADLK.
	SetLock(context.Context, LockOwner, FileLock, LockRequestID) (LockAttempt, error)

	// QueryLock and CancelLock reconcile an admitted request. LockCancelled
	// or LockReleased proves no acquisition from that request survives. If a
	// grant wins cancellation, LockGranted reports the retained acquisition.
	// EINTR is valid only after a result proves no surviving grant. Unknown
	// outcomes are EIO and fence affected I/O; cancellation alone is no proof.
	QueryLock(context.Context, LockOwner, LockRequestID) (LockAttempt, error)
	CancelLock(context.Context, LockOwner, LockRequestID) (LockAttempt, error)

	// DropLocks removes this owner's ranges and pending attempts in one family
	// on this object, including the sticky failure caused by lost continuity.
	DropLocks(context.Context, LockOwner, LockFamily) error

	// Close is idempotent. It retires this reference before draining admitted
	// operations. POSIX close-owner cleanup is explicit through DropLocks;
	// closing one reference must not infer an unrelated owner's identity.
	Close(context.Context) error
}

// FileOpenOptions selects access and atomic creation behavior. ExpectedID zero
// accepts the node resolved by path; nonzero requires that exact identity.
// Mode is the initial creation mode and never changes an existing file's mode.
type FileOpenOptions struct {
	ExpectedID                  uint64
	Read, Write                 bool
	Create, Exclusive, Truncate bool
	Mode                        fs.FileMode
}

// Check rejects invalid access, creation, and mode combinations with EINVAL.
func (o FileOpenOptions) Check() error {
	if !o.Read && !o.Write {
		return fmt.Errorf("file open requires read or write access: %w", syscall.EINVAL)
	}
	if o.Exclusive && !o.Create {
		return fmt.Errorf("exclusive file open requires creation: %w", syscall.EINVAL)
	}
	if o.Truncate && !o.Write {
		return fmt.Errorf("file truncation requires write access: %w", syscall.EINVAL)
	}
	if o.Mode&^SettableMode != 0 {
		return fmt.Errorf("file creation mode sets unsupported bits: %w", syscall.EINVAL)
	}
	return nil
}

// CheckNode adds the identity-open restrictions to Check. ExpectedID may be
// omitted or equal id; an inconsistent requested identity is EINVAL.
func (o FileOpenOptions) CheckNode(id uint64) error {
	if err := o.Check(); err != nil {
		return err
	}
	if id == 0 || o.ExpectedID != 0 && o.ExpectedID != id {
		return fmt.Errorf("file open requires one nonzero node identity: %w", syscall.EINVAL)
	}
	if o.Create || o.Exclusive {
		return fmt.Errorf("opening a node identity cannot create a file: %w", syscall.EINVAL)
	}
	return nil
}

// FileRead owns the requested bytes and attributes captured from the same
// content revision. Attr.Size supplies the corresponding EOF boundary.
type FileRead struct {
	Attr Attr
	Data []byte
}

const DefaultFileMaxSize int64 = 1 << 30

// FileSessionOptions bounds one session. The zero value is invalid; callers
// can explicitly select DefaultFileSessionOptions and replace individual limits.
// Namespace-wide limits additionally bound the aggregate across all sessions.
type FileSessionOptions struct {
	Lease   time.Duration
	History time.Duration
	// MaxFileSize is checked against each captured content revision before
	// materialization or staging. Truncate to zero discards the old body
	// without reading it and is permitted even when that body exceeds the bound.
	MaxFileSize     int64
	MaxFiles        int
	MaxOperations   int
	MaxWaiters      int
	MaxLockOwners   int
	MaxLockRanges   int
	MaxPendingLocks int
	MaxLockActions  int
}

func DefaultFileSessionOptions() FileSessionOptions {
	return FileSessionOptions{
		Lease: 30 * time.Second, History: time.Minute,
		MaxFileSize: DefaultFileMaxSize,
		MaxFiles:    4096, MaxOperations: 64, MaxWaiters: 256,
		MaxLockOwners: 4096, MaxLockRanges: 65536,
		MaxPendingLocks: 1024, MaxLockActions: 16384,
	}
}

// Check rejects invalid limits before any session-owned resource is admitted.
func (o FileSessionOptions) Check() error {
	if o.Lease <= 0 || o.History <= 0 {
		return fmt.Errorf("file session lease and history must be positive: %w", syscall.EINVAL)
	}
	if o.MaxFileSize <= 0 || o.MaxFiles <= 0 || o.MaxOperations <= 0 || o.MaxWaiters < 0 ||
		o.MaxLockOwners <= 0 || o.MaxLockRanges <= 0 ||
		o.MaxPendingLocks <= 0 || o.MaxLockActions <= 0 {
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
// ActionEpoch identifies the current admission window for random lock requests.
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

// LockOwner is an opaque kernel owner scoped by its FileSession. Zero is a
// valid opaque value; a PID or another session's value confers no ownership.
type LockOwner uint64

type LockFamily uint8

const (
	Flock LockFamily = iota + 1
	POSIX
)

type LockType uint8

const (
	Unlock LockType = iota + 1
	Shared
	Exclusive
)

// FileLock uses inclusive byte ranges through MaxInt64, including future EOF.
// Flock always uses the whole file. The families have independent conflict
// domains and neither restricts nonparticipating I/O or named mutations.
// PID is diagnostic only. Wait enrolls a pending acquisition without blocking
// the SetLock call; the session's finite lifetime bounds retained pending work.
type FileLock struct {
	Family     LockFamily
	Type       LockType
	Start, End uint64
	PID        uint32
	Wait       bool
}

// Check rejects invalid families, modes, and ranges with EINVAL. Unlock never
// waits and Flock's range must cover the entire file.
func (l FileLock) Check() error {
	if l.Family != Flock && l.Family != POSIX {
		return fmt.Errorf("unknown advisory lock family: %w", syscall.EINVAL)
	}
	if l.Type != Unlock && l.Type != Shared && l.Type != Exclusive {
		return fmt.Errorf("unknown advisory lock type: %w", syscall.EINVAL)
	}
	if l.Start > l.End || l.End > math.MaxInt64 {
		return fmt.Errorf("advisory lock range must be within 0 through MaxInt64: %w", syscall.EINVAL)
	}
	if l.Family == Flock && (l.Start != 0 || l.End != math.MaxInt64) {
		return fmt.Errorf("flock requires the entire file range: %w", syscall.EINVAL)
	}
	if l.Type == Unlock && l.Wait {
		return fmt.Errorf("advisory unlock cannot wait: %w", syscall.EINVAL)
	}
	return nil
}

// LockRequestID combines the server's current action epoch with a random nonce.
// An unknown request in a retired epoch is ESTALE, never a new acquisition.
// Current-epoch receipts cannot be evicted to make admission room; terminal
// receipts remain for at least History after completion and until epoch retirement.
type LockRequestID string

// NewLockRequestID creates one action identity for the current server-issued
// epoch. Retries must retain the returned identity and the original request.
func NewLockRequestID(epoch uint64) (LockRequestID, error) {
	if epoch == 0 {
		return "", fmt.Errorf("lock request requires a nonzero action epoch: %w", syscall.EINVAL)
	}
	var nonce [16]byte
	rand.Read(nonce[:])
	return LockRequestID(strconv.FormatUint(epoch, 10) + ":" + hex.EncodeToString(nonce[:])), nil
}

// Epoch validates the bounded canonical action identity and returns its epoch.
func (r LockRequestID) Epoch() (uint64, error) {
	if len(r) > 53 {
		return 0, fmt.Errorf("lock request identity exceeds its length bound: %w", syscall.EINVAL)
	}
	epochText, nonce, ok := strings.Cut(string(r), ":")
	if !ok || len(epochText) == 0 || len(epochText) > 20 || len(nonce) != 32 || epochText[0] == '0' {
		return 0, fmt.Errorf("invalid lock request identity: %w", syscall.EINVAL)
	}
	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != epochText {
		return 0, fmt.Errorf("invalid lock request epoch: %w", syscall.EINVAL)
	}
	for _, c := range nonce {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return 0, fmt.Errorf("invalid lock request nonce: %w", syscall.EINVAL)
		}
	}
	return epoch, nil
}

type LockAttemptState uint8

const (
	LockPending LockAttemptState = iota + 1
	LockGranted
	LockRejected
	LockCancelled
	LockReleased
)

// LockAttempt reports a request's current reconciliation state. EverGranted
// preserves whether it acquired a lock before cancellation or release. A grant
// receipt does not freeze ranges against later requests by the same owner.
// Errno records a known rejection and remains available to QueryLock. An unknown
// outcome has an error and must not be represented as a known terminal state.
type LockAttempt struct {
	Request          LockRequestID
	State            LockAttemptState
	Lock             FileLock
	Conflict         LockConflict
	Errno            syscall.Errno
	EverGranted      bool
	HistoryRemaining time.Duration
}

// LockConflict identifies one overlapping incompatible range. Its owner is
// diagnostic and conveys no authority; Lock.PID is zero when unavailable.
type LockConflict struct {
	Found bool
	Owner LockOwner
	Lock  FileLock
}
