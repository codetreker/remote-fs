package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	// lifetime fences, and bound resources before serving file operations. Returned
	// attributes honor CheckAttrResultBudget before payload loading and any
	// mutation or retention that produces their captured result.
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
	// preserving its metadata. InitialMetadata applies only to a newly created file.
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

	// SetAttr permits common time changes on read-only descriptors, subject
	// to volume permission policy. Sync confirms reference health and any
	// durability barrier the backend requires; no dirty content awaits Close.
	SetAttr(context.Context, AttrChange) (Attr, error)
	Sync(context.Context) error

	// Close is idempotent. It retires this reference before draining admitted
	// operations. Owner cleanup is explicit through UseOwners; closing one
	// reference must not infer an unrelated owner's identity.
	Close(context.Context) error
}

// OpenAccess describes the access and atomic creation intent of a file open.
// Opening a retained node cannot carry Create or Exclusive. Validation belongs
// to FileOpenOptions, which also supplies initial metadata and expected identity.
type OpenAccess struct {
	Read      bool
	Write     bool
	Create    bool
	Truncate  bool
	Exclusive bool
}

// FileOpenOptions selects access and atomic creation behavior. ExpectedID zero
// accepts the node resolved by path; nonzero requires that exact identity.
// InitialMetadata applies only when this operation creates a new file.
type FileOpenOptions struct {
	OpenAccess
	ExpectedID      uint64
	InitialMetadata map[string][]byte `json:",omitempty"`
	Use             UseClaim
}

// Check rejects invalid access, creation and initial metadata before admission.
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
	if err := o.Use.Check(); err != nil {
		return err
	}
	if !o.Create && len(o.InitialMetadata) != 0 {
		return fmt.Errorf("initial metadata requires file creation: %w", syscall.EINVAL)
	}
	if err := CheckInitialMetadata(o.InitialMetadata); err != nil {
		return err
	}
	return nil
}

// EffectiveUse returns the explicit claim plus the data uses implied by the
// requested read and write access. Callers cannot omit those uses to bypass a
// conflicting Deny claim; additional neutral uses remain explicit.
func (o FileOpenOptions) EffectiveUse() UseClaim {
	claim := o.Use
	if o.Read {
		claim.Uses |= ReadData
	}
	if o.Write {
		claim.Uses |= WriteData
	}
	return claim
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
// Volume-wide limits additionally bound the aggregate across all sessions.
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
