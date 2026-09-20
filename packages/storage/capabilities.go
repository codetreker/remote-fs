package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"syscall"
	"unicode/utf8"
)

// AtomicFileOpener resolves one exact child and applies its open effect in the
// same authority operation. Callers must not emulate it with lookup followed by
// a path-based open because the parent or child may be replaced between calls.
type AtomicFileOpener interface {
	CheckAtomicFileOpen() error
	OpenAt(context.Context, ChildName, OpenAtOptions) (OpenResult, error)
}

// NamespaceAccess addresses exact byte names beneath directory identities. A
// present Scope must validate the exact live parent reference; implementations
// must not fall back to the bare NodeID when it is invalid.
type NamespaceAccess interface {
	CheckNamespaceAccess() error
	LookupAt(context.Context, ChildName) (Attr, error)
	MutateName(context.Context, NameCommand) (NameResult, error)
}

// NodeReference retains a node identity without granting file byte methods. It
// always exposes the exact live-reference scope and an authoritative state
// capture, so a caller that passed NodeReferences preflight cannot discover
// EOPNOTSUPP after opening a directory or metadata-only reference.
type NodeReference interface {
	ScopedReference
	ReferenceStateAccess
	Stat(context.Context) (Attr, error)
	SetAttr(context.Context, AttrChange) (Attr, error)
	Close(context.Context) error
}

// NodeReferences opens retained identities directly or as an exact child of a
// retained directory. Metadata-only and directory references do not masquerade
// as regular files.
type NodeReferences interface {
	CheckNodeReferences() error
	OpenNodeRef(context.Context, uint64, NodeRefOptions) (NodeOpenResult, error)
	OpenChildRef(context.Context, ChildName, NodeRefOptions) (NodeOpenResult, error)
}

// ReferenceStateAccess captures identity state and pending deletion together.
// Failure cannot be replaced by invented detached or pending values.
type ReferenceStateAccess interface {
	CheckReferenceState() error
	State(context.Context) (ReferenceState, error)
}

// FileActions queries the finite receipt history owned by a FileSession. A
// repeated action with the same ID and input returns its original method result;
// reusing an ID with different input is EINVAL. Unknown and retired receipts do
// not prove that an earlier request was unexecuted.
//
// Delete intents have durable identities independent of the finite action
// history. A new session may query their state after authority restart.
type FileActions interface {
	CheckFileActions() error
	QueryFileAction(context.Context, FileActionID) (FileActionReceipt, error)
	QueryDeleteIntent(context.Context, DeleteIntentID) (DeleteIntentStatus, error)
	AcknowledgeDeleteIntent(context.Context, AcknowledgeDeleteIntentCommand) error
}

// ScopedReference returns an opaque capability for one exact live File reference.
// The token carries no authority until the owning session validates it against the
// target node identity.
type ScopedReference interface {
	CheckScopedReference() error
	Scope(context.Context) (UseScope, error)
}

// MetadataAccess compares and replaces exactly one metadata namespace atomically.
// An empty expected version requires absence; an empty payload stores a present empty
// value. The authority assigns the returned version and advances ChangeTime.
type MetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, uint64, string, []byte, []byte) (OpaquePayload, error)
}

// ReferenceMetadataAccess applies the same metadata namespace operation through a
// retained File identity.
type ReferenceMetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, string, []byte, []byte) (OpaquePayload, error)
}

// UseOwners registers range-control owners against a validated live reference scope.
// Numeric owner values alone convey no authority.
type UseOwners interface {
	CheckUseOwners() error
	NewUseOwner(context.Context, uint64, UseScope, OwnerOptions) (UseOwner, error)
	RetireUseOwner(context.Context, UseOwner) error
}

// RangeControl keeps finite, replayable request receipts. A repeated request identity
// returns its retained outcome; changed intent is rejected. Cancellation does not prove
// that a concurrent grant failed, so callers reconcile the returned attempt.
type RangeControl interface {
	CheckRangeControl() error
	GetConflict(context.Context, UseOwner, RangeCommand) (RangeConflict, error)
	Apply(context.Context, UseOwner, []RangeCommand, LockRequestID) (RangeAttempt, error)
	Query(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Cancel(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Drop(context.Context, UseOwner, ConflictDomain) error
}

// DeleteIntent changes node-level pending deletion under native admission.
// Clearing requires the current generation and does not erase another
// reference's accepted close intent.
type DeleteIntent interface {
	CheckDeleteIntent() error
	SetPendingUnlink(context.Context, PendingUnlinkCommand) (ReferenceState, error)
	ClearPendingUnlink(context.Context, ClearPendingUnlinkCommand) (ReferenceState, error)
}

// ConditionalFileMutation compares explicit size and metadata conditions with
// the selected effect at final publication. Ordinary File writes retain their
// existing actual-order semantics and do not acquire an implicit condition.
type ConditionalFileMutation interface {
	CheckConditionalFileMutation() error
	MutateFile(context.Context, FileMutation) (Attr, error)
}

const MaxScopeBytes = 128

type UseScope struct{ Token string }

func (s UseScope) Check() error {
	if len(s.Token) == 0 || len(s.Token) > MaxScopeBytes || !utf8.ValidString(s.Token) {
		return syscall.EINVAL
	}
	for i := range s.Token {
		if s.Token[i] == 0 {
			return syscall.EINVAL
		}
	}
	return nil
}

type Uses uint8

const (
	ReadData Uses = 1 << iota
	WriteData
	ReadEntries
	DeleteName
)

const AllUses = ReadData | WriteData | ReadEntries | DeleteName

// UseClaim declares what one reference may do and what uses it denies to every
// other reference. Conflicts are symmetric: either claim's Deny set may reject
// the other claim's Uses set.
type UseClaim struct{ Uses, Deny Uses }

func (c UseClaim) Check() error {
	if (c.Uses|c.Deny)&^AllUses != 0 {
		return syscall.EINVAL
	}
	return nil
}

type UseOwner uint64
type OwnerDiagnostic uint64

type OwnerOptions struct {
	Lifetime OwnerLifetime
	// Group identifies a session-local deadlock participant. Zero keeps the
	// owner independent; it never shares claims or use exemptions.
	Group uint64
	// Diagnostic is an opaque caller-owned value reported for conflicts. It
	// conveys no authority and is never substituted with an internal owner ID.
	Diagnostic OwnerDiagnostic
}

type OwnerLifetime uint8

const (
	OwnerReference OwnerLifetime = iota + 1
	OwnerExplicit
)

type ExpectedChild uint8

const (
	Any ExpectedChild = iota + 1
	Absent
	SameNode
)

// ChildCondition states which child identity an operation may affect. SameNode
// requires NodeID; Any and Absent prohibit one.
type ChildCondition struct {
	State  ExpectedChild
	NodeID uint64
	// Empty tokens require namespace absence; nonempty tokens require exact
	// version equality.
	ExpectedMetadata map[string][]byte `json:",omitempty"`
}

// DirectoryTarget identifies a directory. Scope, when present, binds the
// operation to one exact live reference in addition to its durable NodeID.
type DirectoryTarget struct {
	NodeID uint64
	Scope  *UseScope `json:",omitempty"`
}

// ChildName is one raw, platform-neutral leaf beneath an identity-addressed
// parent. RawLeaf is not required to be UTF-8.
type ChildName struct {
	Parent  DirectoryTarget
	RawLeaf []byte
}

// RenameTarget separates the slot observed by the caller from the output leaf.
// The authority must reject an unobserved third occupant at OutputLeaf; it must
// never overwrite a node other than the one named by Expected.
type RenameTarget struct {
	Parent       DirectoryTarget
	ObservedLeaf []byte
	Expected     ChildCondition
	OutputLeaf   []byte
}

// TargetUse supplies an exact live reference for a node whose use claim must be
// considered by a namespace or deletion effect.
type TargetUse struct {
	NodeID uint64
	Scope  UseScope
}

type ExistingEffect uint8

const (
	Keep ExistingEffect = iota + 1
	ResetContent
	ReplaceNode
)

type OpenOutcome uint8

const (
	Opened OpenOutcome = iota + 1
	Created
	Reset
	Replaced
)

type InitialFields struct {
	LinkTarget []byte `json:",omitempty"`
	Attr       AttrChange
	Metadata   map[string][]byte `json:",omitempty"`
}

type InitialState struct {
	OnCreate  InitialFields
	OnReset   InitialFields
	OnReplace InitialFields
}

type OpenAtOptions struct {
	Read, Write       bool
	Create, Exclusive bool
	Target            ChildCondition
	Action            FileActionID
	Use               UseClaim
	Existing          ExistingEffect
	Initial           InitialState
	CloseIntent       *CloseIntent `json:",omitempty"`
}

// A nonnil File transfers cleanup ownership even when OpenAt returns an error.
// Attr and Outcome describe the original atomic result; a later Stat cannot
// reconstruct them.
type OpenResult struct {
	File    File
	Attr    Attr
	Outcome OpenOutcome
}

type MetadataPermissions uint8

const (
	ReadMetadata MetadataPermissions = 1 << iota
	WriteMetadata
)

type NodeRefOptions struct {
	Kind              NodeKind
	Target            ChildCondition
	Action            FileActionID
	Use               UseClaim
	MetadataAccess    MetadataPermissions
	Create, Exclusive bool
	InitialState      InitialState
	CloseIntent       *CloseIntent `json:",omitempty"`
}

// A nonnil Reference transfers cleanup ownership on success and error.
type NodeOpenResult struct {
	Reference NodeReference
	Attr      Attr
	Outcome   OpenOutcome
}

type ReferenceState struct {
	LinkTarget              []byte `json:",omitempty"`
	Attr                    Attr
	Detached, PendingUnlink bool
	PendingGeneration       []byte `json:",omitempty"`
}

type NameOperation uint8

const (
	NameCreate NameOperation = iota + 1
	NameMkdir
	NameRemove
	NameRemoveDir
	NameRename
	NameSymlink
)

// NameCommand applies one identity-conditioned namespace effect. Only fields
// used by Kind may be populated.
type NameCommand struct {
	Kind        NameOperation
	Action      FileActionID
	Name        ChildName
	Target      ChildCondition
	Destination *RenameTarget `json:",omitempty"`
	Initial     InitialFields
	Uses        []TargetUse `json:",omitempty"`
}

// Attr identifies the affected node. Successful removal may omit it. Attr with
// an error means an effect may have occurred; a known rollback returns zero.
type NameResult struct{ Attr *Attr }

type CloseTrigger uint8

const (
	OnReferenceClose CloseTrigger = iota + 1
	Now
)

type UnlinkCondition uint8

const (
	UnlinkFile UnlinkCondition = iota + 1
	UnlinkIfEmpty
)

type CloseIntent struct {
	ID               DeleteIntentID
	Trigger          CloseTrigger
	Condition        UnlinkCondition
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Uses             []TargetUse       `json:",omitempty"`
}

type PendingUnlinkCommand struct {
	Action           FileActionID
	Condition        UnlinkCondition
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Uses             []TargetUse       `json:",omitempty"`
}

type ClearPendingUnlinkCommand struct {
	Action     FileActionID
	Generation []byte
	Uses       []TargetUse `json:",omitempty"`
}

type FileMutationKind uint8

const (
	MutateTruncate FileMutationKind = iota + 1
	MutateAttributes
	MutateWriteAt
	MutateAppend
)

// ExpectedMetadata contains read-only conditions. Metadata contains updates;
// each payload Version is its expected old token, not the newly assigned token.
// Empty expected tokens require absence in both maps.
type FileMutation struct {
	Action           FileActionID
	Offset           int64
	Data             []byte            `json:",omitempty"`
	ExpectedSize     *int64            `json:",omitempty"`
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Kind             FileMutationKind
	Size             int64
	Attr             AttrChange
	Metadata         map[string]OpaquePayload `json:",omitempty"`
	Uses             []TargetUse              `json:",omitempty"`
}

// FileActionID belongs to one FileSession action epoch. It is caller-created,
// bounded, and retained unchanged across retries.
type FileActionID LockRequestID

func NewFileActionID(epoch uint64) (FileActionID, error) {
	id, err := NewLockRequestID(epoch)
	return FileActionID(id), err
}

func (id FileActionID) Epoch() (uint64, error) { return LockRequestID(id).Epoch() }

func (id FileActionID) Check() error {
	_, err := id.Epoch()
	return err
}

type FileActionOutcome uint8

const (
	FileActionPending FileActionOutcome = iota + 1
	FileActionCompleted
	FileActionNotExecuted
	FileActionUnknown
	FileActionRetired
)

// FileActionReceipt reports admission and execution state. Operation is absent
// only when no retained record can identify it. Completed and NotExecuted are
// terminal while retained; Unknown and Retired do not prove the mutation absent.
type FileActionReceipt struct {
	Action    FileActionID
	Operation Operation
	Outcome   FileActionOutcome
}

const DeleteIntentIDBytes = 32

// DeleteIntentID identifies one accepted close-time deletion obligation across
// process and authority restart. It is opaque and caller-generated.
type DeleteIntentID string

func NewDeleteIntentID() (DeleteIntentID, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return DeleteIntentID(hex.EncodeToString(nonce[:])), nil
}

type DeleteIntentOutcome uint8

const (
	DeleteIntentArmed DeleteIntentOutcome = iota + 1
	DeleteIntentPending
	DeleteIntentCompleted
	DeleteIntentNotExecuted
	DeleteIntentCleanupFailed
	DeleteIntentUnknown
	DeleteIntentRetired
)

type DeleteIntentStatus struct {
	ID      DeleteIntentID
	NodeID  uint64
	Outcome DeleteIntentOutcome
	Failure syscall.Errno `json:",omitempty"`
}

type AcknowledgeDeleteIntentCommand struct {
	Action FileActionID
	Intent DeleteIntentID
}

func (o OwnerOptions) Check() error {
	if o.Lifetime != OwnerReference && o.Lifetime != OwnerExplicit {
		return syscall.EINVAL
	}
	return nil
}

type capabilityError struct {
	message string
	errno   syscall.Errno
}

func (e *capabilityError) Error() string { return e.message }
func (e *capabilityError) Unwrap() error { return e.errno }

var (
	ErrUseConflict       error = &capabilityError{"conflicting reference use", syscall.EAGAIN}
	ErrRangeConflict     error = &capabilityError{"conflicting enforced range", syscall.EAGAIN}
	ErrPendingDelete     error = &capabilityError{"node pending unlink", syscall.EBUSY}
	ErrConditionConflict error = &capabilityError{"condition does not match", syscall.EAGAIN}
	ErrInvalidScope      error = &capabilityError{"reference scope invalid", syscall.ESTALE}
)

func CheckMetadataUpdate(namespace string, expectedVersion, payload []byte) error {
	if err := CheckMetadataNamespace(namespace); err != nil {
		return err
	}
	if len(expectedVersion) > MaxObservationTokenBytes {
		return syscall.EINVAL
	}
	if len(payload) > MaxMetadataValueBytes {
		return syscall.EFBIG
	}
	return nil
}
