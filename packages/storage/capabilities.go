package storage

import "context"

// Each optional capability checks its complete backing chain before use. A
// wrapper without underlying support returns EOPNOTSUPP from both check and call.
type AtomicFileOpener interface {
	CheckAtomicFileOpen() error
	OpenAt(context.Context, ChildName, OpenAtOptions) (OpenResult, error)
}

// NamespaceAccess operates on exact byte names under identity-addressed parents.
// LookupAt is metadata lookup; ReadDirNode requires ReadEntries and returns the
// complete bounded name set. A present invalid scope never falls back to a bare ID.
// ReadDirNodeBounded reserves caller-owned name/metadata capacity before loading
// variable payloads and calls result.Fail on failure; it has identical admission
// and capture ordering to ReadDirNode. Guards and name effects are checked in
// the same native mutation transaction.
type NamespaceAccess interface {
	CheckNamespaceAccess() error
	LookupAt(context.Context, ChildName) (Attr, error)
	ReadDirNode(context.Context, DirectoryTarget) (ObservedDirectory, error)
	ReadDirNodeBounded(context.Context, DirectoryTarget, *ListResult) (DirectoryObservation, error)
	MutateName(context.Context, NameCommand) (NameResult, error)
}
type NodeReference interface {
	Stat(context.Context) (Attr, error)
	SetAttr(context.Context, AttrChange) (Attr, error)
	Close(context.Context) error
}

// NodeReferences uses the existing FileSession registry, quotas and Close drain.
// Metadata-only references grant no byte methods and survive name replacement.
type NodeReferences interface {
	CheckNodeReferences() error
	OpenNodeRef(context.Context, uint64, NodeRefOptions) (NodeOpenResult, error)
	OpenChildRef(context.Context, ChildName, NodeRefOptions) (NodeOpenResult, error)
}

// ReferenceStateAccess captures attributes, link target and pending state together.
// Failure or loss of continuity cannot be replaced by detached=false or pending=false.
type ReferenceStateAccess interface {
	CheckReferenceState() error
	State(context.Context) (ReferenceState, error)
}

// ScopedReference returns a capability for this exact live reference. It carries
// no authority until the owning session validates it against the target NodeID.
type ScopedReference interface {
	CheckScopedReference() error
	Scope(context.Context) (UseScope, error)
}

// MetadataAccess compares and replaces exactly one namespace atomically. An empty
// expected version requires absence; empty payload stores a present empty value.
// ErrConditionConflict proves no update occurred. Other namespaces are preserved.
type MetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, uint64, string, []byte, []byte) (OpaquePayload, error)
}

// ReferenceMetadataAccess applies the same namespace CAS to a retained identity.
type ReferenceMetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, string, []byte, []byte) (OpaquePayload, error)
}

// UseOwners registers owners in the existing session, bound to a live node/scope.
// An arbitrary numeric owner conveys no permission. Group affects deadlock detection
// only; it never shares claims, ranges or access exemptions between references.
type UseOwners interface {
	CheckUseOwners() error
	NewUseOwner(context.Context, uint64, UseScope, OwnerOptions) (UseOwner, error)
	RetireUseOwner(context.Context, UseOwner) error
}

// RangeControl keeps the existing lock epoch, nonce and finite receipt history.
// Apply admits a bounded batch once. DropBeforeAcquire may release earlier claims
// even on rejection; Effects records those releases. Cancellation alone does not
// prove a grant absent, and unknown outcomes remain errors until reconciled.
type RangeControl interface {
	CheckRangeControl() error
	GetConflict(context.Context, UseOwner, RangeCommand) (RangeConflict, error)
	Apply(context.Context, UseOwner, []RangeCommand, LockRequestID) (RangeAttempt, error)
	Query(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Cancel(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Drop(context.Context, UseOwner, ConflictDomain) error
}

// DeleteIntent changes node-level pending state under native admission. Clearing
// requires the current generation and does not erase another armed close intent.
// Reference Close retains ownership and charges until its cleanup is known complete.
type DeleteIntent interface {
	CheckDeleteIntent() error
	SetPendingUnlink(context.Context, PendingUnlinkCommand) (ReferenceState, error)
	ClearPendingUnlink(context.Context, ClearPendingUnlinkCommand) (ReferenceState, error)
}

// ConditionalFileMutation compares explicit size/namespace conditions with the
// selected effect at final native publication. Append retries build a new candidate
// at current EOF after a known content-revision conflict; ordinary WriteAt is unchanged.
type ConditionalFileMutation interface {
	CheckConditionalFileMutation() error
	MutateFile(context.Context, FileMutation) (Attr, error)
}

type UseScope struct{ Token string }
type Uses uint8

const (
	ReadData Uses = 1 << iota
	WriteData
	ReadEntries
	DeleteName
)
const AllUses = ReadData | WriteData | ReadEntries | DeleteName

type UseClaim struct{ Uses, Deny Uses }
type UseOwner uint64
type OwnerDiagnostic uint64

// Group only identifies a session-local deadlock participant. Zero is independent.
type OwnerOptions struct {
	Lifetime OwnerLifetime
	Group    uint64
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

type ChildCondition struct {
	State  ExpectedChild
	NodeID uint64
}
type DirectoryTarget struct {
	NodeID uint64
	Scope  *UseScope `json:",omitempty"`
}
type ChildName struct {
	Parent  DirectoryTarget
	RawLeaf []byte
}
type DirectoryObservation struct {
	ParentID uint64
	Revision []byte
}
type ObservedEdge struct {
	ParentID uint64
	RawLeaf  []byte
	ChildID  uint64
}
type NamespaceGuards struct {
	Directories []DirectoryObservation `json:",omitempty"`
	Edges       []ObservedEdge         `json:",omitempty"`
	RootID      uint64
}
type ObservedEntry struct {
	RawLeaf []byte
	Attr    Attr
}
type ObservedDirectory struct {
	Observation DirectoryObservation
	Entries     []ObservedEntry `json:",omitempty"`
}
type RenameTarget struct {
	Parent       DirectoryTarget
	ObservedLeaf []byte
	Expected     ChildCondition
	OutputLeaf   []byte
}
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
type InitialState struct{ OnCreate, OnReset, OnReplace InitialFields }
type OpenAtOptions struct {
	Read, Write       bool
	Create, Exclusive bool
	Target            ChildCondition
	// ExpectedMetadata checks the existing SameNode target before any open effect
	// or use admission. Empty tokens require namespace absence; nonempty tokens
	// require exact version equality. Conflicts return ErrConditionConflict with
	// no effect. Initial assigns updates independently of these conditions.
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Guards           *NamespaceGuards  `json:",omitempty"`
	Use              UseClaim
	Existing         ExistingEffect
	Initial          InitialState
	CloseIntent      *CloseIntent `json:",omitempty"`
}

// A nonnil File transfers cleanup ownership even when OpenAt returns an error.
// Attr and Outcome belong to the original open; later Stat cannot replace them.
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
	Kind   NodeKind
	Target ChildCondition
	// ExpectedMetadata has the same existing-target, zero-effect conflict
	// semantics as OpenAtOptions.ExpectedMetadata, including use and intent admission.
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Guards           *NamespaceGuards  `json:",omitempty"`
	// Use declares compatibility claims independently of the reference's method
	// permissions. ReadData and WriteData do not grant byte-file capabilities;
	// metadata operations still require MetadataAccess.
	Use               UseClaim
	MetadataAccess    MetadataPermissions
	Create, Exclusive bool
	InitialState      InitialState
	CloseIntent       *CloseIntent `json:",omitempty"`
}

// A nonnil Reference transfers cleanup ownership on both success and error.
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

// Only fields used by Kind may be populated. The guards and use scopes are
// checked with the selected mutation before any name or metadata changes.
type NameCommand struct {
	Kind        NameOperation
	Name        ChildName
	Target      ChildCondition
	Destination *RenameTarget `json:",omitempty"`
	Initial     InitialFields
	Guards      *NamespaceGuards `json:",omitempty"`
	Uses        []TargetUse      `json:",omitempty"`
}

// NameResult.Attr identifies the affected node. Successful Remove and RemoveDir
// may omit Attr; every other operation requires this captured result because a
// later Stat cannot reconstruct the same outcome.
// Attr with an error means an effect may have occurred; cancellation cannot then
// be reported as a safe retry. A known rollback returns the zero NameResult.
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
	Trigger   CloseTrigger
	Condition UnlinkCondition
	Guards    *NamespaceGuards `json:",omitempty"`
	Uses      []TargetUse      `json:",omitempty"`
}
type PendingUnlinkCommand struct {
	Condition UnlinkCondition
	Guards    *NamespaceGuards `json:",omitempty"`
	Uses      []TargetUse      `json:",omitempty"`
}
type ClearPendingUnlinkCommand struct {
	Generation []byte
	Guards     *NamespaceGuards `json:",omitempty"`
	Uses       []TargetUse      `json:",omitempty"`
}
type FileMutationKind uint8

const (
	MutateTruncate FileMutationKind = iota + 1
	MutateAttributes
	MutateWriteAt
	MutateAppend
)

// ExpectedMetadata is read-only conditions. Metadata holds updates whose Version
// is the expected namespace token (empty requires absence), not the new version.
// The authority assigns successful update versions inside the final transaction.
type FileMutation struct {
	Offset           int64
	Data             []byte            `json:",omitempty"`
	ExpectedSize     *int64            `json:",omitempty"`
	ExpectedMetadata map[string][]byte `json:",omitempty"`
	Kind             FileMutationKind
	Size             int64
	Attr             AttrChange
	Metadata         map[string]OpaquePayload `json:",omitempty"`
	Guards           *NamespaceGuards         `json:",omitempty"`
	Uses             []TargetUse              `json:",omitempty"`
}
