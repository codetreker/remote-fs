package storage

import (
	"strings"
	"syscall"
	"time"
)

type AccessUse uint64

const (
	ReadContent AccessUse = 1 << iota
	WriteContent
	RemoveEntry
)
const AllAccessUses = ReadContent | WriteContent | RemoveEntry

type AccessClaim struct{ Uses, Excludes AccessUse }

func (c AccessClaim) Check() error {
	if (c.Uses|c.Excludes) & ^AllAccessUses != 0 {
		return syscall.EINVAL
	}
	return nil
}

// EntryTarget binds an exact leaf. Witness optionally guards every observed
// ancestor; relative identity operations may omit it.
// Zero expected IDs require absence; nonzero IDs require that exact entry/node.
type EntryTarget struct {
	Parent                   FileReferenceID
	ParentID                 uint64
	Name                     []byte
	DirectoryRevision        DirectoryRevision
	ExpectedEntryID          EntryID
	ExpectedNodeID           uint64
	ExpectedMetadataRevision NodeMetadataRevision
	Witness                  *EntryLocation
}

type NodeInitial struct {
	Kind                                          NodeKind
	Metadata                                      Metadata
	LinkTarget                                    []byte
	AccessTime, ModTime, CreationTime, ChangeTime *time.Time
}

type RetainRequest struct {
	NodeID                   uint64
	ExpectedMetadataRevision NodeMetadataRevision
	Claim                    AccessClaim
	Witness                  *EntryLocation
	Prepared                 *RemovalCondition
}

type RetainAtRequest struct {
	Target   EntryTarget
	Claim    AccessClaim
	Prepared *RemovalCondition
}
type CreateAndRetainRequest struct {
	Target   EntryTarget
	Initial  NodeInitial
	Claim    AccessClaim
	Prepared *RemovalCondition
}
type ResetAndRetainRequest struct {
	Target           EntryTarget
	ExpectedRevision NodeMetadataRevision
	Change           AttrChange
	Claim            AccessClaim
	Prepared         *RemovalCondition
}

type RenameRequest struct {
	Source      EntryTarget
	Destination EntryTarget
	// NewName selects the output spelling independently of the observed destination.
	// Nil uses Destination.Name; a present empty name is invalid. The output slot
	// must be absent or refer to Source, and only the expected destination may be removed.
	NewName []byte
}

// SetKind applies only to an empty node held by this sole reference. The
// expected metadata revision, current emptiness and reference count are checked
// atomically with the kind, target and complete opaque metadata replacement.
type SetKindRequest struct {
	Owner            *RangeOwnerID
	Witness          *EntryLocation
	ExpectedRevision NodeMetadataRevision
	Kind             NodeKind
	LinkTarget       []byte
	Metadata         Metadata
}

type RemovalCondition uint8

const (
	RemovalFile RemovalCondition = iota + 1
	RemovalIfEmpty
)

type RemovalIntentID uint64
type DrainGeneration uint64
type EntryState uint8

const (
	EntryActive EntryState = iota + 1
	EntryDraining
	EntryDetached
)

type PrepareRemovalRequest struct {
	ExpectedMetadataRevision NodeMetadataRevision
	Entry                    EntryCondition
	Witness                  EntryLocation
	Condition                RemovalCondition
}
type DrainEntryRequest struct {
	ExpectedMetadataRevision NodeMetadataRevision
	Entry                    EntryCondition
	Witness                  EntryLocation
	Condition                RemovalCondition
}
type CancelDrainRequest struct {
	EntryID    EntryID
	Generation DrainGeneration
}

type RemovalStatus struct {
	IntentID          RemovalIntentID
	EntryID           EntryID
	Prepared          bool
	PreparedCondition RemovalCondition
	DrainCondition    RemovalCondition
	State             EntryState
	Generation        DrainGeneration
}

type FileActionState uint8

const (
	FileActionPending FileActionState = iota + 1
	FileActionCompleted
	FileActionNotApplied
	FileActionUnknown
	// FileActionRetired is a present terminal identity fact returned by cleanup,
	// not a historical receipt proving the supplied action ran. Action is empty,
	// Operation and Reference identify the cleanup scope, and Effects, Errno and
	// HistoryRemaining are zero.
	FileActionRetired
)

type FileEffects uint32

const (
	EffectRetained FileEffects = 1 << iota
	EffectCreated
	EffectContentChanged
	EffectMetadataChanged
	EffectEntryMoved
	EffectEntryDetached
	EffectClaimChanged
	EffectRangesChanged
	EffectPreparedChanged
	EffectDrainChanged
	EffectReferenceRetired
)

type FileConflictKind uint8

const (
	ConflictRevision FileConflictKind = iota + 1
	ConflictIdentity
	ConflictClaim
	ConflictRange
	ConflictDraining
	ConflictCapacity
	ConflictRetired
	ConflictDeadlock
)

type FileConflict struct {
	Kind     FileConflictKind
	NodeID   uint64
	EntryID  EntryID
	Revision uint64
	Claim    AccessClaim
	Range    *HeldRange
}

// FileActionReceipt contains values only. Effects remain meaningful alongside
// an error, including a failed close whose reference was already retired.
// Unknown never authorizes planning or submitting a replacement action.
type FileActionReceipt struct {
	Action           FileActionID
	Operation        Operation
	State            FileActionState
	Effects          FileEffects
	Reference        FileReferenceID
	Observation      FileObservation
	RangeRevision    uint64
	Removal          RemovalStatus
	Errno            syscall.Errno
	Conflict         *FileConflict
	HistoryRemaining time.Duration
}

// ObservationCondition validates a completed multi-call observation without
// retaining another reference or publishing a mutation.
type ObservationCondition struct {
	MetadataRevision  NodeMetadataRevision
	DirectoryRevision DirectoryRevision
	Location          EntryLocation
}

func (t EntryTarget) Check() error {
	if t.Parent == 0 || t.ParentID == 0 || t.DirectoryRevision == 0 || (t.ExpectedEntryID == 0) != (t.ExpectedNodeID == 0) {
		return syscall.EINVAL
	}
	if t.ExpectedNodeID == 0 && t.ExpectedMetadataRevision != 0 {
		return syscall.EINVAL
	}
	if err := CheckEntryName(t.Name); err != nil {
		return err
	}
	if t.Witness != nil {
		if err := t.Witness.Check(); err != nil {
			return err
		}
		if t.Witness.NodeID != t.ParentID || t.Witness.State == LocationDetached {
			return syscall.EINVAL
		}
	}
	return nil
}

func (n NodeInitial) Check() error {
	if err := n.Kind.Check(); err != nil {
		return err
	}
	if err := n.Metadata.Check(); err != nil {
		return err
	}
	return checkLinkData(n.Kind, n.LinkTarget)
}

func checkLinkData(kind NodeKind, target []byte) error {
	if kind == NodeSymlink {
		if len(target) == 0 {
			return syscall.EINVAL
		}
		if len(target) > MaxLinkTargetBytes {
			return syscall.EFBIG
		}
	} else if len(target) != 0 {
		return syscall.EINVAL
	}
	return nil
}

func (c RemovalCondition) Check() error {
	if c != RemovalFile && c != RemovalIfEmpty {
		return syscall.EINVAL
	}
	return nil
}

func checkPrepared(c *RemovalCondition) error {
	if c == nil {
		return nil
	}
	return c.Check()
}

func (r RetainRequest) Check() error {
	if r.NodeID == 0 {
		return syscall.EINVAL
	}
	if err := r.Claim.Check(); err != nil {
		return err
	}
	if r.Witness != nil {
		if err := r.Witness.Check(); err != nil {
			return err
		}
		if r.Witness.NodeID != r.NodeID {
			return syscall.EINVAL
		}
	}
	return checkPrepared(r.Prepared)
}

func (r RetainAtRequest) Check() error {
	if err := r.Target.Check(); err != nil {
		return err
	}
	if r.Target.ExpectedNodeID == 0 {
		return syscall.EINVAL
	}
	if err := r.Claim.Check(); err != nil {
		return err
	}
	return checkPrepared(r.Prepared)
}

func (r CreateAndRetainRequest) Check() error {
	if err := r.Target.Check(); err != nil {
		return err
	}
	if err := r.Initial.Check(); err != nil {
		return err
	}
	if err := r.Claim.Check(); err != nil {
		return err
	}
	return checkPrepared(r.Prepared)
}

func (r CreateAndRetainRequest) CheckCreate() error {
	if err := r.Check(); err != nil {
		return err
	}
	if r.Target.ExpectedNodeID != 0 {
		return syscall.EINVAL
	}
	return nil
}

func (r CreateAndRetainRequest) CheckReplace() error {
	if err := r.Check(); err != nil {
		return err
	}
	if r.Target.ExpectedNodeID == 0 {
		return syscall.EINVAL
	}
	return nil
}

func (r ResetAndRetainRequest) Check() error {
	if err := (RetainAtRequest{Target: r.Target, Claim: r.Claim, Prepared: r.Prepared}).Check(); err != nil {
		return err
	}
	if r.ExpectedRevision == 0 || (r.Target.ExpectedMetadataRevision != 0 && r.ExpectedRevision != r.Target.ExpectedMetadataRevision) || (r.Change.ExpectedRevision != 0 && r.Change.ExpectedRevision != r.ExpectedRevision) {
		return syscall.EINVAL
	}
	return r.Change.Check()
}

func (r RenameRequest) Check() error {
	if err := r.Source.Check(); err != nil {
		return err
	}
	if r.Source.ExpectedNodeID == 0 {
		return syscall.EINVAL
	}
	if err := r.Destination.Check(); err != nil {
		return err
	}
	if r.NewName != nil {
		return CheckEntryName(r.NewName)
	}
	return nil
}

func (r SetKindRequest) Check() error {
	if r.Witness != nil {
		if err := r.Witness.Check(); err != nil {
			return err
		}
	}
	if r.ExpectedRevision == 0 {
		return syscall.EINVAL
	}
	return (NodeInitial{Kind: r.Kind, Metadata: r.Metadata, LinkTarget: r.LinkTarget}).Check()
}

func (r PrepareRemovalRequest) Check() error {
	if err := r.Entry.Check(); err != nil {
		return err
	}
	if err := r.Witness.Check(); err != nil {
		return err
	}
	if r.Witness.State != LocationLinked || r.Witness.NodeID != r.Entry.NodeID {
		return syscall.EINVAL
	}
	last := r.Witness.Ancestors[len(r.Witness.Ancestors)-1]
	if last.ParentID != r.Entry.ParentID || last.DirectoryRevision != r.Entry.DirectoryRevision || last.EntryID != r.Entry.EntryID || last.NodeID != r.Entry.NodeID || string(last.Name) != string(r.Entry.Name) {
		return syscall.EINVAL
	}
	return r.Condition.Check()
}

func (r DrainEntryRequest) Check() error {
	return (PrepareRemovalRequest{ExpectedMetadataRevision: r.ExpectedMetadataRevision, Entry: r.Entry, Witness: r.Witness, Condition: r.Condition}).Check()
}

func (r CancelDrainRequest) Check() error {
	if r.EntryID == 0 || r.Generation == 0 {
		return syscall.EINVAL
	}
	return nil
}

func (c ObservationCondition) Check() error {
	if c.MetadataRevision == 0 {
		return syscall.EINVAL
	}
	return c.Location.Check()
}

func (r FileActionReceipt) Clone() FileActionReceipt {
	r.Action = FileActionID(strings.Clone(string(r.Action)))
	r.Operation = Operation(strings.Clone(string(r.Operation)))
	r.Observation = r.Observation.Clone()
	if r.Conflict != nil {
		value := *r.Conflict
		if value.Range != nil {
			held := *value.Range
			held.Owner.Session = strings.Clone(held.Owner.Session)
			value.Range = &held
		}
		r.Conflict = &value
	}
	return r
}

func (r RemovalStatus) Check() error {
	if r.State == 0 {
		if r.IntentID != 0 || r.EntryID != 0 || r.Prepared || r.PreparedCondition != 0 || r.DrainCondition != 0 || r.Generation != 0 {
			return syscall.EIO
		}
		return nil
	}
	if r.State < EntryActive || r.State > EntryDetached {
		return syscall.EIO
	}
	if r.Prepared {
		if r.IntentID == 0 || r.EntryID == 0 || r.PreparedCondition.Check() != nil {
			return syscall.EIO
		}
	} else if r.IntentID != 0 || r.PreparedCondition != 0 {
		return syscall.EIO
	}
	if r.State == EntryDraining {
		if r.EntryID == 0 || r.Generation == 0 || r.DrainCondition.Check() != nil {
			return syscall.EIO
		}
	} else if r.DrainCondition != 0 {
		return syscall.EIO
	}
	if r.EntryID == 0 && r.Generation != 0 {
		return syscall.EIO
	}
	return nil
}
