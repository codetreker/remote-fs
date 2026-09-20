package storage

import (
	"bytes"
	"math"
	"syscall"
)

const (
	MaxLeafBytes  = 4096
	MaxTargetUses = 16
)

func (c ChildCondition) Check() error {
	if c.State < Any || c.State > SameNode || (c.State == SameNode) != (c.NodeID != 0) {
		return syscall.EINVAL
	}
	if len(c.ExpectedMetadata) != 0 && c.State != SameNode {
		return syscall.EINVAL
	}
	return checkMetadataConditions(c.ExpectedMetadata)
}

func (d DirectoryTarget) Check() error {
	if d.NodeID == 0 {
		return syscall.EINVAL
	}
	if d.Scope != nil {
		return d.Scope.Check()
	}
	return nil
}

func CheckLeaf(name []byte) error {
	if len(name) == 0 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.ContainsAny(name, "/\x00") {
		return syscall.EINVAL
	}
	if len(name) > MaxLeafBytes {
		return syscall.ENAMETOOLONG
	}
	return nil
}

func (n ChildName) Check() error {
	if err := n.Parent.Check(); err != nil {
		return err
	}
	return CheckLeaf(n.RawLeaf)
}

func (f InitialFields) Check() error {
	if len(f.LinkTarget) > MaxLinkTargetBytes {
		return syscall.EFBIG
	}
	if err := f.Attr.Check(); err != nil {
		return err
	}
	return CheckInitialMetadata(f.Metadata)
}

func (f InitialFields) Empty() bool {
	return f.Attr.Empty() && len(f.Metadata) == 0 && len(f.LinkTarget) == 0
}

func (s InitialState) Check() error {
	for _, fields := range []InitialFields{s.OnCreate, s.OnReset, s.OnReplace} {
		if err := fields.Check(); err != nil {
			return err
		}
	}
	return nil
}

func (o OpenAtOptions) Check() error {
	if !o.Read && !o.Write || o.Exclusive && !o.Create || o.Existing < Keep || o.Existing > ReplaceNode {
		return syscall.EINVAL
	}
	if (o.Use.Uses&ReadData != 0) != o.Read || (o.Use.Uses&WriteData != 0) != o.Write || o.Use.Uses&ReadEntries != 0 {
		return syscall.EINVAL
	}
	if o.Target.State == Absent && !o.Create || o.Exclusive && o.Target.State == SameNode {
		return syscall.EINVAL
	}
	if o.Existing == ReplaceNode && (!o.Create || o.Use.Uses&DeleteName == 0) {
		return syscall.EINVAL
	}
	if o.Existing == ResetContent && !o.Write {
		return syscall.EINVAL
	}
	if err := o.Target.Check(); err != nil {
		return err
	}
	if err := o.Action.Check(); err != nil {
		return err
	}
	if err := o.Use.Check(); err != nil {
		return err
	}
	if len(o.Initial.OnCreate.LinkTarget) != 0 || len(o.Initial.OnReset.LinkTarget) != 0 || len(o.Initial.OnReplace.LinkTarget) != 0 {
		return syscall.EINVAL
	}
	if err := o.Initial.Check(); err != nil {
		return err
	}
	if !o.Create && !o.Initial.OnCreate.Empty() || o.Existing != ResetContent && !o.Initial.OnReset.Empty() || o.Existing != ReplaceNode && !o.Initial.OnReplace.Empty() {
		return syscall.EINVAL
	}
	if o.CloseIntent != nil {
		if o.Use.Uses&DeleteName == 0 {
			return syscall.EACCES
		}
		return o.CloseIntent.Check()
	}
	return nil
}

func (o NodeRefOptions) Check() error {
	if err := o.Kind.Check(); err != nil {
		return err
	}
	if err := o.Target.Check(); err != nil {
		return err
	}
	if err := o.Action.Check(); err != nil {
		return err
	}
	if err := o.Use.Check(); err != nil {
		return err
	}
	if o.Target.State == Absent && !o.Create || o.Exclusive && o.Target.State == SameNode || o.Kind != NodeDirectory && o.Use.Uses&ReadEntries != 0 {
		return syscall.EINVAL
	}
	if o.MetadataAccess&^(ReadMetadata|WriteMetadata) != 0 || o.Exclusive && !o.Create {
		return syscall.EINVAL
	}
	if o.Kind != NodeSymlink && len(o.InitialState.OnCreate.LinkTarget) != 0 || o.Kind == NodeSymlink && o.Create && len(o.InitialState.OnCreate.LinkTarget) == 0 {
		return syscall.EINVAL
	}
	if err := o.InitialState.Check(); err != nil {
		return err
	}
	if !o.InitialState.OnReset.Empty() || !o.InitialState.OnReplace.Empty() || !o.Create && !o.InitialState.OnCreate.Empty() {
		return syscall.EINVAL
	}
	if o.CloseIntent != nil {
		if o.Use.Uses&DeleteName == 0 {
			return syscall.EACCES
		}
		return o.CloseIntent.Check()
	}
	return nil
}

func CheckTargetUses(uses []TargetUse) error {
	if len(uses) > MaxTargetUses {
		return syscall.EFBIG
	}
	seen := make(map[uint64]struct{}, len(uses))
	for _, use := range uses {
		if use.NodeID == 0 {
			return syscall.EINVAL
		}
		if _, exists := seen[use.NodeID]; exists {
			return syscall.EINVAL
		}
		seen[use.NodeID] = struct{}{}
		if err := use.Scope.Check(); err != nil {
			return err
		}
	}
	return nil
}

func (i CloseIntent) Check() error {
	if i.Trigger != OnReferenceClose || i.Condition < UnlinkFile || i.Condition > UnlinkIfEmpty {
		return syscall.EINVAL
	}
	if err := i.ID.Check(); err != nil {
		return err
	}
	if err := checkMetadataConditions(i.ExpectedMetadata); err != nil {
		return err
	}
	return CheckTargetUses(i.Uses)
}

func (c PendingUnlinkCommand) Check() error {
	if c.Condition < UnlinkFile || c.Condition > UnlinkIfEmpty {
		return syscall.EINVAL
	}
	if err := c.Action.Check(); err != nil {
		return err
	}
	if err := checkMetadataConditions(c.ExpectedMetadata); err != nil {
		return err
	}
	return CheckTargetUses(c.Uses)
}

func (c ClearPendingUnlinkCommand) Check() error {
	if len(c.Generation) == 0 || len(c.Generation) > MaxObservationTokenBytes {
		return syscall.EINVAL
	}
	if err := c.Action.Check(); err != nil {
		return err
	}
	return CheckTargetUses(c.Uses)
}

func (r RenameTarget) Check() error {
	if err := r.Parent.Check(); err != nil {
		return err
	}
	if err := CheckLeaf(r.ObservedLeaf); err != nil {
		return err
	}
	if err := CheckLeaf(r.OutputLeaf); err != nil {
		return err
	}
	return r.Expected.Check()
}

func (c NameCommand) Check() error {
	if c.Kind < NameCreate || c.Kind > NameSymlink {
		return syscall.EINVAL
	}
	if err := c.Name.Check(); err != nil {
		return err
	}
	if err := c.Target.Check(); err != nil {
		return err
	}
	if err := c.Action.Check(); err != nil {
		return err
	}
	if err := CheckTargetUses(c.Uses); err != nil {
		return err
	}
	if c.Kind == NameRename {
		if c.Destination == nil {
			return syscall.EINVAL
		}
		if err := c.Destination.Check(); err != nil {
			return err
		}
	} else if c.Destination != nil {
		return syscall.EINVAL
	}
	if c.Kind == NameSymlink {
		if len(c.Initial.LinkTarget) == 0 {
			return syscall.EINVAL
		}
		return c.Initial.Check()
	}
	if len(c.Initial.LinkTarget) != 0 {
		return syscall.EINVAL
	}
	if c.Kind == NameCreate || c.Kind == NameMkdir {
		return c.Initial.Check()
	}
	if !c.Initial.Empty() {
		return syscall.EINVAL
	}
	return nil
}

func (c FileMutation) Check() error {
	if c.Kind < MutateTruncate || c.Kind > MutateAppend || c.Size < 0 || c.Offset < 0 {
		return syscall.EINVAL
	}
	if c.ExpectedSize != nil && *c.ExpectedSize < 0 {
		return syscall.EINVAL
	}
	if err := c.Action.Check(); err != nil {
		return err
	}
	if err := checkMetadataConditions(c.ExpectedMetadata); err != nil {
		return err
	}
	if len(c.Metadata) > MaxMetadataNamespaces {
		return syscall.EFBIG
	}
	initial := make(map[string][]byte, len(c.Metadata))
	for namespace, value := range c.Metadata {
		if err := CheckMetadataUpdate(namespace, value.Version, value.Data); err != nil {
			return err
		}
		initial[namespace] = value.Data
	}
	if err := CheckInitialMetadata(initial); err != nil {
		return err
	}
	switch c.Kind {
	case MutateAttributes:
		if c.Size != 0 || c.Offset != 0 || len(c.Data) != 0 {
			return syscall.EINVAL
		}
	case MutateTruncate:
		if c.Offset != 0 || len(c.Data) != 0 || !c.Attr.Empty() {
			return syscall.EINVAL
		}
	case MutateWriteAt, MutateAppend:
		if c.Size != 0 || !c.Attr.Empty() || int64(len(c.Data)) > math.MaxInt64-c.Offset {
			return syscall.EINVAL
		}
		if c.Kind == MutateAppend && c.Offset != 0 {
			return syscall.EINVAL
		}
	}
	if err := c.Attr.Check(); err != nil {
		return err
	}
	return CheckTargetUses(c.Uses)
}

// CheckDataLimit applies the caller's existing file-operation byte budget before
// retaining or decoding a conditional mutation body. It does not invent a limit.
func (c FileMutation) CheckDataLimit(maxBytes int64) error {
	if maxBytes <= 0 {
		return syscall.EINVAL
	}
	if int64(len(c.Data)) > maxBytes {
		return syscall.EFBIG
	}
	return c.Check()
}

func (r FileActionReceipt) Check() error {
	if err := r.Action.Check(); err != nil {
		return err
	}
	if r.Operation == "" {
		if r.Outcome != FileActionNotExecuted && r.Outcome != FileActionUnknown && r.Outcome != FileActionRetired {
			return syscall.EINVAL
		}
		return nil
	}
	switch r.Operation {
	case OpFileOpenAt, OpFileMutateName, OpFileOpenNodeRef, OpFileOpenChildRef,
		OpFileSetPendingUnlink, OpFileClearPendingUnlink, OpFileMutate,
		OpFileAcknowledgeDeleteIntent:
	default:
		return syscall.EINVAL
	}
	if r.Outcome < FileActionPending || r.Outcome > FileActionRetired {
		return syscall.EINVAL
	}
	return nil
}

func (id DeleteIntentID) Check() error {
	if len(id) != DeleteIntentIDBytes {
		return syscall.EINVAL
	}
	for i := range id {
		if id[i] < '0' || id[i] > '9' && id[i] < 'a' || id[i] > 'f' {
			return syscall.EINVAL
		}
	}
	return nil
}

func (s DeleteIntentStatus) Check() error {
	if err := s.ID.Check(); err != nil {
		return err
	}
	if s.Outcome < DeleteIntentArmed || s.Outcome > DeleteIntentRetired {
		return syscall.EINVAL
	}
	missing := s.Outcome == DeleteIntentUnknown || s.Outcome == DeleteIntentRetired
	if missing != (s.NodeID == 0) {
		return syscall.EINVAL
	}
	if s.Outcome == DeleteIntentCleanupFailed {
		if s.Failure == 0 {
			return syscall.EINVAL
		}
		if _, ok := ErrnoName(s.Failure); !ok {
			return syscall.EINVAL
		}
	} else if s.Failure != 0 {
		return syscall.EINVAL
	}
	return nil
}

func (c AcknowledgeDeleteIntentCommand) Check() error {
	if err := c.Action.Check(); err != nil {
		return err
	}
	return c.Intent.Check()
}

func checkMetadataConditions(expected map[string][]byte) error {
	if len(expected) > MaxMetadataNamespaces {
		return syscall.EFBIG
	}
	for namespace, version := range expected {
		if err := CheckMetadataUpdate(namespace, version, nil); err != nil {
			return err
		}
	}
	return nil
}
