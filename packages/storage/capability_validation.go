package storage

import (
	"bytes"
	"math"
	"syscall"
)

const (
	MaxLeafBytes           = 4096
	MaxScopeBytes          = 128
	MaxNamespaceGuards     = 256
	MaxNamespaceGuardBytes = 64 << 10
	MaxTargetUses          = 16
	MaxDirectoryEntries    = 65536
	MaxDirectoryBytes      = 8 << 20
)

func (s UseScope) Check() error {
	if len(s.Token) == 0 || len(s.Token) > MaxScopeBytes || bytes.IndexByte([]byte(s.Token), 0) >= 0 {
		return syscall.EINVAL
	}
	return nil
}
func (c UseClaim) Check() error {
	if (c.Uses|c.Deny)&^AllUses != 0 {
		return syscall.EINVAL
	}
	return nil
}
func (c ChildCondition) Check() error {
	if c.State < Any || c.State > SameNode || (c.State == SameNode) != (c.NodeID != 0) {
		return syscall.EINVAL
	}
	return nil
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
func (d DirectoryObservation) Check() error {
	if d.ParentID == 0 || len(d.Revision) == 0 || len(d.Revision) > MaxObservationTokenBytes {
		return syscall.EINVAL
	}
	return nil
}
func (g *NamespaceGuards) Check() error {
	if g == nil {
		return nil
	}
	if len(g.Directories) > MaxNamespaceGuards || len(g.Edges) > MaxNamespaceGuards {
		return syscall.EFBIG
	}
	total := 0
	directories := map[uint64]bool{}
	children := map[uint64]uint64{}
	type slot struct {
		parent uint64
		name   string
	}
	slots := map[slot]bool{}
	for _, d := range g.Directories {
		if err := d.Check(); err != nil {
			return err
		}
		if directories[d.ParentID] {
			return syscall.EINVAL
		}
		directories[d.ParentID] = true
		total += len(d.Revision) + 8
	}
	for _, edge := range g.Edges {
		if edge.ParentID == 0 || edge.ChildID == 0 || edge.ParentID == edge.ChildID {
			return syscall.EINVAL
		}
		if err := CheckLeaf(edge.RawLeaf); err != nil {
			return err
		}
		if _, exists := children[edge.ChildID]; exists {
			return syscall.EINVAL
		}
		key := slot{edge.ParentID, string(edge.RawLeaf)}
		if slots[key] {
			return syscall.EINVAL
		}
		slots[key] = true
		children[edge.ChildID] = edge.ParentID
		total += len(edge.RawLeaf) + 16
	}
	if total > MaxNamespaceGuardBytes {
		return syscall.EFBIG
	}
	for child := range children {
		seen := map[uint64]bool{}
		id := child
		for {
			if seen[id] {
				return syscall.EINVAL
			}
			seen[id] = true
			parent, ok := children[id]
			if !ok {
				if g.RootID != 0 && id != g.RootID {
					return syscall.EINVAL
				}
				break
			}
			id = parent
		}
	}
	if g.RootID != 0 {
		for id := range directories {
			seen := map[uint64]bool{}
			for id != g.RootID {
				if seen[id] {
					return syscall.EINVAL
				}
				seen[id] = true
				parent, ok := children[id]
				if !ok {
					return syscall.EINVAL
				}
				id = parent
			}
		}
	}
	return nil
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
	for _, f := range []InitialFields{s.OnCreate, s.OnReset, s.OnReplace} {
		if err := f.Check(); err != nil {
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
	if len(o.ExpectedMetadata) != 0 && o.Target.State != SameNode {
		return syscall.EINVAL
	}
	if err := checkMetadataConditions(o.ExpectedMetadata); err != nil {
		return err
	}
	if err := o.Guards.Check(); err != nil {
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
	if len(o.ExpectedMetadata) != 0 && o.Target.State != SameNode {
		return syscall.EINVAL
	}
	if err := checkMetadataConditions(o.ExpectedMetadata); err != nil {
		return err
	}
	if err := o.Guards.Check(); err != nil {
		return err
	}
	if err := o.Use.Check(); err != nil {
		return err
	}
	if o.Target.State == Absent && !o.Create || o.Exclusive && o.Target.State == SameNode || o.Kind != NodeDirectory && o.Use.Uses&ReadEntries != 0 {
		return syscall.EINVAL
	}
	if o.MetadataAccess&^(ReadMetadata|WriteMetadata) != 0 || o.Exclusive && !o.Create || o.Use.Uses&(ReadData|WriteData) != 0 {
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
	seen := map[uint64]bool{}
	for _, use := range uses {
		if use.NodeID == 0 || seen[use.NodeID] {
			return syscall.EINVAL
		}
		seen[use.NodeID] = true
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
	if err := i.Guards.Check(); err != nil {
		return err
	}
	return CheckTargetUses(i.Uses)
}
func (c PendingUnlinkCommand) Check() error {
	if c.Condition < UnlinkFile || c.Condition > UnlinkIfEmpty {
		return syscall.EINVAL
	}
	if err := c.Guards.Check(); err != nil {
		return err
	}
	return CheckTargetUses(c.Uses)
}
func (c ClearPendingUnlinkCommand) Check() error {
	if len(c.Generation) == 0 || len(c.Generation) > MaxObservationTokenBytes {
		return syscall.EINVAL
	}
	if err := c.Guards.Check(); err != nil {
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
	if err := c.Guards.Check(); err != nil {
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
		if c.Offset != 0 || len(c.Data) != 0 || !c.Attr.Empty() || len(c.Metadata) != 0 {
			return syscall.EINVAL
		}
	case MutateWriteAt, MutateAppend:
		if c.Size != 0 || !c.Attr.Empty() || len(c.Metadata) != 0 || int64(len(c.Data)) > math.MaxInt64-c.Offset {
			return syscall.EINVAL
		}
		if c.Kind == MutateAppend && c.Offset != 0 {
			return syscall.EINVAL
		}
	}
	if err := c.Attr.Check(); err != nil {
		return err
	}
	if err := c.Guards.Check(); err != nil {
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
	ErrConditionConflict error = &capabilityError{"observation condition changed", syscall.EAGAIN}
	ErrInvalidScope      error = &capabilityError{"reference scope invalid", syscall.ESTALE}
)
var _ interface{ Unwrap() error } = (*capabilityError)(nil)

func (o OwnerOptions) Check() error {
	if o.Lifetime != OwnerReference && o.Lifetime != OwnerExplicit {
		return syscall.EINVAL
	}
	return nil
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

// ObservedEntryBytes charges a bounded retained representation, not wire bytes.
// Native producers apply it to SQL lengths before loading names or metadata.
func ObservedEntryBytes(nameBytes, metadataBytes int64) (int64, error) {
	if nameBytes <= 0 || nameBytes > MaxLeafBytes || metadataBytes < 6 || metadataBytes > MaxMetadataBytes {
		return 0, syscall.EFBIG
	}
	retained, err := MetadataRetentionBytes(metadataBytes)
	if err != nil {
		return 0, err
	}
	return 256 + 4*nameBytes + retained, nil
}
func (d ObservedDirectory) Check() error {
	if err := d.Observation.Check(); err != nil {
		return err
	}
	if len(d.Entries) > MaxDirectoryEntries {
		return syscall.EFBIG
	}
	used := int64(0)
	names := map[string]bool{}
	ids := map[uint64]bool{}
	for _, entry := range d.Entries {
		if err := CheckLeaf(entry.RawLeaf); err != nil {
			return err
		}
		if entry.Attr.ID == 0 || entry.Attr.ID == d.Observation.ParentID || entry.Attr.Kind.Check() != nil || entry.Attr.Size < 0 {
			return syscall.EIO
		}
		key := string(entry.RawLeaf)
		if names[key] || ids[entry.Attr.ID] {
			return syscall.EIO
		}
		names[key] = true
		ids[entry.Attr.ID] = true
		size, err := metadataSize(entry.Attr.Metadata)
		if err != nil {
			return err
		}
		charge, err := ObservedEntryBytes(int64(len(entry.RawLeaf)), int64(size))
		if err != nil {
			return err
		}
		if charge > MaxDirectoryBytes-used {
			return syscall.EFBIG
		}
		used += charge
	}
	return nil
}
