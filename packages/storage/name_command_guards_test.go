package storage

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
)

func guardedRename(t *testing.T) NameCommand {
	t.Helper()
	return NameCommand{
		Kind: NameRename, Action: validFileAction(t),
		Name:   ChildName{Parent: DirectoryTarget{NodeID: 3}, RawLeaf: []byte("source")},
		Target: ChildCondition{State: SameNode, NodeID: 5},
		Destination: &RenameTarget{
			Parent: DirectoryTarget{NodeID: 4}, ObservedLeaf: []byte("target"), OutputLeaf: []byte("TARGET"),
			Expected: ChildCondition{State: SameNode, NodeID: 6},
		},
		RequireNoOpenDescendants: true,
		Guards: &NamespaceGuards{
			RootID: 1,
			Directories: []DirectoryObservation{
				{ParentID: 1, Revision: []byte("root")}, {ParentID: 2, Revision: []byte("common")},
				{ParentID: 3, Revision: []byte("left")}, {ParentID: 4, Revision: []byte("right")},
			},
			Edges: []ObservedEdge{
				{ParentID: 1, RawLeaf: []byte("common"), ChildID: 2},
				{ParentID: 2, RawLeaf: []byte("left"), ChildID: 3},
				{ParentID: 2, RawLeaf: []byte("right"), ChildID: 4},
				{ParentID: 3, RawLeaf: []byte("source"), ChildID: 5},
				{ParentID: 4, RawLeaf: []byte("target"), ChildID: 6},
			},
		},
	}
}

func TestNameCommandGuardsCoverOnlyBothCompletePaths(t *testing.T) {
	valid := guardedRename(t)
	for name, change := range map[string]func(*NameCommand){
		"two branches and file terminals": func(*NameCommand) {},
		"absent target": func(c *NameCommand) {
			c.Destination.Expected = ChildCondition{State: Absent}
			c.Guards.Edges = c.Guards.Edges[:4]
		},
		"raw call": func(c *NameCommand) { c.Guards = nil },
		"same parent and exact source alias": func(c *NameCommand) {
			c.Destination = &RenameTarget{
				Parent: c.Name.Parent, ObservedLeaf: bytes.Clone(c.Name.RawLeaf), OutputLeaf: []byte("SOURCE"), Expected: c.Target,
			}
			c.Guards.Directories = c.Guards.Directories[:3]
			c.Guards.Edges = append(c.Guards.Edges[:2], c.Guards.Edges[3])
		},
		"root parent": func(c *NameCommand) {
			c.Name.Parent.NodeID = 1
			c.Destination.Parent.NodeID = 1
			c.Guards.Directories = c.Guards.Directories[:1]
			c.Guards.Edges = c.Guards.Edges[3:]
			for i := range c.Guards.Edges {
				c.Guards.Edges[i].ParentID = 1
			}
		},
		"source is target ancestor": func(c *NameCommand) {
			c.Name = ChildName{Parent: DirectoryTarget{NodeID: 2}, RawLeaf: []byte("right")}
			c.Target.NodeID = 4
			c.Guards.Directories = append(c.Guards.Directories[:2], c.Guards.Directories[3])
			c.Guards.Edges = []ObservedEdge{c.Guards.Edges[0], c.Guards.Edges[2], c.Guards.Edges[4]}
		},
	} {
		t.Run(name, func(t *testing.T) {
			command := valid
			command.Guards = valid.Guards.Clone()
			destination := *valid.Destination
			command.Destination = &destination
			change(&command)
			if err := command.Check(); err != nil {
				t.Fatalf("valid guarded command = %v", err)
			}
		})
	}
}

func TestNameCommandGuardsRejectMissingAndUnrelatedEvidence(t *testing.T) {
	for name, change := range map[string]func(*NameCommand){
		"empty":                 func(c *NameCommand) { c.Guards = &NamespaceGuards{} },
		"unrooted":              func(c *NameCommand) { c.Guards.RootID = 0 },
		"missing root revision": func(c *NameCommand) { c.Guards.Directories = c.Guards.Directories[1:] },
		"missing common revision": func(c *NameCommand) {
			c.Guards.Directories = append(c.Guards.Directories[:1], c.Guards.Directories[2:]...)
		},
		"missing source parent": func(c *NameCommand) { c.Name.Parent.NodeID = 8 },
		"missing target parent": func(c *NameCommand) { c.Destination.Parent.NodeID = 8 },
		"missing source terminal": func(c *NameCommand) {
			c.Guards.Edges = append(c.Guards.Edges[:3], c.Guards.Edges[4])
		},
		"missing target terminal":         func(c *NameCommand) { c.Guards.Edges = c.Guards.Edges[:4] },
		"wrong source leaf":               func(c *NameCommand) { c.Name.RawLeaf = []byte("other") },
		"wrong target observed leaf":      func(c *NameCommand) { c.Destination.ObservedLeaf = []byte("TARGET") },
		"wrong terminal parent":           func(c *NameCommand) { c.Guards.Edges[3].ParentID = 4 },
		"absence contradicts target edge": func(c *NameCommand) { c.Destination.Expected = ChildCondition{State: Absent} },
		"unrelated terminal": func(c *NameCommand) {
			c.Guards.Edges = append(c.Guards.Edges, ObservedEdge{ParentID: 1, RawLeaf: []byte("unused"), ChildID: 9})
		},
		"unrelated directory": func(c *NameCommand) {
			c.Guards.Directories = append(c.Guards.Directories, DirectoryObservation{ParentID: 5, Revision: []byte("unused")})
		},
		"absent source on target ancestry": func(c *NameCommand) {
			c.Name = ChildName{Parent: DirectoryTarget{NodeID: 2}, RawLeaf: []byte("right")}
			c.Target = ChildCondition{State: Absent}
		},
	} {
		t.Run(name, func(t *testing.T) {
			command := guardedRename(t)
			change(&command)
			if err := command.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid coverage = %v", err)
			}
		})
	}
}

func TestNameCommandGuardsApplyToEveryKindAndOnlyRenameChecksDescendants(t *testing.T) {
	for _, kind := range []NameOperation{NameCreate, NameMkdir, NameRemove, NameRemoveDir, NameRename, NameSymlink} {
		command := NameCommand{
			Kind: kind, Action: validFileAction(t), Name: ChildName{Parent: DirectoryTarget{NodeID: 1}, RawLeaf: []byte("leaf")},
			Target: ChildCondition{State: Absent},
			Guards: &NamespaceGuards{RootID: 1, Directories: []DirectoryObservation{{ParentID: 1, Revision: []byte{1}}}},
		}
		if kind == NameRemove || kind == NameRemoveDir || kind == NameRename {
			command.Target = ChildCondition{State: SameNode, NodeID: 2}
			command.Guards.Edges = []ObservedEdge{{ParentID: 1, RawLeaf: []byte("leaf"), ChildID: 2}}
		}
		if kind == NameRename {
			command.Destination = &RenameTarget{
				Parent: command.Name.Parent, ObservedLeaf: []byte("other"), OutputLeaf: []byte("other"), Expected: ChildCondition{State: Absent},
			}
		}
		if kind == NameSymlink {
			command.Initial.LinkTarget = []byte("target")
		}
		if err := command.Check(); err != nil {
			t.Fatalf("guarded kind %d = %v", kind, err)
		}
		command.RequireNoOpenDescendants = true
		err := command.Check()
		if kind == NameRename && err != nil || kind != NameRename && !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("descendant condition on kind %d = %v", kind, err)
		}
	}
}

func chainNameCommand(t *testing.T, depth int, leafBytes int) NameCommand {
	t.Helper()
	guards := &NamespaceGuards{RootID: 1}
	for i := 1; i <= depth; i++ {
		guards.Directories = append(guards.Directories, DirectoryObservation{ParentID: uint64(i), Revision: []byte{1}})
		guards.Edges = append(guards.Edges, ObservedEdge{ParentID: uint64(i), RawLeaf: bytes.Repeat([]byte{'x'}, leafBytes), ChildID: uint64(i + 1)})
	}
	return NameCommand{
		Kind: NameRemove, Action: validFileAction(t),
		Name:   ChildName{Parent: DirectoryTarget{NodeID: uint64(depth)}, RawLeaf: bytes.Clone(guards.Edges[depth-1].RawLeaf)},
		Target: ChildCondition{State: SameNode, NodeID: uint64(depth + 1)}, Guards: guards,
	}
}

func TestNameCommandGuardsUseOneCountAndByteBudget(t *testing.T) {
	counts := chainNameCommand(t, MaxNamespaceGuards, 1)
	if err := counts.Check(); err != nil {
		t.Fatalf("maximum directory and edge counts = %v", err)
	}
	if err := chainNameCommand(t, MaxNamespaceGuards+1, 1).Check(); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("excess union count = %v", err)
	}
	bytesLimit := chainNameCommand(t, 16, MaxLeafBytes)
	last := &bytesLimit.Guards.Edges[15]
	last.RawLeaf = bytes.Repeat([]byte{'x'}, MaxNamespaceGuardBytes-16*9-15*(MaxLeafBytes+16)-16)
	bytesLimit.Name.RawLeaf = bytes.Clone(last.RawLeaf)
	if err := bytesLimit.Check(); err != nil {
		t.Fatalf("maximum union bytes = %v", err)
	}
	last.RawLeaf = append(last.RawLeaf, 'x')
	bytesLimit.Name.RawLeaf = bytes.Clone(last.RawLeaf)
	if err := bytesLimit.Check(); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("excess union byte = %v", err)
	}
}

func TestAtomicDeleteResetRetainsActualDataPermissions(t *testing.T) {
	base := OpenAtOptions{
		Action: validFileAction(t), Existing: ResetContent, ResetUse: DeleteName,
		Target: ChildCondition{State: SameNode, NodeID: 2}, Use: UseClaim{Uses: DeleteName},
	}
	for _, data := range []Uses{0, ReadData, WriteData, ReadData | WriteData} {
		options := base
		options.Read, options.Write = data&ReadData != 0, data&WriteData != 0
		options.Use.Uses |= data
		if err := options.Check(); err != nil {
			t.Fatalf("delete reset with actual data uses %d = %v", data, err)
		}
	}
	for name, change := range map[string]func(*OpenAtOptions){
		"read reset use":         func(o *OpenAtOptions) { o.ResetUse = ReadData },
		"write reset use":        func(o *OpenAtOptions) { o.ResetUse = WriteData },
		"combined reset use":     func(o *OpenAtOptions) { o.ResetUse = DeleteName | WriteData },
		"non-reset effect":       func(o *OpenAtOptions) { o.Existing = Keep },
		"create":                 func(o *OpenAtOptions) { o.Create = true },
		"any target":             func(o *OpenAtOptions) { o.Target = ChildCondition{State: Any} },
		"absent target":          func(o *OpenAtOptions) { o.Target = ChildCondition{State: Absent} },
		"fake read grant":        func(o *OpenAtOptions) { o.Read = true },
		"fake write grant":       func(o *OpenAtOptions) { o.Write = true },
		"ordinary no-data reset": func(o *OpenAtOptions) { o.ResetUse = 0 },
		"ordinary read-only reset with delete claim": func(o *OpenAtOptions) {
			o.ResetUse, o.Read, o.Use.Uses = 0, true, ReadData|DeleteName
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := base
			change(&options)
			if err := options.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid reset = %v", err)
			}
		})
	}
	missingDelete := base
	missingDelete.Read, missingDelete.Use.Uses = true, ReadData
	if err := missingDelete.Check(); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("missing delete use = %v", err)
	}
}
