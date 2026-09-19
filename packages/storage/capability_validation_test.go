package storage

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestCapabilityValuesRejectMalformedIdentitiesAndBounds(t *testing.T) {
	validScope := UseScope{Token: "reference"}
	for _, s := range []UseScope{{}, {Token: strings.Repeat("x", MaxScopeBytes+1)}, {Token: "bad\x00scope"}} {
		if s.Check() == nil {
			t.Fatalf("invalidscope%+v", s)
		}
	}
	if validScope.Check() != nil {
		t.Fatal("validscope refused")
	}
	for _, c := range []UseClaim{{}, {Uses: AllUses, Deny: AllUses}} {
		if c.Check() != nil {
			t.Fatal("validuses refused")
		}
	}
	if (UseClaim{Uses: 128}).Check() == nil {
		t.Fatal("unknownuses accepted")
	}
	for _, c := range []ChildCondition{{State: Any}, {State: Absent}, {State: SameNode, NodeID: 3}} {
		if c.Check() != nil {
			t.Fatal("validcondition refused")
		}
	}
	for _, c := range []ChildCondition{{}, {State: Any, NodeID: 2}, {State: SameNode}, {State: 4}} {
		if c.Check() == nil {
			t.Fatal("invalidcondition accepted")
		}
	}
	if (DirectoryTarget{NodeID: 1, Scope: &validScope}).Check() != nil {
		t.Fatal("validdirectory refused")
	}
	for _, target := range []DirectoryTarget{{}, {NodeID: 1, Scope: &UseScope{}}} {
		if target.Check() == nil {
			t.Fatal("invaliddirectory accepted")
		}
	}
	for _, name := range [][]byte{nil, []byte("."), []byte(".."), []byte("x/y"), {'x', 0}, bytes.Repeat([]byte{'a'}, MaxLeafBytes+1)} {
		if CheckLeaf(name) == nil {
			t.Fatalf("invalidleaf%q", name)
		}
	}
	if CheckLeaf([]byte{0xff, ':', '\\'}) != nil {
		t.Fatal("platform name policy entered neutral validation")
	}
	if (ChildName{Parent: DirectoryTarget{NodeID: 1}, RawLeaf: []byte("x")}).Check() != nil || (ChildName{}).Check() == nil {
		t.Fatal("child validation failed")
	}
	for _, d := range []DirectoryObservation{{}, {ParentID: 1}, {ParentID: 1, Revision: make([]byte, MaxObservationTokenBytes+1)}} {
		if d.Check() == nil {
			t.Fatal("invalidobservation accepted")
		}
	}
	for _, o := range []OwnerOptions{{Lifetime: OwnerReference}, {Lifetime: OwnerExplicit, Group: 19}} {
		if o.Check() != nil {
			t.Fatal("validowner refused")
		}
	}
	if (OwnerOptions{}).Check() == nil {
		t.Fatal("invalidowner accepted")
	}
	for _, test := range []struct {
		name          string
		version, data []byte
		valid         bool
	}{{"a", nil, nil, true}, {"a", []byte{1}, []byte{0xff}, true}, {"bad key", nil, nil, false}, {"a", make([]byte, 65), nil, false}, {"a", nil, make([]byte, MaxMetadataValueBytes+1), false}} {
		if (CheckMetadataUpdate(test.name, test.version, test.data) == nil) != test.valid {
			t.Fatal("metadata update bound failed")
		}
	}
	for _, err := range []error{ErrUseConflict, ErrRangeConflict, ErrPendingDelete, ErrConditionConflict, ErrInvalidScope} {
		if err.Error() == "" || ErrnoOf(err) == syscall.EIO {
			t.Fatalf("lost neutral conflictclassification:%v", err)
		}
	}
}

func TestNamespaceGuardsCheckCompleteBoundedEdges(t *testing.T) {
	valid := NamespaceGuards{RootID: 1, Directories: []DirectoryObservation{{ParentID: 1, Revision: []byte{1}}, {ParentID: 2, Revision: []byte{2}}}, Edges: []ObservedEdge{{ParentID: 1, RawLeaf: []byte("dir"), ChildID: 2}}}
	if (*NamespaceGuards)(nil).Check() != nil || valid.Check() != nil {
		t.Fatal("validguards refused")
	}
	for _, g := range []NamespaceGuards{
		{Directories: make([]DirectoryObservation, MaxNamespaceGuards+1)}, {Edges: make([]ObservedEdge, MaxNamespaceGuards+1)}, {Directories: []DirectoryObservation{{}}},
		{Directories: []DirectoryObservation{{ParentID: 1, Revision: []byte{1}}, {ParentID: 1, Revision: []byte{2}}}},
		{Edges: []ObservedEdge{{ParentID: 1, ChildID: 1, RawLeaf: []byte("x")}}}, {Edges: []ObservedEdge{{ParentID: 1, ChildID: 2, RawLeaf: []byte("..")}}},
		{Edges: []ObservedEdge{{ParentID: 1, ChildID: 2, RawLeaf: []byte("x")}, {ParentID: 3, ChildID: 2, RawLeaf: []byte("y")}}},
		{Edges: []ObservedEdge{{ParentID: 1, ChildID: 2, RawLeaf: []byte("x")}, {ParentID: 2, ChildID: 1, RawLeaf: []byte("y")}}},
		{RootID: 4, Edges: valid.Edges}, {RootID: 1, Directories: []DirectoryObservation{{ParentID: 3, Revision: []byte{1}}}},
	} {
		if g.Check() == nil {
			t.Fatalf("invalidguards accepted:%+v", g)
		}
	}
	huge := NamespaceGuards{}
	for i := 0; i < 17; i++ {
		name := bytes.Repeat([]byte{'x'}, 4096)
		name[0] = byte('a' + i)
		huge.Edges = append(huge.Edges, ObservedEdge{ParentID: 1, ChildID: uint64(i + 2), RawLeaf: name})
	}
	if !errors.Is(huge.Check(), syscall.EFBIG) {
		t.Fatal("guard bytes unbounded")
	}
}

func TestAtomicOpenAndNodeReferenceValidateActualIntent(t *testing.T) {
	base := OpenAtOptions{Read: true, Target: ChildCondition{State: Any}, Existing: Keep, Use: UseClaim{Uses: ReadData}}
	create := base
	create.Create = true
	create.Initial.OnCreate.Metadata = map[string][]byte{"owner": {1}}
	reset := base
	reset.Write = true
	reset.Use.Uses |= WriteData
	reset.Existing = ResetContent
	replace := reset
	replace.Create = true
	replace.Use.Uses |= DeleteName
	replace.Existing = ReplaceNode
	close := base
	close.Use.Uses |= DeleteName
	close.CloseIntent = &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile}
	for _, o := range []OpenAtOptions{base, create, reset, replace, close} {
		if err := o.Check(); err != nil {
			t.Fatalf("validopen%+v=%v", o, err)
		}
	}
	for _, change := range []func(*OpenAtOptions){func(o *OpenAtOptions) { o.Read = false }, func(o *OpenAtOptions) { o.Use.Uses = 0 }, func(o *OpenAtOptions) { o.Use.Uses |= ReadEntries }, func(o *OpenAtOptions) { o.Exclusive = true }, func(o *OpenAtOptions) { o.Existing = 0 }, func(o *OpenAtOptions) { o.Existing = ResetContent }, func(o *OpenAtOptions) { o.Existing = ReplaceNode }, func(o *OpenAtOptions) { o.Target = ChildCondition{} }, func(o *OpenAtOptions) { o.Guards = &NamespaceGuards{Directories: []DirectoryObservation{{}}} }, func(o *OpenAtOptions) { o.Use.Deny = 128 }, func(o *OpenAtOptions) { o.Initial.OnCreate.LinkTarget = []byte("x") }, func(o *OpenAtOptions) {
		o.Create = true
		o.Initial.OnCreate.Metadata = map[string][]byte{"bad key": nil}
	}, func(o *OpenAtOptions) { o.Initial.OnReset.Metadata = map[string][]byte{"a": nil} }, func(o *OpenAtOptions) { o.CloseIntent = &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile} }} {
		o := base
		change(&o)
		if o.Check() == nil {
			t.Fatalf("invalidopen accepted:%+v", o)
		}
	}
	node := NodeRefOptions{Kind: NodeDirectory, Target: ChildCondition{State: Any}, Use: UseClaim{Uses: ReadEntries}, MetadataAccess: ReadMetadata}
	link := NodeRefOptions{Kind: NodeSymlink, Target: ChildCondition{State: Absent}, Create: true, InitialState: InitialState{OnCreate: InitialFields{LinkTarget: []byte{0xff, 'x'}}}}
	for _, o := range []NodeRefOptions{node, link, {Kind: NodeRegular, Target: ChildCondition{State: SameNode, NodeID: 3}, Use: UseClaim{Uses: DeleteName}, CloseIntent: &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile}}} {
		if o.Check() != nil {
			t.Fatalf("validnoderef refused:%+v", o)
		}
	}
	for _, change := range []func(*NodeRefOptions){func(o *NodeRefOptions) { o.Kind = 0 }, func(o *NodeRefOptions) { o.Target = ChildCondition{} }, func(o *NodeRefOptions) { o.Guards = &NamespaceGuards{Edges: []ObservedEdge{{}}} }, func(o *NodeRefOptions) { o.Use.Uses = 128 }, func(o *NodeRefOptions) { o.MetadataAccess = 4 }, func(o *NodeRefOptions) { o.Exclusive = true }, func(o *NodeRefOptions) { o.InitialState.OnCreate.LinkTarget = []byte("x") }, func(o *NodeRefOptions) { o.InitialState.OnReset.Metadata = map[string][]byte{"a": nil} }, func(o *NodeRefOptions) {
		o.Create = true
		o.InitialState.OnCreate.Metadata = map[string][]byte{"bad key": nil}
	}, func(o *NodeRefOptions) {
		o.CloseIntent = &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile}
	}} {
		o := node
		change(&o)
		if o.Check() == nil {
			t.Fatalf("invalidnoderef accepted:%+v", o)
		}
	}
	if (InitialFields{LinkTarget: make([]byte, MaxLinkTargetBytes+1)}).Check() == nil {
		t.Fatal("linktargetunbounded")
	}
}

func TestNamespaceAndDeleteCommandsKeepTaggedEffects(t *testing.T) {
	child := ChildName{Parent: DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
	condition := ChildCondition{State: SameNode, NodeID: 2}
	uses := []TargetUse{{NodeID: 2, Scope: UseScope{Token: "ref"}}}
	rename := RenameTarget{Parent: DirectoryTarget{NodeID: 1}, ObservedLeaf: []byte("old"), Expected: ChildCondition{State: Any}, OutputLeaf: []byte("NEW")}
	for _, command := range []NameCommand{{Kind: NameCreate, Name: child, Target: ChildCondition{State: Absent}}, {Kind: NameMkdir, Name: child, Target: ChildCondition{State: Absent}}, {Kind: NameSymlink, Name: child, Target: ChildCondition{State: Absent}, Initial: InitialFields{LinkTarget: []byte("target")}}, {Kind: NameRemove, Name: child, Target: condition, Uses: uses}, {Kind: NameRemoveDir, Name: child, Target: condition}, {Kind: NameRename, Name: child, Target: condition, Destination: &rename}} {
		if err := command.Check(); err != nil {
			t.Fatalf("validcommand:%+v %v", command, err)
		}
	}
	for _, command := range []NameCommand{{}, {Kind: NameRemove, Name: child}, {Kind: NameRename, Name: child, Target: condition}, {Kind: NameRemove, Name: child, Target: condition, Destination: &rename}, {Kind: NameSymlink, Name: child, Target: condition}, {Kind: NameCreate, Name: child, Target: condition, Initial: InitialFields{LinkTarget: []byte("bad")}}, {Kind: NameRemove, Name: child, Target: condition, Initial: InitialFields{Metadata: map[string][]byte{"a": nil}}}, {Kind: NameRemove, Name: child, Target: condition, Guards: &NamespaceGuards{Edges: []ObservedEdge{{}}}}, {Kind: NameRemove, Name: child, Target: condition, Uses: []TargetUse{{}}}} {
		if command.Check() == nil {
			t.Fatalf("invalidcommand accepted:%+v", command)
		}
	}
	for _, r := range []RenameTarget{{}, {Parent: child.Parent, ObservedLeaf: []byte(".."), Expected: condition, OutputLeaf: []byte("new")}, {Parent: child.Parent, ObservedLeaf: []byte("old"), Expected: condition}} {
		if r.Check() == nil {
			t.Fatal("invalidrename accepted")
		}
	}
	if CheckTargetUses(uses) != nil {
		t.Fatal("validscopes rejected")
	}
	for _, bad := range [][]TargetUse{make([]TargetUse, MaxTargetUses+1), {{NodeID: 0, Scope: UseScope{Token: "x"}}}, {{NodeID: 1, Scope: UseScope{}}}, {{NodeID: 2, Scope: UseScope{Token: "x"}}, {NodeID: 2, Scope: UseScope{Token: "y"}}}} {
		if CheckTargetUses(bad) == nil {
			t.Fatal("invalidscopes accepted")
		}
	}
	if (CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkIfEmpty, Uses: uses}).Check() != nil || (PendingUnlinkCommand{Condition: UnlinkFile, Uses: uses}).Check() != nil || (ClearPendingUnlinkCommand{Generation: []byte{1}, Uses: uses}).Check() != nil {
		t.Fatal("validdeletion refused")
	}
	for _, bad := range []CloseIntent{{}, {Trigger: Now, Condition: UnlinkFile}, {Trigger: OnReferenceClose, Condition: UnlinkFile, Guards: &NamespaceGuards{Directories: []DirectoryObservation{{}}}}} {
		if bad.Check() == nil {
			t.Fatal("invalidcloseintent accepted")
		}
	}
	for _, bad := range []PendingUnlinkCommand{{}, {Condition: UnlinkFile, Guards: &NamespaceGuards{Edges: []ObservedEdge{{}}}}} {
		if bad.Check() == nil {
			t.Fatal("invalidpending accepted")
		}
	}
	for _, bad := range []ClearPendingUnlinkCommand{{}, {Generation: make([]byte, 65)}, {Generation: []byte{1}, Guards: &NamespaceGuards{Edges: []ObservedEdge{{}}}}} {
		if bad.Check() == nil {
			t.Fatal("invalidpendingclear accepted")
		}
	}
}

func TestConditionalMutationAndDirectoryResultsAreBounded(t *testing.T) {
	for _, c := range []FileMutation{{Kind: MutateTruncate, Size: 3}, {Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"a": {Version: []byte{1}, Data: []byte{2}}}}} {
		if c.Check() != nil {
			t.Fatal("validmutation refused")
		}
	}
	for _, c := range []FileMutation{{}, {Kind: MutateTruncate, Size: -1}, {Kind: MutateAttributes, Size: 1}, {Kind: MutateTruncate, Metadata: map[string]OpaquePayload{"a": {}}}, {Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"bad key": {}}}, {Kind: MutateAttributes, Guards: &NamespaceGuards{Edges: []ObservedEdge{{}}}}} {
		if c.Check() == nil {
			t.Fatal("invalidmutation accepted")
		}
	}
	valid := ObservedDirectory{Observation: DirectoryObservation{ParentID: 1, Revision: []byte{1}}, Entries: []ObservedEntry{{RawLeaf: []byte{0xff}, Attr: Attr{ID: 2, Kind: NodeRegular}}}}
	if valid.Check() != nil {
		t.Fatal("validdirectory observation refused")
	}
	for _, d := range []ObservedDirectory{{}, {Observation: valid.Observation, Entries: make([]ObservedEntry, MaxDirectoryEntries+1)}, {Observation: valid.Observation, Entries: []ObservedEntry{{RawLeaf: []byte("..")}}}, {Observation: valid.Observation, Entries: []ObservedEntry{{RawLeaf: []byte("x"), Attr: Attr{ID: 1, Kind: NodeDirectory}}}}, {Observation: valid.Observation, Entries: append(valid.Entries, valid.Entries...)}, {Observation: valid.Observation, Entries: []ObservedEntry{{RawLeaf: []byte("x"), Attr: Attr{ID: 2, Kind: NodeRegular, Metadata: map[string]OpaquePayload{"a": {}}}}}}} {
		if d.Check() == nil {
			t.Fatal("invaliddirectory result accepted")
		}
	}
	for _, lengths := range [][2]int64{{0, 6}, {MaxLeafBytes + 1, 6}, {1, 5}, {1, MaxMetadataBytes + 1}} {
		if _, err := ObservedEntryBytes(lengths[0], lengths[1]); err == nil {
			t.Fatal("invaliddirectory charge accepted")
		}
	}
	if charge, err := ObservedEntryBytes(1, 6); err != nil || charge <= 7 {
		t.Fatalf("directory charge=%d %v", charge, err)
	}
}

func TestConditionalContentCommandsSeparatePredicatesFromEffects(t *testing.T) {
	size := int64(4)
	valid := []FileMutation{
		{Kind: MutateWriteAt, Offset: 2, Data: []byte("xy"), ExpectedSize: &size, ExpectedMetadata: map[string][]byte{"a": {1}, "absent": nil}},
		{Kind: MutateAppend, Data: []byte("xy"), ExpectedSize: &size},
		{Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"new.namespace": {Data: []byte{1}}}},
	}
	for _, c := range valid {
		if err := c.CheckDataLimit(2); err != nil {
			t.Fatalf("valid conditional command %+v: %v", c, err)
		}
	}
	negative := int64(-1)
	for _, c := range []FileMutation{
		{Kind: MutateWriteAt, Offset: -1}, {Kind: MutateWriteAt, Offset: 1<<63 - 1, Data: []byte{1}},
		{Kind: MutateAppend, Offset: 1}, {Kind: MutateWriteAt, Size: 1}, {Kind: MutateAttributes, Data: []byte{1}}, {Kind: MutateTruncate, Data: []byte{1}},
		{Kind: MutateAppend, ExpectedSize: &negative}, {Kind: MutateAppend, ExpectedMetadata: map[string][]byte{"bad key": nil}},
		{Kind: MutateAppend, ExpectedMetadata: map[string][]byte{"a": make([]byte, 65)}},
		{Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"a": {Data: make([]byte, MaxMetadataValueBytes+1)}}},
	} {
		if c.Check() == nil {
			t.Fatalf("invalid conditional content command accepted:%+v", c)
		}
	}
	if !errors.Is(valid[0].CheckDataLimit(1), syscall.EFBIG) || !errors.Is(valid[0].CheckDataLimit(0), syscall.EINVAL) {
		t.Fatal("conditional byte budget ignored")
	}
	many := map[string][]byte{}
	for i := 0; i <= MaxMetadataNamespaces; i++ {
		many[strings.Repeat("x", i+1)] = nil
	}
	if (FileMutation{Kind: MutateAppend, ExpectedMetadata: many}).Check() == nil {
		t.Fatal("predicate count unbounded")
	}
}

func TestOpenMetadataConditionsRequireObservedIdentity(t *testing.T) {
	for _, target := range []ChildCondition{{State: Any}, {State: Absent}, {State: SameNode, NodeID: 7}} {
		for _, expected := range []map[string][]byte{nil, {}, {"test.attributes": nil}, {"test.attributes": {0, 0xff, 1}}} {
			open := OpenAtOptions{Read: true, Create: true, Existing: Keep, Target: target,
				Use: UseClaim{Uses: ReadData}, ExpectedMetadata: expected}
			node := NodeRefOptions{Kind: NodeRegular, Create: true, Target: target, ExpectedMetadata: expected}
			var want error
			if len(expected) != 0 && target.State != SameNode {
				want = syscall.EINVAL
			}
			for kind, err := range map[string]error{"file": open.Check(), "node": node.Check()} {
				if !errors.Is(err, want) {
					t.Fatalf("%s target %+v conditions %v: got %v, want %v", kind, target, expected, err, want)
				}
			}
		}
	}
}

func TestMetadataConditionBoundsMatchAcrossAdmissions(t *testing.T) {
	maximum := make(map[string][]byte, MaxMetadataNamespaces)
	for i := 0; i < MaxMetadataNamespaces; i++ {
		maximum[strings.Repeat("x", MaxMetadataNamespaceBytes-i)] = bytes.Repeat([]byte{0xff}, MaxObservationTokenBytes)
	}
	tooMany := CloneInitialMetadata(maximum)
	tooMany["extra"] = nil
	for _, test := range []struct {
		name     string
		expected map[string][]byte
		want     error
	}{
		{name: "maximum", expected: maximum},
		{name: "absent", expected: map[string][]byte{"test.attributes": nil}},
		{name: "empty token", expected: map[string][]byte{"test.attributes": {}}},
		{name: "count", expected: tooMany, want: syscall.EFBIG},
		{name: "empty namespace", expected: map[string][]byte{"": nil}, want: syscall.EINVAL},
		{name: "invalid namespace", expected: map[string][]byte{"bad key": nil}, want: syscall.EINVAL},
		{name: "long namespace", expected: map[string][]byte{strings.Repeat("x", MaxMetadataNamespaceBytes+1): nil}, want: syscall.EINVAL},
		{name: "long version", expected: map[string][]byte{"test.attributes": make([]byte, MaxObservationTokenBytes+1)}, want: syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := ChildCondition{State: SameNode, NodeID: 7}
			open := OpenAtOptions{Read: true, Existing: Keep, Target: target, Use: UseClaim{Uses: ReadData}, ExpectedMetadata: test.expected}
			node := NodeRefOptions{Kind: NodeRegular, Target: target, ExpectedMetadata: test.expected}
			mutation := FileMutation{Kind: MutateAttributes, ExpectedMetadata: test.expected}
			for kind, err := range map[string]error{"file": open.Check(), "node": node.Check(), "mutation": mutation.Check()} {
				if !errors.Is(err, test.want) {
					t.Fatalf("%s: got %v, want %v", kind, err, test.want)
				}
			}
		})
	}
}

func TestOpenMetadataConditionsRemainIndependentOfEffects(t *testing.T) {
	expected := map[string][]byte{"test.attributes": {0, 0xff, 1}, "test.absent": nil}
	for _, effect := range []ExistingEffect{Keep, ResetContent, ReplaceNode} {
		open := OpenAtOptions{Read: true, Write: true, Create: true, Existing: effect,
			Target: ChildCondition{State: SameNode, NodeID: 7}, Use: UseClaim{Uses: ReadData | WriteData | DeleteName},
			ExpectedMetadata: CloneInitialMetadata(expected), CloseIntent: &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile}}
		fields := InitialFields{Metadata: map[string][]byte{"test.attributes": []byte("new payload"), "test.absent": []byte("created namespace")}}
		switch effect {
		case Keep:
			open.Initial.OnCreate = fields
		case ResetContent:
			open.Initial.OnReset = fields
		case ReplaceNode:
			open.Initial.OnReplace = fields
		}
		if err := open.Check(); err != nil {
			t.Fatalf("effect %v: %v", effect, err)
		}
		if !bytes.Equal(open.ExpectedMetadata["test.attributes"], expected["test.attributes"]) {
			t.Fatal("validation rewrote an expected token using initial metadata")
		}
		if value, ok := open.ExpectedMetadata["test.absent"]; !ok || value != nil {
			t.Fatal("validation lost the namespace-absence condition")
		}
		open.ExpectedMetadata["test.attributes"][0] = 9
		delete(open.ExpectedMetadata, "test.absent")
		if expected["test.attributes"][0] != 0 || len(expected) != 2 {
			t.Fatal("owned option conditions alias the observed metadata")
		}
	}
	node := NodeRefOptions{Kind: NodeRegular, Target: ChildCondition{State: SameNode, NodeID: 7},
		ExpectedMetadata: expected, Use: UseClaim{Uses: DeleteName},
		CloseIntent: &CloseIntent{Trigger: OnReferenceClose, Condition: UnlinkFile}}
	if err := node.Check(); err != nil {
		t.Fatalf("metadata-only close-intent admission: %v", err)
	}
}

func TestNodeReferenceClaimsRemainIndependentOfMethodPermissions(t *testing.T) {
	for _, kind := range []NodeKind{NodeRegular, NodeDirectory, NodeSymlink} {
		for _, uses := range []Uses{ReadData, WriteData, ReadData | WriteData, DeleteName} {
			options := NodeRefOptions{Kind: kind, Target: ChildCondition{State: Any}, Use: UseClaim{Uses: uses, Deny: ReadData | WriteData | ReadEntries}}
			if err := options.Check(); err != nil {
				t.Fatalf("kind %v claims %v without metadata permission: %v", kind, uses, err)
			}
		}
		options := NodeRefOptions{Kind: kind, Target: ChildCondition{State: Any}, Use: UseClaim{Uses: ReadEntries}}
		if err := options.Check(); (err == nil) != (kind == NodeDirectory) {
			t.Fatalf("enumeration claim kind %v: %v", kind, err)
		}
		for _, claim := range []UseClaim{{Uses: 128}, {Deny: 128}} {
			options.Use = claim
			if err := options.Check(); err == nil {
				t.Fatalf("unknown claim accepted: %+v", options)
			}
		}
	}
}
