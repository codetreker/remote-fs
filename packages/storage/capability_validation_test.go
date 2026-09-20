package storage

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
	"testing"
)

func validFileAction(t *testing.T) FileActionID {
	t.Helper()
	action, err := NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

const validDeleteIntent DeleteIntentID = "0123456789abcdef0123456789abcdef"

func TestFileActionAndDurableDeleteIdentitiesAreBounded(t *testing.T) {
	action := validFileAction(t)
	if epoch, err := action.Epoch(); err != nil || epoch != 1 {
		t.Fatalf("file action epoch = %d, %v", epoch, err)
	}
	for _, invalid := range []FileActionID{"", "0:00000000000000000000000000000000", "1:short"} {
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid file action accepted: %q", invalid)
		}
	}
	generated, err := NewDeleteIntentID()
	if err != nil || generated.Check() != nil || generated == validDeleteIntent {
		t.Fatalf("generated delete intent = %q, %v", generated, err)
	}
	for _, intent := range []DeleteIntentID{validDeleteIntent, generated} {
		if err := intent.Check(); err != nil {
			t.Fatalf("valid delete intent %q: %v", intent, err)
		}
	}
	for _, invalid := range []DeleteIntentID{"", "0123456789ABCDEF0123456789ABCDEF", "0123456789abcdef0123456789abcdeg", DeleteIntentID(strings.Repeat("x", DeleteIntentIDBytes+1))} {
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid delete intent accepted: %q", invalid)
		}
	}
	for _, outcome := range []FileActionOutcome{FileActionPending, FileActionCompleted, FileActionNotExecuted, FileActionUnknown, FileActionRetired} {
		receipt := FileActionReceipt{Action: action, Operation: OpFileMutateName, Outcome: outcome}
		if err := receipt.Check(); err != nil {
			t.Fatalf("valid file action receipt %+v: %v", receipt, err)
		}
	}
	if (FileActionReceipt{Action: action, Operation: OpFileRead, Outcome: FileActionCompleted}).Check() == nil {
		t.Fatal("receipt for a non-action operation accepted")
	}
	for _, outcome := range []FileActionOutcome{FileActionNotExecuted, FileActionUnknown, FileActionRetired} {
		if err := (FileActionReceipt{Action: action, Outcome: outcome}).Check(); err != nil {
			t.Fatalf("truthful receipt without a retained operation rejected: %v", err)
		}
	}
	for _, outcome := range []FileActionOutcome{FileActionPending, FileActionCompleted} {
		if (FileActionReceipt{Action: action, Outcome: outcome}).Check() == nil {
			t.Fatalf("outcome %v accepted without a retained operation", outcome)
		}
	}
	for _, outcome := range []DeleteIntentOutcome{DeleteIntentArmed, DeleteIntentPending, DeleteIntentCompleted, DeleteIntentNotExecuted} {
		status := DeleteIntentStatus{ID: validDeleteIntent, NodeID: 9, Outcome: outcome}
		if err := status.Check(); err != nil {
			t.Fatalf("valid delete intent status %+v: %v", status, err)
		}
	}
	failed := DeleteIntentStatus{ID: validDeleteIntent, NodeID: 9, Outcome: DeleteIntentCleanupFailed, Failure: syscall.EIO}
	if err := failed.Check(); err != nil {
		t.Fatalf("valid cleanup failure: %v", err)
	}
	for _, outcome := range []DeleteIntentOutcome{DeleteIntentUnknown, DeleteIntentRetired} {
		if err := (DeleteIntentStatus{ID: validDeleteIntent, Outcome: outcome}).Check(); err != nil {
			t.Fatalf("valid missing intent status %v: %v", outcome, err)
		}
	}
	for _, invalid := range []DeleteIntentStatus{
		{ID: validDeleteIntent, Outcome: DeleteIntentCompleted},
		{ID: validDeleteIntent, NodeID: 9, Outcome: DeleteIntentUnknown},
		{ID: validDeleteIntent, NodeID: 9, Outcome: DeleteIntentCleanupFailed},
		{ID: validDeleteIntent, NodeID: 9, Outcome: DeleteIntentCompleted, Failure: syscall.EIO},
		{ID: validDeleteIntent, NodeID: 9, Outcome: DeleteIntentCleanupFailed, Failure: syscall.Errno(255)},
	} {
		if invalid.Check() == nil {
			t.Fatalf("invalid delete intent status accepted: %+v", invalid)
		}
	}
	ack := AcknowledgeDeleteIntentCommand{Action: action, Intent: validDeleteIntent}
	if err := ack.Check(); err != nil {
		t.Fatalf("valid delete intent acknowledgement: %v", err)
	}
	if (AcknowledgeDeleteIntentCommand{Intent: validDeleteIntent}).Check() == nil || (AcknowledgeDeleteIntentCommand{Action: action}).Check() == nil {
		t.Fatal("incomplete delete intent acknowledgement accepted")
	}
}

func TestIdentityParentedValuesRejectMalformedInputs(t *testing.T) {
	scope := UseScope{Token: "reference"}
	for _, condition := range []ChildCondition{{State: Any}, {State: Absent}, {State: SameNode, NodeID: 3}} {
		if err := condition.Check(); err != nil {
			t.Fatalf("valid condition %+v: %v", condition, err)
		}
	}
	for _, condition := range []ChildCondition{{}, {State: Any, NodeID: 2}, {State: SameNode}, {State: 4}} {
		if !errors.Is(condition.Check(), syscall.EINVAL) {
			t.Fatalf("invalid condition accepted: %+v", condition)
		}
	}
	if err := (DirectoryTarget{NodeID: 1, Scope: &scope}).Check(); err != nil {
		t.Fatalf("valid directory target: %v", err)
	}
	for _, target := range []DirectoryTarget{{}, {NodeID: 1, Scope: &UseScope{}}} {
		if !errors.Is(target.Check(), syscall.EINVAL) {
			t.Fatalf("invalid directory target accepted: %+v", target)
		}
	}
	for _, name := range [][]byte{nil, []byte("."), []byte(".."), []byte("x/y"), {'x', 0}} {
		if !errors.Is(CheckLeaf(name), syscall.EINVAL) {
			t.Fatalf("invalid leaf accepted: %q", name)
		}
	}
	if !errors.Is(CheckLeaf(bytes.Repeat([]byte{'a'}, MaxLeafBytes+1)), syscall.ENAMETOOLONG) {
		t.Fatal("oversized leaf did not return ENAMETOOLONG")
	}
	if err := CheckLeaf([]byte{0xff, ':', '\\'}); err != nil {
		t.Fatalf("platform name policy entered neutral validation: %v", err)
	}
	if err := (ChildName{Parent: DirectoryTarget{NodeID: 1}, RawLeaf: []byte("x")}).Check(); err != nil {
		t.Fatalf("valid child: %v", err)
	}
}

func TestAtomicOpenAndNodeReferenceValidateEffects(t *testing.T) {
	base := OpenAtOptions{
		Read: true, Target: ChildCondition{State: Any}, Existing: Keep, Action: validFileAction(t),
		Use: UseClaim{Uses: ReadData},
	}
	create := base
	create.Create = true
	create.Action = validFileAction(t)
	create.Initial.OnCreate.Metadata = map[string][]byte{"owner": {1}}
	reset := base
	reset.Write = true
	reset.Use.Uses |= WriteData
	reset.Existing = ResetContent
	reset.Action = validFileAction(t)
	replace := reset
	replace.Create = true
	replace.Use.Uses |= DeleteName
	replace.Existing = ReplaceNode
	closeIntent := base
	closeIntent.Action = validFileAction(t)
	closeIntent.Use.Uses |= DeleteName
	closeIntent.CloseIntent = &CloseIntent{ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkFile}
	for _, options := range []OpenAtOptions{base, create, reset, replace, closeIntent} {
		if err := options.Check(); err != nil {
			t.Fatalf("valid open %+v: %v", options, err)
		}
	}

	invalid := []func(*OpenAtOptions){
		func(o *OpenAtOptions) { o.Read = false },
		func(o *OpenAtOptions) { o.Use.Uses = 0 },
		func(o *OpenAtOptions) { o.Use.Uses |= ReadEntries },
		func(o *OpenAtOptions) { o.Exclusive = true },
		func(o *OpenAtOptions) { o.Existing = 0 },
		func(o *OpenAtOptions) { o.Existing = ResetContent },
		func(o *OpenAtOptions) { o.Existing = ReplaceNode },
		func(o *OpenAtOptions) { o.Target = ChildCondition{} },
		func(o *OpenAtOptions) { o.Use.Deny = 128 },
		func(o *OpenAtOptions) { o.Initial.OnCreate.LinkTarget = []byte("x") },
		func(o *OpenAtOptions) { o.Initial.OnReset.Metadata = map[string][]byte{"a": nil} },
		func(o *OpenAtOptions) {
			o.CloseIntent = &CloseIntent{ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkFile}
		},
	}
	for _, change := range invalid {
		options := base
		change(&options)
		if options.Check() == nil {
			t.Fatalf("invalid open accepted: %+v", options)
		}
	}

	directory := NodeRefOptions{
		Kind: NodeDirectory, Target: ChildCondition{State: Any},
		Action: validFileAction(t), Use: UseClaim{Uses: ReadEntries}, MetadataAccess: ReadMetadata,
	}
	symlink := NodeRefOptions{
		Kind: NodeSymlink, Target: ChildCondition{State: Absent}, Create: true, Action: validFileAction(t),
		InitialState: InitialState{OnCreate: InitialFields{LinkTarget: []byte{0xff, 'x'}}},
	}
	deleting := NodeRefOptions{
		Kind: NodeRegular, Target: ChildCondition{State: SameNode, NodeID: 3},
		Action:      validFileAction(t),
		Use:         UseClaim{Uses: DeleteName},
		CloseIntent: &CloseIntent{ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkFile},
	}
	for _, options := range []NodeRefOptions{directory, symlink, deleting} {
		if err := options.Check(); err != nil {
			t.Fatalf("valid node reference %+v: %v", options, err)
		}
	}

	for _, change := range []func(*NodeRefOptions){
		func(o *NodeRefOptions) { o.Kind = 0 },
		func(o *NodeRefOptions) { o.Target = ChildCondition{} },
		func(o *NodeRefOptions) { o.MetadataAccess = 4 },
		func(o *NodeRefOptions) { o.Exclusive = true },
		func(o *NodeRefOptions) { o.InitialState.OnCreate.LinkTarget = []byte("x") },
		func(o *NodeRefOptions) { o.InitialState.OnReset.Metadata = map[string][]byte{"a": nil} },
		func(o *NodeRefOptions) {
			o.CloseIntent = &CloseIntent{ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkFile}
		},
	} {
		options := directory
		change(&options)
		if options.Check() == nil {
			t.Fatalf("invalid node reference accepted: %+v", options)
		}
	}
	if !errors.Is((InitialFields{LinkTarget: make([]byte, MaxLinkTargetBytes+1)}).Check(), syscall.EFBIG) {
		t.Fatal("oversized link target accepted")
	}
	compatibility := directory
	compatibility.Use.Uses = ReadData | WriteData | ReadEntries
	if err := compatibility.Check(); err != nil {
		t.Fatalf("metadata-only reference refused compatibility data claims: %v", err)
	}
	for _, options := range []OpenAtOptions{
		{Read: true, Target: ChildCondition{State: Any}, Existing: Keep, Use: UseClaim{Uses: ReadData}},
		{Read: true, Create: true, Target: ChildCondition{State: Any}, Existing: Keep, Use: UseClaim{Uses: ReadData}},
		{Read: true, Write: true, Target: ChildCondition{State: Any}, Existing: ResetContent, Use: UseClaim{Uses: ReadData | WriteData}},
	} {
		if !errors.Is(options.Check(), syscall.EINVAL) {
			t.Fatalf("effectful open without action accepted: %+v", options)
		}
	}
	for _, options := range []NodeRefOptions{
		{Kind: NodeDirectory, Target: ChildCondition{State: Any}},
		{Kind: NodeDirectory, Target: ChildCondition{State: Absent}, Create: true},
	} {
		if !errors.Is(options.Check(), syscall.EINVAL) {
			t.Fatalf("node reference without action accepted: %+v", options)
		}
	}
}

func TestNamespaceAndDeletionCommandsValidateTaggedEffects(t *testing.T) {
	action := validFileAction(t)
	child := ChildName{Parent: DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
	condition := ChildCondition{State: SameNode, NodeID: 2, ExpectedMetadata: map[string][]byte{"windows.attributes": nil}}
	uses := []TargetUse{{NodeID: 2, Scope: UseScope{Token: "reference"}}}
	rename := RenameTarget{
		Parent: DirectoryTarget{NodeID: 1}, ObservedLeaf: []byte("old"),
		Expected: ChildCondition{State: SameNode, NodeID: 3, ExpectedMetadata: map[string][]byte{"windows.attributes": {1}}}, OutputLeaf: []byte("NEW"),
	}
	valid := []NameCommand{
		{Kind: NameCreate, Action: action, Name: child, Target: ChildCondition{State: Absent}},
		{Kind: NameMkdir, Action: action, Name: child, Target: ChildCondition{State: Absent}},
		{Kind: NameSymlink, Action: action, Name: child, Target: ChildCondition{State: Absent}, Initial: InitialFields{LinkTarget: []byte("target")}},
		{Kind: NameRemove, Action: action, Name: child, Target: condition, Uses: uses},
		{Kind: NameRemoveDir, Action: action, Name: child, Target: condition},
		{Kind: NameRename, Action: action, Name: child, Target: condition, Destination: &rename},
	}
	for _, command := range valid {
		if err := command.Check(); err != nil {
			t.Fatalf("valid name command %+v: %v", command, err)
		}
	}
	invalid := []NameCommand{
		{},
		{Kind: NameRemove, Name: child, Target: condition},
		{Kind: NameRename, Action: action, Name: child, Target: condition},
		{Kind: NameRemove, Action: action, Name: child, Target: condition, Destination: &rename},
		{Kind: NameSymlink, Action: action, Name: child, Target: condition},
		{Kind: NameCreate, Action: action, Name: child, Target: condition, Initial: InitialFields{LinkTarget: []byte("bad")}},
		{Kind: NameRemove, Action: action, Name: child, Target: condition, Initial: InitialFields{Metadata: map[string][]byte{"a": nil}}},
		{Kind: NameRemove, Action: action, Name: child, Target: condition, Uses: []TargetUse{{}}},
	}
	for _, command := range invalid {
		if command.Check() == nil {
			t.Fatalf("invalid name command accepted: %+v", command)
		}
	}

	if err := CheckTargetUses(uses); err != nil {
		t.Fatalf("valid target uses: %v", err)
	}
	for _, bad := range [][]TargetUse{
		make([]TargetUse, MaxTargetUses+1),
		{{NodeID: 0, Scope: UseScope{Token: "x"}}},
		{{NodeID: 1, Scope: UseScope{}}},
		{{NodeID: 2, Scope: UseScope{Token: "x"}}, {NodeID: 2, Scope: UseScope{Token: "y"}}},
	} {
		if CheckTargetUses(bad) == nil {
			t.Fatalf("invalid target uses accepted: %+v", bad)
		}
	}

	if err := (CloseIntent{ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkIfEmpty, ExpectedMetadata: map[string][]byte{"windows.attributes": nil}, Uses: uses}).Check(); err != nil {
		t.Fatalf("valid close intent: %v", err)
	}
	if err := (PendingUnlinkCommand{Action: action, Condition: UnlinkFile, ExpectedMetadata: map[string][]byte{"windows.attributes": nil}, Uses: uses}).Check(); err != nil {
		t.Fatalf("valid pending unlink: %v", err)
	}
	if err := (ClearPendingUnlinkCommand{Action: action, Generation: []byte{1}, Uses: uses}).Check(); err != nil {
		t.Fatalf("valid pending clear: %v", err)
	}
	for _, bad := range []CloseIntent{{}, {ID: validDeleteIntent, Trigger: Now, Condition: UnlinkFile}, {ID: validDeleteIntent, Trigger: OnReferenceClose, Condition: UnlinkFile, ExpectedMetadata: map[string][]byte{"bad key": nil}}} {
		if bad.Check() == nil {
			t.Fatalf("invalid close intent accepted: %+v", bad)
		}
	}
	if (PendingUnlinkCommand{}).Check() == nil {
		t.Fatal("pending unlink without condition accepted")
	}
	if (PendingUnlinkCommand{Condition: UnlinkFile}).Check() == nil {
		t.Fatal("pending unlink without action accepted")
	}
	for _, bad := range []ClearPendingUnlinkCommand{{}, {Action: action, Generation: make([]byte, MaxObservationTokenBytes+1)}} {
		if bad.Check() == nil {
			t.Fatalf("invalid pending clear accepted: %+v", bad)
		}
	}
}

func TestConditionalMutationsSeparateConditionsFromEffects(t *testing.T) {
	action := validFileAction(t)
	size := int64(4)
	valid := []FileMutation{
		{Action: action, Kind: MutateTruncate, Size: 3},
		{Action: action, Kind: MutateTruncate, Size: 3, Metadata: map[string]OpaquePayload{"archive": {Data: []byte{1}}}},
		{Action: action, Kind: MutateWriteAt, Offset: 2, Data: []byte("xy"), ExpectedSize: &size, ExpectedMetadata: map[string][]byte{"a": {1}, "absent": nil}},
		{Action: action, Kind: MutateWriteAt, Data: []byte("xy"), Metadata: map[string]OpaquePayload{"archive": {Version: []byte{1}, Data: []byte{2}}}},
		{Action: action, Kind: MutateAppend, Data: []byte("xy"), ExpectedSize: &size},
		{Action: action, Kind: MutateAppend, Data: []byte("xy"), Metadata: map[string]OpaquePayload{"archive": {Data: []byte{1}}}},
		{Action: action, Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"new.namespace": {Data: []byte{1}}}},
	}
	for _, command := range valid {
		if err := command.CheckDataLimit(2); err != nil {
			t.Fatalf("valid file mutation %+v: %v", command, err)
		}
	}
	negative := int64(-1)
	invalid := []FileMutation{
		{},
		{Kind: MutateTruncate, Size: 1},
		{Action: action, Kind: MutateTruncate, Size: -1},
		{Action: action, Kind: MutateAttributes, Size: 1},
		{Action: action, Kind: MutateWriteAt, Offset: -1},
		{Action: action, Kind: MutateWriteAt, Offset: 1<<63 - 1, Data: []byte{1}},
		{Action: action, Kind: MutateAppend, Offset: 1},
		{Action: action, Kind: MutateWriteAt, Size: 1},
		{Action: action, Kind: MutateAttributes, Data: []byte{1}},
		{Action: action, Kind: MutateTruncate, Data: []byte{1}},
		{Action: action, Kind: MutateAppend, ExpectedSize: &negative},
		{Action: action, Kind: MutateAppend, ExpectedMetadata: map[string][]byte{"bad key": nil}},
		{Action: action, Kind: MutateAppend, ExpectedMetadata: map[string][]byte{"a": make([]byte, MaxObservationTokenBytes+1)}},
		{Action: action, Kind: MutateAttributes, Metadata: map[string]OpaquePayload{"a": {Data: make([]byte, MaxMetadataValueBytes+1)}}},
	}
	for _, command := range invalid {
		if command.Check() == nil {
			t.Fatalf("invalid file mutation accepted: %+v", command)
		}
	}
	if !errors.Is(valid[2].CheckDataLimit(1), syscall.EFBIG) || !errors.Is(valid[2].CheckDataLimit(0), syscall.EINVAL) {
		t.Fatal("conditional mutation byte budget ignored")
	}
}

func TestExpectedMetadataRequiresSameNodeAndBoundsEveryAdmission(t *testing.T) {
	action := validFileAction(t)
	maximum := make(map[string][]byte, MaxMetadataNamespaces)
	for i := 0; i < MaxMetadataNamespaces; i++ {
		maximum[strings.Repeat("x", MaxMetadataNamespaceBytes-i)] = bytes.Repeat([]byte{0xff}, MaxObservationTokenBytes)
	}
	tooMany := CloneInitialMetadata(maximum)
	tooMany["extra"] = nil
	tests := []struct {
		name     string
		expected map[string][]byte
		want     error
	}{
		{name: "maximum", expected: maximum},
		{name: "nil means absent", expected: map[string][]byte{"test.attributes": nil}},
		{name: "empty means absent", expected: map[string][]byte{"test.attributes": {}}},
		{name: "count", expected: tooMany, want: syscall.EFBIG},
		{name: "empty namespace", expected: map[string][]byte{"": nil}, want: syscall.EINVAL},
		{name: "invalid namespace", expected: map[string][]byte{"bad key": nil}, want: syscall.EINVAL},
		{name: "long namespace", expected: map[string][]byte{strings.Repeat("x", MaxMetadataNamespaceBytes+1): nil}, want: syscall.EINVAL},
		{name: "long version", expected: map[string][]byte{"test.attributes": make([]byte, MaxObservationTokenBytes+1)}, want: syscall.EINVAL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := ChildCondition{State: SameNode, NodeID: 7, ExpectedMetadata: test.expected}
			open := OpenAtOptions{Read: true, Existing: Keep, Target: target, Action: action, Use: UseClaim{Uses: ReadData}}
			node := NodeRefOptions{Kind: NodeRegular, Target: target, Action: action}
			mutation := FileMutation{Action: action, Kind: MutateAttributes, ExpectedMetadata: test.expected}
			for kind, err := range map[string]error{"open": open.Check(), "node": node.Check(), "mutation": mutation.Check()} {
				if !errors.Is(err, test.want) {
					t.Fatalf("%s: got %v, want %v", kind, err, test.want)
				}
			}
		})
	}

	for _, target := range []ChildCondition{{State: Any}, {State: Absent}} {
		target.ExpectedMetadata = map[string][]byte{"test.attributes": nil}
		open := OpenAtOptions{Read: true, Create: true, Existing: Keep, Target: target, Action: action, Use: UseClaim{Uses: ReadData}}
		node := NodeRefOptions{Kind: NodeRegular, Create: true, Target: target, Action: action}
		if !errors.Is(open.Check(), syscall.EINVAL) || !errors.Is(node.Check(), syscall.EINVAL) {
			t.Fatalf("metadata conditions accepted without SameNode: %+v", target)
		}
	}
}
