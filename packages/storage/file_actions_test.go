package storage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func actionTarget() storage.EntryTarget {
	return storage.EntryTarget{Parent: 7, ParentID: 1, DirectoryRevision: 2, Name: []byte("a"), ExpectedEntryID: 11, ExpectedNodeID: 3, ExpectedMetadataRevision: 4,
		Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}}
}
func actionLocation() storage.EntryLocation {
	target := actionTarget()
	return storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: []storage.EntryCondition{{ParentID: 1, DirectoryRevision: 2, EntryID: 11, NodeID: 3, Name: target.Name}}}
}

func TestFixedRetainAndCreateValidateTheCompleteTarget(t *testing.T) {
	target := actionTarget()
	condition := storage.RemovalFile
	claim := storage.AccessClaim{Uses: storage.ReadContent | storage.RemoveEntry, Excludes: storage.WriteContent}
	relative := target
	relative.Witness = nil
	if err := relative.Check(); err != nil {
		t.Fatalf("relative target requires unnecessary ancestry: %v", err)
	}
	retain := storage.RetainAtRequest{Target: target, Claim: claim, Prepared: &condition}
	if err := retain.Check(); err != nil {
		t.Fatal(err)
	}
	linked := actionLocation()
	if err := (storage.RetainRequest{NodeID: 3, Claim: claim, Witness: &linked, Prepared: &condition}).Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.EntryTarget){
		func(r *storage.EntryTarget) { r.Parent = 0 }, func(r *storage.EntryTarget) { r.ParentID = 0 }, func(r *storage.EntryTarget) { r.DirectoryRevision = 0 },
		func(r *storage.EntryTarget) { r.ExpectedEntryID = 0 }, func(r *storage.EntryTarget) { r.ExpectedNodeID = 0 }, func(r *storage.EntryTarget) { r.Name = []byte("../x") },
		func(r *storage.EntryTarget) { r.Witness = &storage.EntryLocation{} }, func(r *storage.EntryTarget) { r.Witness.NodeID = 2 },
	} {
		bad := target
		copy := target.Witness.Clone()
		bad.Witness = &copy
		change(&bad)
		if err := bad.Check(); err == nil {
			t.Fatalf("invalid target %+v", bad)
		}
	}
	absent := target
	absent.ExpectedEntryID = 0
	absent.ExpectedNodeID = 0
	absent.ExpectedMetadataRevision = 0
	create := storage.CreateAndRetainRequest{Target: absent, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim, Prepared: &condition}
	if err := create.CheckCreate(); err != nil {
		t.Fatal(err)
	}
	if err := create.CheckReplace(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("absent replacement %v", err)
	}
	create.Target = target
	if err := create.CheckReplace(); err != nil {
		t.Fatal(err)
	}
	if err := create.CheckCreate(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("create existing %v", err)
	}
	retain.Target = absent
	if err := retain.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("retain absent %v", err)
	}
	for _, r := range []storage.RetainRequest{{}, {NodeID: 3, Claim: storage.AccessClaim{Uses: 8}}, {NodeID: 4, Witness: &linked}, {NodeID: 3, Witness: &storage.EntryLocation{}}} {
		if err := r.Check(); err == nil {
			t.Fatalf("invalid retain %+v", r)
		}
	}
	badCondition := storage.RemovalCondition(0)
	if err := (storage.RetainRequest{NodeID: 3, Prepared: &badCondition}).Check(); err == nil {
		t.Fatal("invalid prepared condition")
	}
	if err := (storage.RetainRequest{NodeID: 3}).Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.AccessClaim{Uses: storage.AllAccessUses, Excludes: storage.AllAccessUses}).Check(); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicMetadataAndKindRequestsPreserveRevisionConditions(t *testing.T) {
	target := actionTarget()
	metadata := storage.Metadata{{Key: "app", Version: 1, Data: []byte{1}}}
	reset := storage.ResetAndRetainRequest{Target: target, ExpectedRevision: 4, Change: storage.AttrChange{ExpectedRevision: 4, Metadata: &metadata}}
	if err := reset.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.ResetAndRetainRequest){func(r *storage.ResetAndRetainRequest) { r.ExpectedRevision = 0 }, func(r *storage.ResetAndRetainRequest) { r.ExpectedRevision = 5 }, func(r *storage.ResetAndRetainRequest) { r.Change.ExpectedRevision = 5 }, func(r *storage.ResetAndRetainRequest) { r.Target.Parent = 0 }} {
		bad := reset
		change(&bad)
		if err := bad.Check(); err == nil {
			t.Fatal("invalid reset accepted")
		}
	}
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		initial := storage.NodeInitial{Kind: kind, Metadata: metadata}
		if kind == storage.NodeSymlink {
			initial.LinkTarget = []byte{255, '/', 'a'}
		}
		if err := initial.Check(); err != nil {
			t.Fatal(err)
		}
		if err := (storage.SetKindRequest{ExpectedRevision: 4, Kind: kind, Metadata: metadata, LinkTarget: initial.LinkTarget}).Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, initial := range []storage.NodeInitial{{}, {Kind: storage.NodeRegular, LinkTarget: []byte("a")}, {Kind: storage.NodeSymlink}, {Kind: storage.NodeSymlink, LinkTarget: make([]byte, storage.MaxLinkTargetBytes+1)}, {Kind: storage.NodeRegular, Metadata: storage.Metadata{{Key: "a", Version: 0}}}} {
		if err := initial.Check(); err == nil {
			t.Fatalf("invalid initial %+v", initial)
		}
	}
	if err := (storage.SetKindRequest{Kind: storage.NodeRegular}).Check(); err == nil {
		t.Fatal("kind conversion without revision")
	}
	rename := storage.RenameRequest{Source: target, Destination: target}
	if err := rename.Check(); err != nil {
		t.Fatal(err)
	}
	rename.Source.ExpectedEntryID = 0
	rename.Source.ExpectedNodeID = 0
	rename.Source.ExpectedMetadataRevision = 0
	if err := rename.Check(); err == nil {
		t.Fatal("rename absent source")
	}
	rename.Source.Parent = 0
	if err := rename.Check(); err == nil {
		t.Fatal("rename invalid source")
	}
}

func TestRemovalPreparationAndDrainCancellationBindEntryIdentity(t *testing.T) {
	location := actionLocation()
	request := storage.PrepareRemovalRequest{ExpectedMetadataRevision: 4, Entry: location.Ancestors[0], Witness: location, Condition: storage.RemovalIfEmpty}
	if err := request.Check(); err != nil {
		t.Fatal(err)
	}
	drain := storage.DrainEntryRequest{ExpectedMetadataRevision: 4, Entry: request.Entry, Witness: location, Condition: storage.RemovalIfEmpty}
	if err := drain.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.PrepareRemovalRequest){func(r *storage.PrepareRemovalRequest) { r.Entry.EntryID = 0 }, func(r *storage.PrepareRemovalRequest) { r.Entry.Name = []byte("replacement") }, func(r *storage.PrepareRemovalRequest) { r.Witness = storage.EntryLocation{} }, func(r *storage.PrepareRemovalRequest) {
		r.Witness = storage.EntryLocation{State: storage.LocationDetached, RootNodeID: 1, NodeID: 3}
	}, func(r *storage.PrepareRemovalRequest) { r.Condition = 0 }} {
		bad := request
		change(&bad)
		if err := bad.Check(); err == nil {
			t.Fatal("invalid prepared entry")
		}
	}
	if err := (storage.CancelDrainRequest{EntryID: 11, Generation: 9}).Check(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []storage.CancelDrainRequest{{}, {EntryID: 11}, {Generation: 9}} {
		if err := r.Check(); err == nil {
			t.Fatal("unscoped drain cancellation")
		}
	}
	if err := (storage.ObservationCondition{MetadataRevision: 4, Location: location}).Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.ObservationCondition{Location: location}).Check(); err == nil {
		t.Fatal("unchecked observation")
	}
}

func TestActionReceiptsOwnAllNestedObservationsAndConflictFacts(t *testing.T) {
	location := actionLocation()
	receipt := storage.FileActionReceipt{Action: "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Operation: storage.OpFileRetain, State: storage.FileActionCompleted,
		Observation: storage.FileObservation{Attr: storage.Attr{ID: 3, Kind: storage.NodeSymlink, Size: 1, MetadataRevision: 4, Metadata: storage.Metadata{{Key: "app", Version: 1, Data: []byte{7}}}}, Location: &location, LinkTarget: []byte("a")},
		Conflict:    &storage.FileConflict{Kind: storage.ConflictRange, Range: &storage.HeldRange{Owner: storage.RangeOwner{Session: "owner", ID: 0}, Range: storage.RangeAcquisition{ID: 1, End: 4}}}}
	copied := receipt.Clone()
	if !reflect.DeepEqual(copied, receipt) {
		t.Fatal("receipt clone changed values")
	}
	copied.Observation.Attr.Metadata[0].Data[0] = 9
	copied.Observation.LinkTarget[0] = 'b'
	copied.Observation.Location.Ancestors[0].Name[0] = 'z'
	copied.Conflict.Range.Range.End = 8
	if receipt.Observation.Attr.Metadata[0].Data[0] != 7 || string(receipt.Observation.LinkTarget) != "a" || string(receipt.Observation.Location.Ancestors[0].Name) != "a" || receipt.Conflict.Range.Range.End != 4 {
		t.Fatal("receipt clone aliases mutable facts")
	}
	if clone := (storage.FileActionReceipt{}).Clone(); clone.Conflict != nil || clone.Observation.Location != nil {
		t.Fatal("clone fabricated optional facts")
	}
}

func TestRemovalObservationsKeepPreparedAndDrainLifetimesSeparate(t *testing.T) {
	for _, state := range []storage.RemovalStatus{
		{}, {State: storage.EntryActive}, {State: storage.EntryDetached},
		{State: storage.EntryActive, EntryID: 1, Generation: 4},
		{State: storage.EntryDraining, EntryID: 1, Generation: 4, DrainCondition: storage.RemovalIfEmpty},
		{State: storage.EntryDetached, EntryID: 1, IntentID: 9, Prepared: true, PreparedCondition: storage.RemovalFile},
		{State: storage.EntryDraining, EntryID: 1, IntentID: 9, Prepared: true, PreparedCondition: storage.RemovalFile, DrainCondition: storage.RemovalIfEmpty, Generation: 4},
	} {
		if err := state.Check(); err != nil {
			t.Fatalf("valid removal %+v: %v", state, err)
		}
	}
	for _, state := range []storage.RemovalStatus{
		{EntryID: 1}, {State: 99}, {State: storage.EntryActive, Prepared: true},
		{State: storage.EntryActive, IntentID: 1}, {State: storage.EntryDraining, EntryID: 1, DrainCondition: storage.RemovalFile},
		{State: storage.EntryDraining, EntryID: 1, Generation: 1},
		{State: storage.EntryActive, EntryID: 1, DrainCondition: 99}, {State: storage.EntryDetached, Generation: 1},
	} {
		if err := state.Check(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid removal %+v: %v", state, err)
		}
	}
}

func TestKindConversionCarriesAnOptionalCompleteLocationWitness(t *testing.T) {
	location := actionLocation()
	request := storage.SetKindRequest{ExpectedRevision: 4, Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Witness: &location}
	if err := request.Check(); err != nil {
		t.Fatal(err)
	}
	broken := location.Clone()
	broken.Ancestors[0].DirectoryRevision = 0
	request.Witness = &broken
	if err := request.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("incomplete conversion witness accepted: %v", err)
	}
}

func TestRenameOutputNameKeepsTheObservedDestinationIndependent(t *testing.T) {
	source := actionTarget()
	destination := actionTarget()
	destination.Name = []byte("Other.txt")
	destination.ExpectedEntryID = 12
	destination.ExpectedNodeID = 4
	request := storage.RenameRequest{Source: source, Destination: destination}
	if err := request.Check(); err != nil {
		t.Fatalf("default destination spelling: %v", err)
	}
	request.NewName = []byte("OTHER.TXT")
	if err := request.Check(); err != nil {
		t.Fatalf("independent output spelling: %v", err)
	}
	if string(request.Destination.Name) != "Other.txt" || request.Destination.ExpectedEntryID != 12 || request.Source.ExpectedEntryID != 11 {
		t.Fatal("output validation changed observed entry conditions")
	}
	for _, name := range [][]byte{{}, []byte("."), []byte(".."), []byte("a/b"), []byte("a\x00b")} {
		request.NewName = name
		if err := request.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid output %q: %v", name, err)
		}
	}
	request.NewName = bytes.Repeat([]byte{255}, storage.MaxEntryNameBytes)
	if err := request.Check(); err != nil {
		t.Fatalf("bounded raw bytes gained platform restrictions: %v", err)
	}
	request.NewName = append(request.NewName, 255)
	if err := request.Check(); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized output name: %v", err)
	}
	request.NewName = []byte("valid")
	request.Destination.Name = []byte("invalid/observed")
	if err := request.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("output name bypassed destination validation: %v", err)
	}
}

func TestKindConversionPreservesOptionalSessionScopedRangeOwner(t *testing.T) {
	location := actionLocation()
	request := storage.SetKindRequest{ExpectedRevision: 4, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Witness: &location}
	if err := request.Check(); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []storage.RangeOwnerID{0, 1, ^storage.RangeOwnerID(0)} {
		request.Owner = &owner
		if err := request.Check(); err != nil {
			t.Fatalf("opaque range owner %d: %v", owner, err)
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var decoded storage.SetKindRequest
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Owner == nil || *decoded.Owner != owner {
			t.Fatal("owner identity lost across value encoding")
		}
		if err := decoded.Check(); err != nil {
			t.Fatal(err)
		}
	}
}
