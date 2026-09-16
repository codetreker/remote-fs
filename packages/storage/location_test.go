package storage

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
)

func linkedLocation() EntryLocation {
	return EntryLocation{State: LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: []EntryCondition{
		{ParentID: 1, DirectoryRevision: 7, EntryID: 11, NodeID: 2, Name: []byte("parent")},
		{ParentID: 2, DirectoryRevision: 9, EntryID: 12, NodeID: 3, Name: []byte("file")},
	}}
}

func TestEntryLocationRequiresCompleteUniqueAncestorChain(t *testing.T) {
	for _, l := range []EntryLocation{{State: LocationRoot, RootNodeID: 1, NodeID: 1}, {State: LocationDetached, RootNodeID: 1, NodeID: 2}, linkedLocation()} {
		if err := l.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name   string
		change func(*EntryLocation)
	}{
		{"unknown state", func(l *EntryLocation) { l.State = 99 }},
		{"zero root", func(l *EntryLocation) { l.RootNodeID = 0 }},
		{"zero subject", func(l *EntryLocation) { l.NodeID = 0 }},
		{"wrong root", func(l *EntryLocation) { l.RootNodeID = 99 }},
		{"gap", func(l *EntryLocation) { l.Ancestors[1].ParentID = 99 }},
		{"wrong final node", func(l *EntryLocation) { l.NodeID = 99 }},
		{"zero revision", func(l *EntryLocation) { l.Ancestors[0].DirectoryRevision = 0 }},
		{"zero entry", func(l *EntryLocation) { l.Ancestors[0].EntryID = 0 }},
		{"zero child", func(l *EntryLocation) { l.Ancestors[0].NodeID = 0 }},
		{"repeated entry", func(l *EntryLocation) { l.Ancestors[1].EntryID = l.Ancestors[0].EntryID }},
		{"cycle", func(l *EntryLocation) { l.Ancestors[1].NodeID = 1; l.NodeID = 1 }},
		{"root with ancestors", func(l *EntryLocation) { l.State = LocationRoot; l.NodeID = 1 }},
		{"detached with ancestors", func(l *EntryLocation) { l.State = LocationDetached }},
	} {
		t.Run(test.name, func(t *testing.T) {
			l := linkedLocation()
			test.change(&l)
			if !errors.Is(l.Check(), syscall.EINVAL) {
				t.Fatalf("accepted %s", test.name)
			}
		})
	}
}

func TestEntryNamesAreExactBytesWithoutPlatformRules(t *testing.T) {
	for _, name := range [][]byte{[]byte("README"), []byte("readme"), {0xff}, []byte("CON"), []byte("name?"), []byte("trailing."), []byte("trailing "), []byte(`back\slash`), bytes.Repeat([]byte("x"), MaxEntryNameBytes)} {
		c := EntryCondition{ParentID: 1, DirectoryRevision: 1, EntryID: 1, NodeID: 2, Name: name}
		if err := c.Check(); err != nil {
			t.Fatalf("name %q: %v", name, err)
		}
	}
	for _, name := range [][]byte{nil, {}, []byte("."), []byte(".."), []byte("a/b"), {'a', 0}, bytes.Repeat([]byte("x"), MaxEntryNameBytes+1)} {
		c := EntryCondition{ParentID: 1, DirectoryRevision: 1, EntryID: 1, NodeID: 2, Name: name}
		if err := c.Check(); err == nil {
			t.Fatalf("accepted malformed name %q", name)
		}
	}
}

func TestEntryLocationBudgets(t *testing.T) {
	l := EntryLocation{State: LocationLinked, RootNodeID: 1}
	for i := 0; i < MaxLocationDepth; i++ {
		l.Ancestors = append(l.Ancestors, EntryCondition{ParentID: uint64(i + 1), DirectoryRevision: 1, EntryID: EntryID(i + 1), NodeID: uint64(i + 2), Name: []byte("x")})
	}
	l.NodeID = uint64(MaxLocationDepth + 1)
	if err := l.Check(); err != nil {
		t.Fatal("depth boundary", err)
	}
	l.Ancestors = append(l.Ancestors, EntryCondition{ParentID: l.NodeID, DirectoryRevision: 1, EntryID: 999, NodeID: l.NodeID + 1, Name: []byte("x")})
	l.NodeID++
	if !errors.Is(l.Check(), syscall.EFBIG) {
		t.Fatal("accepted depth overflow")
	}
	l = EntryLocation{State: LocationLinked, RootNodeID: 1}
	for i := 0; i < MaxLocationNameBytes/MaxEntryNameBytes; i++ {
		l.Ancestors = append(l.Ancestors, EntryCondition{ParentID: uint64(i + 1), DirectoryRevision: 1, EntryID: EntryID(i + 1), NodeID: uint64(i + 2), Name: bytes.Repeat([]byte("x"), MaxEntryNameBytes)})
	}
	l.NodeID = uint64(len(l.Ancestors) + 1)
	if err := l.Check(); err != nil {
		t.Fatal("byte boundary", err)
	}
	l.Ancestors = append(l.Ancestors, EntryCondition{ParentID: l.NodeID, DirectoryRevision: 1, EntryID: 999, NodeID: l.NodeID + 1, Name: []byte("x")})
	l.NodeID++
	if !errors.Is(l.Check(), syscall.EFBIG) {
		t.Fatal("accepted byte overflow")
	}
}

func TestDirectoryPageRequestBindsContinuationRevision(t *testing.T) {
	base := DirectoryPageRequest{MaxEntries: 10, MaxBytes: 4096}
	if err := base.Check(); err != nil {
		t.Fatal(err)
	}
	base.Revision = 5
	base.Cursor = DirectoryCursor{ParentID: 1, Revision: 5, After: []byte("a")}
	if err := base.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*DirectoryPageRequest){
		func(r *DirectoryPageRequest) { r.Revision = 0 }, func(r *DirectoryPageRequest) { r.Revision++ },
		func(r *DirectoryPageRequest) { r.Cursor.ParentID = 0 }, func(r *DirectoryPageRequest) { r.Cursor.After = nil },
		func(r *DirectoryPageRequest) { r.MaxEntries = 0 }, func(r *DirectoryPageRequest) { r.MaxEntries = MaxDirectoryPageEntries + 1 },
		func(r *DirectoryPageRequest) { r.MaxBytes = 0 }, func(r *DirectoryPageRequest) { r.MaxBytes = MaxDirectoryPageBytes + 1 },
	} {
		r := base
		change(&r)
		if err := r.Check(); err == nil {
			t.Fatal("accepted malformed page request", r)
		}
	}
}

func validDirectoryPage() DirectoryPage {
	return DirectoryPage{ParentID: 1, Revision: 5, Entries: []DirectoryEntry{
		{EntryID: 10, Name: []byte("A"), Attr: Attr{ID: 2, Kind: NodeRegular, MetadataRevision: 1, DirectoryRevision: 0}},
		{EntryID: 11, Name: []byte("a"), Attr: Attr{ID: 3, Kind: NodeRegular, MetadataRevision: 1, DirectoryRevision: 0}},
	}, Next: DirectoryCursor{ParentID: 1, Revision: 5, After: []byte("a")}}
}

func TestDirectoryPageChecksIdentityOrderingAndCompletion(t *testing.T) {
	if err := validDirectoryPage().Check(); err != nil {
		t.Fatal(err)
	}
	if err := (DirectoryPage{ParentID: 1, Revision: 1, Done: true}).Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*DirectoryPage){
		func(p *DirectoryPage) { p.ParentID = 0 }, func(p *DirectoryPage) { p.Revision = 0 },
		func(p *DirectoryPage) { p.Entries[1].EntryID = 10 }, func(p *DirectoryPage) { p.Entries[1].Attr.ID = 2 },
		func(p *DirectoryPage) { p.Entries[1].Attr.ID = 1 }, func(p *DirectoryPage) { p.Entries[1].Name = []byte("A") },
		func(p *DirectoryPage) { p.Entries[0].Name = []byte("z") }, func(p *DirectoryPage) { p.Entries[0].Name = nil },
		func(p *DirectoryPage) { p.Next.Revision++ }, func(p *DirectoryPage) { p.Next.ParentID++ },
		func(p *DirectoryPage) { p.Next.After = []byte("z") }, func(p *DirectoryPage) { p.Entries = nil },
		func(p *DirectoryPage) { p.Done = true },
	} {
		p := validDirectoryPage()
		change(&p)
		if err := p.Check(); err == nil {
			t.Fatal("accepted malformed page", p)
		}
	}
}

func TestDirectoryEntryChargeBoundsPayloadBeforeAllocation(t *testing.T) {
	if got, err := DirectoryEntryBytes(MaxEntryNameBytes, MaxMetadataBytes); err != nil || got != 256+MaxEntryNameBytes+4*MaxMetadataBytes {
		t.Fatalf("maximum charge=%d, %v", got, err)
	}
	for _, sizes := range [][2]int{{-1, 6}, {0, 6}, {MaxEntryNameBytes + 1, 6}, {1, -1}, {1, 5}, {1, MaxMetadataBytes + 1}} {
		if _, err := DirectoryEntryBytes(sizes[0], sizes[1]); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("invalid lengths %v: %v", sizes, err)
		}
	}
}

func TestEntryLocationCloneOwnsAncestorNames(t *testing.T) {
	original := linkedLocation()
	cloned := original.Clone()
	cloned.Ancestors[0].Name[0] = 'X'
	cloned.Ancestors[1].NodeID = 99
	if string(original.Ancestors[0].Name) != "parent" || original.Ancestors[1].NodeID != 3 {
		t.Fatal("clone shares mutable witness state")
	}
	if (EntryLocation{}).Clone().Ancestors != nil {
		t.Fatal("nil ancestors became allocated")
	}
	empty := EntryLocation{Ancestors: []EntryCondition{}}
	if empty.Clone().Ancestors == nil {
		t.Fatal("empty ancestors became nil")
	}
}

func TestEntryLookupChecksExactFoundAndAbsentFacts(t *testing.T) {
	name := []byte{0xff}
	absent := EntryLookup{ParentID: 1, DirectoryRevision: 3, Name: name}
	if err := absent.Check(name); err != nil {
		t.Fatal(err)
	}
	found := EntryLookup{ParentID: 1, DirectoryRevision: 3, Name: name, Found: true, EntryID: 7, Attr: Attr{ID: 2, Kind: NodeRegular, MetadataRevision: 5}}
	if err := found.Check(name); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*EntryLookup){
		func(l *EntryLookup) { l.Name = []byte("other") }, func(l *EntryLookup) { l.ParentID = 0 }, func(l *EntryLookup) { l.DirectoryRevision = 0 },
		func(l *EntryLookup) { l.EntryID = 0 }, func(l *EntryLookup) { l.Attr.ID = 1 }, func(l *EntryLookup) { l.Attr.MetadataRevision = 0 },
	} {
		l := found
		change(&l)
		if !errors.Is(l.Check(name), syscall.EIO) {
			t.Fatalf("malformed found lookup=%+v", l)
		}
	}
	for _, change := range []func(*EntryLookup){func(l *EntryLookup) { l.EntryID = 1 }, func(l *EntryLookup) { l.Attr.Metadata = Metadata{} }, func(l *EntryLookup) { l.Attr.ID = 2 }} {
		l := absent
		change(&l)
		if !errors.Is(l.Check(name), syscall.EIO) {
			t.Fatalf("false absence with facts=%+v", l)
		}
	}
	cloned := found.Clone()
	cloned.Name[0] = 'x'
	if found.Name[0] != 0xff {
		t.Fatal("lookup clone shares name")
	}
}
