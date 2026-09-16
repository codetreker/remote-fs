package metastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func notificationChange(kind ChangeKind) Change {
	node := Node{ID: 3, Kind: storage.NodeRegular, MetadataRevision: 2}
	at := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: []storage.EntryCondition{
		{ParentID: 1, NodeID: 2, EntryID: 11, DirectoryRevision: 1, Name: []byte("dir")},
		{ParentID: 2, NodeID: 3, EntryID: 12, DirectoryRevision: 2, Name: []byte{0xff}},
	}}
	image := func() *EventImage { return &EventImage{Attr: node.Attr(), Location: at} }
	n := &Notification{SubjectID: 3, SubjectKind: storage.NodeRegular, ChangeMask: ChangeName}
	c := Change{Kind: kind, Parent: 2, Name: []byte{0xff}, Node: &node, Notification: n}
	switch kind {
	case Created:
		n.After = image()
	case Removed:
		n.Before = image()
		c.Node = nil
	case Modified:
		n.Before = image()
		n.After = image()
		n.ChangeMask = 0
	case Renamed:
		n.Before = image()
		n.After = image()
		c.From = &Location{Parent: 2, Name: []byte{0xff}}
	}
	return c
}

func TestNotificationRoundTripPreservesHistoricalFacts(t *testing.T) {
	for _, kind := range []ChangeKind{Created, Removed, Modified, Renamed} {
		c := notificationChange(kind)
		for _, image := range []*EventImage{c.Notification.Before, c.Notification.After} {
			if image != nil {
				image.Attr.Metadata = storage.Metadata{{Key: "client", Version: 7, Data: []byte{0xff, 0, 1}}}
			}
		}
		if c.Node != nil {
			c.Node.Metadata = c.Notification.After.Attr.Metadata.Clone()
		}
		data, err := EncodeNotification(c)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeNotification(c, data)
		if err != nil || !reflect.DeepEqual(got, c.Notification) {
			t.Fatalf("kind %v: %+v %v", kind, got, err)
		}
		if _, err := DecodeNotification(c, append(data, ' ')); !errors.Is(err, syscall.EIO) {
			t.Fatalf("noncanonical: %v", err)
		}
		if got.Before != nil {
			got.Before.Attr.Metadata[0].Data[0] = 1
		}
		if got.After != nil {
			got.After.Location.Ancestors[1].Name[0] = 1
		}
		image := c.Notification.After
		if image == nil {
			image = c.Notification.Before
		}
		if image.Attr.Metadata[0].Data[0] != 0xff || image.Location.Ancestors[1].Name[0] != 0xff {
			t.Fatal("decoded facts alias source")
		}
	}
}

func TestNotificationRejectsContradictoryOrMissingFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Change)
	}{
		{"missing", func(c *Change) { c.Notification = nil }},
		{"identity", func(c *Change) { c.Notification.SubjectID = 0 }},
		{"kind", func(c *Change) { c.Notification.SubjectKind = 99 }},
		{"unknown mask", func(c *Change) { c.Notification.ChangeMask = 1 << 31 }},
		{"missing after", func(c *Change) { c.Notification.After = nil }},
		{"unexpected before", func(c *Change) { c.Notification.Before = c.Notification.After }},
		{"missing node", func(c *Change) { c.Node = nil }},
		{"wrong node", func(c *Change) { c.Node.ID = 4 }},
		{"wrong node kind", func(c *Change) { c.Node.Kind = storage.NodeSymlink }},
		{"wrong image kind", func(c *Change) { c.Notification.After.Attr.Kind = storage.NodeSymlink }},
		{"wrong image id", func(c *Change) { c.Notification.After.Attr.ID = 4 }},
		{"missing revision", func(c *Change) { c.Notification.After.Attr.MetadataRevision = 0 }},
		{"unexpected directory revision", func(c *Change) { c.Notification.After.Attr.DirectoryRevision = 1 }},
		{"negative size", func(c *Change) { c.Notification.After.Attr.Size = -1 }},
		{"wrong witness subject", func(c *Change) { c.Notification.After.Location.NodeID = 4 }},
		{"missing witness", func(c *Change) { c.Notification.After.Location = storage.EntryLocation{} }},
		{"missing ancestor", func(c *Change) { c.Notification.After.Location.Ancestors = nil }},
		{"cycle", func(c *Change) { c.Notification.After.Location.Ancestors[1].NodeID = 1 }},
		{"invalid entry", func(c *Change) { c.Notification.After.Location.Ancestors[1].EntryID = 0 }},
		{"invalid name", func(c *Change) { c.Notification.After.Location.Ancestors[1].Name = []byte("../bad") }},
		{"bad metadata", func(c *Change) { c.Notification.After.Attr.Metadata = storage.Metadata{{Key: "bad", Version: 0}} }},
		{"metadata limit", func(c *Change) {
			c.Notification.After.Attr.Metadata = storage.Metadata{{Key: "client", Version: 1, Data: make([]byte, storage.MaxMetadataBytes)}}
		}},
		{"target limit", func(c *Change) { c.Notification.After.LinkTarget = make([]byte, storage.MaxLinkTargetBytes+1) }},
		{"unexpected target", func(c *Change) { c.Notification.After.LinkTarget = []byte("target") }},
		{"wrong parent", func(c *Change) { c.Parent = 7 }},
		{"wrong name", func(c *Change) { c.Name = []byte("other") }},
		{"wrong mask", func(c *Change) { c.Notification.ChangeMask = ChangeSize }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := notificationChange(Created)
			test.mutate(&c)
			if err := ValidateNotification(c); err == nil {
				t.Fatal("invalid facts accepted")
			}
		})
	}
	c := notificationChange(Created)
	for _, data := range [][]byte{nil, []byte("null"), []byte("{"), bytes.Repeat([]byte("x"), MaxNotificationBytes+1)} {
		if _, err := DecodeNotification(c, data); err == nil {
			t.Fatalf("invalid encoded length %d accepted", len(data))
		}
	}
}

func TestNotificationTransitionImagesPreserveRemovedSymlinkMetadata(t *testing.T) {
	c := notificationChange(Removed)
	c.Notification.SubjectKind = storage.NodeSymlink
	image := c.Notification.Before
	image.Attr.Kind = storage.NodeSymlink
	image.LinkTarget = []byte{0xff, 'x'}
	image.Attr.Size = 2
	image.Attr.Metadata = storage.Metadata{{Key: "client", Version: 2, Data: []byte{1}}}
	data, err := EncodeNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	image.Attr.Metadata[0].Data[0] = 2
	got, err := DecodeNotification(c, data)
	if err != nil || got.Before.Attr.Metadata[0].Data[0] != 1 || !bytes.Equal(got.Before.LinkTarget, []byte{0xff, 'x'}) || c.Node != nil {
		t.Fatalf("historical metadata: %+v %v", got, err)
	}
	got.Before.Attr.Size = 3
	c.Notification = got
	if err := ValidateNotification(c); !errors.Is(err, syscall.EIO) {
		t.Fatalf("bad symlink size: %v", err)
	}
}

func TestNotificationMasksCompareGenericFacts(t *testing.T) {
	instant := time.Unix(1, 0)
	before := Node{ID: 3, Kind: storage.NodeRegular, MetadataRevision: 1, Size: 1, AccessTime: instant, ModTime: instant, Content: "a"}
	after := before
	if mask := NotificationMask(before, after); mask != 0 {
		t.Fatal(mask)
	}
	later := instant.Add(time.Second)
	after.Size = 2
	after.AccessTime = later
	after.ModTime = later
	after.Content = "b"
	after.CreationTime = &later
	after.ChangeTime = &later
	after.Metadata = storage.Metadata{{Key: "client", Version: 1, Data: []byte{1}}}
	want := ChangeSize | ChangeAccessTime | ChangeModTime | ChangeContent | ChangeAttributes | ChangeCreationTime | ChangeTime
	if mask := NotificationMask(before, after); mask != want {
		t.Fatalf("mask %v want %v", mask, want)
	}
	before.CreationTime = &instant
	before.ChangeTime = &instant
	if mask := NotificationMask(before, after); mask != want {
		t.Fatal(mask)
	}
	after = before
	otherZone := instant.In(time.FixedZone("other", 3600))
	after.CreationTime = &otherZone
	after.ChangeTime = &otherZone
	if mask := NotificationMask(before, after); mask != 0 {
		t.Fatal(mask)
	}
	after.Metadata = storage.Metadata{{Key: "client", Version: 1, Data: []byte{1}}}
	before.Metadata = after.Metadata.Clone()
	after.Metadata[0].Version = 2
	if mask := NotificationMask(before, after); mask != ChangeAttributes {
		t.Fatal(mask)
	}
}

func TestNotificationTransitionsRejectChangedOrIncompleteIdentity(t *testing.T) {
	for _, kind := range []ChangeKind{Modified, Renamed, Removed, 99} {
		c := notificationChange(kind)
		if kind == 99 {
			c = notificationChange(Modified)
			c.Kind = 99
		}
		c.Notification.ChangeMask = ChangeName | ChangeSize
		if err := ValidateNotification(c); err == nil {
			t.Fatalf("invalid kind %v accepted", kind)
		}
	}
	tests := []struct {
		name   string
		kind   ChangeKind
		mutate func(*Change)
	}{
		{"rename source", Renamed, func(c *Change) { c.From = nil }},
		{"rename identity", Renamed, func(c *Change) {
			a := c.Notification.After.Location.Ancestors
			c.Notification.After.Location.Ancestors = append([]storage.EntryCondition(nil), a...)
			c.Notification.After.Location.Ancestors[1].EntryID++
		}},
		{"revision regression", Modified, func(c *Change) { c.Notification.Before.Attr.MetadataRevision = 3 }},
		{"wrong root", Modified, func(c *Change) { c.Notification.Before.Location.RootNodeID = 9 }},
		{"missing before", Modified, func(c *Change) { c.Notification.Before = nil }},
		{"masked size", Modified, func(c *Change) { c.Notification.Before.Attr.Size = 1 }},
		{"different entry", Modified, func(c *Change) {
			a := c.Notification.After.Location.Ancestors
			c.Notification.After.Location.Ancestors = append([]storage.EntryCondition(nil), a...)
			c.Notification.After.Location.Ancestors[1].Name = []byte("other")
		}},
		{"unnamed linked node", Modified, func(c *Change) { c.Parent = 0; c.Name = nil }},
		{"removed after", Removed, func(c *Change) { c.Notification.After = c.Notification.Before }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := notificationChange(test.kind)
			test.mutate(&c)
			if err := ValidateNotification(c); err == nil {
				t.Fatal("invalid transition accepted")
			}
		})
	}
	for _, state := range []storage.LocationState{storage.LocationRoot, storage.LocationDetached} {
		c := notificationChange(Modified)
		c.Parent = 0
		c.Name = nil
		location := storage.EntryLocation{State: state, RootNodeID: 1, NodeID: 3}
		if state == storage.LocationRoot {
			location.RootNodeID = 3
			c.Node.Kind = storage.NodeDirectory
			c.Node.DirectoryRevision = 1
			c.Notification.SubjectKind = storage.NodeDirectory
		}
		c.Notification.Before = &EventImage{Attr: c.Node.Attr(), Location: location}
		c.Notification.After = &EventImage{Attr: c.Node.Attr(), Location: location}
		if err := ValidateNotification(c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNotificationAdmissionIncludesOpaqueAndTargetBytes(t *testing.T) {
	for _, lengths := range []ChangePayloadLengths{{}, {Name: MaxChangePayloadBytes}, {Metadata: storage.MaxMetadataBytes, Target: storage.MaxLinkTargetBytes, Notification: MaxNotificationBytes}} {
		if err := lengths.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, lengths := range []ChangePayloadLengths{{Name: -1}, {FromName: -1}, {Content: -1}, {Metadata: -1}, {Target: -1}, {Notification: -1}, {Metadata: storage.MaxMetadataBytes + 1}, {Target: storage.MaxLinkTargetBytes + 1}, {Notification: MaxNotificationBytes + 1}, {Name: MaxChangePayloadBytes, Metadata: 1}, {Name: math.MaxInt64, Content: math.MaxInt64}} {
		if err := lengths.Check(); err == nil {
			t.Fatalf("invalid %+v accepted", lengths)
		}
	}
	c := notificationChange(Created)
	encoded, err := EncodeNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Node.Content = Key(strings.Repeat("x", MaxChangePayloadBytes-len(encoded)-len(c.Name)-6+1))
	if _, err := DecodeNotification(c, encoded); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("aggregate bound: %v", err)
	}
}

func TestNotificationMissingImageDoesNotBecomeEmptyMetadata(t *testing.T) {
	c := notificationChange(Created)
	encoded, err := EncodeNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	delete(object, "After")
	missing, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNotification(c, missing); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing image: %v", err)
	}
}

func TestNotificationIdentityHighWaterIncludesEveryWitnessIdentity(t *testing.T) {
	c := notificationChange(Removed)
	high, err := NotificationIdentityHighWater(c.Notification)
	if err != nil || high != 12 {
		t.Fatalf("identity high-water %d: %v", high, err)
	}
	c.Notification.Before.Location.Ancestors[0].EntryID = 100
	high, err = NotificationIdentityHighWater(c.Notification)
	if err != nil || high != 100 {
		t.Fatalf("detached entry high-water %d: %v", high, err)
	}
	c.Notification.Before.Location.Ancestors[0].EntryID = storage.EntryID(math.MaxUint64)
	if _, err := NotificationIdentityHighWater(c.Notification); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if _, err := NotificationIdentityHighWater(nil); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	c.Notification.Before.Location.Ancestors = make([]storage.EntryCondition, MaxNotificationAncestors+1)
	if _, err := NotificationIdentityHighWater(c.Notification); !errors.Is(err, syscall.EFBIG) {
		t.Fatal(err)
	}
}

func TestNotificationCombinedWitnessBound(t *testing.T) {
	c := notificationChange(Modified)
	parent := uint64(1)
	var chain []storage.EntryCondition
	for i := 0; i < 16; i++ {
		id := uint64(100 + i)
		if i == 15 {
			id = 3
		}
		chain = append(chain, storage.EntryCondition{ParentID: parent, NodeID: id, EntryID: storage.EntryID(1000 + i), DirectoryRevision: 1, Name: bytes.Repeat([]byte("x"), storage.MaxEntryNameBytes)})
		parent = id
	}
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: chain}
	c.Notification.Before.Location = location
	c.Notification.After.Location = location
	c.Parent = int64(chain[len(chain)-1].ParentID)
	c.Name = chain[len(chain)-1].Name
	if err := ValidateNotification(c); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("combined witness bound: %v", err)
	}
}

func TestNotificationTimesPreserveTheGenericSecondsRange(t *testing.T) {
	maxSeconds := int64(math.MaxInt64) + (time.Time{}).Unix()
	for _, test := range []struct {
		name    string
		instant time.Time
	}{
		{"ordinary", time.Date(2026, 9, 16, 12, 34, 56, 123456789, time.UTC)},
		{"year10000", time.Date(10000, 1, 2, 3, 4, 5, 987654321, time.UTC)},
		{"negative_year", time.Date(-500, 6, 7, 8, 9, 10, 123456789, time.UTC)},
		{"minimum_seconds", time.Unix(math.MinInt64, 1).UTC()},
		{"maximum_supported_seconds", time.Unix(maxSeconds, 999999999).UTC()},
	} {
		t.Run(test.name, func(t *testing.T) {
			instant := test.instant
			node := Node{ID: 1, Kind: storage.NodeDirectory, MetadataRevision: 2, DirectoryRevision: 1,
				AccessTime: instant, ModTime: instant, CreationTime: &instant, ChangeTime: &instant}
			before, after := node.Attr(), node.Attr()
			before.MetadataRevision = 1
			location := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}
			change := Change{Kind: Modified, Node: &node, Notification: &Notification{
				SubjectID: 1, SubjectKind: storage.NodeDirectory,
				Before: &EventImage{Attr: before, Location: location}, After: &EventImage{Attr: after, Location: location},
			}}
			encoded, err := EncodeNotification(change)
			if err != nil {
				t.Fatalf("encode seconds=%d nanos=%d: %v", instant.Unix(), instant.Nanosecond(), err)
			}
			decoded, err := DecodeNotification(change, encoded)
			if err != nil {
				t.Fatal(err)
			}
			for _, image := range []*EventImage{decoded.Before, decoded.After} {
				if image.Attr.CreationTime == nil || image.Attr.ChangeTime == nil {
					t.Fatal("known optional time became unknown")
				}
				for _, got := range []time.Time{image.Attr.AccessTime, image.Attr.ModTime, *image.Attr.CreationTime, *image.Attr.ChangeTime} {
					if got.Unix() != instant.Unix() || got.Nanosecond() != instant.Nanosecond() || !got.Equal(instant) {
						t.Fatalf("time changed: (%d,%d), want (%d,%d)", got.Unix(), got.Nanosecond(), instant.Unix(), instant.Nanosecond())
					}
				}
			}
			change.Notification = decoded
			again, err := EncodeNotification(change)
			if err != nil || !bytes.Equal(again, encoded) {
				t.Fatalf("time encoding was not canonical: %v", err)
			}
		})
	}
}

func TestNotificationOptionalUnknownTimesRemainDistinctFromKnownZero(t *testing.T) {
	change := notificationChange(Removed)
	zero := time.Time{}
	change.Notification.Before.Attr.CreationTime = &zero
	encoded, err := EncodeNotification(change)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeNotification(change, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Before.Attr.CreationTime == nil || !decoded.Before.Attr.CreationTime.IsZero() || decoded.Before.Attr.ChangeTime != nil {
		t.Fatal("known zero time and unknown time were conflated")
	}
	*decoded.Before.Attr.CreationTime = time.Unix(1, 0)
	if !zero.IsZero() {
		t.Fatal("decoded optional timestamp aliases source ownership")
	}
}

func TestNotificationTimestampCodecRejectsMalformedAndNoncanonicalFacts(t *testing.T) {
	instant := time.Unix(0, 0).UTC()
	for _, kind := range []ChangeKind{Created, Removed} {
		change := notificationChange(kind)
		image := change.Notification.After
		if image == nil {
			image = change.Notification.Before
		}
		image.Attr.AccessTime, image.Attr.ModTime = instant, instant
		image.Attr.CreationTime, image.Attr.ChangeTime = &instant, &instant
		if change.Node != nil {
			change.Node.AccessTime, change.Node.ModTime = instant, instant
			change.Node.CreationTime, change.Node.ChangeTime = &instant, &instant
		}
		encoded, err := EncodeNotification(change)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"AccessTime", "ModTime", "CreationTime", "ChangeTime"} {
			prefix := `"` + field + `":`
			original := prefix + `{"unix_sec":0,"nanos":0}`
			for _, malformed := range []string{
				`{"unix_sec":0,"nanos":-1}`,
				`{"unix_sec":0,"nanos":1000000000}`,
				`{"unix_sec":9223372036854775808,"nanos":0}`,
				`{"nanos":0}`,
				`{"unix_sec":0}`,
				`{"unix_sec":0,"nanos":0,"extra":0}`,
				`{"unix_sec":0,"unix_sec":0,"nanos":0}`,
				`{"unix_sec":0,"nanos":0,"nanos":0}`,
			} {
				data := bytes.Replace(encoded, []byte(original), []byte(prefix+malformed), 1)
				if bytes.Equal(data, encoded) {
					t.Fatalf("timestamp fixture has no %s", field)
				}
				if got, err := DecodeNotification(change, data); got != nil || !errors.Is(err, syscall.EIO) {
					t.Fatalf("kind=%d field=%s malformed timestamp accepted: %v", kind, field, err)
				}
			}
			if field == "AccessTime" || field == "ModTime" {
				data := bytes.Replace(encoded, []byte(original), []byte(prefix+`null`), 1)
				if _, err := DecodeNotification(change, data); !errors.Is(err, syscall.EIO) {
					t.Fatalf("required %s became null: %v", field, err)
				}
			}
		}
	}
}
