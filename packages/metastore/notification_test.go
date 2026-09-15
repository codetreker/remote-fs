package metastore

import (
	"bytes"
	"errors"
	"io/fs"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func notificationChange(kind ChangeKind) Change {
	at := &LocationFacts{Ancestors: []DirectoryAncestor{{DirectoryID: 1}, {DirectoryID: 2, Name: []byte("dir")}}, LeafName: []byte{0xff}}
	n := &Notification{SubjectID: 3, ChangeMask: ChangeName}
	c := Change{Kind: kind, Parent: 2, Name: []byte{0xff}, Node: &Node{ID: 3}, Notification: n}
	switch kind {
	case Created:
		n.After = at
	case Removed:
		n.Before = at
		c.Node = nil
	case Modified:
		n.Before = at
		n.After = at
		n.ChangeMask = ChangeSize
	case Renamed:
		n.Before = at
		n.After = at
		c.From = &Location{Parent: 2, Name: []byte{0xff}}
	}
	return c
}

func TestNotificationRoundTripPreservesHistoricalFacts(t *testing.T) {
	for _, kind := range []ChangeKind{Created, Removed, Modified, Renamed} {
		c := notificationChange(kind)
		data, err := EncodeNotification(c)
		if err != nil {
			t.Fatal(err)
		}
		n, err := DecodeNotification(c, data)
		if err != nil || !reflect.DeepEqual(n, c.Notification) {
			t.Fatalf("round trip kind %v: %+v, %v", kind, n, err)
		}
		if _, err := DecodeNotification(c, append(data, ' ')); !errors.Is(err, syscall.EIO) {
			t.Fatalf("noncanonical encoding: %v", err)
		}
	}
	root := Change{Kind: Modified, Node: &Node{ID: 1, Mode: fs.ModeDir}, Notification: &Notification{SubjectID: 1, SubjectKind: fs.ModeDir, Directory: true}}
	if err := ValidateNotification(root); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, []byte("null"), []byte("{"), bytes.Repeat([]byte("x"), MaxNotificationBytes+1)} {
		if _, err := DecodeNotification(root, data); err == nil {
			t.Fatalf("invalid payload accepted: length %d", len(data))
		}
	}
}

func TestNotificationRejectsContradictoryOrMissingFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Change)
	}{
		{"missing", func(c *Change) { c.Notification = nil }},
		{"invalid identity", func(c *Change) { c.Notification.SubjectID = 0 }},
		{"invalid type", func(c *Change) { c.Notification.SubjectKind = fs.ModeSocket }},
		{"wrong node", func(c *Change) { c.Node.ID = 4 }},
		{"wrong type", func(c *Change) { c.Node.Mode = fs.ModeDir }},
		{"unknown mask", func(c *Change) { c.Notification.ChangeMask = 1 << 31 }},
		{"missing location", func(c *Change) { c.Notification.After = nil }},
		{"no ancestry", func(c *Change) { c.Notification.After.Ancestors = nil }},
		{"root has name", func(c *Change) { c.Notification.After.Ancestors[0].Name = []byte("root") }},
		{"cycle", func(c *Change) { c.Notification.After.Ancestors[1].DirectoryID = 1 }},
		{"self ancestor", func(c *Change) { c.Notification.After.Ancestors[1].DirectoryID = 3 }},
		{"invalid ancestor", func(c *Change) { c.Notification.After.Ancestors[1].Name = []byte("..") }},
		{"invalid leaf", func(c *Change) { c.Notification.After.LeafName = []byte("a/b") }},
		{"wrong parent", func(c *Change) { c.Parent = 8 }},
		{"wrong name", func(c *Change) { c.Name = []byte("other") }},
		{"creation before", func(c *Change) { c.Notification.Before = c.Notification.After }},
		{"name bound", func(c *Change) { c.Notification.After.LeafName = bytes.Repeat([]byte("x"), MaxNotificationNameBytes+1) }},
		{"depth bound", func(c *Change) {
			c.Notification.After.Ancestors = make([]DirectoryAncestor, MaxNotificationAncestors+1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := notificationChange(Created)
			tt.mutate(&c)
			if err := ValidateNotification(c); err == nil {
				t.Fatal("invalid facts accepted")
			}
		})
	}
	for _, kind := range []ChangeKind{Removed, Modified, Renamed, ChangeKind(99)} {
		c := notificationChange(kind)
		c.Notification.ChangeMask = ChangeAttributes
		if kind == Modified {
			c.Notification.Before = nil
		}
		if err := ValidateNotification(c); err == nil {
			t.Fatalf("invalid kind %v accepted", kind)
		}
	}
	c := notificationChange(Modified)
	c.Notification.ChangeMask = ChangeName
	if err := ValidateNotification(c); err == nil {
		t.Fatal("modified name mask accepted")
	}
	c = notificationChange(Modified)
	c.Parent = 0
	c.Name = nil
	if err := ValidateNotification(c); err == nil {
		t.Fatal("root entry accepted")
	}
	c = notificationChange(Renamed)
	c.From = nil
	if err := ValidateNotification(c); err == nil {
		t.Fatal("rename without source accepted")
	}
}

func TestNotificationMaskUsesActualDifferences(t *testing.T) {
	before := Node{ID: 1, Mode: 0600, Size: 1, AccessTime: time.Unix(1, 0), ModTime: time.Unix(2, 0), Content: "old"}
	if mask := NotificationMask(before, before); mask != 0 {
		t.Fatalf("unchanged mask %v", mask)
	}
	after := Node{ID: 1, Mode: 0644, Size: 2, AccessTime: time.Unix(2, 0), ModTime: time.Unix(3, 0), Content: "new"}
	want := ChangeSize | ChangeAccessTime | ChangeModTime | ChangeAttributes | ChangeContent
	if mask := NotificationMask(before, after); mask != want {
		t.Fatalf("mask %v, want %v", mask, want)
	}
	after = before
	after.ModTime = before.ModTime.In(time.FixedZone("other", 3600))
	if mask := NotificationMask(before, after); mask != 0 {
		t.Fatalf("equivalent times changed mask %v", mask)
	}
}

func TestNotificationReservationAccountsAndOwnsPayload(t *testing.T) {
	original := notificationChange(Created)
	encoded, err := EncodeNotification(original)
	if err != nil {
		t.Fatal(err)
	}
	meta := original
	meta.Notification = nil
	meta.Name = nil
	lengths := ChangePayloadLengths{Name: int64(len(original.Name)), Notification: int64(len(encoded))}
	result, err := NewChangeResult(lengths.Name+lengths.Notification, 0, func(_ int, c Change, l ChangePayloadLengths) (int64, error) {
		if c.Notification != nil {
			t.Fatal("reservation retained facts before payload admission")
		}
		return l.Name + l.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, fits, err := result.Reserve(meta, lengths)
	if err != nil || !fits {
		t.Fatalf("reserve: %v %v", fits, err)
	}
	if err := reservation.Commit(original.Name, nil, "", encoded); err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		encoded[i] = 0
	}
	original.Name[0] = 0
	changes, err := result.Changes()
	if err != nil || len(changes) != 1 || changes[0].Notification.After.LeafName[0] != 0xff {
		t.Fatalf("result aliases source: %+v %v", changes, err)
	}
	bad, err := NewChangeResult(1024, 0, func(_ int, _ Change, l ChangePayloadLengths) (int64, error) { return l.Notification, nil })
	if err != nil {
		t.Fatal(err)
	}
	reservation, fits, err = bad.Reserve(meta, ChangePayloadLengths{Name: 1, Notification: 2})
	if err != nil || !fits {
		t.Fatal(err)
	}
	if err := reservation.Commit([]byte("f"), nil, "", []byte("{}")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt facts: %v", err)
	}
	if changes, err := bad.Changes(); !errors.Is(err, syscall.EIO) || changes != nil {
		t.Fatalf("corrupt result exposed: %+v %v", changes, err)
	}
}

func TestNotificationWindowsMetadataMasksRoundTrip(t *testing.T) {
	for _, mask := range []ChangeMask{ChangeCreationTime, ChangeTime, ChangeAttributes | ChangeCreationTime | ChangeTime} {
		c := notificationChange(Modified)
		c.Notification.ChangeMask = mask
		encoded, err := EncodeNotification(c)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeNotification(c, encoded)
		if err != nil || decoded.ChangeMask != mask {
			t.Fatalf("Windows mask %v: %+v %v", mask, decoded, err)
		}
	}
}

func TestChangePayloadLimitIncludesEveryFieldWithoutOverflow(t *testing.T) {
	for _, lengths := range []ChangePayloadLengths{
		{}, {Name: MaxChangePayloadBytes},
		{Name: 3, FromName: 5, Content: MaxChangePayloadBytes - 8 - MaxNotificationBytes, Notification: MaxNotificationBytes},
	} {
		if err := lengths.Check(); err != nil {
			t.Fatalf("legal boundary %+v: %v", lengths, err)
		}
	}
	for _, lengths := range []ChangePayloadLengths{
		{Name: -1}, {FromName: -1}, {Content: -1}, {Notification: -1},
		{Name: MaxChangePayloadBytes, FromName: 1},
		{Name: 3, FromName: 5, Content: MaxChangePayloadBytes - 8 - MaxNotificationBytes, Notification: MaxNotificationBytes + 1},
		{Name: math.MaxInt64, Content: math.MaxInt64},
		{Notification: MaxNotificationBytes + 1},
	} {
		if err := lengths.Check(); err == nil {
			t.Fatalf("invalid lengths accepted: %+v", lengths)
		}
	}
	c := notificationChange(Created)
	encoded, err := EncodeNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Node.Content = Key(strings.Repeat("x", MaxChangePayloadBytes-len(c.Name)-len(encoded)+1))
	if _, err := DecodeNotification(c, encoded); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized decoded row: %v", err)
	}
}

func TestNotificationDirectoryHintIsExplicitAndKindConsistent(t *testing.T) {
	for _, directory := range []bool{false, true} {
		c := notificationChange(Created)
		c.Node.Mode = fs.ModeSymlink
		c.Notification.SubjectKind = fs.ModeSymlink
		c.Notification.Directory = directory
		encoded, err := EncodeNotification(c)
		if err != nil {
			t.Fatal(err)
		}
		n, err := DecodeNotification(c, encoded)
		if err != nil || n.Directory != directory {
			t.Fatalf("symlink directory hint %t: %+v %v", directory, n, err)
		}
	}
	c := notificationChange(Created)
	encoded, err := EncodeNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	missing := bytes.Replace(encoded, []byte(`,"Directory":false`), nil, 1)
	if bytes.Equal(missing, encoded) {
		t.Fatal("directory hint missing from canonical schema")
	}
	if _, err := DecodeNotification(c, missing); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing hint accepted: %v", err)
	}
	c.Notification.Directory = true
	if err := ValidateNotification(c); !errors.Is(err, syscall.EIO) {
		t.Fatalf("regular file directory hint accepted: %v", err)
	}
	c.Node.Mode = fs.ModeDir
	c.Notification.SubjectKind = fs.ModeDir
	c.Notification.Directory = false
	if err := ValidateNotification(c); !errors.Is(err, syscall.EIO) {
		t.Fatalf("directory false hint accepted: %v", err)
	}
}
