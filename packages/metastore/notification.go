package metastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	MaxNotificationAncestors = storage.MaxLocationDepth
	MaxNotificationNameBytes = storage.MaxLocationNameBytes
	MaxNotificationBytes     = 256 << 10
)

type ChangeMask uint32

const (
	ChangeName ChangeMask = 1 << iota
	ChangeSize
	ChangeAccessTime
	ChangeModTime
	ChangeAttributes
	ChangeContent
	ChangeCreationTime
	ChangeTime
)

// EventImage owns the metadata and entry witness observed in the mutation's
// transaction. Removed nodes remain classifiable without consulting a later tree.
type EventImage struct {
	Attr       storage.Attr
	Location   storage.EntryLocation
	LinkTarget []byte
}

// Notification retains both sides of a transition. Only creation lacks Before;
// only removal lacks After. A root or detached node has an explicit location state.
type Notification struct {
	SubjectID   int64
	SubjectKind storage.NodeKind
	ChangeMask  ChangeMask
	Before      *EventImage
	After       *EventImage
}

// Calendar-form JSON timestamps cannot represent every instant accepted by the
// authority's seconds/nanoseconds storage. These private wire values preserve
// the same range without changing the public event images.
type notificationTime struct {
	UnixSec int64 `json:"unix_sec"`
	Nanos   int32 `json:"nanos"`
}

type notificationAttr struct {
	ID                uint64
	Kind              storage.NodeKind
	Size              int64
	AccessTime        notificationTime
	ModTime           notificationTime
	CreationTime      *notificationTime
	ChangeTime        *notificationTime
	MetadataRevision  storage.NodeMetadataRevision
	DirectoryRevision storage.DirectoryRevision
	Metadata          storage.Metadata
}

type notificationImage struct {
	Attr       notificationAttr
	Location   storage.EntryLocation
	LinkTarget []byte
}

type notificationWire struct {
	SubjectID   int64
	SubjectKind storage.NodeKind
	ChangeMask  ChangeMask
	Before      *notificationImage
	After       *notificationImage
}

func notificationTimeOf(value time.Time) notificationTime {
	return notificationTime{UnixSec: value.Unix(), Nanos: int32(value.Nanosecond())}
}

func optionalNotificationTimeOf(value *time.Time) *notificationTime {
	if value == nil {
		return nil
	}
	encoded := notificationTimeOf(*value)
	return &encoded
}

func (value notificationTime) time() (time.Time, error) {
	if value.Nanos < 0 || value.Nanos >= int32(time.Second) {
		return time.Time{}, fmt.Errorf("notification time has invalid nanoseconds: %w", syscall.EIO)
	}
	return time.Unix(value.UnixSec, int64(value.Nanos)).UTC(), nil
}

func optionalNotificationTime(value *notificationTime) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	decoded, err := value.time()
	if err != nil {
		return nil, err
	}
	return &decoded, nil
}

func notificationImageOf(image *EventImage) *notificationImage {
	if image == nil {
		return nil
	}
	a := image.Attr
	return &notificationImage{Attr: notificationAttr{
		ID: a.ID, Kind: a.Kind, Size: a.Size,
		AccessTime: notificationTimeOf(a.AccessTime), ModTime: notificationTimeOf(a.ModTime),
		CreationTime: optionalNotificationTimeOf(a.CreationTime), ChangeTime: optionalNotificationTimeOf(a.ChangeTime),
		MetadataRevision: a.MetadataRevision, DirectoryRevision: a.DirectoryRevision, Metadata: a.Metadata,
	}, Location: image.Location, LinkTarget: image.LinkTarget}
}

func (image *notificationImage) eventImage() (*EventImage, error) {
	if image == nil {
		return nil, nil
	}
	a := image.Attr
	access, err := a.AccessTime.time()
	if err != nil {
		return nil, err
	}
	modified, err := a.ModTime.time()
	if err != nil {
		return nil, err
	}
	created, err := optionalNotificationTime(a.CreationTime)
	if err != nil {
		return nil, err
	}
	changed, err := optionalNotificationTime(a.ChangeTime)
	if err != nil {
		return nil, err
	}
	return &EventImage{Attr: storage.Attr{
		ID: a.ID, Kind: a.Kind, Size: a.Size, AccessTime: access, ModTime: modified,
		CreationTime: created, ChangeTime: changed, MetadataRevision: a.MetadataRevision,
		DirectoryRevision: a.DirectoryRevision, Metadata: a.Metadata,
	}, Location: image.Location, LinkTarget: image.LinkTarget}, nil
}

func NotificationMask(before, after Node) ChangeMask {
	mask := notificationAttrMask(before.Attr(), after.Attr())
	if before.Content != after.Content || !bytes.Equal(before.LinkTarget, after.LinkTarget) {
		mask |= ChangeContent
	}
	return mask
}

func notificationAttrMask(before, after storage.Attr) ChangeMask {
	var mask ChangeMask
	if before.Size != after.Size {
		mask |= ChangeSize
	}
	if !before.AccessTime.Equal(after.AccessTime) {
		mask |= ChangeAccessTime
	}
	if !before.ModTime.Equal(after.ModTime) {
		mask |= ChangeModTime
	}
	if before.Kind != after.Kind || !sameMetadata(before.Metadata, after.Metadata) {
		mask |= ChangeAttributes
	}
	if !sameOptionalTime(before.CreationTime, after.CreationTime) {
		mask |= ChangeCreationTime
	}
	if !sameOptionalTime(before.ChangeTime, after.ChangeTime) {
		mask |= ChangeTime
	}
	return mask
}

func sameOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func sameMetadata(a, b storage.Metadata) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Version != b[i].Version || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

// EncodeNotification bounds every image before encoding. Byte names and opaque
// values survive JSON without UTF-8 replacement or platform interpretation.
func EncodeNotification(change Change) ([]byte, error) {
	if err := ValidateNotification(change); err != nil {
		return nil, err
	}
	n := change.Notification
	encoded, err := json.Marshal(notificationWire{SubjectID: n.SubjectID, SubjectKind: n.SubjectKind,
		ChangeMask: n.ChangeMask, Before: notificationImageOf(n.Before), After: notificationImageOf(n.After)})
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxNotificationBytes {
		return nil, fmt.Errorf("notification exceeds encoded byte bound: %w", syscall.EFBIG)
	}
	return encoded, nil
}

// DecodeNotification rejects absent, noncanonical and contradictory history.
func DecodeNotification(change Change, encoded []byte) (*Notification, error) {
	lengths := ChangePayloadLengths{Name: int64(len(change.Name)), Notification: int64(len(encoded))}
	if change.From != nil {
		lengths.FromName = int64(len(change.From.Name))
	}
	if change.Node != nil {
		lengths.Content = int64(len(change.Node.Content))
		lengths.Target = int64(len(change.Node.LinkTarget))
		metadata, err := storage.EncodeMetadata(change.Node.Metadata)
		if err != nil {
			return nil, err
		}
		lengths.Metadata = int64(len(metadata))
	}
	if err := lengths.Check(); err != nil {
		return nil, err
	}
	var wire *notificationWire
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return nil, fmt.Errorf("decode notification: %w: %w", syscall.EIO, err)
	}
	if wire == nil {
		return nil, fmt.Errorf("notification facts are absent: %w", syscall.EIO)
	}
	before, err := wire.Before.eventImage()
	if err != nil {
		return nil, err
	}
	after, err := wire.After.eventImage()
	if err != nil {
		return nil, err
	}
	n := &Notification{SubjectID: wire.SubjectID, SubjectKind: wire.SubjectKind, ChangeMask: wire.ChangeMask,
		Before: before, After: after}
	change.Notification = n
	canonical, err := EncodeNotification(change)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("noncanonical notification facts: %w", syscall.EIO)
	}
	return n, nil
}

func ValidateNotification(change Change) error {
	n := change.Notification
	if n == nil || n.SubjectID <= 0 || n.SubjectKind.Check() != nil || n.ChangeMask & ^(ChangeName|ChangeSize|ChangeAccessTime|ChangeModTime|ChangeAttributes|ChangeContent|ChangeCreationTime|ChangeTime) != 0 {
		return fmt.Errorf("invalid notification subject or change mask: %w", syscall.EIO)
	}
	if _, err := NotificationIdentityHighWater(n); err != nil {
		return err
	}
	if (change.Kind == Removed) != (change.Node == nil) || (change.Kind == Created) != (n.Before == nil) || (change.Kind == Removed) != (n.After == nil) {
		return fmt.Errorf("notification has incomplete transition images: %w", syscall.EIO)
	}
	nameBytes := 0
	for _, image := range []*EventImage{n.Before, n.After} {
		if image == nil {
			continue
		}
		if err := validateEventImage(image, uint64(n.SubjectID)); err != nil {
			return err
		}
		for _, at := range image.Location.Ancestors {
			if len(at.Name) > MaxNotificationNameBytes-nameBytes {
				return fmt.Errorf("notification exceeds name byte bound: %w", syscall.EFBIG)
			}
			nameBytes += len(at.Name)
		}
	}
	subject := n.After
	if subject == nil {
		subject = n.Before
	}
	if subject == nil || subject.Attr.Kind != n.SubjectKind {
		return fmt.Errorf("notification subject disagrees with image: %w", syscall.EIO)
	}
	if change.Node != nil {
		if err := change.Node.Metadata.Check(); err != nil {
			return err
		}
		if len(change.Node.LinkTarget) > storage.MaxLinkTargetBytes {
			return syscall.EFBIG
		}
		a, b := change.Node.Attr(), n.After.Attr
		if a.ID != b.ID || a.MetadataRevision != b.MetadataRevision || a.DirectoryRevision != b.DirectoryRevision || notificationAttrMask(a, b) != 0 || !bytes.Equal(change.Node.LinkTarget, n.After.LinkTarget) {
			return fmt.Errorf("notification image disagrees with node: %w", syscall.EIO)
		}
	}
	if n.Before != nil && n.After != nil {
		if n.Before.Location.RootNodeID != n.After.Location.RootNodeID || n.Before.Attr.MetadataRevision > n.After.Attr.MetadataRevision {
			return fmt.Errorf("notification images disagree about root or revision order: %w", syscall.EIO)
		}
	}
	named := func(image *EventImage, parent int64, name []byte) bool {
		if image == nil || image.Location.State != storage.LocationLinked {
			return false
		}
		entries := image.Location.Ancestors
		at := entries[len(entries)-1]
		return at.ParentID == uint64(parent) && bytes.Equal(at.Name, name)
	}
	switch change.Kind {
	case Created:
		if !named(n.After, change.Parent, change.Name) || n.ChangeMask != ChangeName {
			return fmt.Errorf("invalid created notification: %w", syscall.EIO)
		}
	case Removed:
		if !named(n.Before, change.Parent, change.Name) || n.ChangeMask != ChangeName {
			return fmt.Errorf("invalid removed notification: %w", syscall.EIO)
		}
	case Renamed:
		if change.From == nil || !named(n.Before, change.From.Parent, change.From.Name) || !named(n.After, change.Parent, change.Name) || n.ChangeMask & ^ChangeTime != ChangeName {
			return fmt.Errorf("invalid renamed notification: %w", syscall.EIO)
		}
		before, after := n.Before.Location.Ancestors, n.After.Location.Ancestors
		if before[len(before)-1].EntryID != after[len(after)-1].EntryID || notificationAttrMask(n.Before.Attr, n.After.Attr) & ^ChangeTime != 0 || !bytes.Equal(n.Before.LinkTarget, n.After.LinkTarget) {
			return fmt.Errorf("rename changes unrelated node or entry identity: %w", syscall.EIO)
		}
	case Modified:
		if n.ChangeMask&ChangeName != 0 || !sameEventLocation(n.Before.Location, n.After.Location) {
			return fmt.Errorf("modification changes entry identity: %w", syscall.EIO)
		}
		if change.Parent == 0 && len(change.Name) == 0 {
			if n.Before.Location.State == storage.LocationLinked {
				return fmt.Errorf("unnamed modification carries an entry: %w", syscall.EIO)
			}
		} else if !named(n.Before, change.Parent, change.Name) || !named(n.After, change.Parent, change.Name) {
			return fmt.Errorf("invalid modified notification: %w", syscall.EIO)
		}
		if n.ChangeMask & ^ChangeContent != notificationAttrMask(n.Before.Attr, n.After.Attr) {
			return fmt.Errorf("modification mask disagrees with event images: %w", syscall.EIO)
		}
		if !bytes.Equal(n.Before.LinkTarget, n.After.LinkTarget) && n.ChangeMask&ChangeContent == 0 {
			return fmt.Errorf("modified link target lacks content change: %w", syscall.EIO)
		}
	default:
		return fmt.Errorf("invalid notification change kind: %w", syscall.EIO)
	}
	return nil
}

func validateEventImage(image *EventImage, subject uint64) error {
	a := image.Attr
	if a.ID != subject || a.Kind.Check() != nil || a.Size < 0 || a.MetadataRevision == 0 || uint64(a.MetadataRevision) > math.MaxInt64 || uint64(a.DirectoryRevision) > math.MaxInt64 || (a.Kind == storage.NodeDirectory) != (a.DirectoryRevision != 0) {
		return fmt.Errorf("event image has invalid node facts: %w", syscall.EIO)
	}
	if err := a.Metadata.Check(); err != nil {
		return fmt.Errorf("invalid event metadata: %w", errors.Join(syscall.EIO, err))
	}
	if len(image.LinkTarget) > storage.MaxLinkTargetBytes {
		return fmt.Errorf("event target exceeds byte bound: %w", syscall.EFBIG)
	}
	if a.Kind != storage.NodeSymlink && len(image.LinkTarget) != 0 || a.Kind == storage.NodeSymlink && a.Size != int64(len(image.LinkTarget)) || a.Kind == storage.NodeDirectory && a.Size != 0 {
		return fmt.Errorf("event target or size disagrees with node kind: %w", syscall.EIO)
	}
	if image.Location.NodeID != subject {
		return fmt.Errorf("event location disagrees with subject: %w", syscall.EIO)
	}
	if err := image.Location.Check(); err != nil {
		return fmt.Errorf("invalid event location: %w", errors.Join(syscall.EIO, err))
	}
	return nil
}

func sameEventLocation(a, b storage.EntryLocation) bool {
	if a.State != b.State || a.RootNodeID != b.RootNodeID || a.NodeID != b.NodeID || len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i, at := range a.Ancestors {
		other := b.Ancestors[i]
		if at.ParentID != other.ParentID || at.EntryID != other.EntryID || at.NodeID != other.NodeID || !bytes.Equal(at.Name, other.Name) {
			return false
		}
	}
	return true
}

// NotificationIdentityHighWater bounds allocation against identities retained
// only by historical images after their entries have been detached.
func NotificationIdentityHighWater(n *Notification) (int64, error) {
	if n == nil || n.SubjectID <= 0 {
		return 0, fmt.Errorf("notification has no subject identity: %w", syscall.EIO)
	}
	high := uint64(n.SubjectID)
	for _, image := range []*EventImage{n.Before, n.After} {
		if image == nil {
			continue
		}
		high = max(high, image.Attr.ID, image.Location.RootNodeID, image.Location.NodeID)
		if len(image.Location.Ancestors) > MaxNotificationAncestors {
			return 0, fmt.Errorf("notification ancestry exceeds depth bound: %w", syscall.EFBIG)
		}
		for _, at := range image.Location.Ancestors {
			high = max(high, at.ParentID, at.NodeID, uint64(at.EntryID))
		}
	}
	if high > math.MaxInt64 {
		return 0, fmt.Errorf("notification identity exceeds durable integer range: %w", syscall.EIO)
	}
	return int64(high), nil
}
