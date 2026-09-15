package metastore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"syscall"
)

// Notification limits bound history capture before a mutation commits.
const (
	MaxNotificationAncestors = 256
	MaxNotificationNameBytes = 64 << 10
	MaxNotificationBytes     = 256 << 10
)

// ChangeMask identifies the fields whose values changed at the authority.
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

// DirectoryAncestor identifies one directory at the event's committed position.
// The first ancestor is the root and has no name; each subsequent name belongs
// to the preceding directory.
type DirectoryAncestor struct {
	DirectoryID int64
	Name        []byte
}

// LocationFacts preserves the ancestry of a directory entry independently of
// later moves and removals.
type LocationFacts struct {
	Ancestors []DirectoryAncestor
	LeafName  []byte
}

// Notification is the immutable directory-observation payload of a Change.
// SubjectKind is the fs.FileMode type bits; an ordinary file has kind zero.
type Notification struct {
	SubjectID   int64
	SubjectKind fs.FileMode
	// Directory selects directory-name notifications, including directory symlinks.
	Directory  bool
	ChangeMask ChangeMask
	Before     *LocationFacts
	After      *LocationFacts
}

// NotificationMask compares authoritative node values without broadening a
// metadata update into every notification filter category.
func NotificationMask(before, after Node) ChangeMask {
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
	if before.Mode != after.Mode {
		mask |= ChangeAttributes
	}
	if before.Content != after.Content {
		mask |= ChangeContent
	}
	return mask
}

// EncodeNotification produces a bounded, canonical representation. Names remain
// byte arrays so invalid UTF-8 names in Linux volumes cannot be rewritten by JSON.
func EncodeNotification(change Change) ([]byte, error) {
	if err := ValidateNotification(change); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(change.Notification)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxNotificationBytes {
		return nil, fmt.Errorf("notification exceeds encoded byte bound: %w", syscall.EFBIG)
	}
	return encoded, nil
}

// DecodeNotification refuses missing, noncanonical or corrupt historical facts.
func DecodeNotification(change Change, encoded []byte) (*Notification, error) {
	lengths := ChangePayloadLengths{Name: int64(len(change.Name)), Notification: int64(len(encoded))}
	if change.From != nil {
		lengths.FromName = int64(len(change.From.Name))
	}
	if change.Node != nil {
		lengths.Content = int64(len(change.Node.Content))
	}
	if err := lengths.Check(); err != nil {
		return nil, err
	}
	var n *Notification
	if err := json.Unmarshal(encoded, &n); err != nil {
		return nil, fmt.Errorf("decode notification: %w: %w", syscall.EIO, err)
	}
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

// ValidateNotification verifies historical facts against their replication row.
func ValidateNotification(change Change) error {
	n := change.Notification
	if n == nil || n.SubjectID <= 0 || (n.SubjectKind != 0 && n.SubjectKind != fs.ModeDir && n.SubjectKind != fs.ModeSymlink) ||
		n.ChangeMask & ^(ChangeName|ChangeSize|ChangeAccessTime|ChangeModTime|ChangeAttributes|ChangeContent|ChangeCreationTime|ChangeTime) != 0 {
		return fmt.Errorf("invalid notification subject or change mask: %w", syscall.EIO)
	}
	if n.SubjectKind != fs.ModeSymlink && n.Directory != (n.SubjectKind == fs.ModeDir) {
		return fmt.Errorf("notification directory hint disagrees with subject kind: %w", syscall.EIO)
	}
	if (change.Kind == Removed) != (change.Node == nil) {
		return fmt.Errorf("notification row has inconsistent node presence: %w", syscall.EIO)
	}
	if change.Node != nil && (change.Node.ID != n.SubjectID || change.Node.Mode.Type() != n.SubjectKind) {
		return fmt.Errorf("notification subject disagrees with node: %w", syscall.EIO)
	}
	var nameBytes int
	for _, at := range []*LocationFacts{n.Before, n.After} {
		if at == nil {
			continue
		}
		if len(at.Ancestors) > MaxNotificationAncestors {
			return fmt.Errorf("notification ancestry exceeds depth bound: %w", syscall.EFBIG)
		}
		if len(at.Ancestors) == 0 || !notificationComponent(at.LeafName) {
			return fmt.Errorf("notification has no valid entry: %w", syscall.EIO)
		}
		nameBytes += len(at.LeafName)
		seen := make(map[int64]bool, len(at.Ancestors))
		for i, ancestor := range at.Ancestors {
			if ancestor.DirectoryID <= 0 || ancestor.DirectoryID == n.SubjectID || seen[ancestor.DirectoryID] ||
				(i == 0 && len(ancestor.Name) != 0) || (i > 0 && !notificationComponent(ancestor.Name)) {
				return fmt.Errorf("notification has invalid ancestry: %w", syscall.EIO)
			}
			seen[ancestor.DirectoryID] = true
			nameBytes += len(ancestor.Name)
		}
	}
	if n.Before != nil && n.After != nil && n.Before.Ancestors[0].DirectoryID != n.After.Ancestors[0].DirectoryID {
		return fmt.Errorf("notification locations disagree about root: %w", syscall.EIO)
	}
	if nameBytes > MaxNotificationNameBytes {
		return fmt.Errorf("notification exceeds name byte bound: %w", syscall.EFBIG)
	}
	named := func(at *LocationFacts, parent int64, name []byte) bool {
		return at != nil && at.Ancestors[len(at.Ancestors)-1].DirectoryID == parent && bytes.Equal(at.LeafName, name)
	}
	switch change.Kind {
	case Created:
		if n.Before != nil || !named(n.After, change.Parent, change.Name) || n.ChangeMask != ChangeName {
			return fmt.Errorf("invalid created notification: %w", syscall.EIO)
		}
	case Removed:
		if change.Node != nil || n.After != nil || !named(n.Before, change.Parent, change.Name) || n.ChangeMask != ChangeName {
			return fmt.Errorf("invalid removed notification: %w", syscall.EIO)
		}
	case Renamed:
		if change.From == nil || !named(n.Before, change.From.Parent, change.From.Name) || !named(n.After, change.Parent, change.Name) || (n.ChangeMask & ^ChangeTime) != ChangeName {
			return fmt.Errorf("invalid renamed notification: %w", syscall.EIO)
		}
	case Modified:
		if n.ChangeMask&ChangeName != 0 {
			return fmt.Errorf("modification includes a name change: %w", syscall.EIO)
		}
		if change.Parent == 0 && len(change.Name) == 0 {
			if n.Before != nil || n.After != nil {
				return fmt.Errorf("unnamed modification carries an entry: %w", syscall.EIO)
			}
		} else if !named(n.Before, change.Parent, change.Name) || !named(n.After, change.Parent, change.Name) || !sameLocationFacts(n.Before, n.After) {
			return fmt.Errorf("invalid modified notification: %w", syscall.EIO)
		}
	default:
		return fmt.Errorf("invalid notification change kind: %w", syscall.EIO)
	}
	return nil
}

func notificationComponent(name []byte) bool {
	return len(name) > 0 && !bytes.Equal(name, []byte(".")) && !bytes.Equal(name, []byte("..")) && bytes.IndexByte(name, 0) < 0 && bytes.IndexByte(name, '/') < 0
}

func sameLocationFacts(a, b *LocationFacts) bool {
	if !bytes.Equal(a.LeafName, b.LeafName) || len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i, ancestor := range a.Ancestors {
		if ancestor.DirectoryID != b.Ancestors[i].DirectoryID || !bytes.Equal(ancestor.Name, b.Ancestors[i].Name) {
			return false
		}
	}
	return true
}
