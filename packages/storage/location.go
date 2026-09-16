package storage

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"syscall"
)

const (
	MaxLocationDepth        = 256
	MaxLocationNameBytes    = 64 << 10
	MaxEntryNameBytes       = 4096
	MaxDirectoryPageEntries = 1024
	MaxDirectoryPageBytes   = 1 << 20
	DirectoryPageBaseBytes  = 128
)

type EntryID uint64
type DirectoryRevision uint64

// EntryCondition binds one exact name to its entry and node identities in one
// parent directory revision. Clients interpret name equivalence locally.
type EntryCondition struct {
	ParentID          uint64
	DirectoryRevision DirectoryRevision
	EntryID           EntryID
	NodeID            uint64
	Name              []byte
}

type LocationState uint8

const (
	LocationRoot LocationState = iota + 1
	LocationLinked
	LocationDetached
)

// EntryLocation captures an ordered root-to-node chain in the same observation
// as the node's metadata and link target. An identity-only operation needs no
// location witness; a name operation validates every supplied ancestor condition.
type EntryLocation struct {
	State      LocationState
	RootNodeID uint64
	NodeID     uint64
	Ancestors  []EntryCondition
}

// DirectoryCursor binds pagination to one parent and directory revision. After
// is the last exact byte name returned; the zero value starts a new observation.
type DirectoryCursor struct {
	ParentID uint64
	Revision DirectoryRevision
	After    []byte
}

type DirectoryPageRequest struct {
	Revision   DirectoryRevision
	Cursor     DirectoryCursor
	MaxEntries int
	MaxBytes   int
}

type DirectoryEntry struct {
	EntryID EntryID
	Name    []byte
	Attr    Attr
}

type DirectoryPage struct {
	ParentID uint64
	Revision DirectoryRevision
	Entries  []DirectoryEntry
	Next     DirectoryCursor
	Done     bool
}

func CheckEntryName(name []byte) error {
	if len(name) == 0 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.IndexByte(name, 0) >= 0 || bytes.IndexByte(name, '/') >= 0 {
		return fmt.Errorf("entry name must be a nonempty exact component: %w", syscall.EINVAL)
	}
	if len(name) > MaxEntryNameBytes {
		return fmt.Errorf("entry name exceeds %d bytes: %w", MaxEntryNameBytes, syscall.EFBIG)
	}
	return nil
}

func (c EntryCondition) Check() error {
	if c.ParentID == 0 || c.DirectoryRevision == 0 || c.EntryID == 0 || c.NodeID == 0 || c.ParentID == c.NodeID {
		return fmt.Errorf("entry condition requires distinct node identities and nonzero versions: %w", syscall.EINVAL)
	}
	return CheckEntryName(c.Name)
}

func (l EntryLocation) Check() error {
	if l.RootNodeID == 0 || l.NodeID == 0 {
		return fmt.Errorf("location requires root and node identities: %w", syscall.EINVAL)
	}
	switch l.State {
	case LocationRoot:
		if l.NodeID != l.RootNodeID || len(l.Ancestors) != 0 {
			return fmt.Errorf("root location cannot carry entries: %w", syscall.EINVAL)
		}
		return nil
	case LocationDetached:
		if l.NodeID == l.RootNodeID || len(l.Ancestors) != 0 {
			return fmt.Errorf("detached location cannot carry an ancestor chain: %w", syscall.EINVAL)
		}
		return nil
	case LocationLinked:
		if len(l.Ancestors) == 0 || len(l.Ancestors) > MaxLocationDepth {
			return fmt.Errorf("linked location exceeds its entry depth bound: %w", syscall.EFBIG)
		}
	default:
		return fmt.Errorf("unknown location state: %w", syscall.EINVAL)
	}
	parent := l.RootNodeID
	nodes := map[uint64]bool{parent: true}
	entries := make(map[EntryID]bool, len(l.Ancestors))
	remaining := MaxLocationNameBytes
	for _, at := range l.Ancestors {
		if err := at.Check(); err != nil {
			return err
		}
		if at.ParentID != parent || nodes[at.NodeID] || entries[at.EntryID] {
			return fmt.Errorf("location is not one acyclic identity chain: %w", syscall.EINVAL)
		}
		if len(at.Name) > remaining {
			return fmt.Errorf("location names exceed %d bytes: %w", MaxLocationNameBytes, syscall.EFBIG)
		}
		remaining -= len(at.Name)
		nodes[at.NodeID] = true
		entries[at.EntryID] = true
		parent = at.NodeID
	}
	if parent != l.NodeID {
		return fmt.Errorf("location does not reach its declared node: %w", syscall.EINVAL)
	}
	return nil
}

func (c DirectoryCursor) IsZero() bool {
	return c.ParentID == 0 && c.Revision == 0 && len(c.After) == 0
}

func (c DirectoryCursor) Check() error {
	if c.IsZero() {
		return nil
	}
	if c.ParentID == 0 || c.Revision == 0 {
		return fmt.Errorf("directory cursor requires parent and revision: %w", syscall.EINVAL)
	}
	return CheckEntryName(c.After)
}

func (r DirectoryPageRequest) Check() error {
	if r.MaxEntries < 1 || r.MaxEntries > MaxDirectoryPageEntries || r.MaxBytes < 1 || r.MaxBytes > MaxDirectoryPageBytes {
		return fmt.Errorf("directory page limits are outside the bounded range: %w", syscall.EINVAL)
	}
	if err := r.Cursor.Check(); err != nil {
		return err
	}
	if !r.Cursor.IsZero() && (r.Revision == 0 || r.Revision != r.Cursor.Revision) {
		return fmt.Errorf("directory continuation must retain its original revision: %w", syscall.EINVAL)
	}
	return nil
}

// Check validates page identities and ordering. The producer also accounts for
// metadata payload bytes before loading them under the request's MaxBytes.
func (p DirectoryPage) Check() error {
	if p.ParentID == 0 || p.Revision == 0 || len(p.Entries) > MaxDirectoryPageEntries {
		return fmt.Errorf("directory page has invalid identity, revision or count: %w", syscall.EIO)
	}
	entries := make(map[EntryID]bool, len(p.Entries))
	nodes := make(map[uint64]bool, len(p.Entries))
	var previous []byte
	remaining := MaxDirectoryPageBytes - DirectoryPageBaseBytes
	for _, entry := range p.Entries {
		if err := CheckEntryName(entry.Name); err != nil {
			return fmt.Errorf("directory page has invalid name: %w", errors.Join(syscall.EIO, err))
		}
		if entry.EntryID == 0 || entry.Attr.ID == 0 || entry.Attr.ID == p.ParentID || entries[entry.EntryID] || nodes[entry.Attr.ID] || previous != nil && bytes.Compare(previous, entry.Name) >= 0 {
			return fmt.Errorf("directory page has duplicate or unordered entries: %w", syscall.EIO)
		}
		if err := entry.Attr.Kind.Check(); err != nil || entry.Attr.MetadataRevision == 0 || (entry.Attr.Kind == NodeDirectory) != (entry.Attr.DirectoryRevision != 0) {
			return fmt.Errorf("directory entry has invalid node kind or revision: %w", syscall.EIO)
		}
		metadataBytes, err := entry.Attr.Metadata.EncodedSize()
		if err != nil {
			return errors.Join(syscall.EIO, err)
		}
		charge, err := DirectoryEntryBytes(len(entry.Name), metadataBytes)
		if err != nil {
			return err
		}
		if charge > remaining {
			return fmt.Errorf("directory page exceeds byte bound: %w", syscall.EFBIG)
		}
		remaining -= charge
		entries[entry.EntryID] = true
		nodes[entry.Attr.ID] = true
		previous = entry.Name
	}
	if p.Done {
		if !p.Next.IsZero() {
			return fmt.Errorf("complete directory page has a continuation: %w", syscall.EIO)
		}
	} else {
		if len(p.Entries) == 0 || p.Next.ParentID != p.ParentID || p.Next.Revision != p.Revision || !bytes.Equal(p.Next.After, previous) {
			return fmt.Errorf("directory continuation does not follow the last returned entry: %w", syscall.EIO)
		}
	}
	return nil
}

// DirectoryEntryBytes charges retained entry state before variable payloads are
// loaded. Metadata's canonical byte length bounds its decoded envelope overhead.
func DirectoryEntryBytes(nameBytes, metadataBytes int) (int, error) {
	if nameBytes < 1 || nameBytes > MaxEntryNameBytes || metadataBytes < 6 || metadataBytes > MaxMetadataBytes {
		return 0, fmt.Errorf("directory entry payload lengths exceed bounds: %w", syscall.EFBIG)
	}
	return 256 + nameBytes + 4*metadataBytes, nil
}

// Clone gives the recipient ownership of every entry name in the witness.
func (l EntryLocation) Clone() EntryLocation {
	l.Ancestors = slices.Clone(l.Ancestors)
	for i := range l.Ancestors {
		l.Ancestors[i].Name = bytes.Clone(l.Ancestors[i].Name)
	}
	return l
}

// EntryLookup is one exact-byte name observation in a parent revision.
// Found=false is a confirmed absent entry, with no invented node attributes.
type EntryLookup struct {
	ParentID          uint64
	DirectoryRevision DirectoryRevision
	Name              []byte
	Found             bool
	EntryID           EntryID
	Attr              Attr
}

func (l EntryLookup) Check(name []byte) error {
	if err := CheckEntryName(name); err != nil {
		return err
	}
	if l.ParentID == 0 || l.DirectoryRevision == 0 || !bytes.Equal(l.Name, name) {
		return fmt.Errorf("entry lookup does not match its exact requested name: %w", syscall.EIO)
	}
	if !l.Found {
		if l.EntryID != 0 || !reflect.ValueOf(l.Attr).IsZero() {
			return fmt.Errorf("absent entry lookup carries node facts: %w", syscall.EIO)
		}
		return nil
	}
	if l.EntryID == 0 || l.Attr.ID == l.ParentID {
		return fmt.Errorf("entry lookup has invalid identity: %w", syscall.EIO)
	}
	if err := l.Attr.Check(); err != nil {
		return errors.Join(syscall.EIO, err)
	}
	return nil
}

func (l EntryLookup) Clone() EntryLookup {
	l.Name = bytes.Clone(l.Name)
	l.Attr = l.Attr.Clone()
	return l
}
