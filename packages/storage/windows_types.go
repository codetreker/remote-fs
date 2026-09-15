package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// WindowsAccess is semantic access after protocol generic rights are expanded.
// ReadData and WriteData also mean listing and adding children for directories.
type WindowsAccess uint32

const (
	WindowsReadData WindowsAccess = 1 << iota
	WindowsWriteData
	WindowsAppendData
	WindowsDelete
	WindowsReadAttributes
	WindowsWriteAttributes
	WindowsReadSecurity
	WindowsSynchronize
	WindowsDeleteChild
)
const WindowsAllAccess = WindowsReadData | WindowsWriteData | WindowsAppendData | WindowsDelete | WindowsReadAttributes | WindowsWriteAttributes | WindowsReadSecurity | WindowsSynchronize | WindowsDeleteChild

type WindowsShare uint8

const (
	WindowsShareRead WindowsShare = 1 << iota
	WindowsShareWrite
	WindowsShareDelete
)
const WindowsShareAll = WindowsShareRead | WindowsShareWrite | WindowsShareDelete

type WindowsDisposition uint8

const (
	WindowsSupersede WindowsDisposition = iota + 1
	WindowsOpen
	WindowsCreate
	WindowsOpenIf
	WindowsOverwrite
	WindowsOverwriteIf
)

type WindowsKind uint8

const (
	WindowsAny WindowsKind = iota
	WindowsRegularFile
	WindowsDirectory
)

// WindowsOpenIntent is also the authorization input, so transports do not
// translate away deletion, sharing or metadata-only intent before policy runs.
type WindowsOpenIntent struct {
	Access           WindowsAccess
	Share            WindowsShare
	Disposition      WindowsDisposition
	Kind             WindowsKind
	DeleteOnClose    bool
	OpenReparsePoint bool
}

// WindowsLookup addresses a leaf under a retained or identity-addressed parent.
// The all-zero value addresses the root. ParentReference, when supplied, must
// belong to the same session and identify ParentID. Name never contains a path.
// ExpectedID zero accepts the resolved identity; nonzero requires that object.
type WindowsLookup struct {
	ParentID        uint64
	ParentReference string
	Name            string
	ExpectedID      uint64
}

type WindowsOpenRequest struct {
	WindowsOpenIntent
	Lookup WindowsLookup
	Mode   fs.FileMode
	// DOSAttributes participate in the same atomic create/open decision. The
	// disposition determines whether existing attributes are preserved or replaced.
	DOSAttributes uint32
}

// WindowsRenameRequest identifies both names at the authoritative rename point.
// Source.ExpectedID must equal the retained file identity. Destination.ExpectedID
// zero requires an absent destination; nonzero requires the exact existing node.
// Replace authorizes replacement of that expected existing node only.
type WindowsRenameRequest struct {
	Source      WindowsLookup
	Destination WindowsLookup
	Replace     bool
}

// WindowsBasicAttr is the fixed-size metadata carried by directory entries and
// retained-file observations. All four timestamps and DOS attributes are real
// authoritative values; transports must not synthesize omitted stored metadata.
type WindowsBasicAttr struct {
	Attr
	CreationTime  time.Time
	ChangeTime    time.Time
	DOSAttributes uint32
	DeletePending bool
}

type WindowsNameState uint8

const (
	WindowsNameRoot WindowsNameState = iota + 1
	WindowsNameLinked
	WindowsNameDetached
)

// WindowsMaxNameInfoBytes bounds an authoritative path, including separators,
// after the native 64-KiB ancestor-name budget and 256-component bound.
const WindowsMaxNameInfoBytes = (64 << 10) + 256

// WindowsMaxPathUTF16Units bounds a complete volume-relative path, including
// separators, so its UTF-16 byte length fits the SMB CREATE name field.
const WindowsMaxPathUTF16Units = 32767

// WindowsNameInfo describes the current authoritative name of a retained node.
// Path is a canonical volume-relative UTF-8 path with slash separators for a
// linked node. Root and detached nodes have empty paths and distinct states.
// It is captured with attributes under native ordering, never resolved by a
// transport reopening the object's former path.
type WindowsNameInfo struct {
	State WindowsNameState
	Path  string
}

type WindowsAttr struct {
	WindowsBasicAttr
	NameInfo WindowsNameInfo
}

// WindowsEntry names one child with its authoritative Windows metadata. Full
// name paths belong to retained-file observations, not directory enumeration.
type WindowsEntry struct {
	Name string
	Attr WindowsBasicAttr
}

// Check refuses incomplete or noncanonical authoritative name information.
// Invalid output is EIO; it must not become an empty or replacement path.
func (n WindowsNameInfo) Check() error {
	switch n.State {
	case WindowsNameRoot, WindowsNameDetached:
		if n.Path != "" {
			return fmt.Errorf("unnamed Windows object has a path: %w", syscall.EIO)
		}
		return nil
	case WindowsNameLinked:
		if len(n.Path) > WindowsMaxNameInfoBytes {
			return fmt.Errorf("Windows name information exceeds its %d-byte path bound: %w", WindowsMaxNameInfoBytes, syscall.EIO)
		}
		if n.Path == "" {
			return fmt.Errorf("linked Windows object has no path: %w", syscall.EIO)
		}
		units := 0
		for _, r := range n.Path {
			units++
			if r > 0xffff {
				units++
			}
			if units > WindowsMaxPathUTF16Units {
				return fmt.Errorf("Windows name information exceeds %d UTF-16 code units: %w", WindowsMaxPathUTF16Units, syscall.EIO)
			}
		}
		remaining := n.Path
		for {
			name, next, more := strings.Cut(remaining, "/")
			if err := (WindowsLookup{ParentID: 1, Name: name}).Check(); err != nil {
				return fmt.Errorf("Windows name information has an invalid path component: %w", errors.Join(syscall.EIO, err))
			}
			if !more {
				return nil
			}
			remaining = next
		}
	default:
		return fmt.Errorf("Windows name information has an unknown state: %w", syscall.EIO)
	}
}

type WindowsAttrChange struct {
	AttrChange
	CreationTime  *time.Time
	ChangeTime    *time.Time
	DOSAttributes *uint32
}

const (
	WindowsDOSReadOnly uint32 = 0x1
	WindowsDOSHidden   uint32 = 0x2
	WindowsDOSSystem   uint32 = 0x4
	// WindowsDOSDirectory is an authoritative kind hint, including directory
	// symbolic links. It cannot be set through WindowsAttrChange.
	WindowsDOSDirectory uint32 = 0x10
	WindowsDOSArchive   uint32 = 0x20
	WindowsDOSNormal    uint32 = 0x80
)
const WindowsSettableDOSAttributes = WindowsDOSReadOnly | WindowsDOSHidden | WindowsDOSSystem | WindowsDOSArchive | WindowsDOSNormal

// WindowsLockRange is a half-open byte range; its exact offset and length are
// preserved for unlock matching. Length zero is a valid distinct Windows range.
type WindowsLockRange struct {
	Offset          uint64
	Length          uint64
	Type            LockType
	FailImmediately bool
}

// WindowsLockBatch is one ordered protocol batch, not repeated single requests.
// Authority applies protocol rollback/partial-success rules and records effects.
type WindowsLockBatch struct{ Ranges []WindowsLockRange }

const WindowsMaxLockBatch = 65535
const WindowsMaxReferenceBytes = 128
const WindowsMaxLinkTargetBytes = 4096

// CheckWindowsLinkTarget checks bounded volume-path syntax without changing the
// stored target. Relative parent traversal is validated against the authoritative
// link parent by the backend; this check alone does not prove confinement.
func CheckWindowsLinkTarget(target string) error {
	if len(target) > WindowsMaxLinkTargetBytes {
		return fmt.Errorf("Windows link target exceeds its byte bound: %w", syscall.ENAMETOOLONG)
	}
	if target == "" || !utf8.ValidString(target) || strings.ContainsAny(target, "\\:\x00") || strings.HasPrefix(target, "//") {
		return fmt.Errorf("Windows link target must use volume path syntax: %w", syscall.EINVAL)
	}
	return nil
}

func (i WindowsOpenIntent) Check() error {
	if i.Access & ^WindowsAllAccess != 0 || i.Share & ^WindowsShareAll != 0 || i.Kind > WindowsDirectory || i.Disposition < WindowsSupersede || i.Disposition > WindowsOverwriteIf {
		return fmt.Errorf("invalid Windows open flags: %w", syscall.EINVAL)
	}
	if i.DeleteOnClose && i.Access&WindowsDelete == 0 {
		return fmt.Errorf("delete-on-close requires delete access: %w", syscall.EINVAL)
	}
	if (i.Disposition == WindowsOverwrite || i.Disposition == WindowsOverwriteIf) && i.Access&WindowsWriteData == 0 {
		return fmt.Errorf("overwrite requires write-data access: %w", syscall.EINVAL)
	}
	if i.Disposition == WindowsSupersede && i.Access&WindowsDelete == 0 {
		return fmt.Errorf("supersede requires delete access: %w", syscall.EINVAL)
	}
	if i.Kind == WindowsDirectory && (i.Disposition == WindowsSupersede || i.Disposition == WindowsOverwrite || i.Disposition == WindowsOverwriteIf) {
		return fmt.Errorf("directories cannot be overwritten: %w", syscall.EINVAL)
	}
	return nil
}

func (l WindowsLookup) Check() error {
	if len(l.ParentReference) > WindowsMaxReferenceBytes || strings.IndexByte(l.ParentReference, 0) >= 0 {
		return fmt.Errorf("invalid Windows parent reference: %w", syscall.EINVAL)
	}
	if l.Name == "" {
		if l.ParentID != 0 || l.ParentReference != "" || l.ExpectedID != 0 {
			return fmt.Errorf("root lookup cannot carry a parent or expected identity: %w", syscall.EINVAL)
		}
		return nil
	}
	if l.ParentID == 0 || l.Name == "." || l.Name == ".." || strings.ContainsAny(l.Name, "/\\\x00") || !utf8.ValidString(l.Name) {
		return fmt.Errorf("Windows lookup requires a valid leaf and parent identity: %w", syscall.EINVAL)
	}
	// Windows components are bounded by 255 UTF-16 code units, not UTF-8 bytes.
	units := 0
	for _, r := range l.Name {
		units++
		if r > 0xffff {
			units++
		}
	}
	if units > 255 {
		return fmt.Errorf("Windows leaf exceeds 255 UTF-16 code units: %w", syscall.ENAMETOOLONG)
	}
	return nil
}

func (r WindowsOpenRequest) Check() error {
	if err := r.WindowsOpenIntent.Check(); err != nil {
		return err
	}
	if err := r.Lookup.Check(); err != nil {
		return err
	}
	if r.Mode & ^SettableMode != 0 {
		return fmt.Errorf("unsupported Windows creation mode: %w", syscall.EINVAL)
	}
	if err := checkWindowsDOSAttributes(r.DOSAttributes); err != nil {
		return err
	}
	if r.Lookup.Name == "" && (r.Disposition != WindowsOpen || r.Kind == WindowsRegularFile || r.DeleteOnClose) {
		return fmt.Errorf("root requires an existing directory open: %w", syscall.EINVAL)
	}
	return nil
}

func (r WindowsRenameRequest) Check() error {
	if err := r.Source.Check(); err != nil {
		return err
	}
	if err := r.Destination.Check(); err != nil {
		return err
	}
	if r.Source.Name == "" || r.Destination.Name == "" || r.Source.ExpectedID == 0 {
		return fmt.Errorf("rename requires names and an expected source identity: %w", syscall.EINVAL)
	}
	if r.Destination.ExpectedID != 0 && !r.Replace {
		return fmt.Errorf("existing rename target requires replacement: %w", syscall.EINVAL)
	}
	return nil
}

func (c WindowsAttrChange) Check() error {
	if err := c.AttrChange.Check(); err != nil {
		return err
	}
	if c.DOSAttributes != nil {
		return checkWindowsDOSAttributes(*c.DOSAttributes)
	}
	return nil
}

func checkWindowsDOSAttributes(attributes uint32) error {
	if attributes & ^WindowsSettableDOSAttributes != 0 || attributes&WindowsDOSNormal != 0 && attributes != WindowsDOSNormal {
		return fmt.Errorf("unsupported Windows DOS attributes: %w", syscall.EINVAL)
	}
	return nil
}

// Check validates one lock element immediately before it is applied. Validation
// of a later element must not erase prior effects required by batch semantics.
func (r WindowsLockRange) Check() error {
	if r.Type != Unlock && r.Type != Shared && r.Type != Exclusive {
		return fmt.Errorf("invalid Windows lock type: %w", syscall.EINVAL)
	}
	if r.Offset > math.MaxInt64 || r.Length > math.MaxInt64-r.Offset {
		return fmt.Errorf("Windows lock exceeds supported byte range: %w", syscall.EINVAL)
	}
	if r.Type == Unlock && r.FailImmediately {
		return fmt.Errorf("unlock cannot request immediate conflict failure: %w", syscall.EINVAL)
	}
	return nil
}

// Check bounds the batch before admission without prevalidating later elements.
func (b WindowsLockBatch) Check() error {
	if len(b.Ranges) == 0 || len(b.Ranges) > WindowsMaxLockBatch {
		return fmt.Errorf("Windows lock batch exceeds element bounds: %w", syscall.EINVAL)
	}
	return nil
}

// CheckWindowsRange rejects negative and overflowing I/O before allocation.
func CheckWindowsRange(offset int64, length int64) error {
	if offset < 0 || length < 0 || length > math.MaxInt64-offset {
		return fmt.Errorf("invalid Windows I/O range: %w", syscall.EINVAL)
	}
	return nil
}
