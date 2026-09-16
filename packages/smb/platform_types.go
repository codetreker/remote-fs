package smb

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsAccess uint32

const (
	windowsReadData windowsAccess = 1 << iota
	windowsWriteData
	windowsAppendData
	windowsDelete
	windowsReadAttributes
	windowsWriteAttributes
	windowsReadSecurity
	windowsSynchronize
	windowsDeleteChild
)
const windowsAllAccess = windowsReadData | windowsWriteData | windowsAppendData | windowsDelete | windowsReadAttributes | windowsWriteAttributes | windowsReadSecurity | windowsSynchronize | windowsDeleteChild

type windowsShare uint8

const (
	windowsShareRead windowsShare = 1 << iota
	windowsShareWrite
	windowsShareDelete
)
const windowsShareAll = windowsShareRead | windowsShareWrite | windowsShareDelete

type windowsDisposition uint8

const (
	windowsSupersede windowsDisposition = iota + 1
	windowsOpen
	windowsCreate
	windowsOpenIf
	windowsOverwrite
	windowsOverwriteIf
)

type windowsKind uint8

const (
	windowsAny windowsKind = iota
	windowsRegularFile
	windowsDirectory
)

type windowsOpenIntent struct {
	Access                          windowsAccess
	Share                           windowsShare
	Disposition                     windowsDisposition
	Kind                            windowsKind
	DeleteOnClose, OpenReparsePoint bool
	MaximumAllowed                  bool
}

// A lookup carries the exact generic proof once the local directory view has
// resolved its Windows spelling. The proof is not rebuilt for an unknown action.
type windowsLookup struct {
	ParentID        uint64
	ParentReference storage.FileReferenceID
	Name            string
	ExpectedID      uint64
	proof           *nameProof
}
type windowsOpenRequest struct {
	windowsOpenIntent
	Lookup        windowsLookup
	DOSAttributes uint32
}
type windowsRenameRequest struct {
	Destination windowsLookup
	Replace     bool
}

type windowsBasicAttr struct {
	storage.Attr
	CreationTime, ChangeTime time.Time
	DOSAttributes            uint32
	DeletePending            bool
}
type windowsNameState uint8

const (
	windowsNameRoot windowsNameState = iota + 1
	windowsNameLinked
	windowsNameDetached
)

type windowsNameInfo struct {
	State windowsNameState
	Path  string
}
type windowsAttr struct {
	windowsBasicAttr
	NameInfo windowsNameInfo
}
type windowsEntry struct {
	Name string
	Attr windowsBasicAttr
}
type windowsAttrChange struct {
	storage.AttrChange
	CreationTime, ChangeTime *time.Time
	DOSAttributes            *uint32
}

func (i windowsOpenIntent) Check() error {
	if i.Access & ^windowsAllAccess != 0 || i.Share & ^windowsShareAll != 0 || i.Kind > windowsDirectory || i.Disposition < windowsSupersede || i.Disposition > windowsOverwriteIf {
		return syscall.EINVAL
	}
	if i.DeleteOnClose && !i.MaximumAllowed && i.Access&windowsDelete == 0 {
		return syscall.EINVAL
	}
	if (i.Disposition == windowsOverwrite || i.Disposition == windowsOverwriteIf) && !i.MaximumAllowed && i.Access&windowsWriteData == 0 {
		return syscall.EINVAL
	}
	if i.Disposition == windowsSupersede && !i.MaximumAllowed && i.Access&windowsDelete == 0 {
		return syscall.EINVAL
	}
	if i.Kind == windowsDirectory && (i.Disposition == windowsSupersede || i.Disposition == windowsOverwrite || i.Disposition == windowsOverwriteIf) {
		return syscall.EINVAL
	}
	return nil
}
func (l windowsLookup) Check() error {
	if l.Name == "" {
		if l.ParentID != 0 || l.ParentReference != 0 || l.ExpectedID != 0 {
			return syscall.EINVAL
		}
		return nil
	}
	if l.ParentID == 0 {
		return syscall.EINVAL
	}
	_, err := nameKey([]byte(l.Name))
	return err
}
func (r windowsOpenRequest) Check() error {
	if err := r.windowsOpenIntent.Check(); err != nil {
		return err
	}
	if err := r.Lookup.Check(); err != nil {
		return err
	}
	if !validDOSAttributes(r.DOSAttributes) {
		return syscall.EINVAL
	}
	if r.Lookup.Name == "" && (r.Disposition != windowsOpen || r.Kind == windowsRegularFile || r.DeleteOnClose) {
		return syscall.EINVAL
	}
	return nil
}
func (r windowsRenameRequest) Check() error {
	if err := r.Destination.Check(); err != nil {
		return err
	}
	if r.Destination.Name == "" {
		return syscall.EINVAL
	}
	return nil
}
func (n windowsNameInfo) Check() error {
	switch n.State {
	case windowsNameRoot, windowsNameDetached:
		if n.Path != "" {
			return syscall.EIO
		}
		return nil
	case windowsNameLinked:
		if n.Path == "" {
			return syscall.EIO
		}
		if err := validateWindowsPath(n.Path); err != nil {
			return errors.Join(syscall.EIO, err)
		}
		return nil
	default:
		return syscall.EIO
	}
}
func (c windowsAttrChange) Check() error {
	if c.DOSAttributes != nil && !validDOSAttributes(*c.DOSAttributes) {
		return syscall.EINVAL
	}
	return c.AttrChange.Check()
}

type lockKind uint8

const (
	lockUnlock lockKind = iota
	lockShared
	lockExclusive
	lockInvalid
)

type windowsLockRange struct {
	Offset, Length  uint64
	Type            lockKind
	FailImmediately bool
}
type windowsLockBatch struct{ Ranges []windowsLockRange }

const windowsMaxLockBatch = 65535

func (r windowsLockRange) Check() error {
	if r.Type != lockUnlock && r.Type != lockShared && r.Type != lockExclusive {
		return syscall.EINVAL
	}
	if r.Length != 0 && r.Length-1 > math.MaxUint64-r.Offset {
		return syscall.EINVAL
	}
	return nil
}
func (b windowsLockBatch) Check() error {
	if len(b.Ranges) == 0 || len(b.Ranges) > windowsMaxLockBatch {
		return syscall.EINVAL
	}
	return nil
}

func checkWindowsLinkTarget(target string) error {
	if len(target) > storage.MaxLinkTargetBytes {
		return syscall.ENAMETOOLONG
	}
	if target == "" || !utf8.ValidString(target) || strings.ContainsAny(target, "\\:\x00") || strings.HasPrefix(target, "//") {
		return syscall.EINVAL
	}
	return nil
}

type windowsSymlinkInfo struct {
	Target   string
	Location windowsNameInfo
	Unparsed string
}
type windowsSymlinkError struct {
	windowsSymlinkInfo
	Err error
}

func (e *windowsSymlinkError) Error() string { return "Windows path encounters a symbolic link" }
func (e *windowsSymlinkError) Unwrap() error { return e.Err }

type windowsFailure string

const (
	windowsSharingViolation windowsFailure = "sharing-violation"
	windowsLockConflict     windowsFailure = "lock-conflict"
	windowsDeletePending    windowsFailure = "delete-pending"
	windowsRangeNotLocked   windowsFailure = "range-not-locked"
	windowsNotReparsePoint  windowsFailure = "not-reparse-point"
)

type windowsError struct {
	Code    syscall.Errno
	Failure windowsFailure
	Err     error
}

func (e *windowsError) Error() string { return string(e.Failure) + ": " + e.Err.Error() }
func (e *windowsError) Unwrap() error { return e.Err }
func (e *windowsError) Classification() error {
	if e.Code != 0 {
		return e.Code
	}
	return e.Err
}
func windowsFailureOf(err error) windowsFailure {
	if err == nil {
		return ""
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result windowsFailure
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			next := windowsFailureOf(child)
			if next == "" || result != "" && result != next {
				return ""
			}
			result = next
		}
		return result
	}
	if classified, ok := err.(*windowsError); ok {
		switch classified.Failure {
		case windowsSharingViolation, windowsLockConflict, windowsDeletePending, windowsRangeNotLocked, windowsNotReparsePoint:
			return classified.Failure
		}
		return ""
	}
	var generic *storage.FileError
	if errors.As(err, &generic) && generic.Conflict != nil {
		switch generic.Conflict.Kind {
		case storage.ConflictClaim:
			return windowsSharingViolation
		case storage.ConflictRange:
			return windowsLockConflict
		case storage.ConflictDraining:
			return windowsDeletePending
		}
	}
	return windowsFailureOf(errors.Unwrap(err))
}

func projectWindowsAttr(observation storage.FileObservation, defaults windowsMetadata) (windowsAttr, error) {
	attr := observation.Attr
	if attr.ID == 0 || attr.Size < 0 {
		return windowsAttr{}, syscall.EIO
	}
	if err := attr.Metadata.Check(); err != nil {
		return windowsAttr{}, errors.Join(syscall.EIO, err)
	}
	if err := attr.Kind.Check(); err != nil {
		return windowsAttr{}, errors.Join(syscall.EIO, err)
	}
	value, present := attr.Metadata.Get(windowsMetadataKey)
	metadata, err := decodeWindowsMetadata(value.Data, value.Version, present)
	if err != nil {
		return windowsAttr{}, err
	}
	if !present {
		metadata = defaults
	}
	if !validDOSAttributes(metadata.Attributes) || metadata.DirectorySymlink && attr.Kind != storage.NodeSymlink {
		return windowsAttr{}, syscall.EIO
	}
	out := windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: attr, DOSAttributes: metadata.Attributes, DeletePending: observation.Removal.State == storage.EntryDraining}}
	if attr.CreationTime != nil {
		out.CreationTime = *attr.CreationTime
	}
	if attr.ChangeTime != nil {
		out.ChangeTime = *attr.ChangeTime
	}
	out.Attr.Metadata = nil
	out.Attr.CreationTime = nil
	out.Attr.ChangeTime = nil
	if attr.Kind == storage.NodeDirectory || metadata.DirectorySymlink {
		out.DOSAttributes = (out.DOSAttributes &^ dosNormal) | dosDirectory
	}
	if observation.Location != nil {
		location := observation.Location
		if err := location.Check(); err != nil {
			return windowsAttr{}, errors.Join(syscall.EIO, err)
		}
		if location.NodeID != attr.ID {
			return windowsAttr{}, fmt.Errorf("location and metadata identify different nodes: %w", syscall.EIO)
		}
		switch location.State {
		case storage.LocationRoot:
			out.NameInfo.State = windowsNameRoot
		case storage.LocationDetached:
			out.NameInfo.State = windowsNameDetached
		case storage.LocationLinked:
			parts := make([]string, len(location.Ancestors))
			for i, entry := range location.Ancestors {
				parts[i] = string(entry.Name)
			}
			out.NameInfo = windowsNameInfo{State: windowsNameLinked, Path: strings.Join(parts, "/")}
		}
	}
	return out, nil
}
