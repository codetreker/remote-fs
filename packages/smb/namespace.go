package smb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type resolvedCreateName struct {
	RootID            uint64
	Root              bool
	Selection         storage.ChildSelection
	Condition         storage.ChildCondition
	Attr              *storage.Attr
	DirectoryRequired bool
}

type unknownNamespaceError struct{ cause error }

func (e *unknownNamespaceError) Error() string {
	return "namespace observation failed: " + e.cause.Error()
}
func (e *unknownNamespaceError) Unwrap() error         { return e.cause }
func (e *unknownNamespaceError) Classification() error { return syscall.EIO }

// Backend ENOENT has no component-phase proof. Keep its cause without allowing
// generic errno translation to report a proven final-name absence.
func namespaceFailure(err error) error {
	if errors.Is(err, syscall.ENOENT) {
		return &unknownNamespaceError{cause: err}
	}
	return err
}

func namespaceStatus(err error) uint32 {
	switch {
	case errors.Is(err, errNameInvalid):
		return 0xc0000033
	case errors.Is(err, errPathMissing):
		return 0xc000003a
	default:
		return statusError(err)
	}
}

func checkCreateNameCapability(session storage.FileSession) error {
	observer, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return namespaceFailure(observer.CheckDirectoryMetadataObservation())
}

// resolveCreateName captures the complete Windows-visible namespace before
// selecting one raw child. OpenAt or OpenChildRef checks its guards at authority.
func resolveCreateName(ctx context.Context, t *tree, name string) (resolvedCreateName, error) {
	if t == nil || t.export == nil || t.authority == nil || t.authority.raw == nil {
		return resolvedCreateName{}, ErrConfig
	}
	capture := &referenceNameSelection{export: t.export}
	defer capture.release()
	return resolveCreateNameCaptured(ctx, t, name, capture)
}

func resolveCreateNameCaptured(ctx context.Context, t *tree, name string, capture *referenceNameSelection) (resolvedCreateName, error) {
	server := t.export.server
	authorize := func(ctx context.Context, operation storage.Operation) error {
		return server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation})
	}
	return resolveName(context.WithValue(ctx, nameCaptureKey{}, capture), t.export.share.Backend, t.authority.raw, t.authority.identity.Backend.RootNodeID, name, server.config.Limits, authorize, server.nameComparer)
}

type nameCaptureKey struct{}

func resolveName(ctx context.Context, backend storage.FileStorage, session storage.FileSession, pinnedRootNodeID uint64, name string, limits Limits, authorize func(context.Context, storage.Operation) error, compare nameComparer) (resolvedCreateName, error) {
	path, err := parseSMBPath(name)
	if err != nil {
		return resolvedCreateName{}, err
	}
	if compare == nil {
		compare, err = platformNameComparer()
		if err != nil {
			return resolvedCreateName{}, namespaceFailure(err)
		}
	}
	return resolveNameWithComparer(ctx, backend, session, pinnedRootNodeID, path, limits, authorize, compare)
}

func resolveNameWithComparer(ctx context.Context, backend storage.FileStorage, session storage.FileSession, pinnedRootNodeID uint64, path smbPath, limits Limits, authorize func(context.Context, storage.Operation) error, compare nameComparer) (resolvedCreateName, error) {
	if backend == nil || session == nil || pinnedRootNodeID == 0 || authorize == nil || compare == nil || limits.MaxDirectoryBytes <= 0 || limits.MaxFrameBytes <= 0 {
		return resolvedCreateName{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return resolvedCreateName{}, err
	}
	limit := min(int64(limits.MaxDirectoryBytes), int64(storage.MaxDirectoryBytes))
	if len(path.components) > storage.MaxNamespaceGuards {
		return resolvedCreateName{}, syscall.EFBIG
	}
	inputBytes := 0
	for _, part := range path.components {
		inputBytes += 2*utf16Length(part) + 2
	}
	if inputBytes > limits.MaxFrameBytes {
		return resolvedCreateName{}, syscall.EFBIG
	}
	if err := authorize(ctx, storage.OpVolumeStat); err != nil {
		return resolvedCreateName{}, namespaceFailure(err)
	}
	rootContext := storage.WithBoundedAttrResult(ctx, limit, func(attr storage.Attr, metadataBytes int64) error {
		if attr.ID != pinnedRootNodeID || !attr.IsDir() || attr.Size < 0 {
			return syscall.EIO
		}
		charge, err := storage.MetadataRetentionBytes(metadataBytes)
		if err != nil {
			return err
		}
		if charge+256 > limit {
			return syscall.EFBIG
		}
		if capture, ok := ctx.Value(nameCaptureKey{}).(*referenceNameSelection); ok {
			if err := capture.reserve(int(charge) + 256); err != nil {
				return err
			}
		}
		return nil
	})
	root, err := backend.Stat(rootContext, "")
	if err != nil {
		return resolvedCreateName{}, namespaceFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return resolvedCreateName{}, err
	}
	metadataBytes, err := storage.MetadataSize(root.Metadata)
	if err != nil {
		return resolvedCreateName{}, err
	}
	scalar := root
	scalar.Metadata = nil
	if err := storage.CheckAttrResultBudget(rootContext, scalar, int64(metadataBytes)); err != nil {
		return resolvedCreateName{}, err
	}
	rootID := pinnedRootNodeID
	guards := storage.NamespaceGuards{RootID: rootID}
	if len(path.components) == 0 {
		attr := root.Clone()
		return resolvedCreateName{RootID: rootID, Root: true, Condition: storage.ChildCondition{State: storage.SameNode, NodeID: rootID}, Attr: &attr, DirectoryRequired: true}, nil
	}
	root.Metadata = nil
	observer, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return resolvedCreateName{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		return resolvedCreateName{}, namespaceFailure(err)
	}
	parent := storage.DirectoryTarget{NodeID: rootID}
	for index, part := range path.components {
		if err := ctx.Err(); err != nil {
			return resolvedCreateName{}, err
		}
		if len(guards.Directories) >= storage.MaxNamespaceGuards {
			return resolvedCreateName{}, syscall.EFBIG
		}
		if err := guards.Check(); err != nil {
			return resolvedCreateName{}, err
		}
		if err := authorize(ctx, storage.OpFileObserveDirectoryMetadata); err != nil {
			return resolvedCreateName{}, namespaceFailure(err)
		}
		observation, entry, present, err := captureForwardNameComponent(ctx, observer, parent, guards, part, compare, limit)
		if err != nil {
			return resolvedCreateName{}, err
		}
		if err := ctx.Err(); err != nil {
			return resolvedCreateName{}, err
		}
		guards.Directories = append(guards.Directories, storage.DirectoryObservation{ParentID: observation.Observation.ParentID, Revision: bytes.Clone(observation.Observation.Revision)})
		if err := guards.Check(); err != nil {
			return resolvedCreateName{}, err
		}
		final := index == len(path.components)-1
		if !present {
			if !final {
				return resolvedCreateName{}, fmt.Errorf("component %d: %w", index, errPathMissing)
			}
			selection := storage.ChildSelection{Name: storage.ChildName{Parent: parent, RawLeaf: []byte(part)}, Guards: cloneNamespaceGuards(guards)}
			return resolvedCreateName{RootID: rootID, Selection: selection, Condition: storage.ChildCondition{State: storage.Absent}, DirectoryRequired: path.directoryRequired}, nil
		}
		if entry.Attr.Kind == storage.NodeSymlink {
			return resolvedCreateName{}, syscall.EOPNOTSUPP
		}
		leaf := []byte(entry.Name)
		guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: parent.NodeID, RawLeaf: leaf, ChildID: entry.Attr.ID})
		if err := guards.Check(); err != nil {
			return resolvedCreateName{}, err
		}
		if final {
			if path.directoryRequired && !entry.Attr.IsDir() {
				return resolvedCreateName{}, syscall.ENOTDIR
			}
			attr := entry.Attr.Clone()
			if _, err := projectWindowsAttributes(attr); err != nil {
				return resolvedCreateName{}, err
			}
			var version []byte
			if value, present := attr.Metadata[windowsMetadataKey]; present {
				version = bytes.Clone(value.Version)
			}
			condition := storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID, ExpectedMetadata: map[string][]byte{windowsMetadataKey: version}}
			selection := storage.ChildSelection{Name: storage.ChildName{Parent: parent, RawLeaf: bytes.Clone(leaf)}, Guards: cloneNamespaceGuards(guards)}
			return resolvedCreateName{RootID: rootID, Selection: selection, Condition: condition, Attr: &attr, DirectoryRequired: path.directoryRequired}, nil
		}
		if !entry.Attr.IsDir() {
			return resolvedCreateName{}, syscall.ENOTDIR
		}
		parent = storage.DirectoryTarget{NodeID: entry.Attr.ID}
	}
	return resolvedCreateName{}, syscall.EIO
}

func cloneNamespaceGuards(guards storage.NamespaceGuards) *storage.NamespaceGuards {
	copy := &storage.NamespaceGuards{RootID: guards.RootID, Directories: make([]storage.DirectoryObservation, len(guards.Directories)), Edges: make([]storage.ObservedEdge, len(guards.Edges))}
	for i, directory := range guards.Directories {
		copy.Directories[i] = storage.DirectoryObservation{ParentID: directory.ParentID, Revision: bytes.Clone(directory.Revision)}
	}
	for i, edge := range guards.Edges {
		copy.Edges[i] = storage.ObservedEdge{ParentID: edge.ParentID, ChildID: edge.ChildID, RawLeaf: bytes.Clone(edge.RawLeaf)}
	}
	return copy
}

func nameObservationResult(limit int64, parent uint64, guards storage.NamespaceGuards, excludeAncestors bool, reserve func(int64) error) (*storage.ListResult, error) {
	fixed := int64(512 + 128*(len(guards.Directories)+len(guards.Edges)))
	for _, directory := range guards.Directories {
		fixed += 8 * int64(len(directory.Revision))
	}
	for _, edge := range guards.Edges {
		fixed += 8 * int64(len(edge.RawLeaf))
	}
	return storage.NewListResult(limit, fixed, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		if index >= storage.MaxDirectoryEntries {
			return 0, syscall.EFBIG
		}
		if attr.ID == 0 || attr.ID == parent || attr.Kind.Check() != nil || attr.Size < 0 {
			return 0, syscall.EIO
		}
		if excludeAncestors && attr.ID == guards.RootID {
			return 0, syscall.EIO
		}
		for _, directory := range guards.Directories {
			if excludeAncestors && attr.ID == directory.ParentID {
				return 0, syscall.EIO
			}
		}
		charge, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
		if err != nil {
			return 0, err
		}
		// UTF-16 never needs more than two bytes per UTF-8 byte. Index and
		// duplicate-identity bookkeeping are charged before any leaf is loaded.
		captureCharge := 2*charge + 1536 + 2*nameBytes
		charge += 128 + 2*nameBytes
		if reserve != nil {
			if err := reserve(captureCharge); err != nil {
				return 0, err
			}
		}
		return charge, nil
	})
}

func captureForwardNameComponent(ctx context.Context, observer storage.DirectoryMetadataObserver, parent storage.DirectoryTarget, guards storage.NamespaceGuards, part string, compare nameComparer, limit int64) (storage.DirectoryMetadataObservation, storage.Entry, bool, error) {
	var reserve func(int64) error
	if capture, ok := ctx.Value(nameCaptureKey{}).(*referenceNameSelection); ok {
		var staging int
		var err error
		limit, staging, err = reserveDirectoryCapture(capture.export)
		if err != nil {
			return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
		}
		charged := staging
		defer func() { releaseDirectoryBytes(capture.export, charged) }()
		reserve = func(amount int64) error {
			if err := reserveDirectoryBytes(capture.export, int(amount)); err != nil {
				return err
			}
			charged += int(amount)
			return nil
		}
	}
	result, err := nameObservationResult(limit, parent.NodeID, guards, true, reserve)
	if err != nil {
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
	}
	options := storage.DirectoryMetadataOptions{Guards: cloneNamespaceGuards(guards)}
	observation, err := observer.ObserveDirectoryMetadata(ctx, parent, options, result)
	if err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, namespaceFailure(err)
	}
	if err = ctx.Err(); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
	}
	if err = observation.Check(parent, options); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
	}
	entries, err := result.Entries()
	if err != nil {
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, namespaceFailure(err)
	}
	projected, err := projectDirectory(entries, compare)
	if err != nil {
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
	}
	selected, present, err := selectName(projected, nameUnits(part), compare)
	if err != nil {
		return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
	}
	entry := storage.Entry{}
	charge := 1024 + 8*len(observation.Observation.Revision)
	if present {
		entry = entries[selected]
		metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
		if err != nil {
			return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
		}
		metadataCharge, err := storage.MetadataRetentionBytes(int64(metadataBytes))
		if err != nil {
			return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
		}
		charge += 8*len(entry.Name) + int(metadataCharge)
	}
	if capture, ok := ctx.Value(nameCaptureKey{}).(*referenceNameSelection); ok {
		if err := capture.reserve(charge); err != nil {
			return storage.DirectoryMetadataObservation{}, storage.Entry{}, false, err
		}
	}
	observation.Observation.Revision = bytes.Clone(observation.Observation.Revision)
	if present {
		entry.Name = string(bytes.Clone([]byte(entry.Name)))
		entry.Attr = entry.Attr.Clone()
	}
	return observation, entry, present, nil
}
