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
	return observer.CheckDirectoryMetadataObservation()
}

// resolveCreateName captures the complete Windows-visible namespace before
// selecting one raw child. OpenAt or OpenChildRef checks its guards at authority.
func resolveCreateName(ctx context.Context, t *tree, name string) (resolvedCreateName, error) {
	if t == nil || t.export == nil || t.authority == nil || t.authority.raw == nil {
		return resolvedCreateName{}, ErrConfig
	}
	server := t.export.server
	authorize := func(ctx context.Context, operation storage.Operation) error {
		return server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation})
	}
	return resolveName(ctx, t.export.share.Backend, t.authority.raw, name, server.config.Limits, authorize, server.nameComparer)
}

func resolveName(ctx context.Context, backend storage.FileStorage, session storage.FileSession, name string, limits Limits, authorize func(context.Context, storage.Operation) error, compare nameComparer) (resolvedCreateName, error) {
	path, err := parseSMBPath(name)
	if err != nil {
		return resolvedCreateName{}, err
	}
	if compare == nil {
		if err := nameComparisonAvailable(); err != nil {
			return resolvedCreateName{}, err
		}
		compare = nativeNameCompare
	}
	return resolveNameWithComparer(ctx, backend, session, path, limits, authorize, compare)
}

func resolveNameWithComparer(ctx context.Context, backend storage.FileStorage, session storage.FileSession, path smbPath, limits Limits, authorize func(context.Context, storage.Operation) error, compare nameComparer) (resolvedCreateName, error) {
	if backend == nil || authorize == nil || compare == nil || limits.MaxDirectoryBytes <= 0 || limits.MaxFrameBytes <= 0 {
		return resolvedCreateName{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return resolvedCreateName{}, err
	}
	limit := min(limits.MaxDirectoryBytes, int64(storage.MaxDirectoryBytes))
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
		if attr.ID == 0 || !attr.IsDir() || attr.Size < 0 {
			return syscall.EIO
		}
		charge, err := storage.MetadataRetentionBytes(metadataBytes)
		if err != nil {
			return err
		}
		if charge+256 > limit {
			return syscall.EFBIG
		}
		return nil
	})
	root, err := backend.Stat(rootContext, "")
	if err != nil {
		return resolvedCreateName{}, namespaceFailure(err)
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
	rootID := root.ID
	guards := storage.NamespaceGuards{RootID: rootID}
	if len(path.components) == 0 {
		attr := root.Clone()
		return resolvedCreateName{RootID: rootID, Root: true, Condition: storage.ChildCondition{State: storage.SameNode, NodeID: rootID}, Attr: &attr, DirectoryRequired: true}, nil
	}
	observer, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return resolvedCreateName{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		return resolvedCreateName{}, err
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
		result, err := newNameObservationResult(limit, parent.NodeID, guards)
		if err != nil {
			return resolvedCreateName{}, err
		}
		if err := authorize(ctx, storage.OpReplicationSnapshot); err != nil {
			return resolvedCreateName{}, namespaceFailure(err)
		}
		options := storage.DirectoryMetadataOptions{Guards: cloneNamespaceGuards(guards)}
		observation, err := observer.ObserveDirectoryMetadata(ctx, parent, options, result)
		if err != nil {
			result.Fail(err)
			return resolvedCreateName{}, namespaceFailure(err)
		}
		if err := observation.Check(parent, options); err != nil {
			result.Fail(err)
			return resolvedCreateName{}, err
		}
		entries, err := result.Entries()
		if err != nil {
			return resolvedCreateName{}, namespaceFailure(err)
		}
		projected, err := projectDirectory(entries, compare)
		if err != nil {
			return resolvedCreateName{}, err
		}
		selected, present, err := selectName(projected, nameUnits(part), compare)
		if err != nil {
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
		entry := entries[selected]
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

func newNameObservationResult(limit int64, parent uint64, guards storage.NamespaceGuards) (*storage.ListResult, error) {
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
		charge, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
		if err != nil {
			return 0, err
		}
		// UTF-16 never needs more than two bytes per UTF-8 byte. Index and
		// duplicate-identity bookkeeping are charged before any leaf is loaded.
		return charge + 128 + 2*nameBytes, nil
	})
}
