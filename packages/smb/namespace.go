package smb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type resolvedName struct {
	rootID            uint64
	root              bool
	selection         storage.ChildSelection
	condition         storage.ChildCondition
	attr              *storage.Attr
	directoryRequired bool
	release           func()
}

func (n *resolvedName) releaseCapture() {
	if n.release != nil {
		n.release()
		n.release = nil
	}
}

type unknownNamespaceError struct{ cause error }

func (e *unknownNamespaceError) Error() string {
	return "namespace observation failed: " + e.cause.Error()
}
func (e *unknownNamespaceError) Unwrap() error         { return e.cause }
func (e *unknownNamespaceError) Classification() error { return syscall.EIO }

// A backend ENOENT does not prove which path component was absent. Only a
// complete parent observation can distinguish a missing leaf from unknown state.
func namespaceFailure(err error) error {
	if errors.Is(err, syscall.ENOENT) {
		return &unknownNamespaceError{cause: err}
	}
	return err
}

type directoryCaptureReservation struct {
	registry *handleRegistry
	parent   uint64
	once     sync.Once
	entries  int64
	bytes    int64
}

func (r *handleRegistry) reserveDirectoryCapture(ctx context.Context, parent uint64) (*directoryCaptureReservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.retired {
		r.mu.Unlock()
		return nil, syscall.EBADF
	}
	r.mu.Unlock()
	reservation := &directoryCaptureReservation{registry: r, parent: parent}
	if err := reservation.grow(0, 256); err != nil {
		return nil, err
	}
	return reservation, nil
}

func (r *directoryCaptureReservation) grow(entries, bytes int64) error {
	if entries < 0 || bytes < 0 {
		return syscall.EINVAL
	}
	registry := r.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.retired {
		return syscall.EBADF
	}
	if entries > int64(registry.limits.MaxDirectoryEntries)-registry.directoryEntries ||
		bytes > registry.limits.MaxDirectoryBytes-registry.directoryBytes {
		return syscall.ENOMEM
	}
	server := registry.tree.export.server
	server.mu.Lock()
	defer server.mu.Unlock()
	export := registry.tree.export
	if export.stopping || entries > int64(registry.limits.MaxDirectoryEntries)-export.directoryEntries ||
		bytes > registry.limits.MaxDirectoryBytes-export.directoryBytes {
		return syscall.ENOMEM
	}
	registry.directoryEntries += entries
	registry.directoryBytes += bytes
	export.directoryEntries += entries
	export.directoryBytes += bytes
	r.entries += entries
	r.bytes += bytes
	return nil
}

func (r *directoryCaptureReservation) release() {
	r.once.Do(func() {
		registry := r.registry
		registry.mu.Lock()
		registry.directoryEntries -= r.entries
		registry.directoryBytes -= r.bytes
		server := registry.tree.export.server
		server.mu.Lock()
		registry.tree.export.directoryEntries -= r.entries
		registry.tree.export.directoryBytes -= r.bytes
		server.mu.Unlock()
		registry.mu.Unlock()
	})
}

func (r *directoryCaptureReservation) attributeBudget(_ storage.Attr, metadataBytes int64) error {
	charge, err := storage.MetadataRetentionBytes(metadataBytes)
	if err != nil {
		return err
	}
	return r.grow(0, charge)
}

func (r *directoryCaptureReservation) entryBudget(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
	if index >= r.registry.limits.MaxDirectoryEntries {
		return 0, syscall.ENOMEM
	}
	if attr.ID == 0 || r.parent != 0 && attr.ID == r.parent || attr.Kind.Check() != nil ||
		attr.Kind != storage.NodeDirectory && attr.Size < 0 {
		return 0, syscall.EIO
	}
	charge, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
	if err != nil {
		return 0, err
	}
	// Account for the Windows UTF-16 projection, sort index, and duplicate-ID map
	// before the producer loads the corresponding raw name and metadata.
	retained := charge + 128 + 2*nameBytes
	if err := r.grow(1, retained); err != nil {
		return 0, err
	}
	return retained, nil
}

type namespaceReferenceRetainer func(handleReference)
type namespaceAuthorizer func(context.Context, authz.AccessRequest) error
type namespaceActionFactory func() (storage.FileActionID, error)

func resolveName(
	ctx context.Context,
	tree *tree,
	name string,
	retain namespaceReferenceRetainer,
	authorize namespaceAuthorizer,
	action namespaceActionFactory,
) (resolvedName, error) {
	path, err := parseSMBPath(name)
	if err != nil {
		return resolvedName{}, err
	}
	if err := nameComparisonAvailable(); err != nil {
		return resolvedName{}, err
	}
	return resolveNameWithComparer(ctx, tree, path, retain, authorize, action, nativeNameCompare)
}

// resolveNameWithComparer performs one traversal and never retries a stale
// observation. CREATE owns the bounded retry budget for the final guarded open.
func resolveNameWithComparer(
	ctx context.Context,
	tree *tree,
	path smbPath,
	retain namespaceReferenceRetainer,
	authorize namespaceAuthorizer,
	action namespaceActionFactory,
	compare nameComparer,
) (resolvedName, error) {
	if tree == nil || tree.export == nil || tree.authority == nil || tree.authority.raw == nil || tree.files == nil ||
		retain == nil || authorize == nil || action == nil || compare == nil {
		return resolvedName{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return resolvedName{}, err
	}
	if len(path.components) > storage.MaxNamespaceGuards {
		return resolvedName{}, syscall.EFBIG
	}

	backend := tree.export.share.Backend
	if err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: storage.OpVolumeStat}); err != nil {
		return resolvedName{}, err
	}
	rootBudget, err := tree.files.reserveDirectoryCapture(ctx, 0)
	if err != nil {
		return resolvedName{}, err
	}
	rootContext := storage.WithBoundedAttrResult(ctx, tree.files.limits.MaxDirectoryBytes, rootBudget.attributeBudget)
	root, err := backend.Stat(rootContext, "")
	if err != nil {
		rootBudget.release()
		return resolvedName{}, namespaceFailure(err)
	}
	if root.ID == 0 || root.Kind != storage.NodeDirectory || root.Size < 0 {
		rootBudget.release()
		return resolvedName{}, syscall.EIO
	}
	rootID := root.ID
	if len(path.components) == 0 {
		attr := root.Clone()
		return resolvedName{
			rootID: rootID, root: true,
			condition: storage.ChildCondition{State: storage.SameNode, NodeID: rootID},
			attr:      &attr, directoryRequired: true, release: rootBudget.release,
		}, nil
	}
	rootBudget.release()

	directories, ok := tree.authority.raw.(storage.DirectoryMetadataObserver)
	if !ok {
		return resolvedName{}, syscall.EOPNOTSUPP
	}
	if err := directories.CheckDirectoryMetadataObservation(); err != nil {
		return resolvedName{}, err
	}
	references, ok := tree.authority.raw.(storage.NodeReferences)
	if !ok {
		return resolvedName{}, syscall.EOPNOTSUPP
	}
	if err := references.CheckNodeReferences(); err != nil {
		return resolvedName{}, err
	}

	rootReference, rootScope, err := openResolverDirectory(ctx, tree, references, rootID, storage.ChildSelection{}, true, retain, authorize, action)
	if err != nil {
		return resolvedName{}, err
	}
	_ = rootReference // Ownership transferred through retain.
	parent := storage.DirectoryTarget{NodeID: rootID, Scope: &rootScope}
	guards := storage.NamespaceGuards{RootID: rootID}

	for index, part := range path.components {
		if err := ctx.Err(); err != nil {
			return resolvedName{}, err
		}
		capture, err := tree.files.reserveDirectoryCapture(ctx, parent.NodeID)
		if err != nil {
			return resolvedName{}, err
		}
		result, err := storage.NewListResult(
			min(tree.files.limits.MaxDirectoryBytes, int64(storage.MaxDirectoryBytes)), 0, capture.entryBudget,
		)
		if err != nil {
			capture.release()
			return resolvedName{}, err
		}
		if err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: storage.OpFileObserveDirectoryMetadata}); err != nil {
			capture.release()
			return resolvedName{}, err
		}
		options := storage.DirectoryMetadataOptions{Guards: guards.Clone()}
		observed, readErr := directories.ObserveDirectoryMetadata(ctx, parent, options, result)
		if readErr != nil {
			capture.release()
			return resolvedName{}, namespaceFailure(readErr)
		}
		entries, entriesErr := result.Entries()
		if entriesErr != nil {
			capture.release()
			return resolvedName{}, namespaceFailure(entriesErr)
		}
		if err := observed.Check(parent, options); err != nil {
			capture.release()
			return resolvedName{}, syscall.EIO
		}
		projected, projectionErr := projectDirectory(entries, compare)
		if projectionErr != nil {
			capture.release()
			return resolvedName{}, projectionErr
		}
		selected, present, selectionErr := selectName(projected, nameUnits(part), compare)
		if selectionErr != nil {
			capture.release()
			return resolvedName{}, selectionErr
		}
		guards.Directories = append(guards.Directories, storage.DirectoryObservation{
			ParentID: observed.Observation.ParentID, Revision: bytes.Clone(observed.Observation.Revision),
		})
		final := index == len(path.components)-1
		if !present {
			capture.release()
			if !final {
				return resolvedName{}, fmt.Errorf("component %d: %w", index, errPathMissing)
			}
			selection := storage.ChildSelection{
				Name: storage.ChildName{Parent: parent, RawLeaf: []byte(part)}, Guards: guards.Clone(),
			}
			if err := selection.Check(); err != nil {
				return resolvedName{}, err
			}
			return resolvedName{
				rootID: rootID, selection: selection, condition: storage.ChildCondition{State: storage.Absent},
				directoryRequired: path.directoryRequired,
			}, nil
		}
		entry := entries[selected]
		leaf := []byte(entry.Name)
		attr := entry.Attr.Clone()
		guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: parent.NodeID, RawLeaf: bytes.Clone(leaf), ChildID: attr.ID})
		if err := guards.Check(); err != nil {
			capture.release()
			return resolvedName{}, err
		}
		if final {
			if path.directoryRequired && attr.Kind != storage.NodeDirectory {
				capture.release()
				return resolvedName{}, syscall.ENOTDIR
			}
			selection := storage.ChildSelection{
				Name: storage.ChildName{Parent: parent, RawLeaf: bytes.Clone(leaf)}, Guards: guards.Clone(),
			}
			if err := selection.Check(); err != nil {
				capture.release()
				return resolvedName{}, err
			}
			return resolvedName{
				rootID: rootID, selection: selection,
				condition: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
				attr:      &attr, directoryRequired: path.directoryRequired, release: capture.release,
			}, nil
		}
		capture.release()
		if attr.Kind != storage.NodeDirectory {
			return resolvedName{}, syscall.ENOTDIR
		}
		selection := storage.ChildSelection{
			Name: storage.ChildName{Parent: parent, RawLeaf: bytes.Clone(leaf)}, Guards: guards.Clone(),
		}
		_, scope, err := openResolverDirectory(ctx, tree, references, attr.ID, selection, false, retain, authorize, action)
		if err != nil {
			return resolvedName{}, err
		}
		parent = storage.DirectoryTarget{NodeID: attr.ID, Scope: &scope}
	}
	return resolvedName{}, syscall.EIO
}

func openResolverDirectory(
	ctx context.Context,
	tree *tree,
	references storage.NodeReferences,
	nodeID uint64,
	selection storage.ChildSelection,
	root bool,
	retain namespaceReferenceRetainer,
	authorize namespaceAuthorizer,
	action namespaceActionFactory,
) (storage.NodeReference, storage.UseScope, error) {
	operation := storage.OpFileOpenChildRef
	if root {
		operation = storage.OpFileOpenNodeRef
	}
	if err := authorize(ctx, authz.AccessRequest{
		Volume: tree.export.share.Volume, Operation: operation,
		Open: storage.OpenAccess{Read: true},
	}); err != nil {
		return nil, storage.UseScope{}, err
	}
	if err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: storage.OpFileScope}); err != nil {
		return nil, storage.UseScope{}, err
	}
	actionID, err := action()
	if err != nil {
		return nil, storage.UseScope{}, err
	}
	options := storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: nodeID},
		Action: actionID, MetadataAccess: storage.ReadMetadata,
	}
	capture, err := tree.files.reserveDirectoryCapture(ctx, 0)
	if err != nil {
		return nil, storage.UseScope{}, err
	}
	call := storage.WithBoundedAttrResult(ctx, tree.files.limits.MaxDirectoryBytes, capture.attributeBudget)
	var result storage.NodeOpenResult
	if root {
		result, err = references.OpenNodeRef(call, nodeID, options)
	} else {
		result, err = references.OpenChildRef(call, selection, options)
	}
	capture.release()
	if result.Reference != nil {
		retain(result.Reference)
	}
	if err != nil {
		if result.Reference != nil || result.Outcome != 0 || !zeroCreateAttr(result.Attr) {
			return result.Reference, storage.UseScope{}, errors.Join(namespaceFailure(err), syscall.EIO)
		}
		return result.Reference, storage.UseScope{}, namespaceFailure(err)
	}
	if result.Reference == nil || result.Outcome != storage.Opened || result.Attr.ID != nodeID || result.Attr.Kind != storage.NodeDirectory {
		return result.Reference, storage.UseScope{}, syscall.EIO
	}
	identity, err := storage.ReferenceNodeID(result.Reference)
	if err != nil || identity != nodeID {
		return result.Reference, storage.UseScope{}, errors.Join(err, syscall.EIO)
	}
	if err := checkSMBReference(result.Reference); err != nil {
		return result.Reference, storage.UseScope{}, err
	}
	if err := result.Reference.CheckScopedReference(); err != nil {
		return result.Reference, storage.UseScope{}, err
	}
	scope, err := result.Reference.Scope(ctx)
	if err != nil {
		return result.Reference, storage.UseScope{}, err
	}
	if err := scope.Check(); err != nil {
		return result.Reference, storage.UseScope{}, err
	}
	return result.Reference, scope, nil
}
