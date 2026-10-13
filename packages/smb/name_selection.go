package smb

import (
	"bytes"
	"context"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceNameSelection struct {
	binding storage.NameObservation
	guards  storage.NamespaceGuards
	attr    storage.Attr
	path    string
	charge  int
	export  *Export
}

func (n *referenceNameSelection) release() {
	if n.charge != 0 {
		releaseDirectoryBytes(n.export, n.charge)
		n.charge = 0
	}
}

func (n *referenceNameSelection) reserve(amount int) error {
	if err := reserveDirectoryBytes(n.export, amount); err != nil {
		return err
	}
	n.charge += amount
	return nil
}

func sameNameBinding(left, right storage.NameObservation) bool {
	return left.NodeID == right.NodeID && left.State == right.State && left.ParentID == right.ParentID && bytes.Equal(left.RawLeaf, right.RawLeaf)
}

func (c *connection) observeReferenceName(ctx context.Context, s *session, t *tree, h *fileHandle, guards *storage.NamespaceGuards, reserve func(int) error) (storage.NameObservation, error) {
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileObserveName, nil); err != nil {
		return storage.NameObservation{}, err
	}
	observer, ok := h.reference().(storage.ReferenceNameObserver)
	if !ok {
		return storage.NameObservation{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckReferenceNameObservation(); err != nil {
		return storage.NameObservation{}, err
	}
	call := storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		if scalar.NodeID != h.nodeID {
			return 0, syscall.EIO
		}
		charge, err := storage.NameObservationRetentionBytes(leafBytes)
		if err == nil && reserve != nil {
			err = reserve(int(charge))
		}
		return charge, err
	})
	observed, err := observer.ObserveName(call, guards)
	if err != nil {
		return storage.NameObservation{}, namespaceFailure(err)
	}
	if observed.Check() != nil || observed.NodeID != h.nodeID {
		return storage.NameObservation{}, syscall.EIO
	}
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return storage.NameObservation{}, err
	}
	return observed, nil
}

func (c *connection) captureReferenceName(ctx context.Context, s *session, t *tree, h *fileHandle) (selection *referenceNameSelection, err error) {
	n := &referenceNameSelection{export: t.export}
	defer func() {
		if err != nil {
			n.release()
		}
	}()
	n.binding, err = c.observeReferenceName(ctx, s, t, h, nil, n.reserve)
	if err != nil {
		return nil, err
	}
	root := h.identity.Backend.RootNodeID
	if n.binding.State == storage.NameDetached {
		return nil, syscall.EINVAL
	}
	if n.binding.State == storage.NameRoot {
		if n.binding.NodeID != root || h.nodeKind != storage.NodeDirectory {
			return nil, syscall.EIO
		}
		n.guards.RootID, n.path = root, `\`
	} else {
		current := n.binding
		seen := make(map[uint64]bool)
		parts := make([]string, 0)
		for current.State == storage.NameLinked {
			if len(parts) >= storage.MaxNamespaceGuards {
				return nil, syscall.EFBIG
			}
			if seen[current.NodeID] {
				return nil, syscall.EIO
			}
			seen[current.NodeID] = true
			parent, edge, attr, captureErr := c.captureNameParent(ctx, s, t, h, current, n)
			if captureErr != nil {
				return nil, captureErr
			}
			if len(parts) == 0 {
				n.attr = attr
			}
			parts = append(parts, string(edge.RawLeaf))
			current = parent
		}
		if current.State != storage.NameRoot || current.NodeID != root {
			return nil, syscall.EIO
		}
		n.guards.RootID = root
		if err = n.guards.Check(); err != nil {
			return nil, err
		}
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
		units := len(parts)
		for _, part := range parts {
			units += utf16Length(part)
		}
		if units > maxPathUnits || 2*units > c.server.config.Limits.MaxFrameBytes {
			return nil, syscall.EFBIG
		}
		if err = n.reserve(8 * units); err != nil {
			return nil, err
		}
		n.path = `\` + strings.Join(parts, `\`)
	}
	// Ancestor captures may span several transactions. Revalidating the original
	// reference with every captured edge prevents returning a path that never
	// existed as a whole.
	if err = c.validateReferenceName(ctx, s, t, h, n, &n.guards); err != nil {
		return nil, err
	}
	return n, nil
}

func (c *connection) validateReferenceName(ctx context.Context, s *session, t *tree, h *fileHandle, n *referenceNameSelection, guards *storage.NamespaceGuards) error {
	charged := 0
	defer func() { releaseDirectoryBytes(t.export, charged) }()
	observed, err := c.observeReferenceName(ctx, s, t, h, guards, func(amount int) error {
		if err := reserveDirectoryBytes(t.export, amount); err != nil {
			return err
		}
		charged += amount
		return nil
	})
	if err != nil {
		return err
	}
	if !sameNameBinding(n.binding, observed) {
		return storage.ErrConditionConflict
	}
	return nil
}

func (c *connection) captureNameParent(ctx context.Context, s *session, t *tree, h *fileHandle, current storage.NameObservation, n *referenceNameSelection) (storage.NameObservation, storage.ObservedEdge, storage.Attr, error) {
	observer, ok := t.authority.raw.(storage.DirectoryMetadataObserver)
	if !ok {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileObserveDirectoryMetadata, nil); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	limit, staging, err := reserveDirectoryCapture(t.export)
	if err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	charged := staging
	defer func() { releaseDirectoryBytes(t.export, charged) }()
	reserve := func(amount int64) error {
		if err := reserveDirectoryBytes(t.export, int(amount)); err != nil {
			return err
		}
		charged += int(amount)
		return nil
	}
	result, err := nameObservationResult(limit, current.ParentID, n.guards, false, reserve)
	if err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	call := storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		if scalar.NodeID != current.ParentID {
			return 0, syscall.EIO
		}
		charge, err := storage.NameObservationRetentionBytes(leafBytes)
		if err == nil {
			err = reserve(charge)
		}
		return charge, err
	})
	target := storage.DirectoryTarget{NodeID: current.ParentID}
	options := storage.DirectoryMetadataOptions{Guards: &n.guards, IncludeName: true}
	observed, err := observer.ObserveDirectoryMetadata(call, target, options, result)
	if err != nil {
		result.Fail(err)
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, namespaceFailure(err)
	}
	if err = observed.Check(target, options); err != nil {
		result.Fail(err)
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	entries, err := result.Entries()
	if err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, namespaceFailure(err)
	}
	compare := c.server.nameComparer
	if compare == nil {
		compare, err = platformNameComparer()
		if err != nil {
			return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
		}
	}
	if _, err = projectDirectory(entries, compare); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	index := -1
	for i, entry := range entries {
		if entry.Attr.ID == current.NodeID {
			if entry.Name != string(current.RawLeaf) {
				return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, storage.ErrConditionConflict
			}
			index = i
		}
	}
	if index < 0 {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, storage.ErrConditionConflict
	}
	entry := entries[index]
	if entry.Attr.Kind == storage.NodeSymlink {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, syscall.EOPNOTSUPP
	}
	if current.NodeID == h.nodeID && entry.Attr.Kind != h.nodeKind {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, syscall.EIO
	}
	if current.NodeID != h.nodeID && entry.Attr.Kind != storage.NodeDirectory {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, syscall.EIO
	}
	metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
	if err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	charge := 1024 + 8*len(observed.Observation.Revision) + 8*len(current.RawLeaf) + 4*len(observed.Name.RawLeaf)
	if current.NodeID == h.nodeID {
		metadataCharge, err := storage.MetadataRetentionBytes(int64(metadataBytes))
		if err != nil {
			return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
		}
		charge += int(metadataCharge)
	}
	if err = n.reserve(charge); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	edge := storage.ObservedEdge{ParentID: current.ParentID, ChildID: current.NodeID, RawLeaf: bytes.Clone(current.RawLeaf)}
	n.guards.Edges = append(n.guards.Edges, edge)
	n.guards.Directories = append(n.guards.Directories, storage.DirectoryObservation{ParentID: current.ParentID, Revision: bytes.Clone(observed.Observation.Revision)})
	if err = n.guards.Check(); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	attr := storage.Attr{}
	if current.NodeID == h.nodeID {
		attr = entry.Attr.Clone()
		if _, err = projectWindowsAttributes(attr); err != nil {
			return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
		}
	}
	if err = checkFileIOIdentity(ctx, s, t, h); err != nil {
		return storage.NameObservation{}, storage.ObservedEdge{}, storage.Attr{}, err
	}
	return observed.Name.Clone(), edge, attr, nil
}

func unionNamespaceGuards(left, right *storage.NamespaceGuards) (*storage.NamespaceGuards, error) {
	if left == nil || right == nil || left.RootID != right.RootID {
		return nil, syscall.EIO
	}
	combined := left.Clone()
	for _, item := range right.Directories {
		found := false
		for _, existing := range combined.Directories {
			if existing.ParentID == item.ParentID {
				if !bytes.Equal(existing.Revision, item.Revision) {
					return nil, storage.ErrConditionConflict
				}
				found = true
			}
		}
		if !found {
			combined.Directories = append(combined.Directories, item.Clone())
		}
	}
	for _, item := range right.Edges {
		found := false
		for _, existing := range combined.Edges {
			if existing.ChildID == item.ChildID || existing.ParentID == item.ParentID && bytes.Equal(existing.RawLeaf, item.RawLeaf) {
				if existing.ParentID != item.ParentID || existing.ChildID != item.ChildID || !bytes.Equal(existing.RawLeaf, item.RawLeaf) {
					return nil, storage.ErrConditionConflict
				}
				found = true
			}
		}
		if !found {
			combined.Edges = append(combined.Edges, storage.ObservedEdge{ParentID: item.ParentID, ChildID: item.ChildID, RawLeaf: bytes.Clone(item.RawLeaf)})
		}
	}
	return combined, combined.Check()
}
