package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) guardedDirectory(ctx context.Context, tx *sql.Tx, id uint64) (namespaceIdentity, error) {
	if id == 0 || id > math.MaxInt64 {
		return namespaceIdentity{}, storage.ErrConditionConflict
	}
	node, err := s.namespaceIdentity(ctx, tx, int64(id))
	if errors.Is(err, syscall.ESTALE) {
		return namespaceIdentity{}, storage.ErrConditionConflict
	}
	if err != nil {
		return namespaceIdentity{}, err
	}
	if node.Kind != storage.NodeDirectory || node.Detached {
		return namespaceIdentity{}, storage.ErrConditionConflict
	}
	return node, nil
}

func (s *Store) checkNamespaceGuards(ctx context.Context, tx *sql.Tx, guards *storage.NamespaceGuards) error {
	if err := guards.Check(); err != nil {
		return err
	}
	if guards == nil {
		return nil
	}
	if guards.RootID != 0 {
		if _, err := s.guardedDirectory(ctx, tx, guards.RootID); err != nil {
			return err
		}
	}
	for _, observed := range guards.Directories {
		node, err := s.guardedDirectory(ctx, tx, observed.ParentID)
		if err != nil {
			return err
		}
		if !bytes.Equal(node.DirectoryRevision, observed.Revision) {
			return storage.ErrConditionConflict
		}
	}
	for _, edge := range guards.Edges {
		if _, err := s.guardedDirectory(ctx, tx, edge.ParentID); err != nil {
			return err
		}
		node, found, err := s.lookupNamespaceIdentity(ctx, tx, int64(edge.ParentID), edge.RawLeaf)
		if err != nil {
			return err
		}
		if !found || uint64(node.ID) != edge.ChildID {
			return storage.ErrConditionConflict
		}
	}
	return nil
}

func (s *Store) checkNoOpenDescendants(ctx context.Context, tx *sql.Tx, source int64) error {
	pins := s.coordinator.pins[s.volume]
	if len(pins) > s.fileDomain.maxFiles {
		return syscall.EFBIG
	}
	remaining := s.maxIntegrityRecords
	charge := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if remaining == 0 {
			return syscall.EFBIG
		}
		remaining--
		return nil
	}
	for candidate, count := range pins {
		if count <= 0 || candidate == source {
			continue
		}
		if err := charge(); err != nil {
			return err
		}
		seen := make(map[int64]struct{})
		for id := candidate; ; {
			if err := charge(); err != nil {
				return err
			}
			if _, duplicate := seen[id]; duplicate {
				return syscall.EIO
			}
			seen[id] = struct{}{}
			observed, _, err := s.nameObservationHeader(ctx, tx, id)
			if err != nil {
				return err
			}
			if observed.State == storage.NameDetached {
				if id != candidate {
					return syscall.EIO
				}
				break
			}
			if observed.State == storage.NameRoot {
				break
			}
			if int64(observed.ParentID) == source {
				return syscall.EACCES
			}
			id = int64(observed.ParentID)
		}
	}
	return nil
}
