package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) CheckNamespaceAccess() error { return s.CheckFileStore() }

func checkChildCondition(condition storage.ChildCondition, node metastore.Node, found bool) error {
	if err := condition.Check(); err != nil {
		return err
	}
	switch condition.State {
	case storage.Absent:
		if found {
			return storage.ErrConditionConflict
		}
	case storage.SameNode:
		if !found || uint64(node.ID) != condition.NodeID {
			return storage.ErrConditionConflict
		}
	}
	if found {
		return checkExpectedMetadata(node.Metadata, condition.ExpectedMetadata)
	}
	return nil
}

func (s *Store) lookupNodeID(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (int64, bool, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT node FROM entries WHERE volume=? AND parent=? AND name=?`, s.volume, parent, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if id < 1 {
		return 0, false, syscall.EIO
	}
	return id, true, nil
}

func (s *Store) directoryTarget(ctx context.Context, tx *sql.Tx, target storage.DirectoryTarget, uses storage.Uses) (metastore.FileState, storage.UseScope, error) {
	if target.NodeID > math.MaxInt64 {
		return metastore.FileState{}, storage.UseScope{}, syscall.ESTALE
	}
	var scope storage.UseScope
	if target.Scope != nil {
		file, err := s.resolveUseScope(ctx, *target.Scope, target.NodeID, uses)
		if err != nil {
			return metastore.FileState{}, scope, err
		}
		scope = file.scope
	}
	node, err := s.fileState(ctx, tx, int64(target.NodeID))
	if err != nil {
		return metastore.FileState{}, scope, err
	}
	if !node.IsDir() {
		return metastore.FileState{}, scope, syscall.ENOTDIR
	}
	if node.Detached {
		return metastore.FileState{}, scope, syscall.ESTALE
	}
	if err := s.fileDomain.coordinator.CheckUse(ctx, target.NodeID, scope, uses); err != nil {
		return metastore.FileState{}, scope, err
	}
	return node, scope, nil
}

func (s *Store) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := name.Check(); err != nil {
		return storage.Attr{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.Attr{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.Attr{}, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return storage.Attr{}, err
	}
	var node metastore.Node
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if _, _, err := s.directoryTarget(ctx, tx, name.Parent, 0); err != nil {
			return err
		}
		id, found, err := s.lookupNodeID(ctx, tx, int64(name.Parent.NodeID), name.RawLeaf)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ENOENT
		}
		node, err = s.returnedNode(ctx, tx, id)
		return err
	})
	if err != nil {
		return storage.Attr{}, sqlerr.Failure(err)
	}
	return node.Attr(), nil
}
