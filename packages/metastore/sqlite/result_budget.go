package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) checkNodeResultBudget(ctx context.Context, tx *sql.Tx, id int64) error {
	if !storage.HasAttrResultBudget(ctx) {
		return nil
	}
	var header nodeHeader
	if err := tx.QueryRowContext(ctx, `SELECT `+nodeHeaderColumns+` FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, id).Scan(header.fields()...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ESTALE
		}
		return err
	}
	attr, err := header.attr()
	if err != nil {
		return err
	}
	return storage.CheckAttrResultBudget(ctx, attr, header.metadataBytes)
}

func (s *Store) returnedFileState(ctx context.Context, tx *sql.Tx, id int64) (metastore.FileState, error) {
	if err := s.checkNodeResultBudget(ctx, tx, id); err != nil {
		return metastore.FileState{}, err
	}
	return s.fileState(ctx, tx, id)
}

func (s *Store) returnedNode(ctx context.Context, tx *sql.Tx, id int64) (metastore.Node, error) {
	if err := s.checkNodeResultBudget(ctx, tx, id); err != nil {
		return metastore.Node{}, err
	}
	return s.nodeByID(ctx, tx, id)
}

func (s *Store) returnedReferenceState(ctx context.Context, tx *sql.Tx, id int64) (metastore.ReferenceState, error) {
	if err := s.checkNodeResultBudget(ctx, tx, id); err != nil {
		return metastore.ReferenceState{}, err
	}
	return s.referenceState(ctx, tx, id)
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
	if id <= 0 {
		return 0, false, syscall.EIO
	}
	return id, true, nil
}

func (s *Store) resolveReturnedNode(ctx context.Context, tx *sql.Tx, path string, expected uint64) (metastore.Node, error) {
	if !storage.HasAttrResultBudget(ctx) {
		node, err := s.resolve(ctx, tx, path)
		if err == nil && expected != 0 && expected != uint64(node.ID) {
			return metastore.Node{}, syscall.ESTALE
		}
		return node, err
	}
	id, found, err := s.resolveNodeID(ctx, tx, path)
	if err != nil {
		return metastore.Node{}, err
	}
	if !found {
		return metastore.Node{}, syscall.ENOENT
	}
	if expected != 0 && expected != uint64(id) {
		return metastore.Node{}, syscall.ESTALE
	}
	return s.returnedNode(ctx, tx, id)
}

func (s *Store) resolveNodeID(ctx context.Context, tx *sql.Tx, path string) (int64, bool, error) {
	if path == "" {
		return s.root, true, nil
	}
	parent, name, err := s.resolveParent(ctx, tx, path)
	if err != nil {
		return 0, false, err
	}
	return s.lookupNodeID(ctx, tx, parent.ID, name)
}
