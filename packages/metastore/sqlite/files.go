package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
	"math"
	"syscall"
)

type retainedNode struct{ volume, id int64 }

var _ metastore.FileStore = (*Store)(nil)

func (s *Store) nodeByID(ctx context.Context, tx *sql.Tx, id int64) (metastore.Node, error) {
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, id))
	if errors.Is(err, sql.ErrNoRows) {
		return metastore.Node{}, syscall.ESTALE
	}
	return node, err
}

func (s *Store) fileState(ctx context.Context, tx *sql.Tx, id int64) (metastore.FileState, error) {
	var scan nodeScan
	var revision int64
	var detached bool
	err := tx.QueryRowContext(ctx, `SELECT `+nodeColumns+`,n.content_revision,n.detached FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, id).Scan(append(scan.fields(), &revision, &detached)...)
	if errors.Is(err, sql.ErrNoRows) {
		return metastore.FileState{}, syscall.ESTALE
	}
	if err != nil {
		return metastore.FileState{}, err
	}
	if revision < 1 {
		return metastore.FileState{}, syscall.EIO
	}
	node, err := scan.node()
	if err != nil {
		return metastore.FileState{}, err
	}
	return metastore.FileState{Node: node, Revision: uint64(revision), Detached: detached}, nil
}

func (s *Store) advanceContentRevision(ctx context.Context, tx *sql.Tx, id int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET content_revision=content_revision+1 WHERE id=? AND volume=? AND content_revision < ?`, id, s.volume, int64(math.MaxInt64))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("file content revision cannot advance: %w", syscall.EOVERFLOW)
	}
	return nil
}

func (s *Store) replaceNodeContent(ctx context.Context, tx *sql.Tx, node metastore.Node, object metastore.Object, change *storage.AttrChange) error {
	if object.Key == "" {
		if object.Size != 0 {
			return syscall.EINVAL
		}
	} else {
		var state int
		if err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key=? AND volume=?`, string(object.Key), s.volume).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return syscall.EINVAL
			}
			return err
		}
		if state != stateReserved {
			return syscall.EINVAL
		}
	}
	if err := s.advanceContentRevision(ctx, tx, node.ID); err != nil {
		return err
	}
	if err := s.account(ctx, tx, object.Size-node.Size); err != nil {
		return err
	}
	sec, nsec := sqlvalue.StoredTime(object.ModTime)
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET size=?,mtime_sec=?,mtime_nsec=?,content=? WHERE volume=? AND id=?`, object.Size, sec, nsec, sqlvalue.StoredKey(object.Key), s.volume, node.ID); err != nil {
		return err
	}
	if object.Key != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=?,size=?,digest=? WHERE key=? AND volume=?`, stateReferenced, object.Size, object.Digest, string(object.Key), s.volume); err != nil {
			return err
		}
	}
	if node.Content != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE key=? AND volume=?`, stateGarbage, string(node.Content), s.volume); err != nil {
			return err
		}
	}
	if change != nil && !change.Empty() {
		if err := applyChange(ctx, tx, node, *change); err != nil {
			return err
		}
	}
	return s.recordNamedChanged(ctx, tx, node)
}

func (s *Store) recordNamedChanged(ctx context.Context, tx *sql.Tx, before metastore.Node) error {
	return s.recordNamedChangedMask(ctx, tx, before, 0)
}

func (s *Store) recordNamedChangedMask(ctx context.Context, tx *sql.Tx, before metastore.Node, extra metastore.ChangeMask) error {
	var detached bool
	if err := tx.QueryRowContext(ctx, `SELECT detached FROM nodes WHERE volume=? AND id=?`, s.volume, before.ID).Scan(&detached); err != nil {
		return err
	}
	if detached {
		current, err := s.nodeByID(ctx, tx, before.ID)
		if err != nil {
			return err
		}
		if current.MetadataRevision == before.MetadataRevision {
			_, err = s.updateChangeTime(ctx, tx, before.ID)
		}
		return err
	}
	if extra != 0 {
		return s.recordChangedMask(ctx, tx, before, extra)
	}
	return s.recordChanged(ctx, tx, before)
}

func (s *Store) setNodeAttr(ctx context.Context, tx *sql.Tx, id int64, change storage.AttrChange) error {
	node, err := s.nodeByID(ctx, tx, id)
	if err != nil {
		return err
	}
	if change.ExpectedRevision != 0 && change.ExpectedRevision != node.MetadataRevision {
		return &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: uint64(id), Revision: uint64(node.MetadataRevision)}}
	}
	if change.Empty() {
		return nil
	}
	if err := applyChange(ctx, tx, node, change); err != nil {
		return err
	}
	return s.recordNamedChanged(ctx, tx, node)
}

func (s *Store) StatNode(ctx context.Context, id uint64) (metastore.Node, error) {
	if id == 0 || id > math.MaxInt64 {
		return metastore.Node{}, syscall.ESTALE
	}
	var node metastore.Node
	err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; node, err = s.nodeByID(ctx, tx, int64(id)); return err })
	return node, sqlerr.Failure(err)
}

func (s *Store) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (metastore.Node, error) {
	if id == 0 || id > math.MaxInt64 {
		return metastore.Node{}, syscall.ESTALE
	}
	if err := change.Check(); err != nil {
		return metastore.Node{}, err
	}
	var node metastore.Node
	err := s.mutatePublication(ctx, &volumeIntent{kind: locking.SetAttrMutation, node: int64(id)}, func(tx *sql.Tx) error {
		if err := s.setNodeAttr(ctx, tx, int64(id), change); err != nil {
			return err
		}
		var err error
		node, err = s.nodeByID(ctx, tx, int64(id))
		return err
	})
	return node, sqlerr.Failure(err)
}

func (s *Store) Usage(ctx context.Context) (int64, error) {
	var used int64
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT used FROM volumes WHERE id=?`, s.volume).Scan(&used)
	})
	if err == nil && used < 0 {
		err = syscall.EIO
	}
	return used, sqlerr.Failure(err)
}

func (s *Store) CheckFileStore() error {
	// The owner pointer and exclusive mode are immutable after construction.
	// Its live descriptor and volume state are checked under publication ordering.
	if s.leaseOwner == nil || !s.leaseOwner.Exclusive() {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func (s *Store) checkFileOwnership() error {
	if s.files == nil {
		return syscall.ESTALE
	}
	if s.leaseOwner == nil || !s.leaseOwner.Exclusive() {
		return fmt.Errorf("retained files require exclusive native database ownership: %w", syscall.EOPNOTSUPP)
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	return nativelease.VerifyExclusiveOwnership(s.leaseOwner)
}
