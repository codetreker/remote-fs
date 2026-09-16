package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) checkEntryCondition(ctx context.Context, tx *sql.Tx, condition storage.EntryCondition) error {
	if err := condition.Check(); err != nil {
		return err
	}
	if condition.ParentID > math.MaxInt64 || condition.NodeID > math.MaxInt64 || uint64(condition.EntryID) > math.MaxInt64 || uint64(condition.DirectoryRevision) > math.MaxInt64 {
		return syscall.EINVAL
	}
	revision, err := s.directoryRevision(ctx, tx, int64(condition.ParentID))
	if err != nil {
		return err
	}
	if revision != condition.DirectoryRevision {
		return syscall.EAGAIN
	}
	var entry, node int64
	err = tx.QueryRowContext(ctx, `SELECT id,node FROM entries WHERE volume=? AND parent=? AND name=?`, s.volume, condition.ParentID, condition.Name).Scan(&entry, &node)
	if errors.Is(err, sql.ErrNoRows) {
		return syscall.ESTALE
	}
	if err != nil {
		return err
	}
	if entry != int64(condition.EntryID) || node != int64(condition.NodeID) {
		return syscall.ESTALE
	}
	return nil
}

// validateLocation checks every observed parent revision, including directories
// above the final parent. The caller holds the commit gate through the effect.
func (s *Store) validateLocation(ctx context.Context, tx *sql.Tx, witness storage.EntryLocation) error {
	if err := witness.Check(); err != nil {
		return err
	}
	if witness.RootNodeID > math.MaxInt64 || witness.NodeID > math.MaxInt64 {
		return syscall.EINVAL
	}
	if _, err := s.directoryRevision(ctx, tx, int64(witness.RootNodeID)); err != nil {
		return err
	}
	switch witness.State {
	case storage.LocationRoot:
		return nil
	case storage.LocationDetached:
		var detached bool
		err := tx.QueryRowContext(ctx, `SELECT detached FROM nodes WHERE volume=? AND id=?`, s.volume, witness.NodeID).Scan(&detached)
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ESTALE
		}
		if err != nil {
			return err
		}
		if !detached {
			return syscall.ESTALE
		}
		return nil
	}
	for _, condition := range witness.Ancestors {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.checkEntryCondition(ctx, tx, condition); err != nil {
			return err
		}
	}
	return nil
}
