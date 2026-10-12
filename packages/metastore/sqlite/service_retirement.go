package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.VolumeRetirementCheck = (*Store)(nil)

func (s *Store) CheckVolumeRetirement() error { return s.CheckFileStore() }

func (s *Store) CheckDeletionReady(ctx context.Context) error {
	tx, err := s.beginReadSnapshotChecked(ctx, s.read, s.beginRetirementRead)
	if err != nil {
		return err
	}
	return finishReadTransaction(ctx, "retirement transaction", tx, s.deletionReady(ctx, tx.Tx))
}

func (s *Store) beginRetirementRead(ctx context.Context) error {
	if err := s.coordinator.beginHealthyRead(); err != nil {
		return err
	}
	if s.locks != nil {
		if err := s.locks.CheckRetirement(ctx); err != nil {
			s.coordinator.endHealthyRead()
			return err
		}
	}
	return nil
}

func (s *Store) deletionReady(ctx context.Context, tx *sql.Tx) error {
	var pending bool
	if err := tx.QueryRowContext(ctx, `SELECT
		 EXISTS(SELECT 1 FROM imports WHERE volume=?) OR
		 EXISTS(SELECT 1 FROM authority_actions WHERE volume=? AND outcome IN(1,4))`, s.volume, s.volume).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return syscall.EBUSY
	}
	return nil
}
