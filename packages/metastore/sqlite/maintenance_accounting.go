package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.MaintenanceAccounting = (*Store)(nil)

func (s *Store) CheckMaintenanceAccounting() error { return s.CheckFileStore() }

func (s *Store) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	if initialize == nil {
		return syscall.EINVAL
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	if s.fileDomain == nil {
		return syscall.ESTALE
	}
	var used int64
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT used FROM volumes WHERE id=?`, s.volume).Scan(&used)
	}); err != nil {
		return err
	}
	if used < 0 {
		return syscall.EIO
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	initialize(used)
	s.fileDomain.maintenanceAccounting = chain
	return nil
}
