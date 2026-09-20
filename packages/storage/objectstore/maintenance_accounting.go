package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Storage) CheckMaintenanceAccounting() error {
	native, ok := s.meta.(storage.MaintenanceAccounting)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckMaintenanceAccounting()
}

func (s *Storage) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	if err := s.CheckMaintenanceAccounting(); err != nil {
		return err
	}
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	return s.meta.(storage.MaintenanceAccounting).BindMaintenanceAccounting(ctx, chain, initialize)
}

var _ storage.MaintenanceAccounting = (*Storage)(nil)
