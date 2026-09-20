package limited

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Storage) CheckMaintenanceAccounting() error {
	if err := s.healthy(); err != nil {
		return err
	}
	_, err := capability(s.backing, storage.MaintenanceAccounting.CheckMaintenanceAccounting)
	return err
}

func (s *Storage) maintenanceAccounting(previous, next int64) (storage.PublicationSettlement, error) {
	return s.preparePublication("pending cleanup", previous, next)
}

func (s *Storage) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		return err
	}
	backing, err := capability(s.backing, storage.MaintenanceAccounting.CheckMaintenanceAccounting)
	if err != nil {
		return err
	}
	return backing.BindMaintenanceAccounting(ctx, chain.With(s.maintenanceAccounting), initialize)
}

var _ storage.MaintenanceAccounting = (*Storage)(nil)
