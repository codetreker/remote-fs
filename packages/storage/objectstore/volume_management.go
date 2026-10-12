package objectstore

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.VolumeSummary    = (*Storage)(nil)
	_ storage.QuotaControl     = (*Storage)(nil)
	_ storage.ActionNamespaces = (*Storage)(nil)
)

func (s *Storage) CheckVolumeSummary() error {
	native, ok := s.meta.(storage.VolumeSummary)
	if !ok {
		return fmt.Errorf("metastore cannot summarize a volume: %w", syscall.EOPNOTSUPP)
	}
	return native.CheckVolumeSummary()
}

func (s *Storage) Summary(ctx context.Context) (storage.VolumeSummaryResult, error) {
	if err := s.CheckVolumeSummary(); err != nil {
		return storage.VolumeSummaryResult{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.VolumeSummaryResult{}, err
	}
	defer s.endOperation()
	result, err := s.meta.(storage.VolumeSummary).Summary(ctx)
	if err != nil {
		return storage.VolumeSummaryResult{}, err
	}
	if err := result.Check(); err != nil {
		return storage.VolumeSummaryResult{}, fmt.Errorf("invalid native volume summary: %w", err)
	}
	return result, nil
}

func (s *Storage) CheckQuotaControl() error {
	native, ok := s.meta.(storage.QuotaControl)
	if !ok {
		return fmt.Errorf("metastore cannot change durable quota: %w", syscall.EOPNOTSUPP)
	}
	if err := s.CheckPublicationAccounting(); err != nil {
		return err
	}
	return native.CheckQuotaControl()
}

func (s *Storage) SetQuota(ctx context.Context, command storage.QuotaCommand) (storage.QuotaReceipt, error) {
	if err := s.CheckQuotaControl(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := command.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	defer s.endOperation()
	return s.meta.(storage.QuotaControl).SetQuota(ctx, command)
}

func (s *Storage) QueryQuotaAction(ctx context.Context, key storage.ActionKey) (storage.QuotaReceipt, error) {
	if err := s.CheckQuotaControl(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := key.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	defer s.endOperation()
	return s.meta.(storage.QuotaControl).QueryQuotaAction(ctx, key)
}

func (s *Storage) CheckActionNamespaces() error {
	native, ok := s.meta.(storage.ActionNamespaces)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckActionNamespaces()
}

func (s *Storage) RegisterActionNamespace(ctx context.Context, namespace storage.ActionNamespace) error {
	if err := s.CheckActionNamespaces(); err != nil {
		return err
	}
	if err := namespace.Check(); err != nil {
		return err
	}
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	return s.meta.(storage.ActionNamespaces).RegisterActionNamespace(ctx, namespace)
}

func (s *Storage) RetireActionNamespaces(ctx context.Context, retirement storage.ActionNamespaceRetirement) error {
	if err := s.CheckActionNamespaces(); err != nil {
		return err
	}
	if err := retirement.Check(); err != nil {
		return err
	}
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	gate, err := s.retainActionRetirement(retirement)
	if err != nil {
		return err
	}
	defer s.releaseTransfer(gate)
	return s.meta.(storage.ActionNamespaces).RetireActionNamespaces(ctx, retirement)
}

type actionRetirementGate struct {
	retirement storage.ActionNamespaceRetirement
}

func (g *actionRetirementGate) includes(key storage.ActionKey) bool {
	return key.SubjectID == g.retirement.SubjectID && key.Namespace < g.retirement.Before
}

func (s *Storage) importActionRetiringLocked(key storage.ActionKey) bool {
	for transfer := range s.transfers {
		if gate, ok := transfer.(*actionRetirementGate); ok && gate.includes(key) {
			return true
		}
	}
	return false
}

func (s *Storage) retainActionRetirement(retirement storage.ActionNamespaceRetirement) (*actionRetirementGate, error) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closeDone != nil || s.transfersClosing {
		return nil, syscall.EIO
	}
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	gate := &actionRetirementGate{retirement: retirement}
	for transfer := range s.transfers {
		switch owner := transfer.(type) {
		case *fileImport:
			if gate.includes(owner.action) {
				return nil, syscall.EBUSY
			}
		case *actionRetirementGate:
			if owner.retirement.SubjectID == retirement.SubjectID {
				return nil, syscall.EBUSY
			}
		}
	}
	for key := range s.importCancels {
		if gate.includes(key) {
			return nil, syscall.EBUSY
		}
	}
	if len(s.transfers)+len(s.importCancels) >= maxRetainedTransfers {
		return nil, syscall.EAGAIN
	}
	if s.transfers == nil {
		s.transfers = make(map[any]struct{})
	}
	s.transfers[gate] = struct{}{}
	return gate, nil
}

var _ storage.QuotaAcceptance = (*Storage)(nil)

func (s *Storage) CheckQuotaAcceptance() error {
	if err := s.CheckQuotaControl(); err != nil {
		return err
	}
	native, ok := s.meta.(storage.QuotaAcceptance)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckQuotaAcceptance()
}

func (s *Storage) AcceptQuotaAction(ctx context.Context, accepted storage.AcceptedQuotaAction) (storage.QuotaReceipt, error) {
	if err := s.CheckQuotaAcceptance(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := accepted.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	defer s.endOperation()
	return s.meta.(storage.QuotaAcceptance).AcceptQuotaAction(ctx, accepted)
}

var _ storage.VolumeRetirementCheck = (*Storage)(nil)

func (s *Storage) CheckVolumeRetirement() error {
	if err := s.CheckFileStorage(); err != nil {
		return err
	}
	native, ok := s.meta.(storage.VolumeRetirementCheck)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckVolumeRetirement()
}

func (s *Storage) CheckDeletionReady(ctx context.Context) error {
	if err := s.CheckVolumeRetirement(); err != nil {
		return err
	}
	if err := s.beginOperation(); err != nil {
		return err
	}
	defer s.endOperation()
	return s.meta.(storage.VolumeRetirementCheck).CheckDeletionReady(ctx)
}
