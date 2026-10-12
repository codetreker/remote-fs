package localstore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.VolumeSummary    = (*Store)(nil)
	_ storage.QuotaControl     = (*Store)(nil)
	_ storage.ActionNamespaces = (*Store)(nil)
	_ storage.FileTransfer     = (*Store)(nil)
)

func (s *Store) CheckVolumeSummary() error { return s.volume.CheckVolumeSummary() }
func (s *Store) Summary(ctx context.Context) (storage.VolumeSummaryResult, error) {
	return s.volume.Summary(ctx)
}
func (s *Store) CheckQuotaControl() error { return s.volume.CheckQuotaControl() }
func (s *Store) SetQuota(ctx context.Context, c storage.QuotaCommand) (storage.QuotaReceipt, error) {
	return s.volume.SetQuota(ctx, c)
}
func (s *Store) QueryQuotaAction(ctx context.Context, k storage.ActionKey) (storage.QuotaReceipt, error) {
	return s.volume.QueryQuotaAction(ctx, k)
}
func (s *Store) CheckActionNamespaces() error { return s.volume.CheckActionNamespaces() }
func (s *Store) RegisterActionNamespace(ctx context.Context, n storage.ActionNamespace) error {
	return s.volume.RegisterActionNamespace(ctx, n)
}
func (s *Store) RetireActionNamespaces(ctx context.Context, r storage.ActionNamespaceRetirement) error {
	return s.volume.RetireActionNamespaces(ctx, r)
}
func (s *Store) CheckFileTransfer() error { return s.volume.CheckFileTransfer() }
func (s *Store) BeginImport(ctx context.Context, c storage.ImportCommand) (storage.FileImport, error) {
	return s.volume.BeginImport(ctx, c)
}
func (s *Store) ResumeImport(ctx context.Context, k storage.ActionKey) (storage.FileImport, error) {
	return s.volume.ResumeImport(ctx, k)
}
func (s *Store) QueryImport(ctx context.Context, k storage.ActionKey) (storage.ImportReceipt, error) {
	return s.volume.QueryImport(ctx, k)
}
func (s *Store) ExportFile(ctx context.Context, c storage.ExportCommand) (storage.FileExport, error) {
	return s.volume.ExportFile(ctx, c)
}

func (s *Store) LiveTransferCount() int { return s.volume.LiveTransferCount() }

var _ storage.QuotaAcceptance = (*Store)(nil)

func (s *Store) CheckQuotaAcceptance() error { return s.volume.CheckQuotaAcceptance() }
func (s *Store) AcceptQuotaAction(ctx context.Context, a storage.AcceptedQuotaAction) (storage.QuotaReceipt, error) {
	return s.volume.AcceptQuotaAction(ctx, a)
}

var _ storage.VolumeRetirementCheck = (*Store)(nil)

func (s *Store) CheckVolumeRetirement() error { return s.volume.CheckVolumeRetirement() }
func (s *Store) CheckDeletionReady(ctx context.Context) error {
	return s.volume.CheckDeletionReady(ctx)
}

func (s *Store) CancelImport(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	return s.volume.CancelImport(ctx, key)
}
