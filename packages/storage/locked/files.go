package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileStorage = (*Storage)(nil)

func (s *Storage) CheckFileStorage() error {
	backend, ok := s.backend.(storage.FileStorage)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return backend.CheckFileStorage()
}

func (s *Storage) CheckPublicationAccounting() error {
	backend, ok := s.backend.(interface{ CheckPublicationAccounting() error })
	if !ok {
		return syscall.ENOSYS
	}
	return backend.CheckPublicationAccounting()
}

// Usage preserves authoritative accounting for retained, unnamed objects.
func (s *Storage) Usage(ctx context.Context) (int64, error) {
	backend, ok := s.backend.(interface {
		Usage(context.Context) (int64, error)
	})
	if !ok {
		return 0, syscall.ENOSYS
	}
	return backend.Usage(readContext(ctx))
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	if err := s.CheckFileStorage(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	inner, status, err := s.backend.(storage.FileStorage).NewFileSession(readContext(ctx), options)
	if inner == nil {
		return nil, status, err
	}
	return &fileSession{FileSession: inner, storage: s}, status, err
}

type fileSession struct {
	storage.FileSession
	storage *Storage
}

func (s *Storage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	if err := s.CheckFileStorage(); err != nil {
		return storage.FileVolumeState{}, err
	}
	return s.backend.(storage.FileStorage).FileState(readContext(ctx))
}

func (s *fileSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	inner, err := s.FileSession.Reference(readContext(ctx), id)
	if inner == nil {
		return nil, err
	}
	return &file{File: inner, storage: s.storage}, err
}

func (s *fileSession) Retain(ctx context.Context, request storage.RetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	if request.Prepared != nil {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	return s.FileSession.Retain(ctx, request, action)
}

func (s *fileSession) RetainAt(ctx context.Context, request storage.RetainAtRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	if request.Prepared != nil {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	return s.FileSession.RetainAt(ctx, request, action)
}

func (s *fileSession) CreateAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.CreateAndRetainAt(s.storage.mutationContext(ctx), request, action)
}

func (s *fileSession) ResetAndRetainAt(ctx context.Context, request storage.ResetAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.ResetAndRetainAt(s.storage.mutationContext(ctx), request, action)
}

func (s *fileSession) ReplaceAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.ReplaceAndRetainAt(s.storage.mutationContext(ctx), request, action)
}

func (s *fileSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	return s.FileSession.StatNode(readContext(ctx), id, options)
}
func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.SetNodeAttr(s.storage.mutationContext(ctx), id, change, action)
}
func (s *fileSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.RetireRangeOwner(readContext(ctx), owner, action)
}

func (s *fileSession) QueryAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.QueryAction(readContext(ctx), action)
}

func (s *fileSession) CancelAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.CancelAction(readContext(ctx), action)
}

func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.FileSession.Renew(readContext(ctx))
}

func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.FileSession.Status(readContext(ctx))
}

func (s *fileSession) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.FileSession.Close(s.storage.mutationContext(ctx), action)
}

type file struct {
	storage.File
	storage *Storage
}

func (f *file) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	return f.File.Stat(readContext(ctx), options)
}
func (f *file) ReadAt(ctx context.Context, request storage.FileReadRequest) (storage.FileRead, error) {
	return f.File.ReadAt(readContext(ctx), request)
}
func (f *file) ListAt(ctx context.Context, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	return f.File.ListAt(readContext(ctx), request)
}
func (f *file) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	return f.File.RangeSnapshot(readContext(ctx), owner, scope)
}

func (f *file) WriteAt(ctx context.Context, request storage.FileWriteRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.WriteAt(f.storage.mutationContext(ctx), request, action)
}

func (f *file) Truncate(ctx context.Context, request storage.FileTruncateRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.Truncate(f.storage.mutationContext(ctx), request, action)
}

func (f *file) SetAttr(ctx context.Context, request storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.SetAttr(f.storage.mutationContext(ctx), request, action)
}

func (f *file) SetKind(ctx context.Context, request storage.SetKindRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.SetKind(f.storage.mutationContext(ctx), request, action)
}

func (f *file) Rename(ctx context.Context, request storage.RenameRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.Rename(f.storage.mutationContext(ctx), request, action)
}

func (f *file) ReplaceClaim(ctx context.Context, request storage.AccessClaim, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.ReplaceClaim(readContext(ctx), request, action)
}

func (f *file) PrepareRemoval(ctx context.Context, request storage.PrepareRemovalRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.PrepareRemoval(f.storage.mutationContext(ctx), request, action)
}

func (f *file) DrainEntry(ctx context.Context, request storage.DrainEntryRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.DrainEntry(f.storage.mutationContext(ctx), request, action)
}

func (f *file) CancelDrain(ctx context.Context, request storage.CancelDrainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.CancelDrain(f.storage.mutationContext(ctx), request, action)
}

func (f *file) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.ReplaceRanges(readContext(ctx), request, action)
}

func (f *file) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.WaitRanges(readContext(ctx), request, action)
}

func (f *file) Sync(ctx context.Context) error { return f.File.Sync(readContext(ctx)) }
func (f *file) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.Close(f.storage.mutationContext(ctx), action)
}

func (f *file) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.CancelPrepared(f.storage.mutationContext(ctx), intent, action)
}

func (f *file) CheckObservation(ctx context.Context, condition storage.ObservationCondition) (storage.FileObservation, error) {
	return f.File.CheckObservation(readContext(ctx), condition)
}
func (f *file) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.File.RetireRangeOwner(readContext(ctx), owner, scope, action)
}

func (f *file) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	return f.File.LookupAt(readContext(ctx), name)
}
