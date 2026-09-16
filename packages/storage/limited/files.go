package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileStorage = (*Storage)(nil)

func (s *Storage) CheckFileStorage() error {
	if err := s.CheckPublicationAccounting(); err != nil {
		return err
	}
	backend, ok := s.backing.(storage.FileStorage)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return backend.CheckFileStorage()
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.CheckFileStorage(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	// The backend retains this hook for final close and autonomous expiry. Ordinary
	// operations carry independent hooks, so cleanup never reuses a request's charge.
	inner, status, err := s.backing.(storage.FileStorage).NewFileSession(s.accountingContext(ctx, "retained file"), options)
	if inner == nil {
		return nil, status, s.publicationError(err)
	}
	return &fileSession{FileSession: inner, storage: s}, status, s.publicationError(err)
}

type fileSession struct {
	storage.FileSession
	storage *Storage
}

func (s *Storage) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	if err := s.CheckFileStorage(); err != nil {
		return storage.FileVolumeState{}, err
	}
	return s.backing.(storage.FileStorage).FileState(ctx)
}
func (s *fileSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	inner, err := s.FileSession.Reference(ctx, id)
	if inner == nil {
		return nil, s.storage.publicationError(err)
	}
	return &file{File: inner, storage: s.storage}, s.storage.publicationError(err)
}

func (s *fileSession) Retain(ctx context.Context, request storage.RetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.Retain(ctx, request, action)
	})
}

func (s *fileSession) RetainAt(ctx context.Context, request storage.RetainAtRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.RetainAt(ctx, request, action)
	})
}

func (s *fileSession) CreateAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.CreateAndRetainAt(ctx, request, action)
	})
}

func (s *fileSession) ResetAndRetainAt(ctx context.Context, request storage.ResetAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.ResetAndRetainAt(ctx, request, action)
	})
}

func (s *fileSession) ReplaceAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.ReplaceAndRetainAt(ctx, request, action)
	})
}

func (s *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(s.storage, ctx, "retained node", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return s.FileSession.SetNodeAttr(ctx, id, change, action)
	})
}

func (s *fileSession) QueryAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.storage.fileResult(s.FileSession.QueryAction(ctx, action))
}

func (s *fileSession) CancelAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.storage.fileResult(s.FileSession.CancelAction(ctx, action))
}

func (s *fileSession) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.storage.fileResult(s.FileSession.Close(ctx, action))
}

func (s *fileSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.storage.fileResult(s.FileSession.RetireRangeOwner(ctx, owner, action))
}
func (s *Storage) fileResult(result storage.FileActionReceipt, err error) (storage.FileActionReceipt, error) {
	return result, s.publicationError(err)
}

type file struct {
	storage.File
	storage *Storage
}

func (f *file) WriteAt(ctx context.Context, request storage.FileWriteRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.WriteAt(ctx, request, action)
	})
}

func (f *file) Truncate(ctx context.Context, request storage.FileTruncateRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.Truncate(ctx, request, action)
	})
}

func (f *file) SetAttr(ctx context.Context, request storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.SetAttr(ctx, request, action)
	})
}

func (f *file) SetKind(ctx context.Context, request storage.SetKindRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.SetKind(ctx, request, action)
	})
}

func (f *file) Rename(ctx context.Context, request storage.RenameRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.Rename(ctx, request, action)
	})
}

func (f *file) ReplaceClaim(ctx context.Context, request storage.AccessClaim, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.storage.fileResult(f.File.ReplaceClaim(ctx, request, action))
}

func (f *file) PrepareRemoval(ctx context.Context, request storage.PrepareRemovalRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.PrepareRemoval(ctx, request, action)
	})
}

func (f *file) DrainEntry(ctx context.Context, request storage.DrainEntryRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.DrainEntry(ctx, request, action)
	})
}

func (f *file) CancelDrain(ctx context.Context, request storage.CancelDrainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.CancelDrain(ctx, request, action)
	})
}

func (f *file) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.storage.fileResult(f.File.ReplaceRanges(ctx, request, action))
}

func (f *file) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.storage.fileResult(f.File.WaitRanges(ctx, request, action))
}

func (f *file) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.storage.fileResult(f.File.Close(ctx, action))
}

func fileMutation[T any](s *Storage, ctx context.Context, name string, operation func(context.Context) (T, error)) (T, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.healthy(); err != nil {
		var zero T
		return zero, err
	}
	result, err := operation(s.accountingContext(ctx, name))
	return result, s.publicationError(err)
}

func (f *file) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileMutation(f.storage, ctx, "retained object", func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.File.CancelPrepared(ctx, intent, action)
	})
}

func (f *file) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.storage.fileResult(f.File.RetireRangeOwner(ctx, owner, scope, action))
}
