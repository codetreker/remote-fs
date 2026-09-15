package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.WindowsStorage = (*Storage)(nil)

func (s *Storage) CheckWindowsStorage() error {
	backend, ok := s.backend.(storage.WindowsStorage)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return backend.CheckWindowsStorage()
}

func (s *Storage) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsState{}, err
	}
	return s.backend.(storage.WindowsStorage).WindowsState(readContext(ctx))
}

func (s *Storage) EnableWindows(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsActivation{}, err
	}
	return s.backend.(storage.WindowsStorage).EnableWindows(s.mutationContext(ctx), action)
}

func (s *Storage) QueryWindowsActivation(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return storage.WindowsActivation{}, err
	}
	return s.backend.(storage.WindowsStorage).QueryWindowsActivation(readContext(ctx), action)
}

func (s *Storage) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	if err := s.CheckWindowsStorage(); err != nil {
		return nil, err
	}
	inner, err := s.backend.(storage.WindowsStorage).NewWindowsSession(readContext(ctx), options)
	if err != nil {
		return nil, err
	}
	return &windowsSession{WindowsSession: inner, storage: s}, nil
}

type windowsSession struct {
	storage.WindowsSession
	storage *Storage
}

func (s *windowsSession) Open(ctx context.Context, request storage.WindowsOpenRequest, action storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	if request.Disposition != storage.WindowsOpen || request.DeleteOnClose {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := s.WindowsSession.Open(ctx, request, action)
	result.File = s.storage.wrapWindowsFile(result.File)
	return result, err
}

func (s *windowsSession) QueryAction(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.storage.wrapWindowsResult(s.WindowsSession.QueryAction(readContext(ctx), action))
}

func (s *windowsSession) CancelAction(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.storage.wrapWindowsResult(s.WindowsSession.CancelAction(readContext(ctx), action))
}

func (s *windowsSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.WindowsSession.Renew(readContext(ctx))
}

func (s *windowsSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.WindowsSession.Status(readContext(ctx))
}

func (s *windowsSession) Close(ctx context.Context) error {
	return s.WindowsSession.Close(s.storage.mutationContext(ctx))
}

func (s *Storage) wrapWindowsFile(inner storage.WindowsFile) storage.WindowsFile {
	if inner == nil {
		return nil
	}
	return &windowsFile{WindowsFile: inner, storage: s}
}

func (s *Storage) wrapWindowsResult(result storage.WindowsActionResult, err error) (storage.WindowsActionResult, error) {
	result.File = s.wrapWindowsFile(result.File)
	return result, err
}

type windowsFile struct {
	storage.WindowsFile
	storage *Storage
}

func (f *windowsFile) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	return f.WindowsFile.Stat(readContext(ctx))
}

func (f *windowsFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	return f.WindowsFile.ReadAt(readContext(ctx), offset, length)
}

func (f *windowsFile) WriteAt(ctx context.Context, offset int64, data []byte, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.WriteAt(f.storage.mutationContext(ctx), offset, data, action))
}

func (f *windowsFile) Truncate(ctx context.Context, size int64, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.Truncate(f.storage.mutationContext(ctx), size, action))
}

func (f *windowsFile) SetAttr(ctx context.Context, change storage.WindowsAttrChange, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.SetAttr(f.storage.mutationContext(ctx), change, action))
}

func (f *windowsFile) ListBounded(ctx context.Context, result *storage.WindowsListResult) error {
	return f.WindowsFile.ListBounded(readContext(ctx), result)
}

func (f *windowsFile) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	return f.WindowsFile.ReadLink(readContext(ctx))
}

func (f *windowsFile) SetLink(ctx context.Context, target string, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.SetLink(f.storage.mutationContext(ctx), target, action))
}

func (f *windowsFile) Rename(ctx context.Context, request storage.WindowsRenameRequest, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.Rename(f.storage.mutationContext(ctx), request, action))
}

func (f *windowsFile) SetDeletePending(ctx context.Context, pending bool, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.SetDeletePending(f.storage.mutationContext(ctx), pending, action))
}

func (f *windowsFile) LockBatch(ctx context.Context, batch storage.WindowsLockBatch, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.LockBatch(readContext(ctx), batch, action))
}

func (f *windowsFile) Sync(ctx context.Context) error {
	return f.WindowsFile.Sync(readContext(ctx))
}

func (f *windowsFile) Close(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.wrapWindowsResult(f.WindowsFile.Close(f.storage.mutationContext(ctx), action))
}
