package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.WindowsStorage = (*Storage)(nil)

func (s *Storage) windowsBackend() (storage.WindowsStorage, error) {
	backend, ok := s.backing.(storage.WindowsStorage)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := backend.CheckWindowsStorage(); err != nil {
		return nil, err
	}
	return backend, nil
}

func (s *Storage) CheckWindowsStorage() error {
	if err := s.CheckPublicationAccounting(); err != nil {
		return err
	}
	_, err := s.windowsBackend()
	return err
}

func (s *Storage) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	backend, err := s.windowsBackend()
	if err != nil {
		return storage.WindowsState{}, err
	}
	return backend.WindowsState(ctx)
}

func (s *Storage) EnableWindows(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	backend, err := s.windowsBackend()
	if err != nil {
		return storage.WindowsActivation{}, err
	}
	return fileMutation(s, ctx, "Windows naming policy", func(ctx context.Context) (storage.WindowsActivation, error) {
		return backend.EnableWindows(ctx, action)
	})
}

func (s *Storage) QueryWindowsActivation(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActivation, error) {
	backend, err := s.windowsBackend()
	if err != nil {
		return storage.WindowsActivation{}, err
	}
	result, err := backend.QueryWindowsActivation(ctx, action)
	return result, s.publicationError(err)
}

func (s *Storage) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.CheckWindowsStorage(); err != nil {
		return nil, err
	}
	// The native session retains this hook for final close and expiry. Mutations
	// use their own request hooks, so releasing a reference settles each allowance once.
	inner, err := s.backing.(storage.WindowsStorage).NewWindowsSession(s.accountingContext(ctx, "retained Windows file"), options)
	if err != nil {
		return nil, s.publicationError(err)
	}
	return &windowsSession{WindowsSession: inner, storage: s}, nil
}

type windowsSession struct {
	storage.WindowsSession
	storage *Storage
}

func (s *windowsSession) Open(ctx context.Context, request storage.WindowsOpenRequest, action storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	result, err := fileMutation(s.storage, ctx, "retained Windows file", func(ctx context.Context) (storage.WindowsOpenResult, error) {
		return s.WindowsSession.Open(ctx, request, action)
	})
	result.File = s.storage.wrapWindowsFile(result.File)
	return result, err
}

func (s *windowsSession) QueryAction(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.storage.windowsResult(s.WindowsSession.QueryAction(ctx, action))
}

func (s *windowsSession) CancelAction(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.storage.windowsResult(s.WindowsSession.CancelAction(ctx, action))
}

func (s *windowsSession) Close(ctx context.Context) error {
	return s.storage.publicationError(s.WindowsSession.Close(ctx))
}

func (s *Storage) wrapWindowsFile(inner storage.WindowsFile) storage.WindowsFile {
	if inner == nil {
		return nil
	}
	return &windowsFile{WindowsFile: inner, storage: s}
}

func (s *Storage) windowsResult(result storage.WindowsActionResult, err error) (storage.WindowsActionResult, error) {
	result.File = s.wrapWindowsFile(result.File)
	return result, s.publicationError(err)
}

type windowsFile struct {
	storage.WindowsFile
	storage *Storage
}

func (f *windowsFile) WriteAt(ctx context.Context, offset int64, data []byte, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.WriteAt(ctx, offset, data, action)
	})
}

func (f *windowsFile) Truncate(ctx context.Context, size int64, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.Truncate(ctx, size, action)
	})
}

func (f *windowsFile) SetAttr(ctx context.Context, change storage.WindowsAttrChange, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.SetAttr(ctx, change, action)
	})
}

func (f *windowsFile) SetLink(ctx context.Context, target string, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.SetLink(ctx, target, action)
	})
}

func (f *windowsFile) Rename(ctx context.Context, request storage.WindowsRenameRequest, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.Rename(ctx, request, action)
	})
}

func (f *windowsFile) SetDeletePending(ctx context.Context, pending bool, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, func(ctx context.Context) (storage.WindowsActionResult, error) {
		return f.WindowsFile.SetDeletePending(ctx, pending, action)
	})
}

func (f *windowsFile) LockBatch(ctx context.Context, batch storage.WindowsLockBatch, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.windowsResult(f.WindowsFile.LockBatch(ctx, batch, action))
}

func (f *windowsFile) Close(ctx context.Context, action storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.storage.windowsResult(f.WindowsFile.Close(ctx, action))
}

func (f *windowsFile) mutate(ctx context.Context, operation func(context.Context) (storage.WindowsActionResult, error)) (storage.WindowsActionResult, error) {
	return f.storage.windowsResult(fileMutation(f.storage, ctx, "retained Windows file", operation))
}
