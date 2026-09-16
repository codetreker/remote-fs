package objectstore

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func failedClose(done <-chan struct{}, err error) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return err != nil
	default:
		return false
	}
}

func (f *openFile) retryClose(ctx context.Context) (bool, error) {
	f.closeMu.Lock()
	retry := failedClose(f.closeDone, f.closeErr)
	f.closeMu.Unlock()
	if !retry {
		return false, nil
	}
	return true, f.Close(ctx)
}

func (r *nodeReference) retryClose(ctx context.Context) (bool, error) {
	r.closeMu.Lock()
	retry := failedClose(r.closeDone, r.closeErr)
	r.closeMu.Unlock()
	if !retry {
		return false, nil
	}
	return true, r.Close(ctx)
}

func (s *Storage) retryReferenceCleanup(ctx context.Context, limit int) error {
	s.fileMu.Lock()
	sessions := make([]*fileSession, 0, len(s.fileSessions))
	for fs := range s.fileSessions {
		sessions = append(sessions, fs)
	}
	s.fileMu.Unlock()
	var failures []error
	for _, fs := range sessions {
		if limit == 0 {
			break
		}
		fs.closeMu.Lock()
		retry := failedClose(fs.closeDone, fs.closeErr)
		fs.closeMu.Unlock()
		operation, cancel := fs.operationContext(ctx)
		if retry {
			limit--
			failures = append(failures, fs.Close(operation))
		} else {
			fs.mu.Lock()
			refs := make([]retainedReference, 0, len(fs.files))
			for ref := range fs.files {
				refs = append(refs, ref)
			}
			fs.mu.Unlock()
			for _, ref := range refs {
				if limit == 0 {
					break
				}
				retried, err := ref.retryClose(operation)
				if retried {
					limit--
				}
				failures = append(failures, err)
			}
		}
		cancel()
	}
	return errors.Join(failures...)
}

func (s *Storage) retryPendingUnlinks(ctx context.Context, limit int) error {
	native, ok := s.meta.(interface {
		RetryPendingUnlinks(context.Context, int) error
	})
	if !ok {
		return nil
	}
	accounting, ok := s.meta.(storage.MaintenanceAccounting)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := accounting.CheckMaintenanceAccounting(); err != nil {
		if err == syscall.EOPNOTSUPP {
			return nil
		}
		return err
	}
	return native.RetryPendingUnlinks(ctx, limit)
}
