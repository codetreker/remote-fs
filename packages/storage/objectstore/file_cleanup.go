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
	retry := !r.closeResult.Released && failedClose(r.closeDone, r.closeErr)
	r.closeMu.Unlock()
	if !retry {
		return false, nil
	}
	return true, r.Close(ctx)
}

func (s *Storage) retryReferenceCleanup(ctx context.Context, limit int) error {
	s.fileMu.Lock()
	sessions := make([]*fileSession, 0, len(s.fileSessions))
	for session := range s.fileSessions {
		sessions = append(sessions, session)
	}
	s.fileMu.Unlock()
	var failures []error
	for _, session := range sessions {
		if limit == 0 {
			break
		}
		session.closeMu.Lock()
		retry := failedClose(session.closeDone, session.closeErr)
		session.closeMu.Unlock()
		operation, cancel := session.operationContext(ctx)
		if retry {
			limit--
			failures = append(failures, session.Close(operation))
		} else {
			session.mu.Lock()
			references := make([]retainedReference, 0, len(session.files))
			for reference := range session.files {
				references = append(references, reference)
			}
			session.mu.Unlock()
			for _, reference := range references {
				if limit == 0 {
					break
				}
				retried, err := reference.retryClose(operation)
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
		return nil
	}
	if err := accounting.CheckMaintenanceAccounting(); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			return nil
		}
		return err
	}
	if limit < 1 {
		return syscall.EINVAL
	}
	return native.RetryPendingUnlinks(ctx, limit)
}
