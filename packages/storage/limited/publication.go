package limited

import (
	"context"
	"fmt"
	"io"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

// CheckPublicationAccounting reports whether byte accounting participates in the
// backing namespace's final publication. A legacy backend returns ENOSYS.
func (s *Storage) CheckPublicationAccounting() error {
	if !s.accounted {
		return syscall.ENOSYS
	}
	if err := s.healthy(); err != nil {
		return err
	}
	return s.backing.(interface{ CheckPublicationAccounting() error }).CheckPublicationAccounting()
}

// LockService returns the authority paired with the backing namespace, or nil
// when that namespace has no lock authority.
func (s *Storage) LockService() locking.Service {
	if source, ok := s.backing.(interface{ LockService() locking.Service }); ok {
		return source.LockService()
	}
	return nil
}

// Close delegates lifecycle ownership to the backing namespace. A backend with no
// Close method has no lifecycle resource for this wrapper to release.
func (s *Storage) Close() error {
	if closer, ok := s.backing.(io.Closer); ok {
		return s.publicationError(closer.Close())
	}
	return nil
}

func (s *Storage) healthy() error {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	return s.fault
}

func (s *Storage) publicationError(err error) error {
	if !storage.IsPublicationAccountingUncertain(err) {
		return err
	}
	s.countMu.Lock()
	defer s.countMu.Unlock()
	if !storage.IsPublicationAccountingUncertain(s.fault) {
		s.fault = err
	}
	return err
}

func (s *Storage) mutationContext(ctx context.Context, name string) context.Context {
	if s.accounted {
		return s.accountingContext(ctx, name)
	}
	return ctx
}

func (s *Storage) accountingContext(ctx context.Context, name string) context.Context {
	return storage.WithPublicationAccounting(ctx, func(previous, next int64) (storage.PublicationSettlement, error) {
		return s.preparePublication(name, previous, next)
	})
}

func (s *Storage) preparePublication(name string, previous, next int64) (storage.PublicationSettlement, error) {
	delta := next - previous
	reserved, err := s.reserve(name, delta)
	if err != nil {
		return nil, err
	}
	return func(result storage.PublicationResult) error {
		switch result {
		case storage.PublicationNotApplied:
			s.release(reserved)
		case storage.PublicationApplied:
			if delta < 0 {
				s.release(-delta)
			}
		default:
			s.countMu.Lock()
			defer s.countMu.Unlock()
			if s.fault == nil {
				s.fault = fmt.Errorf("publication outcome for %q is unknown; reopen the namespace before using its allowance: %w", name, syscall.EIO)
			}
			return s.fault
		}
		return nil
	}, nil
}
