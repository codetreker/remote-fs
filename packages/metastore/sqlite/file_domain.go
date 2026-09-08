package sqlite

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/advisory"
)

type fileDomain struct {
	config      advisory.Config
	coordinator *advisory.Coordinator
	stores      int
	maxFiles    int
	files       int
}

func (s *Store) attachFileDomain(options Options) error {
	domain := s.coordinator.domains[s.namespace]
	if domain == nil {
		coordinator, err := advisory.New(options.Advisory)
		if err != nil {
			return err
		}
		domain = &fileDomain{config: options.Advisory, coordinator: coordinator, maxFiles: options.MaxRetainedFiles}
		s.coordinator.domains[s.namespace] = domain
	} else if domain.config != options.Advisory || domain.maxFiles != options.MaxRetainedFiles {
		return fmt.Errorf("shared SQLite namespace file limits differ from its active owner: %w", syscall.EINVAL)
	}
	domain.stores++
	s.fileDomain = domain
	return nil
}

func (s *Store) releaseFileDomain() {
	if s.fileDomain == nil {
		return
	}
	s.fileDomain.stores--
	if s.fileDomain.stores == 0 {
		delete(s.coordinator.domains, s.namespace)
	}
	s.fileDomain = nil
}

// Advisory returns the authority shared by all Store values for this namespace.
func (s *Store) Advisory(ctx context.Context) (*advisory.Coordinator, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.coordinator.commit.release()
	if s.fileDomain == nil || s.files == nil {
		return nil, syscall.ESTALE
	}
	if err := s.checkFileOwnership(); err != nil {
		return nil, err
	}
	if err := s.coordinator.healthy(); err != nil {
		return nil, err
	}
	return s.fileDomain.coordinator, nil
}

func (s *Store) CheckFileStore() error {
	// The owner pointer and exclusive mode are immutable after construction.
	// Its live descriptor and namespace state are checked under publication ordering.
	if s.leaseOwner == nil || !s.leaseOwner.exclusive {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func (s *Store) checkFileOwnership() error {
	if s.files == nil {
		return syscall.ESTALE
	}
	if s.leaseOwner == nil || !s.leaseOwner.exclusive {
		return fmt.Errorf("retained files require exclusive native database ownership: %w", syscall.EOPNOTSUPP)
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	return s.verifyLeaseOwnership()
}
