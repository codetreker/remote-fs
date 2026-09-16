package sqlite

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

var errFileIOActive = errors.New("retired reference still owns admitted content operations")

type fileIOMembership struct {
	file     *fileReference
	closed   bool
	finished atomic.Bool
}

func (f *fileReference) AcquireIO(ctx context.Context) (metastore.IOMembership, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return nil, err
	}
	d := f.session.store.fileDomain
	if f.session.activeIO >= f.session.options.MaxOperations || d.activeIO >= d.contentConfig.MaxMaterializations {
		return nil, syscall.EAGAIN
	}
	f.ioUsers++
	f.session.activeIO++
	d.activeIO++
	m := &fileIOMembership{file: f}
	d.memberships[m] = struct{}{}
	return m, nil
}

func (m *fileIOMembership) Close(ctx context.Context) error {
	m.finished.Store(true)
	f := m.file
	s := f.session
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cleanupTimeout)
	defer cancel()
	if err := s.store.coordinator.commit.acquire(cleanup); err != nil {
		s.store.coordinator.poisonWith(err)
		return err
	}
	defer s.store.coordinator.commit.release()
	if m.closed {
		return nil
	}
	if err := s.store.reapFinishedIOLocked(); err != nil {
		return err
	}
	if s.activeIO == 0 && (s.cleanupPending || !time.Now().Before(s.expires)) {
		cleanup, cancel := context.WithTimeout(s.cleanup, s.cleanupTimeout)
		defer cancel()
		_, err := s.closeLocked(cleanup)
		if err != nil && err != errFileIOActive {
			s.store.coordinator.poisonWith(err)
		}
		return err
	}
	return nil
}

func (s *Store) reapFinishedIOLocked() error {
	if s.fileDomain == nil {
		return nil
	}
	for membership := range s.fileDomain.memberships {
		if !membership.finished.Load() {
			continue
		}
		f := membership.file
		if f.ioUsers < 1 || f.session.activeIO < 1 || s.fileDomain.activeIO < 1 {
			s.coordinator.poisonWith(syscall.EIO)
			return syscall.EIO
		}
		f.ioUsers--
		f.session.activeIO--
		s.fileDomain.activeIO--
		membership.closed = true
		delete(s.fileDomain.memberships, membership)
	}
	return nil
}
