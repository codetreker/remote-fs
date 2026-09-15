package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *windowsSession) expire() {
	ctx, cancel := context.WithTimeout(s.cleanup, s.cleanupTimeout)
	defer cancel()
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		s.store.coordinator.poisonWith(err)
		return
	}
	if s.closed {
		s.store.coordinator.commit.release()
		return
	}
	if time.Now().Before(s.expires) {
		s.timer.Reset(time.Until(s.expires))
		s.store.coordinator.commit.release()
		return
	}
	s.active = false
	if s.timer != nil {
		s.timer.Stop()
	}
	s.store.coordinator.commit.release()
	if err := s.Close(ctx); err != nil {
		s.store.coordinator.poisonWith(err)
	}
}

func (s *windowsSession) Close(ctx context.Context) error {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.store.coordinator.commit.release()
	if s.closed {
		return nil
	}
	s.active = false
	var failures []error
	for _, f := range s.files {
		f.active = false
	}
	for _, f := range s.files {
		if err := f.closeLocked(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	for _, a := range s.actions {
		if a.result.State == storage.WindowsActionPending {
			s.finish(a, syscall.ESTALE)
		}
	}
	delete(s.store.fileDomain.windows.sessions, s)
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	s.store.wakeWindowsWaitersLocked()
	return nil
}

func (f *windowsFile) Close(ctx context.Context, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	fingerprint, _ := windowsFingerprint(struct{ Op string }{"close"})
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fingerprint, f)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	if !fresh {
		return f.session.result(a)
	}
	f.active = false
	err = f.closeLocked(f.session.nativeContext(ctx, f))
	if err == nil {
		f.session.store.wakeWindowsWaitersLocked()
	}
	return f.session.finish(a, err)
}

func (f *windowsFile) closeLocked(ctx context.Context) error {
	if f.closed {
		return nil
	}
	s := f.session.store
	if err := s.closeWindowsPinLocked(ctx, f.id, f.handle); err != nil {
		return err
	}
	f.closed = true
	delete(f.session.files, f.reference)
	return nil
}

func (s *Store) closeWindowsPinLocked(ctx context.Context, id int64, handle uint64) error {
	key := retainedNode{s.volume, id}
	count := s.coordinator.pins[key]
	if count < 1 {
		return syscall.EIO
	}
	if err := s.finishPendingWindowsDeleteLocked(ctx, id, handle); err != nil {
		return err
	}
	if count == 1 {
		var state metastore.FileState
		if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; state, err = s.fileState(ctx, tx, id); return err }); err != nil {
			return err
		}
		if state.Detached {
			err := s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.RemoveMutation, node: id, cleanup: true}, func(tx *sql.Tx) error { return s.discardNode(ctx, tx, state.Node) })
			if err != nil {
				return err
			}
		}
		delete(s.coordinator.pins, key)
	} else {
		s.coordinator.pins[key] = count - 1
	}
	if _, err := s.fileDomain.windows.access.Close(handle); err != nil {
		return windowsError(err)
	}
	s.fileDomain.files--
	return nil
}

func (s *Store) finishPendingWindowsDeleteLocked(ctx context.Context, id int64, handle uint64) error {
	d := s.fileDomain.windows
	if d.access.OpenCount(uint64(id)) != 1 || !d.access.DeletePending(uint64(id)) {
		return nil
	}
	if id == s.root {
		return syscall.EBUSY
	}
	ctx = withWindowsActor(ctx, handle)
	return s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.RemoveMutation, nodes: []int64{id}, totalUsage: true}, func(tx *sql.Tx) error {
		state, err := s.fileState(ctx, tx, id)
		if err != nil {
			return err
		}
		if state.Detached {
			return nil
		}
		if state.IsDir() {
			empty, err := s.isEmpty(ctx, tx, id)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		at, err := s.locate(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := s.recordRemoved(ctx, tx, at, state.Node); err != nil {
			return err
		}
		if err := s.unlink(ctx, tx, at.Parent, at.Name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, state.Node); err != nil {
			return err
		}
		return s.touch(ctx, tx, at.Parent, time.Now())
	})
}
