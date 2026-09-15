package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"slices"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *windowsSession) QueryAction(ctx context.Context, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if _, err := id.Epoch(); err != nil {
		return metastore.WindowsResult{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	s.rotate()
	a := s.actions[id]
	if a == nil {
		return metastore.WindowsResult{}, syscall.ESTALE
	}
	s.store.wakeWindowsWaitersLocked()
	return s.result(a)
}

func (s *windowsSession) CancelAction(ctx context.Context, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if _, err := id.Epoch(); err != nil {
		return metastore.WindowsResult{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	a := s.actions[id]
	if a == nil {
		return metastore.WindowsResult{}, syscall.ESTALE
	}
	if a.result.State != storage.WindowsActionPending {
		return s.result(a)
	}
	a.wait = nil
	a.result.State = storage.WindowsActionCancelled
	a.expires = time.Now().Add(s.options.History)
	s.store.wakeWindowsWaitersLocked()
	return s.result(a)
}

func windowsLockRequests(batch storage.WindowsLockBatch) []windowsaccess.LockRequest {
	requests := make([]windowsaccess.LockRequest, len(batch.Ranges))
	for i, r := range batch.Ranges {
		flags := windowsaccess.LockFlags(0)
		switch r.Type {
		case storage.Shared:
			flags = windowsaccess.Shared
		case storage.Exclusive:
			flags = windowsaccess.Exclusive
		case storage.Unlock:
			flags = windowsaccess.Unlock
		}
		if r.FailImmediately {
			flags |= windowsaccess.FailImmediately
		}
		if r.Check() != nil {
			flags = 0
		}
		requests[i] = windowsaccess.LockRequest{Range: windowsaccess.Range{Offset: r.Offset, Length: r.Length}, Flags: flags}
	}
	return requests
}

const windowsLockComparisonBudget = 1 << 20

func (f *windowsFile) LockBatch(ctx context.Context, batch storage.WindowsLockBatch, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if err := batch.Check(); err != nil {
		return metastore.WindowsResult{}, err
	}
	fingerprint, err := windowsFingerprint(struct {
		Op    string
		Batch storage.WindowsLockBatch
	}{"lock", batch})
	if err != nil {
		return metastore.WindowsResult{}, err
	}
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
	if err := f.check(0); err != nil {
		return f.session.finish(a, err)
	}
	// Directory streams reject LOCK before any batch element can take effect.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/369103ec-a8af-452b-8006-aff07b925b61
	if err := f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var mode fs.FileMode
		var attributes uint32
		if err := tx.QueryRowContext(ctx, `SELECT mode,windows_attributes FROM nodes WHERE volume=? AND id=?`, f.session.store.volume, f.id).Scan(&mode, &attributes); err != nil {
			return err
		}
		if mode.IsDir() || mode&fs.ModeSymlink != 0 && attributes&storage.WindowsDOSDirectory != 0 {
			return syscall.EINVAL
		}
		return nil
	}); err != nil {
		return f.session.finish(a, err)
	}
	if err := f.session.store.fileDomain.windows.access.CheckBatchWork(f.handle, len(batch.Ranges), windowsLockComparisonBudget); err != nil {
		return f.session.finish(a, err)
	}
	if batch.Ranges[0].Type != storage.Unlock && len(batch.Ranges) > f.session.remainingRanges() {
		return f.session.finish(a, syscall.ENOLCK)
	}
	result, err := f.session.store.fileDomain.windows.access.LockBatch(f.handle, windowsLockRequests(batch))
	a.result.Applied = result.Applied
	if errors.Is(err, windowsaccess.ErrConflict) && len(batch.Ranges) == 1 && !batch.Ranges[0].FailImmediately {
		waiting := 0
		for _, old := range f.session.actions {
			if old.wait != nil {
				waiting++
			}
		}
		if waiting >= f.session.options.MaxPendingLocks || len(f.session.store.fileDomain.windows.waiters) >= f.session.store.fileDomain.config.MaxWaiters {
			return f.session.finish(a, syscall.ENOLCK)
		}
		copyBatch := storage.WindowsLockBatch{Ranges: slices.Clone(batch.Ranges)}
		a.wait = &copyBatch
		f.session.store.fileDomain.windows.waiters = append(f.session.store.fileDomain.windows.waiters, a)
		return f.session.result(a)
	}
	r, e := f.session.finish(a, err)
	f.session.store.wakeWindowsWaitersLocked()
	return r, e
}

func (s *Store) wakeWindowsWaitersLocked() {
	d := s.fileDomain.windows
	for i := 0; i < len(d.waiters); {
		a := d.waiters[i]
		if a.wait == nil || a.result.State != storage.WindowsActionPending {
			d.waiters = slices.Delete(d.waiters, i, i+1)
			continue
		}
		if err := a.file.session.check(context.Background()); err != nil {
			a.file.session.finish(a, err)
			d.waiters = slices.Delete(d.waiters, i, i+1)
			continue
		}
		if err := a.file.check(0); err != nil {
			a.file.session.finish(a, err)
			d.waiters = slices.Delete(d.waiters, i, i+1)
			continue
		}
		if len(a.wait.Ranges) > a.file.session.remainingRanges() {
			a.file.session.finish(a, syscall.ENOLCK)
			d.waiters = slices.Delete(d.waiters, i, i+1)
			continue
		}
		result, err := d.access.LockBatch(a.file.handle, windowsLockRequests(*a.wait))
		if errors.Is(err, windowsaccess.ErrConflict) {
			i++
			continue
		}
		a.result.Applied = result.Applied
		a.file.session.finish(a, err)
		d.waiters = slices.Delete(d.waiters, i, i+1)
	}
}

func (s *windowsSession) remainingRanges() int {
	count := 0
	for _, f := range s.files {
		count += s.store.fileDomain.windows.access.RangeCount(f.handle)
	}
	return s.options.MaxLockRanges - count
}
