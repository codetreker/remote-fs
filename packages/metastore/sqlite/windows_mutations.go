package sqlite

import (
	"context"
	"database/sql"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *windowsFile) SetAttr(ctx context.Context, change storage.WindowsAttrChange, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if err := change.Check(); err != nil {
		return metastore.WindowsResult{}, err
	}
	fingerprint, err := windowsFingerprint(struct {
		Op     string
		Change storage.WindowsAttrChange
	}{"setattr", change})
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
	if err := f.check(storage.WindowsWriteAttributes); err != nil {
		return f.session.finish(a, err)
	}
	ctx = f.session.nativeContext(ctx, f)
	err = f.session.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		before, err := f.session.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		oldAttr, err := f.attr(ctx, tx)
		if err != nil {
			return err
		}
		if !change.AttrChange.Empty() {
			if err := applyChange(ctx, tx, before, change.AttrChange); err != nil {
				return err
			}
		}
		if change.CreationTime != nil {
			sec, nsec := sqlvalue.StoredTime(*change.CreationTime)
			if _, err := tx.ExecContext(ctx, `UPDATE nodes SET windows_creation_sec=?,windows_creation_nsec=? WHERE volume=? AND id=?`, sec, nsec, f.session.store.volume, f.id); err != nil {
				return err
			}
		}
		at := time.Now()
		if change.ChangeTime != nil {
			at = *change.ChangeTime
		}
		sec, nsec := sqlvalue.StoredTime(at)
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET windows_change_sec=?,windows_change_nsec=? WHERE volume=? AND id=?`, sec, nsec, f.session.store.volume, f.id); err != nil {
			return err
		}
		if change.DOSAttributes != nil {
			if _, err := tx.ExecContext(ctx, `UPDATE nodes SET windows_attributes=(windows_attributes & ?) | ? WHERE volume=? AND id=?`, storage.WindowsDOSDirectory, *change.DOSAttributes, f.session.store.volume, f.id); err != nil {
				return err
			}
		}
		a.result.Attr, err = f.attr(ctx, tx)
		if err != nil {
			return err
		}
		var mask metastore.ChangeMask
		if !oldAttr.CreationTime.Equal(a.result.Attr.CreationTime) {
			mask |= metastore.ChangeCreationTime
		}
		if !oldAttr.ChangeTime.Equal(a.result.Attr.ChangeTime) {
			mask |= metastore.ChangeTime
		}
		if oldAttr.DOSAttributes != a.result.Attr.DOSAttributes {
			mask |= metastore.ChangeAttributes
		}
		if a.result.Attr.NameInfo.State != storage.WindowsNameDetached {
			return f.session.store.recordChangedMask(ctx, tx, before, mask)
		}
		return nil
	})
	return f.session.finish(a, err)
}

func (f *windowsFile) SetDeletePending(ctx context.Context, pending bool, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	fingerprint, _ := windowsFingerprint(struct {
		Op      string
		Pending bool
	}{"delete", pending})
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
	if err := f.check(storage.WindowsDelete); err != nil {
		return f.session.finish(a, err)
	}
	if f.id == f.session.store.root {
		return f.session.finish(a, syscall.EBUSY)
	}
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		node, err := f.session.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if pending && node.IsDir() {
			empty, err := f.session.store.isEmpty(ctx, tx, f.id)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		a.result.Attr, err = f.attr(ctx, tx)
		if err == nil && pending && a.result.Attr.DOSAttributes&storage.WindowsDOSDirectory == 0 && a.result.Attr.DOSAttributes&storage.WindowsDOSReadOnly != 0 {
			return syscall.EACCES
		}
		return err
	})
	if err == nil {
		err = f.session.check(ctx)
	}
	if err == nil {
		err = f.session.store.fileDomain.windows.access.SetDeletePending(f.handle, pending)
		if err == nil {
			a.result.Attr.DeletePending = f.session.store.fileDomain.windows.access.DeletePending(uint64(f.id))
		}
	}
	return f.session.finish(a, err)
}

func (f *windowsFile) Rename(ctx context.Context, request storage.WindowsRenameRequest, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if err := request.Check(); err != nil {
		return metastore.WindowsResult{}, err
	}
	fingerprint, err := windowsFingerprint(struct {
		Op      string
		Request storage.WindowsRenameRequest
	}{"rename", request})
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
	if err := f.check(storage.WindowsDelete); err != nil {
		return f.session.finish(a, err)
	}
	if request.Source.ExpectedID != uint64(f.id) || f.id == f.session.store.root {
		return f.session.finish(a, syscall.ESTALE)
	}
	ctx = f.session.nativeContext(ctx, f)
	var moving, fromParent, displaced, toParent metastore.Node
	var fromName, toName []byte
	var occupied bool
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		var found bool
		moving, fromParent, fromName, found, err = f.session.resolveLookup(ctx, tx, request.Source)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ESTALE
		}
		displaced, toParent, toName, occupied, err = f.session.resolveLookup(ctx, tx, request.Destination)
		if err != nil {
			return err
		}
		if occupied && request.Destination.ExpectedID == 0 {
			return syscall.EEXIST
		}
		return nil
	})
	if err != nil {
		return f.session.finish(a, err)
	}
	ids := []int64{moving.ID}
	if occupied && displaced.ID != moving.ID {
		ids = append(ids, displaced.ID)
	}
	err = f.session.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.RenameMutation, nodes: ids, totalUsage: true}, func(tx *sql.Tx) error {
		s := f.session.store
		if moving.IsDir() {
			ancestor := toParent.ID
			for depth := 0; ; depth++ {
				if depth > metastore.MaxNotificationAncestors {
					return syscall.EFBIG
				}
				if ancestor == moving.ID {
					return syscall.EINVAL
				}
				if ancestor == s.root {
					break
				}
				at, err := s.locate(ctx, tx, ancestor)
				if err != nil {
					return err
				}
				ancestor = at.Parent
			}
		}
		if occupied && displaced.ID != moving.ID {
			if !request.Replace {
				return syscall.EEXIST
			}
			if displaced.IsDir() != moving.IsDir() {
				return syscall.EISDIR
			}
			if displaced.IsDir() {
				empty, err := s.isEmpty(ctx, tx, displaced.ID)
				if err != nil {
					return err
				}
				if !empty {
					return syscall.ENOTEMPTY
				}
			}
			if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: toParent.ID, Name: toName}, displaced); err != nil {
				return err
			}
			if err := s.unlink(ctx, tx, toParent.ID, toName); err != nil {
				return err
			}
			if err := s.discard(ctx, tx, displaced); err != nil {
				return err
			}
		}
		if err := s.windowsNameAllowed(ctx, tx, toParent.ID, []byte(request.Destination.Name), moving.ID); err != nil {
			return err
		}
		if err := s.recordRenamed(ctx, tx, metastore.Location{Parent: toParent.ID, Name: []byte(request.Destination.Name)}, metastore.Location{Parent: fromParent.ID, Name: fromName}, moving); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE entries SET parent=?,name=? WHERE volume=? AND parent=? AND name=?`, toParent.ID, []byte(request.Destination.Name), s.volume, fromParent.ID, fromName); err != nil {
			return err
		}
		if err := s.touch(ctx, tx, fromParent.ID, time.Now()); err != nil {
			return err
		}
		if toParent.ID != fromParent.ID {
			if err := s.touch(ctx, tx, toParent.ID, time.Now()); err != nil {
				return err
			}
		}
		a.result.Attr, err = f.attr(ctx, tx)
		return err
	})
	return f.session.finish(a, err)
}

func (s *Store) checkWindowsParentLocked(parent int64) error {
	if s.fileDomain != nil && s.fileDomain.windows.access.DeletePending(uint64(parent)) {
		return windowsError(windowsaccess.ErrDeletePending)
	}
	return nil
}
