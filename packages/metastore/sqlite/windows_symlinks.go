package sqlite

import (
	"context"
	"database/sql"
	"io/fs"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsLinkConfined(target string, at storage.WindowsNameInfo) error {
	if err := storage.CheckWindowsLinkTarget(target); err != nil {
		return err
	}
	depth := 0
	if !strings.HasPrefix(target, "/") && at.State == storage.WindowsNameLinked {
		depth = strings.Count(at.Path, "/")
	}
	for _, part := range strings.Split(target, "/") {
		switch part {
		case "", ".":
			continue
		case "..":
			if depth == 0 {
				return syscall.EACCES
			}
			depth--
		default:
			if _, err := windowsNameKey([]byte(part)); err != nil {
				return err
			}
			depth++
		}
	}
	return nil
}

func (f *windowsFile) linkTarget(ctx context.Context, tx *sql.Tx) (string, storage.WindowsNameInfo, error) {
	attr, err := f.attr(ctx, tx)
	if err != nil {
		return "", storage.WindowsNameInfo{}, err
	}
	if attr.Mode&fs.ModeSymlink == 0 {
		return "", attr.NameInfo, &storage.WindowsError{Failure: storage.WindowsNotReparsePoint, Err: syscall.EINVAL}
	}
	var length int64
	var target []byte
	err = tx.QueryRowContext(ctx, `SELECT length(windows_link_target),CASE WHEN length(windows_link_target)<=? THEN windows_link_target ELSE NULL END FROM nodes WHERE volume=? AND id=?`, storage.WindowsMaxLinkTargetBytes, f.session.store.volume, f.id).Scan(&length, &target)
	if err != nil {
		return "", attr.NameInfo, err
	}
	if length != int64(len(target)) || length != attr.Size {
		return "", attr.NameInfo, syscall.EIO
	}
	if err := windowsLinkConfined(string(target), attr.NameInfo); err != nil {
		return "", attr.NameInfo, err
	}
	return string(target), attr.NameInfo, nil
}

func (f *windowsFile) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.WindowsSymlinkInfo{}, err
	}
	defer done()
	if err := f.check(storage.WindowsReadAttributes); err != nil {
		return storage.WindowsSymlinkInfo{}, err
	}
	var result storage.WindowsSymlinkInfo
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		result.Target, result.Location, err = f.linkTarget(ctx, tx)
		return err
	})
	return result, err
}

func (f *windowsFile) SetLink(ctx context.Context, target string, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if err := storage.CheckWindowsLinkTarget(target); err != nil {
		return metastore.WindowsResult{}, err
	}
	fingerprint, err := windowsFingerprint(struct{ Op, Target string }{"setlink", target})
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
	if err := f.check(storage.WindowsWriteData); err != nil {
		return f.session.finish(a, err)
	}
	if f.session.store.fileDomain.windows.access.OpenCount(uint64(f.id)) != 1 {
		return f.session.finish(a, windowsError(windowsaccess.ErrSharing))
	}
	ctx = f.session.nativeContext(ctx, f)
	err = f.session.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, nodes: []int64{f.id}, totalUsage: true}, func(tx *sql.Tx) error {
		before, err := f.session.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if f.id == f.session.store.root {
			return syscall.EBUSY
		}
		if (!before.Mode.IsRegular() && !before.IsDir()) || before.Size != 0 {
			return syscall.EINVAL
		}
		if before.IsDir() {
			empty, err := f.session.store.isEmpty(ctx, tx, f.id)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		attr, err := f.attr(ctx, tx)
		if err != nil {
			return err
		}
		if err := windowsLinkConfined(target, attr.NameInfo); err != nil {
			return err
		}
		if err := f.session.store.account(ctx, tx, int64(len(target))); err != nil {
			return err
		}
		if err := f.session.store.advanceContentRevision(ctx, tx, f.id); err != nil {
			return err
		}
		if before.Content != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE volume=? AND key=?`, stateGarbage, f.session.store.volume, string(before.Content)); err != nil {
				return err
			}
		}
		sec, nsec := sqlvalue.StoredTime(time.Now())
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET mode=?,size=?,content=NULL,windows_link_target=?,mtime_sec=?,mtime_nsec=?,windows_change_sec=?,windows_change_nsec=?,windows_attributes=windows_attributes | ? WHERE volume=? AND id=?`, int64(before.Mode&^fs.ModeType|fs.ModeSymlink), len(target), []byte(target), sec, nsec, sec, nsec, func() uint32 {
			if before.IsDir() {
				return storage.WindowsDOSDirectory
			}
			return 0
		}(), f.session.store.volume, f.id); err != nil {
			return err
		}
		if err := f.session.store.recordNamedChanged(ctx, tx, before); err != nil {
			return err
		}
		a.result.Attr, err = f.attr(ctx, tx)
		return err
	})
	return f.session.finish(a, err)
}
