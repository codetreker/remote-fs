package localstore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

type rootAnchor struct {
	fd     int
	path   string
	device uint64
	mount  uint64
	inode  uint64
	closed bool
	statx  func(int, string, int, int, *unix.Statx_t) error
}

func openRootAnchor(root string) (*rootAnchor, error) {
	if err := requireControlledPath(root); err != nil {
		return nil, err
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open local store root", Path: root, Err: err}
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(
			&os.PathError{Op: "stat local store root", Path: root, Err: err},
			pathFailure("close local store root", root, unix.Close(fd)),
		)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uint32(unix.Geteuid()) ||
		fs.FileMode(stat.Mode).Perm() != 0o700 {
		return nil, errors.Join(
			&os.PathError{Op: "validate local store root", Path: root,
				Err: fmt.Errorf("the root must be an owner-only directory owned by the serving user: %w", syscall.EACCES)},
			pathFailure("close local store root", root, unix.Close(fd)),
		)
	}
	mount, err := mountID(fd, unix.Statx)
	if err != nil {
		return nil, errors.Join(err, pathFailure("close local store root", root, unix.Close(fd)))
	}
	return &rootAnchor{
		fd: fd, path: root, device: uint64(stat.Dev), mount: mount, inode: stat.Ino, statx: unix.Statx,
	}, nil
}

func requireControlledPath(root string) error {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return &os.PathError{Op: "open local store path root", Path: "/", Err: err}
	}
	current := "/"
	for _, component := range strings.Split(strings.TrimPrefix(root, "/"), "/") {
		if component == "" {
			continue
		}
		var parent unix.Stat_t
		if err := unix.Fstat(fd, &parent); err != nil {
			return errors.Join(
				&os.PathError{Op: "stat local store ancestor", Path: current, Err: err},
				pathFailure("close local store ancestor", current, unix.Close(fd)),
			)
		}
		nextPath := filepath.Join(current, component)
		nextFD, err := unix.Openat(fd, component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return errors.Join(
				&os.PathError{Op: "open local store path component", Path: nextPath, Err: err},
				pathFailure("close local store ancestor", current, unix.Close(fd)),
			)
		}
		var child unix.Stat_t
		statErr := unix.Fstat(nextFD, &child)
		protectionErr := error(nil)
		if statErr == nil {
			protectionErr = requireRenameProtection(current, parent, child)
		}
		closeErr := unix.Close(fd)
		if err := errors.Join(
			pathFailure("stat local store path component", nextPath, statErr),
			protectionErr,
			pathFailure("close local store ancestor", current, closeErr),
		); err != nil {
			return errors.Join(err, pathFailure("close local store path component", nextPath, unix.Close(nextFD)))
		}
		fd = nextFD
		current = nextPath
	}
	return pathFailure("close local store root validation", root, unix.Close(fd))
}

func requireRenameProtection(parentPath string, parent, child unix.Stat_t) error {
	owner := uint32(unix.Geteuid())
	if parent.Mode&unix.S_IFMT != unix.S_IFDIR || (parent.Uid != owner && parent.Uid != 0) {
		return &os.PathError{Op: "validate local store ancestor", Path: parentPath,
			Err: fmt.Errorf("the directory is not deployment-owned: %w", syscall.EACCES)}
	}
	if fs.FileMode(parent.Mode).Perm()&0o022 == 0 {
		return nil
	}
	if parent.Mode&unix.S_ISVTX != 0 && (child.Uid == owner || child.Uid == 0) {
		return nil
	}
	return &os.PathError{Op: "validate local store ancestor", Path: parentPath,
		Err: fmt.Errorf("the directory permits another user to replace the next path component: %w", syscall.EACCES)}
}

func pathFailure(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: path, Err: err}
}

func mountID(fd int, statx func(int, string, int, int, *unix.Statx_t) error) (uint64, error) {
	var extended unix.Statx_t
	if err := statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_MNT_ID, &extended); err != nil {
		return 0, fmt.Errorf("read local store mount identity: %v: %w", err, syscall.EOPNOTSUPP)
	}
	if extended.Mask&unix.STATX_MNT_ID == 0 {
		return 0, fmt.Errorf("statx did not report a local store mount identity: %w", syscall.EOPNOTSUPP)
	}
	return extended.Mnt_id, nil
}

func validatePrivateFile(
	fd int,
	path string,
	device, expectedMount uint64,
	allowUnlinked bool,
	statx func(int, string, int, int, *unix.Statx_t) error,
) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return unix.Stat_t{}, &os.PathError{Op: "stat private local store file", Path: path, Err: err}
	}
	validLinks := stat.Nlink == 1 || allowUnlinked && stat.Nlink == 0
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(unix.Geteuid()) ||
		fs.FileMode(stat.Mode).Perm() != 0o600 || !validLinks || uint64(stat.Dev) != device {
		return unix.Stat_t{}, &os.PathError{Op: "validate private local store file", Path: path,
			Err: fmt.Errorf("the file must be on the root filesystem, not multiply linked, owner-only, regular, and owned by the serving user: %w",
				syscall.EIO)}
	}
	actualMount, err := mountID(fd, statx)
	if err != nil {
		return unix.Stat_t{}, &os.PathError{Op: "validate private local store mount", Path: path, Err: err}
	}
	if actualMount != expectedMount {
		return unix.Stat_t{}, &os.PathError{Op: "validate private local store mount", Path: path,
			Err: fmt.Errorf("the file is on mount %d, want root mount %d: %w",
				actualMount, expectedMount, syscall.EOPNOTSUPP)}
	}
	return stat, nil
}

func (a *rootAnchor) CreatePrivateMetastore() error {
	path := filepath.Join(a.path, metastoreFilename)
	fd, err := unix.Openat(a.fd, metastoreFilename,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return &os.PathError{Op: "create private local store metastore", Path: path, Err: err}
	}
	chmodErr := unix.Fchmod(fd, 0o600)
	syncErr := error(nil)
	if chmodErr == nil {
		syncErr = unix.Fsync(fd)
	}
	closeErr := unix.Close(fd)
	if err := errors.Join(chmodErr, syncErr, closeErr); err != nil {
		return &os.PathError{Op: "prepare private local store metastore", Path: path, Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync private local store metastore", Path: a.path, Err: err}
	}
	return nil
}

func (a *rootAnchor) MakeMetastoreFilesPrivate() error {
	for _, name := range append([]string{metastoreFilename}, metastoreAuxiliaryFilenames[:]...) {
		path := filepath.Join(a.path, name)
		fd, err := unix.Openat(a.fd, name,
			unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			if errors.Is(err, syscall.ELOOP) {
				return &os.PathError{Op: "validate local store entry", Path: path,
					Err: fmt.Errorf("the entry is a symbolic link: %w", syscall.EIO)}
			}
			return &os.PathError{Op: "open local store entry", Path: path, Err: err}
		}
		var stat unix.Stat_t
		statErr := unix.Fstat(fd, &stat)
		validLinks := stat.Nlink == 1 || name != metastoreFilename && stat.Nlink == 0
		if statErr == nil && (stat.Mode&unix.S_IFMT != unix.S_IFREG ||
			stat.Uid != uint32(unix.Geteuid()) || !validLinks || uint64(stat.Dev) != a.device) {
			statErr = fmt.Errorf("the entry must be on the root filesystem, not multiply linked, regular, and owned by the serving user: %w",
				syscall.EIO)
		}
		if statErr == nil {
			actualMount, mountErr := mountID(fd, a.statx)
			switch {
			case mountErr != nil:
				statErr = mountErr
			case actualMount != a.mount:
				statErr = fmt.Errorf("the entry is on mount %d, want root mount %d: %w",
					actualMount, a.mount, syscall.EOPNOTSUPP)
			}
		}
		chmodErr := error(nil)
		syncErr := error(nil)
		if statErr == nil {
			chmodErr = unix.Fchmod(fd, 0o600)
		}
		if statErr == nil && chmodErr == nil {
			syncErr = unix.Fsync(fd)
		}
		closeErr := unix.Close(fd)
		if err := errors.Join(statErr, chmodErr, syncErr, closeErr); err != nil {
			return &os.PathError{Op: "make local store entry private", Path: path, Err: err}
		}
	}
	return nil
}

func (a *rootAnchor) RequireRegularEntry(name string) (bool, error) {
	fd, err := unix.Openat(a.fd, name,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			return false, &os.PathError{Op: "validate local store entry", Path: filepath.Join(a.path, name),
				Err: fmt.Errorf("the entry is a symbolic link: %w", syscall.EIO)}
		}
		return false, &os.PathError{Op: "open local store entry", Path: filepath.Join(a.path, name), Err: err}
	}
	path := filepath.Join(a.path, name)
	_, validateErr := validatePrivateFile(
		fd, path, a.device, a.mount, name != metastoreFilename, a.statx,
	)
	closeErr := unix.Close(fd)
	if err := errors.Join(validateErr, pathFailure("close local store entry", path, closeErr)); err != nil {
		return false, err
	}
	return true, nil
}

func (a *rootAnchor) RequireMetastoreFiles() (bool, error) {
	databaseExists, err := a.RequireRegularEntry(metastoreFilename)
	if err != nil {
		return false, err
	}
	for _, name := range metastoreAuxiliaryFilenames {
		if _, err := a.RequireRegularEntry(name); err != nil {
			return false, err
		}
	}
	return databaseExists, nil
}

func verifyAnchoredRoot(objects *localdisk.Objects, anchor *rootAnchor) error {
	return errors.Join(objects.VerifyRootPath(), anchor.VerifyPath())
}

func (a *rootAnchor) VerifyPath() error {
	var stat unix.Stat_t
	if err := unix.Lstat(a.path, &stat); err != nil {
		return fmt.Errorf("local store root %q no longer names the locked directory: %v: %w",
			a.path, err, syscall.EIO)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(stat.Dev) != a.device || stat.Ino != a.inode {
		return fmt.Errorf("local store root %q was replaced while it was being opened: %w", a.path, syscall.EIO)
	}
	var extended unix.Statx_t
	if err := a.statx(unix.AT_FDCWD, a.path, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_MNT_ID, &extended); err != nil {
		return fmt.Errorf("read configured local store mount identity: %v: %w", err, syscall.EOPNOTSUPP)
	}
	if extended.Mask&unix.STATX_MNT_ID == 0 {
		return fmt.Errorf("statx did not report the configured local store mount identity: %w", syscall.EOPNOTSUPP)
	}
	if extended.Mnt_id != a.mount {
		return fmt.Errorf("local store root %q moved from mount %d to %d: %w",
			a.path, a.mount, extended.Mnt_id, syscall.EOPNOTSUPP)
	}
	return nil
}

func (a *rootAnchor) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	if err := unix.Close(a.fd); err != nil {
		return fmt.Errorf("close local store root anchor: %w", err)
	}
	return nil
}
