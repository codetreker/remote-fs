package nativelease

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type Database struct {
	fd        int
	path      string
	exclusive bool
	acquired  time.Time
	closeFD   func(int) error
	closeErr  error
}

func AcquireDatabase(database string, exclusive, create bool) (*Database, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NONBLOCK
	if exclusive {
		flags |= unix.O_NOFOLLOW
	}
	if create {
		flags |= unix.O_CREAT
	}
	fd, err := unix.Open(database, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening native metadata ownership: %w", err)
	}
	owner := &Database{fd: fd, path: database, exclusive: exclusive, closeFD: unix.Close}
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	if err := unix.Flock(fd, mode|unix.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = syscall.EBUSY
		}
		return nil, errors.Join(fmt.Errorf("acquiring native metadata ownership: %w", err), owner.Close())
	}
	owner.acquired = time.Now()
	if err := VerifyDatabase(owner); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func VerifyExclusiveOwnership(owner *Database) error {
	if owner == nil || !owner.exclusive || owner.fd < 0 || owner.acquired.IsZero() {
		return fmt.Errorf("lease control requires exclusive native database ownership: %w", syscall.EIO)
	}
	if err := VerifyDatabase(owner); err != nil {
		return err
	}
	if err := unix.Flock(owner.fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("verifying exclusive metadata ownership: %w", err)
	}
	probe, err := unix.Open(owner.path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	conflict := unix.Flock(probe, unix.LOCK_SH|unix.LOCK_NB)
	closed := unix.Close(probe)
	if !errors.Is(conflict, syscall.EWOULDBLOCK) {
		return fmt.Errorf("native metadata ownership did not exclude a conflicting reader: %w", errors.Join(syscall.EIO, conflict, closed))
	}
	return closed
}

func (f *Database) Close() error {
	if f.fd < 0 {
		return f.closeErr
	}
	fd := f.fd
	f.fd = -1
	f.closeErr = f.closeFD(fd)
	return f.closeErr
}

func VerifyDatabase(file *Database) error {
	var held, current unix.Stat_t
	if err := unix.Fstat(file.fd, &held); err != nil {
		return err
	}
	err := unix.Stat(file.path, &current)
	if err != nil {
		return errors.Join(syscall.EIO, err)
	}
	if held.Mode&unix.S_IFMT != unix.S_IFREG || held.Dev != current.Dev || held.Ino != current.Ino ||
		file.exclusive && (held.Nlink != 1 || held.Uid != uint32(os.Geteuid()) || held.Mode&0o022 != 0) {
		return fmt.Errorf("the metadata database is not the owned, single-link regular file: %w", syscall.EIO)
	}
	return nil
}

func VerifyDatabaseOwner(a *Anchor, owner *Database) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return err
	}
	var parent, database, binding unix.Stat_t
	if err := unix.Stat(filepath.Dir(owner.path), &parent); err != nil {
		return err
	}
	if err := unix.Fstat(owner.fd, &database); err != nil {
		return err
	}
	if err := unix.Fstat(a.bindingFD, &binding); err != nil {
		return err
	}
	if parent.Dev != a.directoryStat.Dev || parent.Ino != a.directoryStat.Ino {
		return leaseAnchorFailure("lease state is not anchored beside the owned database", syscall.EIO)
	}
	boundDatabase := binding.Dev == database.Dev && binding.Ino == database.Ino
	boundRoot := binding.Dev == parent.Dev && binding.Ino == parent.Ino
	if !boundDatabase && !boundRoot {
		return leaseAnchorFailure("lease binding does not identify the owned database or its root", syscall.EIO)
	}
	return nil
}

func ValidateOpening(database string, owned bool) error {
	if strings.ContainsAny(database, "%?#\x00") {
		return fmt.Errorf("the SQLite database must be a native pathname without URI parameters or escapes: %w", syscall.EINVAL)
	}
	if owned {
		return nil
	}
	for _, name := range []string{database, filepath.Dir(database)} {
		_, err := unix.Getxattr(name, leaseBindingAttribute, nil)
		if err == nil {
			return fmt.Errorf("the native database requires its lease recovery owner: %w", syscall.EIO)
		}
		if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ENODATA) && !errors.Is(err, syscall.ENOTSUP) {
			return fmt.Errorf("checking native database lease binding: %w", err)
		}
	}
	return nil
}

func (f *Database) FD() int { return f.fd }

func (f *Database) Path() string { return f.path }

func (f *Database) Exclusive() bool { return f.exclusive }

func (f *Database) Acquired() time.Time { return f.acquired }
