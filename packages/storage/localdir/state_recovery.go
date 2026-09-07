package localdir

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

func (s *directoryState) directoryNames(fd int, limit int) ([]string, error) {
	scan, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 32768)
	var names []string
	var scanErr error
	for {
		n, err := unix.ReadDirent(scan, buffer)
		if err != nil {
			scanErr = err
			break
		}
		if n == 0 {
			break
		}
		_, _, names = unix.ParseDirent(buffer[:n], -1, names)
		if len(names) > limit {
			scanErr = fmt.Errorf("private state recovery entry limit exceeded: %w", syscall.E2BIG)
			break
		}
		for _, name := range names {
			if len(name) > s.limits.MaxPathBytes {
				scanErr = fmt.Errorf("private state entry path limit exceeded: %w", syscall.ENAMETOOLONG)
				break
			}
		}
		if scanErr != nil {
			break
		}
	}
	return names, errors.Join(scanErr, s.consumeFD(scan))
}

func (s *directoryState) inspectStateDirectory(recorded bool) error {
	names, err := s.directoryNames(s.stateFD, 3)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == stagingDirectory {
			continue
		}
		if recorded && (name == stateFilename || name == stateFilename+".next") {
			continue
		}
		return stateFailure("private state directory contains an unrecognized entry")
	}
	return nil
}

func (s *directoryState) cleanRecoveryFiles() error {
	names, err := s.directoryNames(s.stagingFD, s.limits.MaxRecoveryEntries)
	if err != nil {
		return err
	}
	var total int64
	for _, name := range names {
		fd, err := unix.Openat(s.stagingFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		stat, validateErr := s.validatePrivateFile(fd, s.limits.MaxStagingBytes-total)
		err = errors.Join(validateErr, s.consumeFD(fd))
		if err != nil {
			return err
		}
		total += stat.Size
	}
	for _, name := range names {
		if err := unix.Unlinkat(s.stagingFD, name, 0); err != nil {
			return err
		}
	}
	if len(names) != 0 {
		if err := s.ops.fsync(s.stagingFD); err != nil {
			return err
		}
	}
	next := stateFilename + ".next"
	fd, err := unix.Openat(s.stateFD, next, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	_, validateErr := s.validatePrivateFile(fd, stateRecordLimit)
	if err := errors.Join(validateErr, s.consumeFD(fd)); err != nil {
		return err
	}
	if err := unix.Unlinkat(s.stateFD, next, 0); err != nil {
		return err
	}
	return s.ops.fsync(s.stateFD)
}

func (s *directoryState) probeCapabilities() error {
	var fs unix.Statfs_t
	if err := unix.Fstatfs(s.rootFD, &fs); err != nil {
		return err
	}
	switch fs.Type {
	case unix.NFS_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC, unix.V9FS_MAGIC, unix.FUSE_SUPER_MAGIC:
		return fmt.Errorf("lease persistence requires a local filesystem: %w", syscall.EOPNOTSUPP)
	}
	second, err := unix.Openat(s.rootFD, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	lockErr := unix.Flock(second, unix.LOCK_EX|unix.LOCK_NB)
	closeErr := s.consumeFD(second)
	if !errors.Is(lockErr, unix.EWOULDBLOCK) {
		return errors.Join(fmt.Errorf("filesystem does not exclude a second namespace flock: %w", syscall.EOPNOTSUPP), lockErr, closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return s.probePublication()
}

func (s *directoryState) probePublication() (result error) {
	id, err := newStateID()
	if err != nil {
		return err
	}
	name := ".remote-fs-probe-" + id
	attribute := "user.remote-fs.probe"
	fd, err := unix.Openat(s.stagingFD, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	currentName := name
	attrInstalled := false
	defer func() {
		result = errors.Join(result, s.consumeFD(fd))
		if attrInstalled {
			result = errors.Join(result, unix.Fremovexattr(s.rootFD, attribute))
		}
		result = errors.Join(result, unix.Unlinkat(s.stagingFD, currentName, 0), s.ops.fsync(s.stagingFD), s.ops.fsync(s.rootFD))
	}()
	if err := unix.Fchmod(fd, 0o600); err != nil {
		return err
	}
	payload := []byte("lease capability probe")
	n, err := s.ops.write(fd, payload[:1])
	if err != nil {
		return err
	}
	if n != 1 {
		return stateFailure("capability probe write was incomplete")
	}
	if err := s.ops.fsync(fd); err != nil {
		return err
	}
	previous, attrErr := readStateAttribute(s.rootFD, attribute)
	if attrErr == nil {
		if !bytes.Equal(previous, payload) {
			return stateFailure("incomplete capability probe has unexpected evidence")
		}
		if err := unix.Fremovexattr(s.rootFD, attribute); err != nil {
			return err
		}
	} else if !errors.Is(attrErr, unix.ENODATA) {
		return attrErr
	}
	if err := s.ops.setxattr(s.rootFD, attribute, payload, unix.XATTR_CREATE); err != nil {
		return err
	}
	attrInstalled = true
	actual, err := readStateAttribute(s.rootFD, attribute)
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, payload) {
		return stateFailure("filesystem xattr probe returned different bytes")
	}
	if err := s.ops.fsync(s.rootFD); err != nil {
		return err
	}
	if err := s.ops.rename(s.stagingFD, name, s.stagingFD, name+"-moved"); err != nil {
		return err
	}
	currentName = name + "-moved"
	if err := errors.Join(s.ops.fsync(s.rootFD), s.ops.fsync(s.stagingFD)); err != nil {
		return err
	}
	if err := s.ops.rename(s.stagingFD, currentName, s.stagingFD, name); err != nil {
		return err
	}
	currentName = name
	if err := s.ops.fsync(s.stagingFD); err != nil {
		return err
	}
	return nil
}
