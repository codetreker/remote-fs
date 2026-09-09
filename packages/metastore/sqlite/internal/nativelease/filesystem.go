package nativelease

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	leaseProbeAttribute = "user.remote-fs.lease-probe"
	leaseProbeContents  = "lease persistence capability probe"
)

type leaseAnchorOperations struct {
	fstatfs   func(int, *unix.Statfs_t) error
	flock     func(int, int) error
	renameat  func(int, string, int, string) error
	renameat2 func(int, string, int, string, uint) error
	setxattr  func(int, string, []byte, int) error
	getxattr  func(int, string, []byte) (int, error)
	fsync     func(int) error
}

var leaseAnchorSystemOperations = leaseAnchorOperations{
	fstatfs: unix.Fstatfs, flock: unix.Flock, renameat: unix.Renameat, renameat2: unix.Renameat2,
	setxattr: unix.Fsetxattr, getxattr: unix.Fgetxattr, fsync: unix.Fsync,
}

func (a *Anchor) verify() error {
	if err := a.verifyDirectory(); err != nil {
		return err
	}
	if err := a.validateBindingFD(); err != nil {
		return err
	}
	expected, err := encodeLeaseRecord("binding", a.intent.Binding)
	if err != nil {
		return err
	}
	actual, exists, err := a.readBinding()
	if err != nil {
		return err
	}
	if !exists || !bytes.Equal(actual, expected) {
		return leaseAnchorFailure("native lease binding changed while open", syscall.EIO)
	}
	return nil
}

func (a *Anchor) verifyDirectory() error {
	if a.closed {
		return leaseAnchorFailure("lease anchor is closed", syscall.EBADF)
	}
	fd, err := openLeaseDirectory(a.intent.Binding.Directory)
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	if statErr == nil && (stat.Dev != a.directoryStat.Dev || stat.Ino != a.directoryStat.Ino ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Mode&0o022 != 0) {
		statErr = leaseAnchorFailure("configured lease state directory no longer names the owned directory", syscall.EIO)
	}
	if statErr == nil {
		mount, err := leaseMountID(fd)
		statErr = err
		if err == nil && mount != a.mount {
			statErr = leaseAnchorFailure("configured lease state directory changed mount", syscall.EIO)
		}
	}
	return leaseAnchorFailure("verify lease state directory", errors.Join(statErr, unix.Close(fd)))
}

func (a *Anchor) validateBindingFD() error {
	var stat unix.Stat_t
	if err := unix.Fstat(a.bindingFD, &stat); err != nil {
		return leaseAnchorFailure("stat native lease binding", err)
	}
	kind := stat.Mode & unix.S_IFMT
	if kind != unix.S_IFREG && kind != unix.S_IFDIR || stat.Uid != uint32(unix.Geteuid()) ||
		stat.Mode&0o022 != 0 || stat.Nlink == 0 || kind == unix.S_IFREG && stat.Nlink != 1 ||
		stat.Dev != a.directoryStat.Dev {
		return leaseAnchorFailure("native lease binding must be owned, protected, and on the state filesystem", syscall.EIO)
	}
	mount, err := leaseMountID(a.bindingFD)
	if err != nil {
		return err
	}
	if mount != a.mount {
		return leaseAnchorFailure("native lease binding is on another mount", syscall.EOPNOTSUPP)
	}
	filesystem, err := leaseFilesystemType(a.bindingFD, a.operations.fstatfs)
	if err != nil {
		return err
	}
	if filesystem != a.filesystem {
		return leaseAnchorFailure("native lease binding is on another filesystem", syscall.EOPNOTSUPP)
	}
	return nil
}

func leaseFilesystemType(fd int, statfs func(int, *unix.Statfs_t) error) (int64, error) {
	var stat unix.Statfs_t
	if err := statfs(fd, &stat); err != nil {
		return 0, leaseAnchorFailure("identify lease state filesystem", err)
	}
	switch stat.Type {
	case unix.NFS_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC, unix.AFS_FS_MAGIC, unix.AFS_SUPER_MAGIC, unix.CEPH_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC, unix.NCP_SUPER_MAGIC:
		return 0, fmt.Errorf("lease state filesystem type %#x is remote and cannot provide local crash semantics: %w", stat.Type, syscall.EOPNOTSUPP)
	}
	return stat.Type, nil
}

func (a *Anchor) validateProbeFD(fd int) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stat, leaseAnchorFailure("stat lease capability probe", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Dev != a.directoryStat.Dev ||
		stat.Size < 0 || stat.Size > int64(len(leaseProbeContents)) {
		return stat, leaseAnchorFailure("lease capability probe must be bounded, private, regular, and single-linked", syscall.EIO)
	}
	mount, err := leaseMountID(fd)
	if err != nil {
		return stat, err
	}
	filesystem, err := leaseFilesystemType(fd, a.operations.fstatfs)
	if err != nil {
		return stat, err
	}
	if mount != a.mount || filesystem != a.filesystem {
		return stat, leaseAnchorFailure("lease capability probe is on another filesystem or mount", syscall.EOPNOTSUPP)
	}
	return stat, nil
}

func (a *Anchor) inspectProbe(name string) (unix.Stat_t, bool, error) {
	fd, err := unix.Openat(a.directoryFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return unix.Stat_t{}, false, nil
	}
	if err != nil {
		return unix.Stat_t{}, false, leaseAnchorFailure("open lease capability probe", err)
	}
	stat, validateErr := a.validateProbeFD(fd)
	return stat, true, errors.Join(validateErr, leaseAnchorFailure("close lease capability probe inspection", unix.Close(fd)))
}

func (a *Anchor) removeProbe(name string) error {
	stat, exists, err := a.inspectProbe(name)
	if err != nil || !exists {
		return err
	}
	var named unix.Stat_t
	if err := unix.Fstatat(a.directoryFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return leaseAnchorFailure("verify lease capability probe before removal", err)
	}
	if named.Dev != stat.Dev || named.Ino != stat.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG || named.Nlink != 1 {
		return leaseAnchorFailure("lease capability probe changed before removal", syscall.EIO)
	}
	if err := unix.Unlinkat(a.directoryFD, name, 0); err != nil {
		return leaseAnchorFailure("remove lease capability probe", err)
	}
	return leaseAnchorFailure("sync lease capability probe removal", a.syncFile(a.directoryFD))
}

func (a *Anchor) createProbe(name string) (int, unix.Stat_t, error) {
	fd, err := unix.Openat(a.directoryFD, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return -1, unix.Stat_t{}, leaseAnchorFailure("create lease capability probe", err)
	}
	if err := unix.Fchmod(fd, 0o600); err != nil {
		return -1, unix.Stat_t{}, errors.Join(leaseAnchorFailure("make lease capability probe private", err), unix.Close(fd))
	}
	stat, err := a.validateProbeFD(fd)
	if err != nil {
		return -1, unix.Stat_t{}, errors.Join(err, unix.Close(fd))
	}
	return fd, stat, nil
}

func (a *Anchor) probeCapabilities(prefix string) (returned error) {
	source, target := prefix+".probe-source", prefix+".probe-target"
	for _, name := range []string{source, target} {
		if _, _, err := a.inspectProbe(name); err != nil {
			return err
		}
	}
	defer func() {
		for _, name := range []string{source, target} {
			returned = errors.Join(returned, a.removeProbe(name))
		}
	}()
	for _, name := range []string{source, target} {
		if err := a.removeProbe(name); err != nil {
			return err
		}
	}
	fd, original, err := a.createProbe(source)
	if err != nil {
		return err
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close lease capability probe", unix.Close(fd)))
	}()
	if n, err := unix.Write(fd, []byte(leaseProbeContents)); err != nil || n != len(leaseProbeContents) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return leaseAnchorFailure("write lease capability probe", err)
	}
	original.Size = int64(len(leaseProbeContents))
	if err := a.probeXattr(fd); err != nil {
		return err
	}
	if err := a.probeFlock(fd, source, original); err != nil {
		return err
	}
	if err := a.syncFile(fd); err != nil {
		return leaseAnchorFailure("sync lease capability probe file", err)
	}
	if err := a.operations.renameat2(a.directoryFD, source, a.directoryFD, target, unix.RENAME_NOREPLACE); err != nil {
		return leaseAnchorFailure("probe create-only lease state rename", err)
	}
	if err := a.requireProbeIdentity(target, original); err != nil {
		return err
	}
	if err := a.requireProbeAbsent(source); err != nil {
		return err
	}
	if err := a.syncFile(a.directoryFD); err != nil {
		return leaseAnchorFailure("sync lease capability probe rename", err)
	}
	occupiedFD, occupied, err := a.createProbe(source)
	if err != nil {
		return err
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close occupied lease capability probe", unix.Close(occupiedFD)))
	}()
	if err := a.operations.renameat2(a.directoryFD, target, a.directoryFD, source, unix.RENAME_NOREPLACE); !errors.Is(err, syscall.EEXIST) {
		return leaseAnchorFailure("lease state create-only rename did not reject an occupied destination", errors.Join(err, syscall.EOPNOTSUPP))
	}
	if err := a.requireProbeIdentity(source, occupied); err != nil {
		return err
	}
	if err := a.requireProbeIdentity(target, original); err != nil {
		return err
	}
	if err := a.operations.renameat(a.directoryFD, target, a.directoryFD, source); err != nil {
		return leaseAnchorFailure("probe atomic lease state replacement", err)
	}
	if err := a.requireProbeIdentity(source, original); err != nil {
		return err
	}
	if err := a.requireProbeAbsent(target); err != nil {
		return err
	}
	return leaseAnchorFailure("sync lease capability probe replacement", a.syncFile(a.directoryFD))
}

func (a *Anchor) probeXattr(fd int) error {
	value := []byte(leaseProbeContents)
	if err := a.operations.setxattr(fd, leaseProbeAttribute, value, unix.XATTR_CREATE); err != nil {
		return leaseAnchorFailure("probe lease binding xattr creation", err)
	}
	if err := a.operations.setxattr(fd, leaseProbeAttribute, []byte("replacement"), unix.XATTR_CREATE); !errors.Is(err, syscall.EEXIST) {
		return leaseAnchorFailure("create-only lease binding xattr did not reject replacement", errors.Join(err, syscall.EOPNOTSUPP))
	}
	actual := make([]byte, len(value)+1)
	n, err := a.operations.getxattr(fd, leaseProbeAttribute, actual)
	if err != nil {
		return leaseAnchorFailure("probe lease binding xattr read", err)
	}
	if !bytes.Equal(actual[:n], value) {
		return leaseAnchorFailure("lease binding xattr did not preserve its value", syscall.EOPNOTSUPP)
	}
	return nil
}

func (a *Anchor) probeFlock(fd int, name string, expected unix.Stat_t) (returned error) {
	if err := a.operations.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return leaseAnchorFailure("probe exclusive lease state flock", err)
	}
	other, err := unix.Openat(a.directoryFD, name, unix.O_RDWR|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return leaseAnchorFailure("open independent lease flock probe", err)
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close independent lease flock probe", unix.Close(other)))
	}()
	actual, err := a.validateProbeFD(other)
	if err != nil {
		return err
	}
	if actual.Dev != expected.Dev || actual.Ino != expected.Ino {
		return leaseAnchorFailure("independent lease flock probe opened another inode", syscall.EIO)
	}
	if err := a.operations.flock(other, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		return leaseAnchorFailure("lease state flock admitted a conflicting independent owner", errors.Join(err, syscall.EOPNOTSUPP))
	}
	return nil
}

func (a *Anchor) requireProbeIdentity(name string, expected unix.Stat_t) error {
	actual, exists, err := a.inspectProbe(name)
	if err != nil {
		return err
	}
	if !exists || actual.Dev != expected.Dev || actual.Ino != expected.Ino || actual.Size != expected.Size {
		return leaseAnchorFailure("lease state rename did not preserve the source inode and size", syscall.EOPNOTSUPP)
	}
	return nil
}

func (a *Anchor) requireProbeAbsent(name string) error {
	_, exists, err := a.inspectProbe(name)
	if err != nil {
		return err
	}
	if exists {
		return leaseAnchorFailure("lease state rename retained its source name", syscall.EOPNOTSUPP)
	}
	return nil
}

func leaseMountID(fd int) (uint64, error) {
	var stat unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT, unix.STATX_MNT_ID, &stat); err != nil {
		return 0, leaseAnchorFailure("read lease state mount identity", errors.Join(err, syscall.EOPNOTSUPP))
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return 0, leaseAnchorFailure("lease state filesystem does not report mount identity", syscall.EOPNOTSUPP)
	}
	return stat.Mnt_id, nil
}

func openLeaseDirectory(path string) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, leaseAnchorFailure("open lease path root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" {
			continue
		}
		var parent unix.Stat_t
		statErr := unix.Fstat(fd, &parent)
		if statErr != nil || parent.Uid != 0 && parent.Uid != uint32(unix.Geteuid()) ||
			parent.Mode&0o022 != 0 && parent.Mode&unix.S_ISVTX == 0 {
			return -1, errors.Join(leaseAnchorFailure("lease path ancestor permits replacement by another user", errors.Join(statErr, syscall.EACCES)), unix.Close(fd))
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if err := errors.Join(openErr, closeErr); err != nil {
			if next >= 0 {
				err = errors.Join(err, unix.Close(next))
			}
			return -1, leaseAnchorFailure("open lease path component", err)
		}
		var child unix.Stat_t
		if err := unix.Fstat(next, &child); err != nil ||
			parent.Mode&0o022 != 0 && child.Uid != 0 && child.Uid != uint32(unix.Geteuid()) {
			return -1, errors.Join(leaseAnchorFailure("lease path entry is not protected by its sticky parent", errors.Join(err, syscall.EACCES)), unix.Close(next))
		}
		fd = next
	}
	return fd, nil
}
