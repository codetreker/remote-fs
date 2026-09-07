package localdisk

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

type filesystemIdentity struct {
	device         uint64
	filesystemType int64
	mountID        uint64
}

func identifyFilesystem(fd int, ops fileOperations) (filesystemIdentity, error) {
	var st unix.Stat_t
	if err := ops.fstat(fd, &st); err != nil {
		return filesystemIdentity{}, err
	}
	var filesystem unix.Statfs_t
	if err := ops.fstatfs(fd, &filesystem); err != nil {
		return filesystemIdentity{}, err
	}
	var extended unix.Statx_t
	if err := ops.statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_MNT_ID, &extended); err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
			return filesystemIdentity{}, fmt.Errorf("mount identity is unavailable: %v: %w", err, syscall.EOPNOTSUPP)
		}
		return filesystemIdentity{}, fmt.Errorf("read mount identity: %w", err)
	}
	if extended.Mask&unix.STATX_MNT_ID == 0 {
		return filesystemIdentity{}, fmt.Errorf("statx did not report a mount identity: %w", syscall.EOPNOTSUPP)
	}
	return filesystemIdentity{
		device:         uint64(st.Dev),
		filesystemType: filesystem.Type,
		mountID:        extended.Mnt_id,
	}, nil
}

func rejectRemoteFilesystem(identity filesystemIdentity) error {
	switch identity.filesystemType {
	case unix.NFS_SUPER_MAGIC,
		unix.CIFS_SUPER_MAGIC,
		unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC,
		unix.AFS_FS_MAGIC,
		unix.AFS_SUPER_MAGIC,
		unix.CEPH_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC,
		unix.NCP_SUPER_MAGIC:
		return fmt.Errorf("filesystem type %#x is remote and cannot provide local crash semantics: %w",
			identity.filesystemType, syscall.EOPNOTSUPP)
	default:
		return nil
	}
}

func requireFilesystemIdentity(fd int, expected filesystemIdentity, ops fileOperations) error {
	actual, err := identifyFilesystem(fd, ops)
	if err != nil {
		return fmt.Errorf("cannot verify object-tree filesystem identity: %w", err)
	}
	if err := rejectRemoteFilesystem(actual); err != nil {
		return fmt.Errorf("object-tree entry is on a remote filesystem: %v: %w", err, syscall.EIO)
	}
	if actual != expected {
		return fmt.Errorf("object tree crosses from device %#x, filesystem %#x, mount %d to device %#x, filesystem %#x, mount %d: %w",
			expected.device, expected.filesystemType, expected.mountID,
			actual.device, actual.filesystemType, actual.mountID, syscall.EIO)
	}
	return nil
}
