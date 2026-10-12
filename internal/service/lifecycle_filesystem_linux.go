package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/internal/management"
)

const reclaimProofXattr = "user.remote-fs.reclaim-proof"
const createProofXattr = "user.remote-fs.create-proof"

func lifecycleDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, syscall.EINVAL
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err == nil && (stat.Mode&0777 != 0700 || stat.Uid != uint32(os.Geteuid())) {
		err = syscall.EACCES
	}
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func validateLifecycleDirectories(volumes, quarantine string) error {
	a, err := lifecycleDirectory(volumes)
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := lifecycleDirectory(quarantine)
	if err != nil {
		return err
	}
	defer b.Close()
	var left, right unix.Stat_t
	if err := unix.Fstat(int(a.Fd()), &left); err != nil {
		return err
	}
	if err := unix.Fstat(int(b.Fd()), &right); err != nil {
		return err
	}
	if left.Dev != right.Dev || left.Ino == right.Ino {
		return syscall.EINVAL
	}
	return nil
}

func syncLifecycleDirectory(path string) error {
	file, err := lifecycleDirectory(path)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func (l *Lifecycle) volumePath(volume management.Volume) string {
	return filepath.Join(l.config.VolumesDir, volume.StorageName)
}
func (l *Lifecycle) quarantinePath(op management.Operation) string {
	return filepath.Join(l.config.QuarantineDir, op.QuarantineLeaf)
}

func lifecycleXattr(fd int, name string) ([]byte, error) {
	n, err := unix.Fgetxattr(fd, name, nil)
	if err != nil {
		return nil, err
	}
	if n < 1 || n > 8192 {
		return nil, syscall.EIO
	}
	data := make([]byte, n)
	n, err = unix.Fgetxattr(fd, name, data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func installLifecycleXattr(path, name string, data []byte) error {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return installLifecycleXattrFD(int(root.Fd()), name, data)
}

func installLifecycleXattrFD(fd int, name string, data []byte) error {
	if len(data) == 0 || len(data) > 8192 {
		return syscall.EINVAL
	}
	err := unix.Fsetxattr(fd, name, data, unix.XATTR_CREATE)
	if errors.Is(err, syscall.EEXIST) {
		actual, readErr := lifecycleXattr(fd, name)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(actual, data) {
			return syscall.EIO
		}
		return nil
	}
	return err
}

func (l *Lifecycle) prepareCreatedRoot(op management.Operation, volume management.Volume) error {
	parent, err := lifecycleDirectory(l.config.VolumesDir)
	if err != nil {
		return err
	}
	defer parent.Close()
	created := true
	if err := unix.Mkdirat(int(parent.Fd()), volume.StorageName, 0700); err != nil {
		if !errors.Is(err, syscall.EEXIST) {
			return err
		}
		created = false
	}
	fd, err := unix.Openat(int(parent.Fd()), volume.StorageName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	marker, err := json.Marshal(struct {
		Volume    management.ID `json:"volume"`
		Operation management.ID `json:"operation"`
	}{volume.ID, op.ID})
	if err != nil {
		return err
	}
	if created {
		if err := installLifecycleXattrFD(fd, createProofXattr, marker); err != nil {
			return err
		}
	} else {
		actual, err := lifecycleXattr(fd, createProofXattr)
		if err != nil {
			return err
		}
		if !bytes.Equal(actual, marker) {
			return syscall.EIO
		}
	}
	if err := unix.Fsync(fd); err != nil {
		return err
	}
	return parent.Sync()
}

func readLifecycleBinding(path string) ([]byte, error) {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	fd, err := unix.Openat(int(root.Fd()), "LOCALSTORE", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(path, "LOCALSTORE"))
	defer file.Close()
	var rootStat, fileStat unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &rootStat); err != nil {
		return nil, err
	}
	if err := unix.Fstat(fd, &fileStat); err != nil {
		return nil, err
	}
	if fileStat.Mode&unix.S_IFMT != unix.S_IFREG || fileStat.Mode&0777 != 0600 || fileStat.Uid != uint32(os.Geteuid()) || fileStat.Nlink != 1 || fileStat.Dev != rootStat.Dev || fileStat.Size < 72 || fileStat.Size > 1096 {
		return nil, syscall.EIO
	}
	data, err := io.ReadAll(io.LimitReader(file, 1097))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != fileStat.Size {
		return nil, syscall.EIO
	}
	return data, nil
}

func (l *Lifecycle) quarantineRoot(ctx context.Context, op management.Operation, volume management.Volume) error {
	source := l.volumePath(volume)
	target := l.quarantinePath(op)
	if root, err := lifecycleDirectory(target); err == nil {
		root.Close()
		if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return syscall.EEXIST
			}
			return err
		}
		if err := l.verifyRootProof(ctx, op, volume, target); err != nil {
			return err
		}
	} else {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := l.verifyRootProof(ctx, op, volume, source); err != nil {
			return err
		}
		parent, err := lifecycleDirectory(l.config.VolumesDir)
		if err != nil {
			return err
		}
		defer parent.Close()
		quarantine, err := lifecycleDirectory(l.config.QuarantineDir)
		if err != nil {
			return err
		}
		defer quarantine.Close()
		root, err := lifecycleDirectory(source)
		if err != nil {
			return err
		}
		defer root.Close()
		if err := lifecycleFence(int(parent.Fd()), volume.StorageName, int(root.Fd())); err != nil {
			return err
		}
		if err := unix.Renameat2(int(parent.Fd()), volume.StorageName, int(quarantine.Fd()), op.QuarantineLeaf, unix.RENAME_NOREPLACE); err != nil {
			return err
		}
		if err := lifecycleFence(int(quarantine.Fd()), op.QuarantineLeaf, int(root.Fd())); err != nil {
			return err
		}
	}
	return errors.Join(syncLifecycleDirectory(l.config.VolumesDir), syncLifecycleDirectory(l.config.QuarantineDir))
}

func lifecycleFence(parent int, leaf string, fd int) error {
	var opened, current unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return err
	}
	if err := unix.Fstatat(parent, leaf, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if opened.Dev != current.Dev || opened.Ino != current.Ino || opened.Mode&unix.S_IFMT != unix.S_IFDIR {
		return syscall.EIO
	}
	return nil
}

func (l *Lifecycle) reclaim(ctx context.Context, op management.Operation) (bool, error) {
	path := l.quarantinePath(op)
	var proof lifecycleReclaimProof
	if err := json.Unmarshal(op.ReclaimingProof, &proof); err != nil {
		return false, errors.Join(management.ErrCorrupt, err)
	}
	if proof.Path != path || !bytes.Equal(proof.Marker, op.PlannedReclaimProof) || !bytes.Equal(proof.Root, op.VerifiedRootProof) {
		return false, management.ErrCorrupt
	}
	parent, err := lifecycleDirectory(l.config.QuarantineDir)
	if err != nil {
		return false, err
	}
	defer parent.Close()
	root, err := lifecycleDirectory(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		return true, parent.Sync()
	}
	defer root.Close()
	actual, err := lifecycleXattr(int(root.Fd()), reclaimProofXattr)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(actual, proof.Marker) {
		return false, syscall.EIO
	}
	if err := lifecycleFence(int(parent.Fd()), op.QuarantineLeaf, int(root.Fd())); err != nil {
		return false, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &stat); err != nil {
		return false, err
	}
	remaining := l.config.MaxReclaimEntries
	empty, err := reclaimLifecycleChildren(ctx, root, stat.Dev, &remaining, 0)
	if err != nil {
		return false, err
	}
	if err := root.Sync(); err != nil {
		return false, err
	}
	if !empty {
		return false, nil
	}
	if err := lifecycleFence(int(parent.Fd()), op.QuarantineLeaf, int(root.Fd())); err != nil {
		return false, err
	}
	if err := unix.Unlinkat(int(parent.Fd()), op.QuarantineLeaf, unix.AT_REMOVEDIR); err != nil {
		return false, err
	}
	return true, parent.Sync()
}

func reclaimLifecycleChildren(ctx context.Context, root *os.File, device uint64, remaining *int, depth int) (bool, error) {
	if depth > 64 {
		return false, syscall.ELOOP
	}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if *remaining == 0 {
			return false, nil
		}
		entries, err := root.ReadDir(1)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		if len(entries) == 0 {
			return true, nil
		}
		leaf := entries[0].Name()
		var stat unix.Stat_t
		if err := unix.Fstatat(int(root.Fd()), leaf, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return false, err
		}
		if stat.Dev != device {
			return false, syscall.EXDEV
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			fd, err := unix.Openat(int(root.Fd()), leaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				return false, err
			}
			child := os.NewFile(uintptr(fd), leaf)
			empty, childErr := reclaimLifecycleChildren(ctx, child, device, remaining, depth+1)
			if childErr == nil && empty {
				childErr = child.Sync()
			}
			if childErr == nil && empty {
				childErr = lifecycleFence(int(root.Fd()), leaf, fd)
			}
			childErr = errors.Join(childErr, child.Close())
			if childErr != nil {
				return false, childErr
			}
			if !empty {
				return false, nil
			}
			if *remaining == 0 {
				return false, nil
			}
			if err := unix.Unlinkat(int(root.Fd()), leaf, unix.AT_REMOVEDIR); err != nil {
				return false, err
			}
		} else {
			if err := unix.Unlinkat(int(root.Fd()), leaf, 0); err != nil {
				return false, err
			}
		}
		*remaining--
	}
}

func validateLifecycleReclaimMarker(path string, expected []byte, allowMissing bool) error {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return err
	}
	defer root.Close()
	actual, err := lifecycleXattr(int(root.Fd()), reclaimProofXattr)
	if allowMissing && errors.Is(err, unix.ENODATA) {
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, expected) {
		return syscall.EIO
	}
	return nil
}

func validateLifecycleCreateMarker(path string, op management.Operation) error {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return err
	}
	defer root.Close()
	marker, err := lifecycleXattr(int(root.Fd()), createProofXattr)
	if err != nil {
		return err
	}
	expected, err := json.Marshal(struct {
		Volume    management.ID `json:"volume"`
		Operation management.ID `json:"operation"`
	}{op.VolumeID, op.ID})
	if err != nil {
		return err
	}
	if !bytes.Equal(marker, expected) {
		return syscall.EIO
	}
	return nil
}
