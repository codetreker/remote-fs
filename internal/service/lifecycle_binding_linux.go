package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func (l *Lifecycle) closedBinding(ctx context.Context, path string) (lifecycleClosedBinding, error) {
	witness, _, err := hashLifecycleBindingFile(ctx, path, "METASTORE")
	if err != nil {
		return lifecycleClosedBinding{}, err
	}
	database, size, err := hashLifecycleBindingFile(ctx, path, "metastore.sqlite")
	if err != nil {
		return lifecycleClosedBinding{}, err
	}
	return lifecycleClosedBinding{WitnessDigest: witness, DatabaseDigest: database, DatabaseBytes: size}, nil
}

// Captured only after complete native Close, these hashes preserve the checked
// database and witness across quarantine relocation without reopening a retired
// authority at a different canonical path. Hashing uses fixed working memory.
func hashLifecycleBindingFile(ctx context.Context, path, leaf string) (result [sha256.Size]byte, size int64, resultErr error) {
	root, err := lifecycleDirectory(path)
	if err != nil {
		return result, 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	fd, err := unix.Openat(int(root.Fd()), leaf, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return result, 0, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(path, leaf))
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	var rootStat, before unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &rootStat); err != nil {
		return result, 0, err
	}
	if err := unix.Fstat(fd, &before); err != nil {
		return result, 0, err
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&0777 != 0600 || before.Uid != uint32(os.Geteuid()) || before.Nlink != 1 || before.Dev != rootStat.Dev || before.Size < 1 {
		return result, 0, syscall.EIO
	}
	digest := sha256.New()
	var chunk [64 << 10]byte
	for {
		if err := ctx.Err(); err != nil {
			return result, 0, err
		}
		n, err := file.Read(chunk[:])
		if n > 0 {
			size += int64(n)
			if size > before.Size {
				return result, 0, syscall.EIO
			}
			_, _ = digest.Write(chunk[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, 0, err
		}
		if n == 0 {
			return result, 0, io.ErrNoProgress
		}
	}
	var after, current unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return result, 0, err
	}
	if err := unix.Fstatat(int(root.Fd()), leaf, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return result, 0, err
	}
	if size != before.Size || before.Size != after.Size || before.Mtim != after.Mtim || after.Dev != current.Dev || after.Ino != current.Ino {
		return result, 0, syscall.EIO
	}
	copy(result[:], digest.Sum(nil))
	return result, size, nil
}
