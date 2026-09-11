package localstore

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type witnessOperations struct {
	openat   func(int, string, int, uint32) (int, error)
	pread    func(int, []byte, int64) (int, error)
	write    func(int, []byte) (int, error)
	fsync    func(int) error
	close    func(int) error
	renameat func(int, string, int, string) error
	unlinkat func(int, string, int) error
}

func defaultWitnessOperations() witnessOperations {
	return witnessOperations{
		openat: unix.Openat, pread: unix.Pread, write: unix.Write,
		fsync: unix.Fsync, close: unix.Close, renameat: unix.Renameat, unlinkat: unix.Unlinkat,
	}
}

func (ops witnessOperations) readFull(fd int, content []byte) error {
	for offset := int64(0); len(content) > 0; {
		read, err := ops.pread(fd, content, offset)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if read == 0 {
			return syscall.EIO
		}
		content = content[read:]
		offset += int64(read)
	}
	return nil
}

func (ops witnessOperations) writeFull(fd int, content []byte) error {
	for len(content) > 0 {
		written, err := ops.write(fd, content)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if written == 0 {
			return syscall.EIO
		}
		content = content[written:]
	}
	return nil
}

func witnessPathFailure(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: path, Err: errors.Join(err, syscall.EIO)}
}
