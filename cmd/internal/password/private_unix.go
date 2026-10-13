//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package password

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func readPrivateFile(path string, maxBytes int) (_ string, returned error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", fmt.Errorf("open password file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { returned = errors.Join(returned, file.Close()) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return "", err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0777 != 0600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return "", errors.New("password file must be a single-link regular file owned by this user with mode 0600")
	}
	return ReadLine(file, maxBytes)
}
