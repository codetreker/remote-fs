//go:build linux

package password

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func readTTY(ctx context.Context, terminal *os.File, output io.Writer, prompt string, maxBytes int) (_ string, returned error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fd := int(terminal.Fd())
	settings, err := getTermios(fd)
	if err != nil {
		return "", errors.New("password input requires a terminal or a protected password file")
	}
	private := *settings
	private.Lflag &^= unix.ECHO | unix.ECHONL
	private.Lflag |= unix.ICANON
	if err := setTermios(fd, &private); err != nil {
		return "", fmt.Errorf("disable terminal password echo: %w", err)
	}
	defer func() {
		returned = errors.Join(returned, setTermios(fd, settings))
		_, err := fmt.Fprintln(output)
		returned = errors.Join(returned, err)
	}()
	if _, err := fmt.Fprint(output, prompt); err != nil {
		return "", err
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		ready := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		if _, err := unix.Poll(ready, 100); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return "", fmt.Errorf("wait for password input: %w", err)
		}
		if ready[0].Revents&unix.POLLIN != 0 {
			buffer := make([]byte, maxBytes+3)
			count, err := unix.Read(fd, buffer)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				return "", fmt.Errorf("read password input: %w", err)
			}
			return ReadLine(strings.NewReader(string(buffer[:count])), maxBytes)
		}
		if ready[0].Revents&(unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0 {
			return "", errors.New("password terminal disconnected")
		}
	}
}
