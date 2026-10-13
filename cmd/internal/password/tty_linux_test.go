package password

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTerminalCancellationRestoresEcho(t *testing.T) {
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	number, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	original, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := ReadTTY(ctx, terminal, io.Discard, "Password: ", 1024)
		finished <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TCGETS)
		if err != nil {
			t.Fatal(err)
		}
		if current.Lflag&unix.ECHO == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("password input did not disable terminal echo")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("terminal cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled terminal input remained blocked")
	}
	restored, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TCGETS)
	if err != nil || restored.Lflag != original.Lflag {
		t.Fatalf("terminal flags were not restored: %v", err)
	}
}
