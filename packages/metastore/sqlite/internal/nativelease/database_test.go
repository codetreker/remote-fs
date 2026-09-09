package nativelease

import (
	"errors"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLeaseDatabaseCloseNeverRetriesAReusedDescriptor(t *testing.T) {
	fd, err := unix.Open(filepath.Join(t.TempDir(), "held"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("close reported failure after descriptor release")
	owner := &Database{fd: fd, closeFD: func(fd int) error {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
		return fault
	}}
	if err := owner.Close(); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	reused, err := unix.Open(filepath.Join(t.TempDir(), "replacement"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(reused)
	owner.closeFD = func(int) error { t.Fatal("descriptor closure was retried"); return nil }
	if err := owner.Close(); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(reused, &stat); err != nil {
		t.Fatalf("replacement descriptor was closed: %v", err)
	}
}
