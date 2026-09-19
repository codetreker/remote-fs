package fuse_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

type directoryCalls struct {
	open     func(string, int, uint32) (int, error)
	getdents func(int, []byte) (int, error)
	close    func(int) error
}

func retryDirectoryInterruption(call func() (int, error)) (int, error) {
	var result int
	var err error
	for range 8 {
		result, err = call()
		if !errors.Is(err, syscall.EINTR) {
			return result, err
		}
	}
	return result, err
}

func directoryInodes(dir string, calls directoryCalls) (listed map[string]uint64, resultErr error) {
	fd, err := retryDirectoryInterruption(func() (int, error) {
		return calls.open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	})
	if err != nil {
		return nil, fmt.Errorf("open directory %q: %w", dir, err)
	}
	defer func() {
		if err := calls.close(fd); err != nil {
			listed = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("close directory %q: %w", dir, err))
		}
	}()
	listed = make(map[string]uint64)
	buf := make([]byte, 8192)
	for {
		// With a valid buffer, getdents64 returns delivered entries as a positive
		// count. EINTR leaves no batch to replay; the descriptor keeps its position.
		// https://github.com/torvalds/linux/blob/e8f897f4afef0031fe618a8e94127a0934896aba/fs/readdir.c#L409-L423
		n, err := retryDirectoryInterruption(func() (int, error) { return calls.getdents(fd, buf) })
		if err != nil {
			return nil, fmt.Errorf("getdents directory %q: %w", dir, err)
		}
		if n == 0 {
			return listed, nil
		}
		for offset := 0; offset < n; {
			entry := (*unix.Dirent)(unsafe.Pointer(&buf[offset]))
			offset += int(entry.Reclen)
			name := make([]byte, 0, len(entry.Name))
			for _, c := range entry.Name {
				if c == 0 {
					break
				}
				name = append(name, byte(c))
			}
			if string(name) != "." && string(name) != ".." {
				listed[string(name)] = entry.Ino
			}
		}
	}
}

func TestDirectoryInodesRetriesOnlyInterruptedCallAndPreservesPosition(t *testing.T) {
	dir := t.TempDir()
	want := make(map[string]uint64)
	for _, name := range []string{"first", "second", "third"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		want[name] = ino(t, path)
	}
	opens, closes, reads, positives := 0, 0, 0, 0
	fd := -1
	interrupted := false
	calls := directoryCalls{
		open: func(path string, flags int, mode uint32) (int, error) {
			opens++
			if path != dir || flags != unix.O_RDONLY|unix.O_DIRECTORY || mode != 0 {
				t.Fatal("changed open arguments")
			}
			if opens == 1 {
				return -1, syscall.EINTR
			}
			var err error
			fd, err = unix.Open(path, flags, mode)
			return fd, err
		},
		getdents: func(current int, buf []byte) (int, error) {
			reads++
			if current != fd {
				t.Fatal("directory descriptor changed")
			}
			if positives == 1 && !interrupted {
				interrupted = true
				return -1, syscall.EINTR
			}
			n, err := unix.Getdents(current, buf[:128])
			if n > 0 {
				positives++
			}
			return n, err
		},
		close: func(current int) error {
			closes++
			if current != fd {
				t.Fatal("closed another descriptor")
			}
			return unix.Close(current)
		},
	}
	got, err := directoryInodes(dir, calls)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("listing=%v want=%v", got, want)
	}
	for name, number := range want {
		if got[name] != number {
			t.Fatalf("inode %s=%d want=%d", name, got[name], number)
		}
	}
	if opens != 2 || closes != 1 || !interrupted || positives < 2 || reads != positives+2 {
		t.Fatalf("calls open=%d close=%d reads=%d positive=%d interrupted=%v", opens, closes, reads, positives, interrupted)
	}
}

func TestDirectoryInodesPreservesFailuresAndClosesOnce(t *testing.T) {
	for _, test := range []struct {
		name, operation string
		cause           error
		attempts        int
		afterBatch      bool
	}{
		{"open interruptions exhausted", "open", syscall.EINTR, 8, false},
		{"open refusal", "open", syscall.EIO, 1, false},
		{"read interruptions exhausted", "getdents", syscall.EINTR, 8, false},
		{"read refusal", "getdents", syscall.EIO, 1, false},
		{"read refusal after data", "getdents", syscall.EIO, 2, true},
		{"close interruption", "close", syscall.EINTR, 1, false},
		{"close refusal", "close", syscall.EIO, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "entry"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			opens, reads, closes := 0, 0, 0
			calls := directoryCalls{
				open: func(path string, flags int, mode uint32) (int, error) {
					opens++
					if test.operation == "open" {
						return -1, test.cause
					}
					return unix.Open(path, flags, mode)
				},
				getdents: func(fd int, buf []byte) (int, error) {
					reads++
					if test.operation == "getdents" && (!test.afterBatch || reads > 1) {
						return -1, test.cause
					}
					return unix.Getdents(fd, buf)
				},
				close: func(fd int) error {
					closes++
					if err := unix.Close(fd); err != nil {
						return err
					}
					if test.operation == "close" {
						return test.cause
					}
					return nil
				},
			}
			listed, err := directoryInodes(dir, calls)
			if !errors.Is(err, test.cause) || listed != nil {
				t.Fatalf("listing=%v error=%v want=%v", listed, err, test.cause)
			}
			attempts := map[string]int{"open": opens, "getdents": reads, "close": closes}[test.operation]
			if attempts != test.attempts {
				t.Fatalf("%s calls=%d want=%d", test.operation, attempts, test.attempts)
			}
			if test.operation == "open" {
				if reads != 0 || closes != 0 {
					t.Fatalf("failed open read=%d close=%d", reads, closes)
				}
			} else if opens != 1 || closes != 1 {
				t.Fatalf("reopened or reclosed directory: open=%d close=%d", opens, closes)
			}
		})
	}
}
