package cmd_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse/fusetest"
)

func TestBinariesRetainLiveFilesAndExclusiveAdvisoryLocks(t *testing.T) {
	requireFUSE(t)
	for _, mode := range []string{"local-store", "azure-blob"} {
		t.Run(mode, func(t *testing.T) {
			server := startServerBinary(t, append(binaryLockNamespace(t, mode), "-initialize-lock-state")...)
			remote := dialLockServer(t, server)
			if err := remote.Write(t.Context(), "artifact", []byte("original")); err != nil {
				t.Fatal(err)
			}
			a, b := t.TempDir(), t.TempDir()
			first := startMountBinary(t, "-server", server.url, "-mountpoint", a)
			second := startMountBinary(t, "-server", server.url, "-mountpoint", b)
			writer := openBinaryFile(t, filepath.Join(a, "artifact"), os.O_RDWR, false)
			reader := openBinaryFile(t, filepath.Join(b, "artifact"), os.O_RDONLY, false)
			if err := syscall.Flock(int(writer.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatalf("exclusive flock: %v", err)
			}
			if err := syscall.Flock(int(reader.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
				t.Fatalf("competing read-only exclusive flock = %v, want EWOULDBLOCK", err)
			}
			if err := os.WriteFile(filepath.Join(b, "artifact"), []byte("current"), 0o600); err != nil {
				t.Fatalf("nonparticipating writer: %v", err)
			}
			assertBinaryFileRead(t, writer, "current")
			if err := os.Rename(filepath.Join(b, "artifact"), filepath.Join(b, "moved")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(b, "moved")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(b, "moved"), []byte("replacement"), 0o600); err != nil {
				t.Fatal(err)
			}
			if n, err := writer.WriteAt([]byte("LIVE"), 0); err != nil || n != 4 {
				t.Fatalf("write detached descriptor = %d, %v", n, err)
			}
			assertBinaryFileRead(t, reader, "LIVEent")
			if info, err := writer.Stat(); err != nil || info.Size() != 7 {
				t.Fatalf("detached fstat = %v, %v", info, err)
			}
			if err := writer.Sync(); err != nil {
				t.Fatalf("sync confirmed write: %v", err)
			}
			assertBinaryLockRead(t, remote, "moved", "replacement")
			if err := syscall.Flock(int(writer.Fd()), syscall.LOCK_UN); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Flock(int(reader.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatalf("read-only exclusive flock after release: %v", err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			stopBinaryMount(t, second, b, false)
			stopBinaryMount(t, first, a, false)
		})
	}
}

func TestBinaryRestartRetiresOldDescriptorsAndAdvisoryLocks(t *testing.T) {
	requireFUSE(t)
	for _, mode := range []string{"local-store", "azure-blob"} {
		t.Run(mode, func(t *testing.T) {
			args := append(binaryLockNamespace(t, mode), "-lock-max-lease", "200ms")
			first := startServerBinary(t, append(args, "-initialize-lock-state")...)
			remote := dialLockServer(t, first)
			if err := remote.Write(t.Context(), "artifact", []byte("original")); err != nil {
				t.Fatal(err)
			}
			oldPath := t.TempDir()
			oldMount := startMountBinary(t, "-server", first.url, "-mountpoint", oldPath)
			oldFile := openBinaryFile(t, filepath.Join(oldPath, "artifact"), os.O_RDWR, true)
			if err := syscall.Flock(int(oldFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatal(err)
			}
			if n, err := oldFile.WriteAt([]byte("durable!"), 0); err != nil || n != 8 {
				t.Fatalf("write before restart = %d, %v", n, err)
			}
			if err := oldFile.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := first.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := first.wait(t); err == nil {
				t.Fatal("killed server exited successfully")
			}
			restarted := startServerBinary(t, append(args, "-listen", strings.TrimPrefix(first.url, "http://"))...)
			reopened := dialLockServer(t, restarted)
			deadline := time.Now().Add(10 * time.Second)
			for {
				status, err := reopened.Status(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if !status.Recovering {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("restarted authority did not finish recovery")
				}
				time.Sleep(20 * time.Millisecond)
			}
			buf := make([]byte, 8)
			if n, err := oldFile.ReadAt(buf, 0); n != 0 || !retiredBinaryFileError(err) {
				t.Fatalf("old descriptor read = %d, %v; want failed retired reference", n, err)
			}
			if n, err := oldFile.WriteAt([]byte("stale"), 0); n != 0 || !retiredBinaryFileError(err) {
				t.Fatalf("old descriptor write = %d, %v; want failed retired reference", n, err)
			}
			newPath := t.TempDir()
			newMount := startMountBinary(t, "-server", restarted.url, "-mountpoint", newPath)
			newFile := openBinaryFile(t, filepath.Join(newPath, "artifact"), os.O_RDONLY, false)
			assertBinaryFileRead(t, newFile, "durable!")
			if err := syscall.Flock(int(newFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatalf("fresh session exclusive flock after restart: %v", err)
			}
			if err := newFile.Close(); err != nil {
				t.Fatal(err)
			}
			if err := oldFile.Close(); err != nil && !retiredBinaryFileError(err) {
				t.Fatalf("close retired descriptor: %v", err)
			}
			stopBinaryMount(t, newMount, newPath, false)
			stopBinaryMount(t, oldMount, oldPath, true)
			assertBinaryLockRead(t, reopened, "artifact", "durable!")
		})
	}
}

func openBinaryFile(t *testing.T, path string, flags int, expectRetirement bool) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) && !(expectRetirement && retiredBinaryFileError(err)) {
			t.Errorf("close binary descriptor: %v", err)
		}
	})
	return f
}

func assertBinaryFileRead(t *testing.T, f *os.File, want string) {
	t.Helper()
	buf := make([]byte, len(want)+1)
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read retained descriptor: %v", err)
	}
	if string(buf[:n]) != want {
		t.Fatalf("retained descriptor = %q, want %q", buf[:n], want)
	}
}

func retiredBinaryFileError(err error) bool {
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ESTALE)
}

func stopBinaryMount(t *testing.T, mount *process, path string, expectRetirement bool) {
	t.Helper()
	mount.interrupt(t)
	if err := mount.wait(t); err != nil {
		output := mount.output()
		if !expectRetirement || (!strings.Contains(output, "stale file handle") && !strings.Contains(output, "input/output error")) {
			t.Fatalf("mount shutdown: %v\n%s", err, output)
		}
		t.Logf("retired mount reported its cleanup failure: %s", output)
	}
	if mounted, err := fusetest.Mounted(path); err != nil || mounted {
		t.Fatalf("mount remained after shutdown: mounted=%t, error=%v", mounted, err)
	}
}
