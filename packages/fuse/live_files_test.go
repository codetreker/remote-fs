//go:build linux

package fuse_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// Each mount owns its HTTP client, metadata replica, file session, and kernel
// inode cache. Direct backing observations cannot accidentally consult either replica.
func serveLiveFiles(t *testing.T, contents map[string]string, allowance int64) (storage.FileStorage, string) {
	t.Helper()
	requireFUSE(t)
	meta, backing := memoryfixture.New(t, "live-files", allowance, locking.DefaultOptions())
	for name, body := range contents {
		if err := backing.Write(t.Context(), name, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	handler, err := httprest.NewHandler(backing, meta)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Errorf("close file HTTP handler: %v", err)
		}
	})
	return backing, server.URL
}

func liveHTTPMounts(t *testing.T, contents map[string]string) (string, string, storage.Storage) {
	t.Helper()
	return liveHTTPMountsWithStorage(t, contents, func(s storage.FileStorage) storage.FileStorage { return s })
}

func liveHTTPMountsWithStorage(t *testing.T, contents map[string]string, decorate func(storage.FileStorage) storage.FileStorage) (string, string, storage.Storage) {
	t.Helper()
	backing, serverURL := serveLiveFiles(t, contents, 0)
	var mounts [2]string
	for i := range mounts {
		client := &http.Client{Timeout: 5 * time.Second}
		remote, err := httprest.Dial(serverURL, client)
		if err != nil {
			t.Fatal(err)
		}
		copy, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
		if err != nil {
			t.Fatal(err)
		}
		replica, err := replicated.New(t.Context(), copy, remote)
		if err != nil {
			if closeErr := copy.Close(); closeErr != nil {
				t.Errorf("close failed replica construction: %v", closeErr)
			}
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := replica.Close(); err != nil {
				t.Errorf("close file replica: %v", err)
			}
			client.CloseIdleConnections()
		})
		mounts[i] = mountStorage(t, decorate(replica), fuse.Options{Logger: testLogger(t)})
	}
	return mounts[0], mounts[1], backing
}

func openLiveFile(t *testing.T, name string, flags int) *os.File {
	t.Helper()
	f, err := os.OpenFile(name, flags, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close live descriptor: %v", err)
		}
	})
	return f
}

func checkLiveDescriptor(t *testing.T, f *os.File, want []byte, inode uint64) {
	t.Helper()
	data := make([]byte, len(want)+1)
	n, err := f.ReadAt(data, 0)
	if n != len(want) || !errors.Is(err, io.EOF) || !bytes.Equal(data[:n], want) {
		t.Fatalf("retained read has %d bytes, %v; want %d matching bytes and EOF", n, err, len(want))
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Sys().(*syscall.Stat_t).Ino; got != inode || info.Size() != int64(len(want)) {
		t.Fatalf("retained fstat inode=%d size=%d, want inode=%d size=%d", got, info.Size(), inode, len(want))
	}
}

func liveInode(t *testing.T, f *os.File) uint64 {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return info.Sys().(*syscall.Stat_t).Ino
}

func writeLiveAt(t *testing.T, f *os.File, data []byte, offset int64) {
	t.Helper()
	if n, err := f.WriteAt(data, offset); err != nil || n != len(data) {
		t.Fatalf("write at %d returned %d, %v; want %d", offset, n, err, len(data))
	}
}

func TestLiveDescriptorReadsOverwriteGrowthAndShrinkAcrossHTTPMounts(t *testing.T) {
	initial := bytes.Repeat([]byte("a"), 3*4096)
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": string(initial)})
	reader := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	writer := openLiveFile(t, filepath.Join(right, "file"), os.O_RDWR)
	inode := liveInode(t, reader)
	checkLiveDescriptor(t, reader, initial, inode)

	for _, body := range [][]byte{
		bytes.Repeat([]byte("b"), len(initial)),
		bytes.Repeat([]byte("c"), 5*4096+17),
	} {
		writeLiveAt(t, writer, body, 0)
		checkLiveDescriptor(t, reader, body, inode)
	}
	if err := writer.Truncate(3); err != nil {
		t.Fatal(err)
	}
	checkLiveDescriptor(t, reader, []byte("ccc"), inode)
	if err := writer.Truncate(0); err != nil {
		t.Fatal(err)
	}
	checkLiveDescriptor(t, reader, nil, inode)
}

func TestLiveDescriptorStatThenReadAfter65536ByteShrink(t *testing.T) {
	initial := bytes.Repeat([]byte("A"), 65536)
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": string(initial)})
	held := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	inode := liveInode(t, held)
	checkLiveDescriptor(t, held, initial, inode)
	if err := os.WriteFile(filepath.Join(right, "file"), []byte("BBB"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := held.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 3 || info.Sys().(*syscall.Stat_t).Ino != inode {
		t.Fatalf("old descriptor fstat has size=%d inode=%d, want size=3 inode=%d", info.Size(), info.Sys().(*syscall.Stat_t).Ino, inode)
	}
	data := make([]byte, 65536)
	n, err := held.ReadAt(data, 0)
	if n != 3 || !errors.Is(err, io.EOF) || string(data[:n]) != "BBB" {
		t.Fatalf("old descriptor after fstat read %q (%d bytes), %v; want BBB and EOF", data[:n], n, err)
	}
}

func TestOldDescriptorCannotRefillFreshDescriptorWithStalePages(t *testing.T) {
	const size = 1 << 20
	initial := bytes.Repeat([]byte("A"), size)
	current := bytes.Repeat([]byte("B"), size)
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": string(initial)})
	name := filepath.Join(left, "file")
	old := openLiveFile(t, name, os.O_RDONLY)
	inode := liveInode(t, old)
	checkLiveDescriptor(t, old, initial, inode)
	if err := os.WriteFile(filepath.Join(right, "file"), current, 0600); err != nil {
		t.Fatal(err)
	}
	// The interval crosses the attribute timestamp boundary in the original
	// page-refill sequence; reads after the fresh open still begin with old.
	timer := time.NewTimer(1200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal("waiting before fresh descriptor open")
	}
	fresh := openLiveFile(t, name, os.O_RDONLY)
	for _, file := range []*os.File{old, fresh} {
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != size || info.Sys().(*syscall.Stat_t).Ino != inode {
			t.Fatalf("descriptor has size=%d inode=%d, want size=%d inode=%d", info.Size(), info.Sys().(*syscall.Stat_t).Ino, size, inode)
		}
	}
	const chunk = 64 << 10
	data := make([]byte, chunk)
	for offset := int64(0); offset < size; offset += chunk {
		for _, descriptor := range []struct {
			name string
			file *os.File
		}{{"old", old}, {"fresh", fresh}} {
			n, err := descriptor.file.ReadAt(data, offset)
			if err != nil || n != chunk || !bytes.Equal(data[:n], current[offset:offset+chunk]) {
				t.Fatalf("%s descriptor at offset %d returned %d bytes, %v; want current B bytes", descriptor.name, offset, n, err)
			}
		}
	}
	for _, file := range []*os.File{old, fresh} {
		if n, err := file.ReadAt(data[:1], size); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("descriptor read beyond current size returned %d, %v", n, err)
		}
	}
}

func TestLiveDescriptorsKeepTheirObjectThroughRemoteNameChanges(t *testing.T) {
	for _, change := range []string{"rename", "unlink", "replace"} {
		t.Run(change, func(t *testing.T) {
			left, right, backing := liveHTTPMounts(t, map[string]string{"file": "original"})
			held := openLiveFile(t, filepath.Join(left, "file"), os.O_RDWR)
			readonly := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
			inode := liveInode(t, held)
			name := filepath.Join(right, "file")
			switch change {
			case "rename":
				if err := os.Rename(name, filepath.Join(right, "moved")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, []byte("replacement"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unlink":
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
			case "replace":
				incoming := filepath.Join(right, "incoming")
				if err := os.WriteFile(incoming, []byte("replacement"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(incoming, name); err != nil {
					t.Fatal(err)
				}
			}
			checkLiveDescriptor(t, held, []byte("original"), inode)
			writeLiveAt(t, held, []byte("OLD"), 0)
			checkLiveDescriptor(t, readonly, []byte("OLDginal"), inode)
			if err := held.Truncate(4); err != nil {
				t.Fatal(err)
			}
			if err := readonly.Chmod(0640); err != nil {
				t.Fatalf("chmod through read-only retained descriptor: %v", err)
			}
			checkLiveDescriptor(t, held, []byte("OLDg"), inode)
			info, err := readonly.Stat()
			if err != nil || info.Mode().Perm() != 0640 {
				t.Fatalf("retained descriptor mode: %v, %v", info, err)
			}
			if change == "unlink" {
				if _, err := backing.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("detached writes recreated the removed name: %v", err)
				}
			} else if body, err := backing.Read(t.Context(), "file"); err != nil || string(body) != "replacement" {
				t.Fatalf("retained writes changed the replacement: %q, %v", body, err)
			}
			if change == "rename" {
				if body, err := backing.Read(t.Context(), "moved"); err != nil || string(body) != "OLDg" {
					t.Fatalf("renamed object contains %q, %v", body, err)
				}
			}
		})
	}
}

func TestLiveWritesAndZeroWriteTruncatePublishBeforeClose(t *testing.T) {
	left, right, backing := liveHTTPMounts(t, map[string]string{"file": "initial"})
	name := filepath.Join(left, "file")
	writer := openLiveFile(t, name, os.O_RDWR)
	reader := openLiveFile(t, name, os.O_RDONLY)
	remoteWriter := openLiveFile(t, filepath.Join(right, "file"), os.O_RDWR)
	inode := liveInode(t, reader)
	writeLiveAt(t, writer, []byte("changed"), 0)
	checkLiveDescriptor(t, reader, []byte("changed"), inode)
	if body, err := backing.Read(t.Context(), "file"); err != nil || string(body) != "changed" {
		t.Fatalf("successful write has not reached the authority: %q, %v", body, err)
	}
	writeLiveAt(t, remoteWriter, []byte("NOW"), 4)
	checkLiveDescriptor(t, reader, []byte("chanNOW"), inode)
	writeLiveAt(t, writer, []byte("X"), 0)
	checkLiveDescriptor(t, reader, []byte("XhanNOW"), inode)
	if body, err := backing.Read(t.Context(), "file"); err != nil || string(body) != "XhanNOW" {
		t.Fatalf("range writes did not preserve other completed patches: %q, %v", body, err)
	}
	truncating := openLiveFile(t, name, os.O_WRONLY|os.O_TRUNC)
	checkLiveDescriptor(t, reader, nil, inode)
	if body, err := backing.Read(t.Context(), "file"); err != nil || len(body) != 0 {
		t.Fatalf("zero-write O_TRUNC has not reached the authority: %q, %v", body, err)
	}
	if err := truncating.Close(); err != nil {
		t.Fatal(err)
	}
	checkLiveDescriptor(t, reader, nil, inode)
}

func TestCreateOpenAcrossHTTPMountsHasOneAtomicResult(t *testing.T) {
	left, right, backing := liveHTTPMounts(t, nil)
	for _, exclusive := range []bool{true, false} {
		t.Run(fmt.Sprintf("exclusive=%v", exclusive), func(t *testing.T) {
			name := fmt.Sprintf("created-%v", exclusive)
			type result struct {
				mode  os.FileMode
				inode uint64
				mount int
				err   error
			}
			const writers = 8
			results := make(chan result, writers)
			start := make(chan struct{})
			var ready sync.WaitGroup
			ready.Add(writers)
			for i := range writers {
				go func() {
					ready.Done()
					<-start
					flags := os.O_CREATE | os.O_RDWR
					if exclusive {
						flags |= os.O_EXCL
					}
					mountpoint := []string{left, right}[i%2]
					f, err := os.OpenFile(filepath.Join(mountpoint, name), flags, []os.FileMode{0600, 0640}[i%2])
					if err != nil {
						results <- result{err: err}
						return
					}
					info, statErr := f.Stat()
					closeErr := f.Close()
					if err := errors.Join(statErr, closeErr); err != nil {
						results <- result{err: err}
						return
					}
					results <- result{mode: info.Mode().Perm(), inode: info.Sys().(*syscall.Stat_t).Ino, mount: i % 2}
				}()
			}
			ready.Wait()
			close(start)
			var successes []result
			for range writers {
				select {
				case outcome := <-results:
					if outcome.err == nil {
						successes = append(successes, outcome)
					} else if !exclusive || !errors.Is(outcome.err, syscall.EEXIST) {
						t.Errorf("concurrent create/open: %v", outcome.err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("concurrent create/open did not finish")
				}
			}
			want := writers
			if exclusive {
				want = 1
			}
			if len(successes) != want {
				t.Fatalf("%d successful create/open calls, want %d", len(successes), want)
			}
			attr, err := backing.Stat(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			for _, outcome := range successes {
				if outcome.mode != attr.Mode.Perm() {
					t.Fatalf("open reported mode %v before the final creation mode %v", outcome.mode, attr.Mode.Perm())
				}
				if current := ino(t, filepath.Join([]string{left, right}[outcome.mount], name)); outcome.inode != current {
					t.Fatalf("open returned inode %d, but its mount resolves the winning file to %d", outcome.inode, current)
				}
			}
		})
	}
}

type observedLifetimeStorage struct {
	storage.FileStorage
	hold     string
	retained storage.File
	renewals atomic.Int32
	closes   atomic.Int32
}

func (s *observedLifetimeStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	if s.hold != "" {
		s.retained, err = session.OpenFile(ctx, s.hold, storage.FileOpenOptions{Read: true, Write: true})
		if err != nil {
			return nil, errors.Join(err, session.Close(ctx))
		}
	}
	return &observedLifetimeSession{FileSession: session, owner: s}, nil
}

type observedLifetimeSession struct {
	storage.FileSession
	owner *observedLifetimeStorage
}

func (s *observedLifetimeSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := s.FileSession.Renew(ctx)
	if err == nil {
		s.owner.renewals.Add(1)
	}
	return status, err
}

func (s *observedLifetimeSession) Close(ctx context.Context) error {
	err := s.FileSession.Close(ctx)
	if err == nil {
		s.owner.closes.Add(1)
	}
	return err
}

func observedLifetimeMount(t *testing.T, url, hold string, lease time.Duration) (*fuse.Mount, string, *observedLifetimeStorage) {
	t.Helper()
	remote, err := httprest.Dial(url, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedLifetimeStorage{FileStorage: remote, hold: hold}
	options := storage.DefaultFileSessionOptions()
	options.Lease = lease
	point := t.TempDir()
	mount, err := fuse.New(point, observed, fuse.Options{Logger: testLogger(t), FileSession: &options})
	if mount != nil {
		t.Cleanup(func() {
			select {
			case <-mount.Done():
				if err := mount.Wait(); err != nil {
					t.Errorf("wait for retired mount: %v", err)
				}
			default:
				unmount(t, mount, point)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return mount, point, observed
}

func TestBusyUnmountPreservesRenewalAndAdvisoryContinuity(t *testing.T) {
	_, url := serveLiveFiles(t, map[string]string{"file": "content"}, 0)
	const lease = 600 * time.Millisecond
	mount, point, observed := observedLifetimeMount(t, url, "", lease)
	remote, err := httprest.Dial(url, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	other := mountStorage(t, remote, fuse.Options{Logger: testLogger(t)})
	held := openLiveFile(t, filepath.Join(point, "file"), os.O_RDWR)
	contender := openLiveFile(t, filepath.Join(other, "file"), os.O_RDONLY)
	inode := liveInode(t, held)
	checkFlock(t, held, unix.LOCK_EX|unix.LOCK_NB, 0)
	if err := mount.Unmount(); err == nil {
		t.Fatal("unmount succeeded while a descriptor keeps the mount busy")
	}
	select {
	case <-mount.Done():
		t.Fatal("failed unmount retired the file session")
	default:
	}
	timer := time.NewTimer(2 * lease)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal("waiting past the initial file lease")
	}
	if observed.renewals.Load() == 0 || observed.closes.Load() != 0 {
		t.Fatalf("busy mount renewals=%d closes=%d", observed.renewals.Load(), observed.closes.Load())
	}
	checkLiveDescriptor(t, held, []byte("content"), inode)
	writeLiveAt(t, held, []byte("current"), 0)
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mount.Unmount(); err != nil {
		t.Fatalf("unmount after closing descriptor: %v", err)
	}
	if err := mount.Wait(); err != nil {
		t.Fatal(err)
	}
	if observed.closes.Load() != 1 {
		t.Fatalf("kernel teardown closed the session %d times", observed.closes.Load())
	}
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, 0)
}

// This reference is enrolled in the mount's real session but never handed to the
// kernel, so no per-file RELEASE can reclaim it during external kernel teardown.
func TestExternalKernelTeardownRetiresReferencesWithoutRelease(t *testing.T) {
	const contents = "retained without a kernel handle"
	backing, url := serveLiveFiles(t, map[string]string{"orphan": contents}, 64<<10)
	mount, point, observed := observedLifetimeMount(t, url, "orphan", 30*time.Second)
	if observed.retained == nil {
		t.Fatal("mount session did not retain the unreturned reference")
	}
	if err := backing.Remove(t.Context(), "orphan"); err != nil {
		t.Fatal(err)
	}
	space, err := backing.Space(t.Context())
	if err != nil || space.Used != int64(len(contents)) {
		t.Fatalf("detached reference charge before teardown: %+v, %v", space, err)
	}
	bin, err := exec.LookPath("fusermount3")
	if err != nil {
		bin, err = exec.LookPath("fusermount")
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, bin, "-u", point).CombinedOutput(); err != nil {
		t.Fatalf("external unmount: %v: %s", err, output)
	}
	select {
	case <-mount.Done():
	case <-ctx.Done():
		t.Fatal("external kernel teardown did not finish session cleanup")
	}
	if err := mount.Wait(); err != nil {
		t.Fatal(err)
	}
	if observed.closes.Load() != 1 {
		t.Fatalf("external teardown closed session %d times", observed.closes.Load())
	}
	if _, err := observed.retained.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EBADF) {
		t.Fatalf("reference survived session retirement: %v", err)
	}
	space, err = backing.Space(t.Context())
	if err != nil || space.Used != 0 {
		t.Fatalf("retired orphan remains charged: %+v, %v", space, err)
	}
	if _, err := backing.Stat(t.Context(), "orphan"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("retirement recreated orphan name: %v", err)
	}
}
