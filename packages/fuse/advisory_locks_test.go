//go:build linux

package fuse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage"
)

const advisoryChildMode = "REMOTE_FS_ADVISORY_CHILD"

type advisoryCommand struct {
	Op, File, Path string
	Flags          int
	Lock           unix.Flock_t
}

type advisoryReply struct {
	Started bool
	Errno   syscall.Errno
	Failure string
	Lock    unix.Flock_t
}

type advisoryProcess struct {
	input  *json.Encoder
	output *json.Decoder
	log    *safeBuffer
}

// A separate file table supplies real traditional-POSIX ownership. ExtraFiles
// additionally preserves an inherited open description for fork/dup flock tests.
func newAdvisoryProcess(t *testing.T, inherited *os.File) *advisoryProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	requests, send, err := os.Pipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	receive, replies, err := os.Pipe()
	if err != nil {
		requests.Close()
		send.Close()
		cancel()
		t.Fatal(err)
	}
	output := &safeBuffer{}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdvisoryFlockSharedExclusiveConversionAndBlockingAcrossHTTPMounts$", "-test.timeout=25s")
	child.Env = append(os.Environ(), advisoryChildMode+"=1")
	child.ExtraFiles = []*os.File{requests, replies}
	if inherited != nil {
		child.ExtraFiles = append(child.ExtraFiles, inherited)
		child.Env = append(child.Env, "REMOTE_FS_ADVISORY_INHERITED=1")
	}
	child.Stdout, child.Stderr = output, output
	if err := child.Start(); err != nil {
		requests.Close()
		send.Close()
		receive.Close()
		replies.Close()
		cancel()
		t.Fatal(err)
	}
	requests.Close()
	replies.Close()
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	t.Cleanup(func() {
		send.Close()
		cancel()
		receive.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("advisory child did not exit after cancellation: %s", output.String())
		}
	})
	return &advisoryProcess{input: json.NewEncoder(send), output: json.NewDecoder(receive), log: output}
}

func (p *advisoryProcess) begin(t *testing.T, command advisoryCommand) <-chan advisoryReply {
	t.Helper()
	if err := p.input.Encode(command); err != nil {
		t.Fatalf("send advisory command: %v\n%s", err, p.log.String())
	}
	var started advisoryReply
	if err := p.output.Decode(&started); err != nil || !started.Started {
		t.Fatalf("advisory child did not start command: %+v, %v\n%s", started, err, p.log.String())
	}
	result := make(chan advisoryReply, 1)
	go func() {
		var reply advisoryReply
		if err := p.output.Decode(&reply); err != nil {
			reply.Failure = fmt.Sprintf("read advisory reply: %v", err)
		}
		result <- reply
	}()
	return result
}

func advisoryResult(t *testing.T, result <-chan advisoryReply) advisoryReply {
	t.Helper()
	select {
	case reply := <-result:
		if reply.Failure != "" {
			t.Fatal(reply.Failure)
		}
		return reply
	case <-time.After(5 * time.Second):
		t.Fatal("advisory command did not finish")
		return advisoryReply{}
	}
}

func (p *advisoryProcess) command(t *testing.T, command advisoryCommand, want syscall.Errno) advisoryReply {
	t.Helper()
	reply := advisoryResult(t, p.begin(t, command))
	if reply.Errno != want {
		t.Fatalf("advisory command %+v returned %v, want %v", command, reply.Errno, want)
	}
	return reply
}

func runAdvisoryLockChild(t *testing.T) {
	requests := os.NewFile(3, "advisory requests")
	replies := os.NewFile(4, "advisory replies")
	defer requests.Close()
	defer replies.Close()
	files := make(map[string]*os.File)
	if os.Getenv("REMOTE_FS_ADVISORY_INHERITED") == "1" {
		files["inherited"] = os.NewFile(5, "inherited description")
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	input, output := json.NewDecoder(requests), json.NewEncoder(replies)
	for {
		var command advisoryCommand
		if err := input.Decode(&command); errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(advisoryReply{Started: true}); err != nil {
			t.Fatal(err)
		}
		reply := advisoryReply{Lock: command.Lock}
		var err error
		switch command.Op {
		case "open":
			var file *os.File
			file, err = os.OpenFile(command.Path, command.Flags, 0600)
			if err == nil {
				files[command.File] = file
			}
		case "flock":
			err = retryAdvisoryInterruption(func() error {
				return unix.Flock(int(files[command.File].Fd()), command.Flags)
			})
		case "fcntl":
			err = retryAdvisoryInterruption(func() error {
				reply.Lock = command.Lock
				return unix.FcntlFlock(files[command.File].Fd(), command.Flags, &reply.Lock)
			})
		case "close":
			err = files[command.File].Close()
			delete(files, command.File)
		default:
			reply.Failure = "unknown advisory child command"
		}
		if err != nil && !errors.As(err, &reply.Errno) {
			reply.Failure = err.Error()
		}
		if err := output.Encode(reply); err != nil {
			t.Fatal(err)
		}
	}
}

func checkFlock(t *testing.T, f *os.File, flags int, want syscall.Errno) {
	t.Helper()
	err := retryAdvisoryInterruption(func() error { return unix.Flock(int(f.Fd()), flags) })
	if want == 0 && err != nil || want != 0 && !errors.Is(err, want) {
		t.Fatalf("flock %d: %v, want %v", flags, err, want)
	}
}

// Unrelated runtime signals can interrupt these kernel calls. Repeating only
// known EINTR with identical arguments keeps the lock scenario independent of
// signal timing; an unknown acquisition outcome remains EIO and fails the test.
func retryAdvisoryInterruption(call func() error) error {
	var err error
	for range 8 {
		if err = call(); !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
	return err
}

func posixRange(kind int16, start, length int64) unix.Flock_t {
	return unix.Flock_t{Type: kind, Whence: int16(io.SeekStart), Start: start, Len: length}
}

func checkPOSIX(t *testing.T, f *os.File, command int, lock unix.Flock_t, want syscall.Errno) unix.Flock_t {
	t.Helper()
	requested := lock
	err := retryAdvisoryInterruption(func() error {
		lock = requested
		return unix.FcntlFlock(f.Fd(), command, &lock)
	})
	if want == 0 && err != nil || want != 0 && !errors.Is(err, want) {
		t.Fatalf("fcntl %d range %+v: %v, want %v", command, lock, err, want)
	}
	return lock
}

func TestAdvisoryFlockSharedExclusiveConversionAndBlockingAcrossHTTPMounts(t *testing.T) {
	if os.Getenv(advisoryChildMode) == "1" {
		runAdvisoryLockChild(t)
		return
	}
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": "content"})
	a := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	b := openLiveFile(t, filepath.Join(right, "file"), os.O_RDONLY)
	checkFlock(t, a, unix.LOCK_SH|unix.LOCK_NB, 0)
	checkFlock(t, b, unix.LOCK_SH|unix.LOCK_NB, 0)
	checkFlock(t, a, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	checkFlock(t, b, unix.LOCK_EX|unix.LOCK_NB, 0)
	checkFlock(t, b, unix.LOCK_UN, 0)
	checkFlock(t, a, unix.LOCK_EX|unix.LOCK_NB, 0)
	checkFlock(t, a, unix.LOCK_EX|unix.LOCK_NB, 0)
	checkFlock(t, b, unix.LOCK_SH|unix.LOCK_NB, syscall.EWOULDBLOCK)
	checkFlock(t, b, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)

	child := newAdvisoryProcess(t, nil)
	child.command(t, advisoryCommand{Op: "open", File: "file", Path: filepath.Join(right, "file"), Flags: os.O_RDONLY}, 0)
	waiting := child.begin(t, advisoryCommand{Op: "flock", File: "file", Flags: unix.LOCK_EX})
	select {
	case reply := <-waiting:
		t.Fatalf("blocking flock returned while another description holds EX: %+v", reply)
	case <-time.After(50 * time.Millisecond):
	}
	checkFlock(t, a, unix.LOCK_UN, 0)
	if reply := advisoryResult(t, waiting); reply.Errno != 0 {
		t.Fatalf("blocking flock after unlock: %v", reply.Errno)
	}
	child.command(t, advisoryCommand{Op: "flock", File: "file", Flags: unix.LOCK_UN}, 0)
	checkFlock(t, b, unix.LOCK_EX|unix.LOCK_NB, 0)
}

// FUSE sends the final release in the background after its synchronous flush.
// Holding actual cleanup distinguishes close acknowledgement from lock handoff.
// https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/file.c#L102-L123
type flockReleaseGate struct {
	pending chan struct{}
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *flockReleaseGate) unblock() { g.once.Do(func() { close(g.release) }) }

type observedFlockStorage struct {
	storage.FileStorage
	gate *flockReleaseGate
}

func (s *observedFlockStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &observedFlockSession{FileSession: session, gate: s.gate}, nil
}

type observedFlockSession struct {
	storage.FileSession
	gate *flockReleaseGate
}

func (s *observedFlockSession) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return &observedFlockFile{File: file, gate: s.gate}, nil
}

func (s *observedFlockSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenNode(ctx, id, options)
	if err != nil {
		return nil, err
	}
	return &observedFlockFile{File: file, gate: s.gate}, nil
}

type observedFlockFile struct {
	storage.File
	gate *flockReleaseGate
}

func (f *observedFlockFile) SetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID) (storage.LockAttempt, error) {
	attempt, err := f.File.SetLock(ctx, owner, lock, request)
	if err == nil && lock.Family == storage.Flock && lock.Wait && attempt.State == storage.LockPending {
		select {
		case f.gate.pending <- struct{}{}:
		default:
		}
	}
	return attempt, err
}

func (f *observedFlockFile) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	if family == storage.Flock {
		select {
		case f.gate.entered <- struct{}{}:
		default:
		}
		select {
		case <-f.gate.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.File.DropLocks(ctx, owner, family)
}

func TestAdvisoryFlockLastDuplicateAcrossForkControlsRelease(t *testing.T) {
	gate := &flockReleaseGate{pending: make(chan struct{}, 1), entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer gate.unblock()
	left, right, _ := liveHTTPMountsWithStorage(t, map[string]string{"file": "content"}, func(s storage.FileStorage) storage.FileStorage {
		return &observedFlockStorage{FileStorage: s, gate: gate}
	})
	original := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	contender := openLiveFile(t, filepath.Join(right, "file"), os.O_RDONLY)
	checkFlock(t, original, unix.LOCK_EX|unix.LOCK_NB, 0)
	// ExtraFiles supplies the sole inherited duplicate; its original descriptor
	// number must close across exec so the child can perform the final close.
	fd, err := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := os.NewFile(uintptr(fd), "duplicate flock description")
	t.Cleanup(func() { duplicate.Close() })
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	child := newAdvisoryProcess(t, duplicate)
	child.command(t, advisoryCommand{Op: "flock", File: "inherited", Flags: unix.LOCK_EX | unix.LOCK_NB}, 0)
	if err := duplicate.Close(); err != nil {
		t.Fatal(err)
	}
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	waiter := newAdvisoryProcess(t, nil)
	waiter.command(t, advisoryCommand{Op: "open", File: "file", Path: filepath.Join(right, "file"), Flags: os.O_RDONLY}, 0)
	waiting := waiter.begin(t, advisoryCommand{Op: "flock", File: "file", Flags: unix.LOCK_EX})
	select {
	case <-gate.pending:
	case reply := <-waiting:
		t.Fatalf("blocking flock completed while the inherited description still holds EX: %+v", reply)
	case <-time.After(5 * time.Second):
		t.Fatal("blocking flock did not enroll at the authority")
	}
	select {
	case <-gate.entered:
		t.Fatal("flock cleanup started while the inherited description remained open")
	default:
	}
	child.command(t, advisoryCommand{Op: "close", File: "inherited"}, 0)
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("last description close did not reach flock cleanup")
	}
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	select {
	case reply := <-waiting:
		t.Fatalf("flock waiter completed before retained cleanup: %+v", reply)
	default:
	}
	gate.unblock()
	if reply := advisoryResult(t, waiting); reply.Errno != 0 {
		t.Fatalf("flock waiter after final release cleanup: %v", reply.Errno)
	}
	waiter.command(t, advisoryCommand{Op: "flock", File: "file", Flags: unix.LOCK_UN}, 0)
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, 0)
}

func TestAdvisoryPOSIXRangesQuerySplitAndPreserveFailedConversion(t *testing.T) {
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": "content"})
	a := openLiveFile(t, filepath.Join(left, "file"), os.O_RDWR)
	b := openLiveFile(t, filepath.Join(right, "file"), os.O_RDWR)
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 100), 0)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_RDLCK, 100, 100), 0)
	conflict := checkPOSIX(t, b, unix.F_GETLK, posixRange(unix.F_WRLCK, 0, 200), 0)
	if conflict.Type != unix.F_WRLCK || conflict.Start != 0 || conflict.Len != 100 {
		t.Fatalf("GETLK reported %+v, want write conflict [0,99]", conflict)
	}
	if own := checkPOSIX(t, a, unix.F_GETLK, posixRange(unix.F_WRLCK, 0, 100), 0); own.Type != unix.F_UNLCK {
		t.Fatalf("GETLK conflicts with its own process: %+v", own)
	}
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_UNLCK, 40, 20), 0)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_WRLCK, 40, 20), 0)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 40), syscall.EAGAIN)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_WRLCK, 60, 40), syscall.EAGAIN)
	for _, start := range []int64{0, 60} {
		conflict := checkPOSIX(t, b, unix.F_GETLK, posixRange(unix.F_WRLCK, start, 40), 0)
		if conflict.Type != unix.F_WRLCK || conflict.Start != start || conflict.Len != 40 {
			t.Fatalf("split lock at %d reported %+v", start, conflict)
		}
	}
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_UNLCK, 0, 0), 0)
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_RDLCK, 0, 100), 0)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_RDLCK, 0, 100), 0)
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 100), syscall.EAGAIN)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 100), syscall.EAGAIN)
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_UNLCK, 0, 0), 0)
	checkPOSIX(t, b, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
	checkPOSIX(t, a, unix.F_SETLK, posixRange(unix.F_WRLCK, 1<<20, 1), syscall.EAGAIN)
}

func TestAdvisoryPOSIXAccessModesAreCheckedByTheKernel(t *testing.T) {
	left, _, _ := liveHTTPMounts(t, map[string]string{"file": "content"})
	reader := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	writer := openLiveFile(t, filepath.Join(left, "file"), os.O_WRONLY)
	checkPOSIX(t, reader, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), syscall.EBADF)
	checkPOSIX(t, writer, unix.F_SETLK, posixRange(unix.F_RDLCK, 0, 0), syscall.EBADF)
	checkPOSIX(t, reader, unix.F_SETLK, posixRange(unix.F_RDLCK, 0, 0), 0)
	checkPOSIX(t, reader, unix.F_SETLK, posixRange(unix.F_UNLCK, 0, 0), 0)
	checkPOSIX(t, writer, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
}

func TestAdvisoryPOSIXForkAndAnyDescriptorCloseRespectProcessAndFile(t *testing.T) {
	left, _, _ := liveHTTPMounts(t, map[string]string{"file": "content", "other": "other"})
	file := openLiveFile(t, filepath.Join(left, "file"), os.O_RDWR)
	other := openLiveFile(t, filepath.Join(left, "other"), os.O_RDWR)
	spareFile := openLiveFile(t, filepath.Join(left, "file"), os.O_RDONLY)
	spareOther := openLiveFile(t, filepath.Join(left, "other"), os.O_RDONLY)
	checkPOSIX(t, file, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
	checkPOSIX(t, other, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
	child := newAdvisoryProcess(t, file)
	child.command(t, advisoryCommand{Op: "open", File: "file", Path: filepath.Join(left, "file"), Flags: os.O_RDWR}, 0)
	child.command(t, advisoryCommand{Op: "open", File: "other", Path: filepath.Join(left, "other"), Flags: os.O_RDWR}, 0)
	lock := advisoryCommand{Op: "fcntl", File: "inherited", Flags: unix.F_SETLK, Lock: posixRange(unix.F_WRLCK, 0, 0)}
	child.command(t, lock, syscall.EAGAIN)
	child.command(t, advisoryCommand{Op: "close", File: "inherited"}, 0)
	lock.File = "file"
	child.command(t, lock, syscall.EAGAIN)
	if err := spareFile.Close(); err != nil {
		t.Fatal(err)
	}
	child.command(t, lock, 0)
	lock.File = "other"
	child.command(t, lock, syscall.EAGAIN)
	if err := spareOther.Close(); err != nil {
		t.Fatal(err)
	}
	child.command(t, lock, 0)
}

func TestAdvisoryPOSIXBlockingWaitEndsAfterExplicitUnlock(t *testing.T) {
	left, right, _ := liveHTTPMounts(t, map[string]string{"file": "content"})
	held := openLiveFile(t, filepath.Join(left, "file"), os.O_RDWR)
	checkPOSIX(t, held, unix.F_SETLK, posixRange(unix.F_WRLCK, 7, 11), 0)
	child := newAdvisoryProcess(t, nil)
	child.command(t, advisoryCommand{Op: "open", File: "file", Path: filepath.Join(right, "file"), Flags: os.O_RDWR}, 0)
	waiting := child.begin(t, advisoryCommand{Op: "fcntl", File: "file", Flags: unix.F_SETLKW, Lock: posixRange(unix.F_WRLCK, 7, 11)})
	select {
	case reply := <-waiting:
		t.Fatalf("blocking POSIX lock returned before unlock: %+v", reply)
	case <-time.After(50 * time.Millisecond):
	}
	checkPOSIX(t, held, unix.F_SETLK, posixRange(unix.F_UNLCK, 7, 11), 0)
	if reply := advisoryResult(t, waiting); reply.Errno != 0 {
		t.Fatalf("blocking POSIX lock after unlock: %v", reply.Errno)
	}
}

func TestAdvisoryFamiliesRemainIndependentThroughUncooperativeIOAndUnlink(t *testing.T) {
	left, right, backing := liveHTTPMounts(t, map[string]string{"file": "content"})
	flock := openLiveFile(t, filepath.Join(left, "file"), os.O_RDWR)
	posix := openLiveFile(t, filepath.Join(right, "file"), os.O_RDWR)
	contender := openLiveFile(t, filepath.Join(right, "file"), os.O_RDWR)
	inode := liveInode(t, flock)
	checkFlock(t, flock, unix.LOCK_EX|unix.LOCK_NB, 0)
	checkPOSIX(t, posix, unix.F_SETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
	writeLiveAt(t, contender, []byte("changed"), 0)
	checkLiveDescriptor(t, flock, []byte("changed"), inode)
	if err := os.Rename(filepath.Join(right, "file"), filepath.Join(right, "moved")); err != nil {
		t.Fatalf("advisory locks blocked rename: %v", err)
	}
	if err := os.Remove(filepath.Join(right, "moved")); err != nil {
		t.Fatalf("advisory locks blocked unlink: %v", err)
	}
	writeLiveAt(t, contender, []byte("live"), 0)
	checkLiveDescriptor(t, flock, []byte("liveged"), inode)
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, syscall.EWOULDBLOCK)
	conflict := checkPOSIX(t, flock, unix.F_GETLK, posixRange(unix.F_WRLCK, 0, 0), 0)
	if conflict.Type != unix.F_WRLCK {
		t.Fatalf("unlink lost the POSIX holder: %+v", conflict)
	}
	checkFlock(t, flock, unix.LOCK_UN, 0)
	checkFlock(t, contender, unix.LOCK_EX|unix.LOCK_NB, 0)
	for _, name := range []string{"file", "moved"} {
		if _, err := backing.Stat(t.Context(), name); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("retained advisory I/O recreated %q: %v", name, err)
		}
	}
}
