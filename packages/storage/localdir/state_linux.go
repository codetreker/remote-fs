package localdir

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	bindingAttribute = "user.remote-fs.binding"
	witnessAttribute = "user.remote-fs.lease"
	stateFilename    = "lease-state"
	stagingDirectory = "staging"
	stateRecordLimit = 16384
)

type stateSyscalls struct {
	write    func(int, []byte) (int, error)
	fsync    func(int) error
	close    func(int) error
	rename   func(int, string, int, string) error
	setxattr func(int, string, []byte, int) error
}

func defaultStateSyscalls() stateSyscalls {
	return stateSyscalls{unix.Write, unix.Fsync, unix.Close, unix.Renameat, unix.Fsetxattr}
}

type directoryState struct {
	// Lifecycle readers pin the native handles without serializing ordinary
	// health checks with durable lease preparation. Closing drains these readers.
	lifetime                   sync.RWMutex
	raiseMu                    sync.Mutex
	mu                         sync.Mutex
	rootFD, stateFD, stagingFD int
	ownerFD                    int
	rootPath, statePath        string
	mount                      uint64
	bound                      bool
	start                      time.Time
	limits                     Limits
	ops                        stateSyscalls
	record                     leaseRecord
	binding                    directoryBinding
	failure                    error
	closeErr                   error
	closed                     bool
	closeAttempted             bool
	closeDone                  chan struct{}
}

func (s *directoryState) RecoveryStart() time.Time { return s.start }

func (s *directoryState) stateErrorLocked() error {
	if s.closed || s.closeAttempted {
		return errors.Join(os.ErrClosed, s.closeErr)
	}
	return s.failure
}

func (s *directoryState) stateError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateErrorLocked()
}

func (s *directoryState) fence(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failure = errors.Join(s.failure, err)
	return s.failure
}

func (s *directoryState) health() error {
	if err := s.stateError(); err != nil {
		return err
	}
	s.lifetime.RLock()
	defer s.lifetime.RUnlock()
	return s.healthActive()
}

// The caller retains lifecycle ownership while configured paths are verified.
func (s *directoryState) healthActive() error {
	if err := s.stateError(); err != nil {
		return err
	}
	if err := verifyDirectoryPath(s.rootPath, s.rootFD, s.mount, false); err != nil {
		return s.fence(err)
	}
	if s.stateFD >= 0 {
		if err := verifyDirectoryPath(s.statePath, s.stateFD, s.mount, true); err != nil {
			return s.fence(err)
		}
	}
	return s.stateError()
}

// Close retains the namespace ownership description if any earlier close is
// uncertain. A descriptor passed to close is consumed even when close fails;
// repeating Close never risks closing a descriptor reused by another goroutine.
func (s *directoryState) Close() error {
	s.mu.Lock()
	if s.closeAttempted {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closeErr
	}
	s.closeAttempted = true
	s.closeDone = make(chan struct{})
	s.mu.Unlock()

	s.lifetime.Lock()
	defer s.lifetime.Unlock()
	s.mu.Lock()
	closeErr := s.closeErr
	s.mu.Unlock()
	for _, fd := range []*int{&s.stagingFD, &s.stateFD, &s.rootFD} {
		if *fd < 0 {
			continue
		}
		value := *fd
		*fd = -1
		if err := s.ops.close(value); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close local directory handle: %w", err))
		}
	}
	if closeErr == nil && s.ownerFD >= 0 {
		fd := s.ownerFD
		s.ownerFD = -1
		closeErr = s.ops.close(fd)
	}
	s.mu.Lock()
	s.closeErr = closeErr
	s.closed = true
	close(s.closeDone)
	s.mu.Unlock()
	return closeErr
}

func stateFailure(message string) error { return fmt.Errorf("%s: %w", message, syscall.EIO) }

func canonicalDirectory(path string, limit int) (string, error) {
	if path == "" {
		return "", fmt.Errorf("directory path is empty: %w", syscall.EINVAL)
	}
	result, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if len(result) > limit {
		return "", fmt.Errorf("directory path exceeds configured limit: %w", syscall.ENAMETOOLONG)
	}
	return result, nil
}

func openControlledDirectory(path string, private bool) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" {
			continue
		}
		var parent, child unix.Stat_t
		if err := unix.Fstat(fd, &parent); err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		err = unix.Fstat(next, &child)
		if err == nil {
			owner := uint32(unix.Geteuid())
			if parent.Uid != owner && parent.Uid != 0 || parent.Mode&0o022 != 0 &&
				!(parent.Mode&unix.S_ISVTX != 0 && (child.Uid == owner || child.Uid == 0)) {
				err = fmt.Errorf("directory ancestor permits replacement by another user: %w", syscall.EACCES)
			}
		}
		err = errors.Join(err, unix.Close(fd))
		if err != nil {
			return -1, errors.Join(err, unix.Close(next))
		}
		fd = next
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	if err == nil && (stat.Uid != uint32(unix.Geteuid()) || stat.Mode&0o022 != 0 || private && stat.Mode&0o777 != 0o700) {
		err = fmt.Errorf("directory must be deployment-owned and protected; private state requires mode 0700: %w", syscall.EACCES)
	}
	if err != nil {
		return -1, errors.Join(err, unix.Close(fd))
	}
	return fd, nil
}

func directoryMount(fd int) (uint64, error) {
	var stat unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT, unix.STATX_MNT_ID, &stat); err != nil {
		return 0, errors.Join(fmt.Errorf("read directory mount identity: %w", err), syscall.EOPNOTSUPP)
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return 0, fmt.Errorf("mount identity is unavailable: %w", syscall.EOPNOTSUPP)
	}
	return stat.Mnt_id, nil
}

func verifyDirectoryPath(path string, fd int, mount uint64, private bool) error {
	current, err := openControlledDirectory(path, private)
	if err != nil {
		return errors.Join(stateFailure("configured directory no longer resolves safely"), err)
	}
	var a, b unix.Stat_t
	first := unix.Fstat(current, &a)
	second := unix.Fstat(fd, &b)
	currentMount, mountErr := directoryMount(current)
	closeErr := unix.Close(current)
	if err := errors.Join(first, second, mountErr, closeErr); err != nil {
		return err
	}
	if a.Dev != b.Dev || a.Ino != b.Ino || currentMount != mount {
		return stateFailure("configured directory no longer names the owned directory")
	}
	return nil
}

func openDirectoryState(ctx context.Context, root string, limits Limits, now func() time.Time) (*directoryState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limits, err := limits.Effective()
	if err != nil {
		return nil, err
	}
	root, err = canonicalDirectory(root, limits.MaxPathBytes)
	if err != nil {
		return nil, err
	}
	fd, err := openControlledDirectory(root, false)
	if err != nil {
		return nil, fmt.Errorf("open namespace root: %w", err)
	}
	s := &directoryState{rootFD: fd, stateFD: -1, stagingFD: -1, ownerFD: -1,
		rootPath: root, limits: limits, ops: defaultStateSyscalls()}
	fail := func(err error) (*directoryState, error) { return nil, errors.Join(err, s.Close()) }
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("acquire namespace ownership: %w", err))
	}
	s.start = now()
	s.ownerFD, err = unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	s.mount, err = directoryMount(fd)
	if err != nil {
		return fail(err)
	}
	if err := s.health(); err != nil {
		return fail(err)
	}
	return s, nil
}

func openRawState(dir string, limits Limits) (*directoryState, error) {
	s, err := openDirectoryState(context.Background(), dir, limits, time.Now)
	if err != nil {
		return nil, err
	}
	for _, attribute := range []string{bindingAttribute, witnessAttribute} {
		_, err := readStateAttribute(s.rootFD, attribute)
		if !errors.Is(err, unix.ENODATA) {
			if err == nil {
				err = stateFailure("namespace has persistent lease evidence; use Open with its StateRoot")
			}
			return nil, errors.Join(err, s.Close())
		}
	}
	return s, nil
}
