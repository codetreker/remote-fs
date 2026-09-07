package localdir

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type operationSyscalls struct {
	write  func(int, []byte) (int, error)
	sync   func(int) error
	close  func(int) error
	rename func(int, string, int, string) error
	chmod  func(int, uint32) error
	times  func(int, string, []unix.Timespec, int) error
	unlink func(int, string, int) error
	link   func(int, string, int, string, int) error
}

// Procfs addresses the pinned inode without read permission or a dependency on
// fchmodat2. Final symlinks are refused before mode changes reach this helper.
func defaultOperationSyscalls() operationSyscalls {
	return operationSyscalls{
		write: unix.Write, sync: unix.Fsync, close: unix.Close, rename: unix.Renameat,
		chmod: func(fd int, mode uint32) error { return unix.Chmod("/proc/self/fd/"+strconv.Itoa(fd), mode) },
		times: unix.UtimesNanoAt, unlink: unix.Unlinkat, link: unix.Linkat,
	}
}

func (s *Storage) health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.resources.Lock()
	failure := s.failure
	s.resources.Unlock()
	if failure != nil {
		return locking.Wrap(locking.Unavailable, "local directory is fenced", failure)
	}
	if s.authority != nil {
		if err := s.authority.Check(ctx); err != nil {
			return err
		}
	}
	if err := s.state.health(); err != nil {
		s.fence(err)
		return err
	}
	return nil
}

func (s *Storage) fence(err error) {
	if err == nil {
		return
	}
	s.resources.Lock()
	first := s.failure == nil
	if first {
		s.failure = err
	}
	s.resources.Unlock()
	if first && s.authority != nil {
		s.authority.Fence(err)
	}
}

func (s *Storage) closeFD(fd int) error {
	if fd < 0 {
		return nil
	}
	return s.recordClose(s.ops.close(fd))
}

func (s *Storage) recordClose(err error) error {
	if err != nil {
		s.resources.Lock()
		s.handleFailure = errors.Join(s.handleFailure, err)
		s.resources.Unlock()
		s.fence(err)
	}
	return err
}

func (s *Storage) clean(name string) (string, error) {
	if len(name) > s.state.limits.MaxPathBytes {
		return "", syscall.ENAMETOOLONG
	}
	return storage.CleanPath(name)
}

func (s *Storage) openPath(name string, flags int) (int, error) {
	if name == "" {
		name = "."
	}
	return unix.Openat2(s.state.rootFD, name, &unix.OpenHow{
		Flags:   uint64(flags | unix.O_CLOEXEC),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
}

func (s *Storage) openParent(name string) (int, string, error) {
	parent, base := path.Split(name)
	if parent == "" {
		parent = "."
	}
	fd, err := s.openPath(parent, unix.O_RDONLY|unix.O_DIRECTORY)
	return fd, base, err
}

type nativeInfo struct{ stat unix.Stat_t }

func (i nativeInfo) Size() int64        { return i.stat.Size }
func (i nativeInfo) ModTime() time.Time { return time.Unix(i.stat.Mtim.Sec, i.stat.Mtim.Nsec) }
func (i nativeInfo) IsDir() bool        { return i.Mode().IsDir() }
func (i nativeInfo) Mode() fs.FileMode {
	mode := fs.FileMode(i.stat.Mode & 0777)
	if i.stat.Mode&unix.S_ISUID != 0 {
		mode |= fs.ModeSetuid
	}
	if i.stat.Mode&unix.S_ISGID != 0 {
		mode |= fs.ModeSetgid
	}
	if i.stat.Mode&unix.S_ISVTX != 0 {
		mode |= fs.ModeSticky
	}
	switch i.stat.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		mode |= fs.ModeDir
	case unix.S_IFLNK:
		mode |= fs.ModeSymlink
	case unix.S_IFIFO:
		mode |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		mode |= fs.ModeSocket
	case unix.S_IFBLK:
		mode |= fs.ModeDevice
	case unix.S_IFCHR:
		mode |= fs.ModeDevice | fs.ModeCharDevice
	}
	return mode
}
func fileInfo(fd int) (*nativeInfo, error) {
	var value unix.Stat_t
	if err := unix.Fstat(fd, &value); err != nil {
		return nil, err
	}
	return &nativeInfo{value}, nil
}

func (s *Storage) inspect(name string) (int, *nativeInfo, error) {
	fd, err := s.openPath(name, unix.O_PATH|unix.O_NOFOLLOW)
	if err != nil {
		return -1, nil, err
	}
	info, err := fileInfo(fd)
	if err != nil {
		return -1, nil, errors.Join(err, s.closeFD(fd))
	}
	return fd, info, nil
}

func regularSize(info *nativeInfo) int64 {
	if info != nil && info.Mode().IsRegular() {
		return info.Size()
	}
	return 0
}

func modeBits(mode fs.FileMode) uint32 {
	result := uint32(mode.Perm())
	if mode&fs.ModeSetuid != 0 {
		result |= unix.S_ISUID
	}
	if mode&fs.ModeSetgid != 0 {
		result |= unix.S_ISGID
	}
	if mode&fs.ModeSticky != 0 {
		result |= unix.S_ISVTX
	}
	return result
}

func nativeUnknown(err error) bool {
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINTR)
}

func (s *Storage) publish(ctx context.Context, kind locking.MutationKind, targets []locking.BackendKey, previous, next int64, effect func() locking.PublicationOutcome) error {
	transition := func() locking.PublicationOutcome {
		settlement, err := storage.PreparePublication(ctx, previous, next)
		if err != nil {
			if storage.IsPublicationAccountingUncertain(err) {
				s.fence(err)
			}
			return locking.PublicationOutcome{Known: true, Err: err}
		}
		outcome := effect()
		result := storage.PublicationNotApplied
		if !outcome.Known {
			result = storage.PublicationUnknown
		} else if outcome.Changed {
			result = storage.PublicationApplied
		}
		if err := settlement(result); err != nil {
			outcome.Err = errors.Join(outcome.Err, err)
			s.fence(err)
		}
		if !outcome.Known {
			s.fence(outcome.Err)
		}
		return outcome
	}
	if s.authority == nil {
		if locking.HasScope(ctx) {
			return locking.Wrap(locking.Unavailable, "namespace has no lock authority", syscall.EOPNOTSUPP)
		}
		outcome := transition()
		return outcome.Err
	}
	return s.authority.Publish(ctx, locking.Publication{Kind: kind, Targets: targets, Scope: locking.ScopeFromContext(ctx)}, transition)
}

func keys(values ...locking.BackendKey) []locking.BackendKey {
	var result []locking.BackendKey
	for _, value := range values {
		if value != "" {
			present := false
			for _, previous := range result {
				present = present || previous == value
			}
			if !present {
				result = append(result, value)
			}
		}
	}
	return result
}

func (s *Storage) Stat(ctx context.Context, name string) (result storage.Attr, returned error) {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return result, err
	}
	returned = s.withPaths(ctx, []pathIntent{{name, false}}, func() error {
		if err := s.health(ctx); err != nil {
			return err
		}
		fd, info, err := s.inspect(name)
		if err != nil {
			return err
		}
		result = attrOf(info)
		return s.closeFD(fd)
	})
	return
}

// CheckBounded confirms that payload and result limits are enforced during capture.
func (s *Storage) CheckBounded() error { return nil }

func (s *Storage) read(ctx context.Context, name string, maxBytes int64) (content []byte, returned error) {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return nil, err
	}
	fd := -1
	err = s.withPaths(ctx, []pathIntent{{name, false}}, func() error {
		if err := s.health(ctx); err != nil {
			return err
		}
		var err error
		fd, err = s.openPath(name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK)
		if err != nil {
			return err
		}
		info, err := fileInfo(fd)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return syscall.EISDIR
		}
		if !info.Mode().IsRegular() {
			return syscall.EOPNOTSUPP
		}
		if maxBytes > 0 && info.Size() > maxBytes {
			return syscall.EFBIG
		}
		return nil
	})
	if fd >= 0 {
		defer func() { returned = errors.Join(returned, s.closeFD(fd)) }()
	}
	if err != nil {
		return nil, err
	}
	reader := &descriptorReader{fd: fd}
	if maxBytes > 0 {
		return readLocalBounded(ctx, reader, maxBytes)
	}
	return io.ReadAll(&contextReader{ctx: ctx, from: reader})
}

type descriptorReader struct{ fd int }

func (r *descriptorReader) Read(p []byte) (int, error) {
	n, err := unix.Read(r.fd, p)
	if err == nil && n == 0 {
		err = io.EOF
	}
	return n, err
}

func (s *Storage) Read(ctx context.Context, name string) ([]byte, error) { return s.read(ctx, name, 0) }
func (s *Storage) ReadBounded(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, syscall.EINVAL
	}
	return s.read(ctx, name, maxBytes)
}

func (s *Storage) reserveSnapshot(bytes int64, entries int) error {
	s.resources.Lock()
	defer s.resources.Unlock()
	if bytes > s.state.limits.MaxSnapshotBytes-s.snapshotBytes || entries > s.state.limits.MaxSnapshotEntries-s.snapshotEntries {
		return locking.Wrap(locking.Capacity, "directory snapshot capacity is exhausted", syscall.EAGAIN)
	}
	s.snapshotBytes += bytes
	s.snapshotEntries += entries
	return nil
}
func (s *Storage) releaseSnapshot(bytes int64, entries int) {
	s.resources.Lock()
	defer s.resources.Unlock()
	s.snapshotBytes -= bytes
	s.snapshotEntries -= entries
}

func (s *Storage) captureList(ctx context.Context, name string) (entries []storage.Entry, release func(), returned error) {
	var bytes int64
	release = func() { s.releaseSnapshot(bytes, len(entries)) }
	returned = s.withPaths(ctx, []pathIntent{{name, true}}, func() (returned error) {
		if err := s.health(ctx); err != nil {
			return err
		}
		fd, err := s.openPath(name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
		if err != nil {
			return err
		}
		dir := os.NewFile(uintptr(fd), "directory snapshot")
		defer func() { returned = errors.Join(returned, s.recordClose(dir.Close())) }()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch, readErr := dir.ReadDir(64)
			for _, entry := range batch {
				child := path.Join(name, entry.Name())
				childFD, info, err := s.inspect(child)
				if err != nil {
					return err
				}
				if err := s.closeFD(childFD); err != nil {
					return err
				}
				charge := int64(len(entry.Name()) + 128)
				if err := s.reserveSnapshot(charge, 1); err != nil {
					return err
				}
				bytes += charge
				entries = append(entries, storage.Entry{Name: entry.Name(), Attr: attrOf(info)})
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			if readErr != nil {
				return readErr
			}
		}
	})
	return
}

func (s *Storage) List(ctx context.Context, name string) ([]storage.Entry, error) {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return nil, err
	}
	entries, release, err := s.captureList(ctx, name)
	defer release()
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}
func (s *Storage) ListBounded(ctx context.Context, name string, result *storage.ListResult) (returned error) {
	if result == nil {
		return syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return err
	}
	entries, release, err := s.captureList(ctx, name)
	defer release()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := result.Add(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return storage.Space{}, err
	}
	defer done()
	if err := s.health(ctx); err != nil {
		return storage.Space{}, err
	}
	var value unix.Statfs_t
	if err := unix.Fstatfs(s.state.rootFD, &value); err != nil {
		return storage.Space{}, err
	}
	return spaceOf(value)
}
