// Package localdir implements storage.Storage over one directory on the local
// filesystem.
//
// It is both the implementation the reference server ships with and the one the mount is
// exercised against. Because the mount knows nothing but storage.Storage, mounting this
// produces a filesystem with no network anywhere in it, which can then be compared
// against a plain directory operation by operation.
package localdir

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Storage is a namespace held in one directory.
type Storage struct {
	root string
}

var _ storage.Storage = (*Storage)(nil)

// New opens the namespace rooted at dir, which must already be a directory.
//
// A missing root is refused here rather than per operation, because per operation it
// would surface as ENOENT — which says "that file is not there" when the truth is "the
// namespace is not there", and callers act very differently on those two.
func New(dir string) (*Storage, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "open", Path: dir, Err: syscall.ENOTDIR}
	}
	return &Storage{root: dir}, nil
}

func (s *Storage) Stat(_ context.Context, path string) (storage.Attr, error) {
	host, err := s.host(path)
	if err != nil {
		return storage.Attr{}, err
	}
	// os.Lstat, not os.Stat: the node reported is the one at the name. os.ReadDir in List
	// is an lstat too, so the two agree about what kind of node a link is.
	info, err := os.Lstat(host)
	if err != nil {
		return storage.Attr{}, err
	}
	return attrOf(info), nil
}

func (s *Storage) SetAttr(_ context.Context, path string, change storage.AttrChange) error {
	if err := change.Check(); err != nil {
		return &os.PathError{Op: "setattr", Path: path, Err: err}
	}
	host, err := s.host(path)
	if err != nil {
		return err
	}

	// A change that names nothing still answers for the node it names, and none of the
	// calls below would be made to answer for it. utimensat cannot stand in either: with
	// both times omitted it answers 0 without so much as resolving the path (measured on
	// Linux 6.8), so a node that is not there would come back as a change that happened.
	if change.Empty() {
		_, err := os.Lstat(host)
		return err
	}

	if change.AccessTime != nil || change.ModTime != nil {
		if err := setTimes(host, path, change); err != nil {
			return err
		}
	}
	if change.Mode != nil {
		if err := setMode(host, path, *change.Mode); err != nil {
			return err
		}
	}
	return nil
}

// setTimes applies the times a change names. utimensat takes both at once, so the one a
// change leaves alone is passed as UTIME_OMIT rather than read back and written out again
// — which would put a window between the two in which somebody else's change is lost.
func setTimes(host, path string, change storage.AttrChange) error {
	times := [2]unix.Timespec{{Nsec: unix.UTIME_OMIT}, {Nsec: unix.UTIME_OMIT}}
	for i, t := range [2]*time.Time{change.AccessTime, change.ModTime} {
		if t == nil {
			continue
		}
		ts, err := unix.TimeToTimespec(*t)
		if err != nil {
			// Only reachable where a timespec's seconds are narrower than an int64, which
			// is the case on a 32-bit host. EOVERFLOW is what the kernel itself reports for
			// a value it has no room for.
			return &os.PathError{Op: "setattr", Path: path, Err: syscall.EOVERFLOW}
		}
		times[i] = ts
	}
	// AT_SYMLINK_NOFOLLOW, so that the times land on the node at the name. Times are the
	// one attribute a symbolic link can carry of its own, and utimensat is the one call
	// here that can put them there: it succeeds on a link and leaves what the link points
	// at untouched (measured on Linux 6.8).
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, host, times[:], unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return &os.PathError{Op: "utimensat", Path: path, Err: err}
	}
	return nil
}

// setMode applies the mode a change names to the node at the name.
//
// Linux has no lchmod, and a symbolic link's mode is not a thing that can be set: chmod(2)
// and fchmodat(2) both follow, and fchmodat2(2) with AT_SYMLINK_NOFOLLOW — the one call
// that does not — answers EOPNOTSUPP for a link (measured on Linux 6.8, both through the
// raw syscall and through golang.org/x/sys/unix.Fchmodat).
//
// That call is not the one made here, because it has only existed since Linux 6.6: where
// it is missing, unix.Fchmodat reports EOPNOTSUPP for every node rather than only for a
// link, which would leave every mode change on this storage failing. So what is at the
// name is read first, and a link is refused under the errno the kernel gives for it.
func setMode(host, path string, mode fs.FileMode) error {
	info, err := os.Lstat(host)
	if err != nil {
		return err
	}
	if info.Mode().Type() == fs.ModeSymlink {
		return &os.PathError{Op: "chmod", Path: path, Err: syscall.EOPNOTSUPP}
	}
	// os.Chmod carries exactly the bits storage.SettableMode holds and no others, so a
	// change that got this far needs no further translation.
	return os.Chmod(host, mode)
}

func (s *Storage) List(_ context.Context, path string) ([]storage.Entry, error) {
	host, err := s.host(path)
	if err != nil {
		return nil, err
	}
	// os.ReadDir already sorts by name, which is what the contract asks for.
	read, err := os.ReadDir(host)
	if err != nil {
		return nil, err
	}
	entries := make([]storage.Entry, 0, len(read))
	for _, e := range read {
		info, err := e.Info()
		if err != nil {
			// The entry was removed between listing the directory and stat-ing it. It
			// is genuinely gone, so omitting it is the truthful answer; inventing
			// attributes for it would not be.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
		entries = append(entries, storage.Entry{Name: e.Name(), Attr: attrOf(info)})
	}
	return entries, nil
}

// Read opens with O_NOFOLLOW, so that the bytes it hands back are the ones held at the
// name it was given: open(2) answers ELOOP for a symbolic link rather than opening what
// the link points at (measured on Linux 6.8). A directory opens and then fails at the
// read, which is where EISDIR comes from.
func (s *Storage) Read(_ context.Context, path string) ([]byte, error) {
	host, err := s.host(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(host, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Write stages the content in a temporary file alongside the target and renames it into
// place, so that a reader sees either the whole of the old contents or the whole of the
// new ones. The staging file is created in the target's own directory because rename is
// only atomic within a filesystem.
func (s *Storage) Write(_ context.Context, path string, content []byte) error {
	host, err := s.host(path)
	if err != nil {
		return err
	}

	// A new file gets the mode a normal tool would give it; an existing one keeps the
	// mode it already had, since replacing the contents is not a request to change it.
	// Every settable bit is carried over, not the nine permission bits alone: dropping a
	// setuid bit here would be a change nobody asked for and nothing reported.
	//
	// os.Lstat, because the rename below replaces whatever is at the name: asking about
	// the target while replacing the link is how a write ends up carrying one node's mode
	// onto a file that took another node's place.
	mode := fs.FileMode(0o644)
	switch info, err := os.Lstat(host); {
	case err != nil:
		// Nothing is there to keep the mode of. Whether the file can be made where it was
		// asked for is the staging file's question, and it answers it below.
	case info.IsDir():
		return &os.PathError{Op: "write", Path: path, Err: syscall.EISDIR}
	case info.Mode().Type() == fs.ModeSymlink:
		// ELOOP is what open(2) with O_NOFOLLOW answers for a link (measured on Linux
		// 6.8), and it is named here rather than provoked: the open that would provoke it
		// is an open for writing, which would also check the caller's permission — at the
		// commit rather than at the open the caller made, so a file created 0444 and
		// written through its own descriptor would fail to be written back.
		return &os.PathError{Op: "write", Path: path, Err: syscall.ELOOP}
	default:
		mode = info.Mode() & storage.SettableMode
	}

	dir, name := filepath.Split(host)
	staged, err := os.CreateTemp(dir, "."+name+".*")
	if err != nil {
		return err
	}
	defer os.Remove(staged.Name())

	if _, err := staged.Write(content); err != nil {
		staged.Close()
		return err
	}
	// The mode goes on after the contents rather than before them. Writing to a file
	// clears its setuid bit unless the writer holds CAP_FSETID, so a mode applied first
	// would not survive the write that follows it. It also means the staging file is
	// readable only by its owner for as long as it is being filled, which is what
	// os.CreateTemp makes it.
	if err := staged.Chmod(mode); err != nil {
		staged.Close()
		return err
	}
	if err := staged.Close(); err != nil {
		return err
	}
	return os.Rename(staged.Name(), host)
}

func (s *Storage) Create(_ context.Context, path string) error {
	host, err := s.host(path)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(host, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

func (s *Storage) Mkdir(_ context.Context, path string) error {
	host, err := s.host(path)
	if err != nil {
		return err
	}
	return os.Mkdir(host, 0o755)
}

// Remove unlinks rather than calling os.Remove, which falls back to rmdir and would
// therefore delete a directory that the contract says must fail with EISDIR.
func (s *Storage) Remove(_ context.Context, path string) error {
	host, err := s.host(path)
	if err != nil {
		return err
	}
	if err := syscall.Unlink(host); err != nil {
		return &os.PathError{Op: "unlink", Path: path, Err: err}
	}
	return nil
}

// RemoveDir refuses the root before reaching the filesystem. To the kernel the root is an
// ordinary directory, so rmdir(2) empties the namespace out of existence without
// complaint, and every operation afterwards answers ENOENT — a missing file where the
// truth is a missing namespace, which is what New refuses to let happen.
func (s *Storage) RemoveDir(_ context.Context, path string) error {
	host, err := s.host(path)
	if err != nil {
		return err
	}
	if host == s.root {
		return &os.PathError{Op: "rmdir", Path: path, Err: syscall.EBUSY}
	}
	if err := syscall.Rmdir(host); err != nil {
		return &os.PathError{Op: "rmdir", Path: path, Err: err}
	}
	return nil
}

// Rename refuses the root as either operand. Neither direction can succeed underneath —
// a directory cannot be moved inside itself, and the destination root always holds at
// least the source — but the errnos that come back, EINVAL and EEXIST, each name an
// incidental obstacle rather than the rule, and they differ by direction and by host.
func (s *Storage) Rename(_ context.Context, from, to string) error {
	hostFrom, err := s.host(from)
	if err != nil {
		return err
	}
	hostTo, err := s.host(to)
	if err != nil {
		return err
	}
	if hostFrom == s.root || hostTo == s.root {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.EBUSY}
	}
	return os.Rename(hostFrom, hostTo)
}

// host maps a namespace path onto a path in the underlying directory. Everything that
// escapes the root has already been rejected by storage.CleanPath, so the join cannot
// climb out.
func (s *Storage) host(path string) (string, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return "", err
	}
	if cleaned == "" {
		return s.root, nil
	}
	return filepath.Join(s.root, filepath.FromSlash(cleaned)), nil
}

// attrOf renders what the operating system reports about one node.
//
// The access time comes out of the raw stat structure because io/fs.FileInfo has no field
// for it. os.Stat and fs.DirEntry.Info both produce a *syscall.Stat_t underneath on this
// platform, so the assertion holds for every caller here.
func attrOf(info fs.FileInfo) storage.Attr {
	accessed := info.Sys().(*syscall.Stat_t).Atim
	return storage.Attr{
		Mode:       info.Mode(),
		Size:       info.Size(),
		AccessTime: time.Unix(accessed.Unix()),
		ModTime:    info.ModTime(),
	}
}
