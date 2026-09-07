package localdir

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type stagedContent struct {
	fd, dir int
	name    string
	bytes   int64
}

func (s *Storage) stage(ctx context.Context, content []byte) (*stagedContent, error) {
	size := int64(len(content))
	s.resources.Lock()
	if size > s.state.limits.MaxStagingBytes-s.stagedBytes {
		s.resources.Unlock()
		return nil, locking.Wrap(locking.Capacity, "content staging capacity is exhausted", syscall.EAGAIN)
	}
	s.stagedBytes += size
	s.resources.Unlock()
	stage := &stagedContent{fd: -1, dir: s.state.rootFD, bytes: size}
	if s.state.bound {
		stage.dir = s.state.stagingFD
	}
	fail := func(err error) (*stagedContent, error) { return nil, errors.Join(err, s.discardStage(stage)) }
	var fd int
	var err error
	if s.state.bound {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return fail(err)
		}
		stage.name = ".rfs-stage-" + hex.EncodeToString(token[:])
		fd, err = unix.Openat(stage.dir, stage.name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	} else {
		fd, err = unix.Openat(stage.dir, ".", unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0600)
	}
	if err != nil {
		if !nativeUnknown(err) {
			stage.name = ""
		}
		return fail(err)
	}
	stage.fd = fd
	for len(content) > 0 {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		count := len(content)
		if count > 1<<20 {
			count = 1 << 20
		}
		n, err := s.ops.write(fd, content[:count])
		if err != nil {
			return fail(err)
		}
		if n <= 0 || n > count {
			return fail(io.ErrShortWrite)
		}
		content = content[n:]
	}
	if err := s.ops.sync(fd); err != nil {
		return fail(err)
	}
	return stage, nil
}

func (s *Storage) discardStage(stage *stagedContent) error {
	var err error
	if stage.fd >= 0 {
		err = s.closeFD(stage.fd)
		stage.fd = -1
	}
	if stage.name != "" {
		unlinkErr := s.ops.unlink(stage.dir, stage.name, 0)
		if unlinkErr != nil && !errors.Is(unlinkErr, syscall.ENOENT) {
			err = errors.Join(err, unlinkErr)
			s.fence(err)
			return err
		}
		stage.name = ""
	}
	s.resources.Lock()
	s.stagedBytes -= stage.bytes
	s.resources.Unlock()
	stage.bytes = 0
	return err
}

func (s *Storage) Write(ctx context.Context, name string, content []byte) (returned error) {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return err
	}
	if name == "" {
		return syscall.EISDIR
	}
	stage, err := s.stage(ctx, content)
	if err != nil {
		return err
	}
	defer func() {
		if s.state.bound || stage.fd >= 0 {
			returned = errors.Join(returned, s.discardStage(stage))
		}
	}()
	intents := []pathIntent{{name, true}}
	if !s.state.bound {
		intents = []pathIntent{{"", true}}
	}
	return s.withPaths(ctx, intents, func() (returned error) {
		if !s.state.bound {
			defer func() { returned = errors.Join(returned, s.discardStage(stage)) }()
		}
		parent, base, err := s.openParent(name)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(parent)) }()
		oldFD, info, err := s.inspect(name)
		if err != nil && !errors.Is(err, syscall.ENOENT) {
			return err
		}
		if oldFD >= 0 {
			defer func() { returned = errors.Join(returned, s.closeFD(oldFD)) }()
		}
		mode := fs.FileMode(0644)
		if info != nil {
			if info.IsDir() {
				return syscall.EISDIR
			}
			if info.Mode()&fs.ModeSymlink != 0 {
				return syscall.ELOOP
			}
			if !info.Mode().IsRegular() {
				return syscall.EOPNOTSUPP
			}
			mode = info.Mode() & storage.SettableMode
		}
		key, err := s.target(oldFD, name)
		if err != nil {
			return err
		}
		return s.publish(ctx, locking.WriteMutation, keys(key), regularSize(info), int64(len(content)), func() locking.PublicationOutcome {
			if !s.state.bound {
				if err := s.attachStage(stage); err != nil {
					return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
				}
			}
			if err := s.ops.rename(stage.dir, stage.name, parent, base); err != nil {
				return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
			}
			stage.name = ""
			if err := s.replaceTarget(oldFD, stage.fd, name); err != nil {
				return locking.PublicationOutcome{Known: false, Changed: true, Err: err}
			}
			return locking.PublicationOutcome{Known: true, Changed: true, Err: errors.Join(s.ops.chmod(stage.fd, modeBits(mode)), s.ops.sync(stage.fd), s.ops.sync(parent), s.ops.sync(stage.dir))}
		})
	})
}

func (s *Storage) Create(ctx context.Context, name string) error {
	return s.createEntry(ctx, name, false)
}
func (s *Storage) Mkdir(ctx context.Context, name string) error {
	return s.createEntry(ctx, name, true)
}
func (s *Storage) createEntry(ctx context.Context, name string, directory bool) error {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return err
	}
	if name == "" {
		return syscall.EEXIST
	}
	return s.withPaths(ctx, []pathIntent{{name, true}}, func() (returned error) {
		parent, base, err := s.openParent(name)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(parent)) }()
		return s.publish(ctx, locking.CreateMutation, nil, 0, 0, func() locking.PublicationOutcome {
			if directory {
				err := unix.Mkdirat(parent, base, 0755)
				if err != nil {
					return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
				}
				return locking.PublicationOutcome{Known: true, Changed: true, Err: s.ops.sync(parent)}
			}
			fd, err := unix.Openat(parent, base, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0644)
			if err != nil {
				return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
			}
			return locking.PublicationOutcome{Known: true, Changed: true, Err: errors.Join(s.closeFD(fd), s.ops.sync(parent))}
		})
	})
}

func (s *Storage) Remove(ctx context.Context, name string) error {
	return s.removeEntry(ctx, name, false)
}
func (s *Storage) RemoveDir(ctx context.Context, name string) error {
	return s.removeEntry(ctx, name, true)
}
func (s *Storage) removeEntry(ctx context.Context, name string, directory bool) error {
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return err
	}
	if name == "" {
		if directory {
			return syscall.EBUSY
		}
		return syscall.EISDIR
	}
	return s.withPaths(ctx, []pathIntent{{name, true}}, func() (returned error) {
		parent, base, err := s.openParent(name)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(parent)) }()
		fd, info, err := s.inspect(name)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(fd)) }()
		key, err := s.target(fd, name)
		if err != nil {
			return err
		}
		flags := 0
		if directory {
			flags = unix.AT_REMOVEDIR
		}
		return s.publish(ctx, locking.RemoveMutation, keys(key), regularSize(info), 0, func() locking.PublicationOutcome {
			if err := s.ops.unlink(parent, base, flags); err != nil {
				return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
			}
			retired, err := s.removeTarget(fd)
			if err != nil {
				return locking.PublicationOutcome{Known: false, Changed: true, Err: err}
			}
			return locking.PublicationOutcome{Known: true, Changed: true, Retired: retired, Err: s.ops.sync(parent)}
		})
	})
}

func (s *Storage) Rename(ctx context.Context, from, to string) (returned error) {
	originalFrom, originalTo := from, to
	defer func() {
		if returned != nil {
			returned = &os.LinkError{Op: "rename", Old: originalFrom, New: originalTo, Err: returned}
		}
	}()
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	from, err = s.clean(from)
	if err != nil {
		return err
	}
	to, err = s.clean(to)
	if err != nil {
		return err
	}
	if from == "" || to == "" {
		return syscall.EBUSY
	}
	return s.withPaths(ctx, []pathIntent{{from, true}, {to, true}}, func() (returned error) {
		sourceFD, _, err := s.inspect(from)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(sourceFD)) }()
		targetFD, targetInfo, err := s.inspect(to)
		if err != nil && !errors.Is(err, syscall.ENOENT) {
			return err
		}
		if targetFD >= 0 {
			defer func() { returned = errors.Join(returned, s.closeFD(targetFD)) }()
		}
		sourceKey, err := s.target(sourceFD, from)
		if err != nil {
			return err
		}
		targetKey, err := s.target(targetFD, to)
		if err != nil {
			return err
		}
		fromParent, fromBase, err := s.openParent(from)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(fromParent)) }()
		toParent, toBase, err := s.openParent(to)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(toParent)) }()
		if err := s.validateRenameTargets(from, to); err != nil {
			return err
		}
		sameEntry := from == to
		if targetFD >= 0 {
			sourceIdentity, _, err := identityOf(sourceFD)
			if err != nil {
				return err
			}
			targetIdentity, _, err := identityOf(targetFD)
			if err != nil {
				return err
			}
			sameEntry = sourceIdentity == targetIdentity
		}
		previous := regularSize(targetInfo)
		if sameEntry {
			previous = 0
		}
		return s.publish(ctx, locking.RenameMutation, keys(sourceKey, targetKey), previous, 0, func() locking.PublicationOutcome {
			if err := s.ops.rename(fromParent, fromBase, toParent, toBase); err != nil {
				return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
			}
			if sameEntry {
				return locking.PublicationOutcome{Known: true}
			}
			retired, err := s.renameTargets(from, to, sourceFD, targetFD)
			if err != nil {
				return locking.PublicationOutcome{Known: false, Changed: true, Err: err}
			}
			return locking.PublicationOutcome{Known: true, Changed: true, Retired: retired, Err: errors.Join(s.ops.sync(fromParent), s.ops.sync(toParent))}
		})
	})
}

func (s *Storage) SetAttr(ctx context.Context, name string, change storage.AttrChange) error {
	if err := change.Check(); err != nil {
		return err
	}
	ctx, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	name, err = s.clean(name)
	if err != nil {
		return err
	}
	return s.withPaths(ctx, []pathIntent{{name, true}}, func() (returned error) {
		fd, info, err := s.inspect(name)
		if err != nil {
			return err
		}
		defer func() { returned = errors.Join(returned, s.closeFD(fd)) }()
		key, err := s.target(fd, name)
		if err != nil {
			return err
		}
		return s.publish(ctx, locking.SetAttrMutation, keys(key), 0, 0, func() locking.PublicationOutcome {
			changed := false
			if change.AccessTime != nil || change.ModTime != nil {
				times := [2]unix.Timespec{{Nsec: unix.UTIME_OMIT}, {Nsec: unix.UTIME_OMIT}}
				if change.AccessTime != nil {
					times[0], err = unix.TimeToTimespec(*change.AccessTime)
					if err != nil {
						return locking.PublicationOutcome{Known: true, Err: err}
					}
				}
				if change.ModTime != nil {
					times[1], err = unix.TimeToTimespec(*change.ModTime)
					if err != nil {
						return locking.PublicationOutcome{Known: true, Err: err}
					}
				}
				err = s.ops.times(fd, "", times[:], unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW)
				if err != nil {
					return locking.PublicationOutcome{Known: !nativeUnknown(err), Err: err}
				}
				changed = true
			}
			if change.Mode != nil {
				if info.Mode()&fs.ModeSymlink != 0 {
					return locking.PublicationOutcome{Known: true, Changed: changed, Err: syscall.EOPNOTSUPP}
				}
				err = s.ops.chmod(fd, modeBits(*change.Mode))
				if err != nil {
					return locking.PublicationOutcome{Known: !nativeUnknown(err), Changed: changed, Err: err}
				}
				changed = true
			}
			return locking.PublicationOutcome{Known: true, Changed: changed}
		})
	})
}

func (s *Storage) attachStage(stage *stagedContent) error {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	stage.name = ".rfs-stage-" + hex.EncodeToString(token[:])
	err := s.ops.link(unix.AT_FDCWD, "/proc/self/fd/"+strconv.Itoa(stage.fd), stage.dir, stage.name, unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		if nativeUnknown(err) {
			s.fence(err)
		} else {
			stage.name = ""
		}
	}
	return err
}

func (s *Storage) probeOperations(ctx context.Context) (returned error) {
	stage, err := s.stage(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, s.discardStage(stage)) }()
	if err := s.ops.chmod(stage.fd, 0600); err != nil {
		return err
	}
	if !s.state.bound {
		return s.attachStage(stage)
	}
	return nil
}
