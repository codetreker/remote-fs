package localdir

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"golang.org/x/sys/unix"
)

type nativeIdentity struct {
	device uint64
	inode  uint64
}

type nativeTarget struct {
	key      locking.BackendKey
	identity nativeIdentity
	path     string
	fd       int
	live     bool
}

func identityOf(fd int) (nativeIdentity, unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nativeIdentity{}, stat, err
	}
	return nativeIdentity{device: uint64(stat.Dev), inode: stat.Ino}, stat, nil
}

func supportedTarget(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return locking.Wrap(locking.UnsupportedTarget, "lock target must be a single-link regular file", nil)
	}
	return nil
}

// Discover retains one O_PATH descriptor only when the callback adopts a new
// backend key. Repeated discovery returns the existing logical identity.
func (s *Storage) Discover(ctx context.Context, path string, accept func(locking.BackendKey) (bool, error)) error {
	path, err := s.clean(path)
	if err != nil {
		return err
	}
	ctx, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	return s.withPaths(ctx, []pathIntent{{path: path, write: true}}, func() (result error) {
		fd, err := s.openPath(path, unix.O_PATH|unix.O_NOFOLLOW)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
				return locking.Wrap(locking.UnsupportedTarget, "lock target must exist", err)
			}
			return err
		}
		keep := false
		defer func() {
			if !keep {
				result = errors.Join(result, s.closeFD(fd))
			}
		}()
		identity, stat, err := identityOf(fd)
		if err != nil {
			return err
		}
		if err := supportedTarget(stat); err != nil {
			return err
		}
		r := s.runtime
		r.pinsMu.Lock()
		key, exists := r.physical[identity]
		if exists {
			r.pinsMu.Unlock()
			adopt, err := accept(key)
			if adopt {
				return errors.Join(err, locking.Wrap(locking.Unavailable, "existing native target was adopted twice", nil))
			}
			return err
		}
		if len(r.pins) >= r.limits.MaxPinnedTargets || r.sequence == math.MaxUint64 {
			r.pinsMu.Unlock()
			return locking.Wrap(locking.Capacity, "local directory target capacity exhausted", nil)
		}
		r.sequence++
		key = locking.BackendKey("local:" + strconv.FormatUint(r.sequence, 10))
		target := &nativeTarget{key: key, identity: identity, path: path, fd: fd, live: true}
		r.pins[key], r.physical[identity] = target, key
		r.pinsMu.Unlock()
		adopt, err := accept(key)
		if adopt {
			keep = true
			return err
		}
		r.pinsMu.Lock()
		delete(r.pins, key)
		if r.physical[identity] == key {
			delete(r.physical, identity)
		}
		r.pinsMu.Unlock()
		return err
	})
}

// Guard validates reachability under the same path ordering used by mutations.
// Rename may move a pin while this operation waits for its previous path.
func (s *Storage) Guard(ctx context.Context, key locking.BackendKey, transition func() error) error {
	ctx, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	for {
		r := s.runtime
		r.pinsMu.Lock()
		target := r.pins[key]
		if target == nil || !target.live {
			r.pinsMu.Unlock()
			return locking.Wrap(locking.StaleResource, "native target is retired", nil)
		}
		path := target.path
		r.pinsMu.Unlock()
		retry := false
		err := s.withPaths(ctx, []pathIntent{{path: path, write: true}}, func() (result error) {
			r.pinsMu.Lock()
			current := r.pins[key]
			if current == nil || !current.live {
				r.pinsMu.Unlock()
				return locking.Wrap(locking.StaleResource, "native target is retired", nil)
			}
			if current.path != path {
				retry = true
				r.pinsMu.Unlock()
				return nil
			}
			identity := current.identity
			r.pinsMu.Unlock()
			fd, err := s.openPath(path, unix.O_PATH|unix.O_NOFOLLOW)
			if err != nil {
				if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
					return locking.Wrap(locking.StaleResource, "native target is unreachable", err)
				}
				return err
			}
			defer func() { result = errors.Join(result, s.closeFD(fd)) }()
			actual, stat, err := identityOf(fd)
			if err != nil {
				return err
			}
			if err := supportedTarget(stat); err != nil {
				return locking.Wrap(locking.StaleResource, "native target is no longer supported", err)
			}
			if actual != identity {
				return locking.Wrap(locking.StaleResource, "native target identity changed", nil)
			}
			return transition()
		})
		if err != nil || !retry {
			return err
		}
	}
}

// Forget releases the authority's adopted pin after its references drain. Its
// exclusive path claim also orders concurrent discovery before pin release.
// Cleanup remains available after namespace admission has stopped.
func (s *Storage) Forget(ctx context.Context, key locking.BackendKey) error {
	r := s.runtime
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.pinsMu.Lock()
		target := r.pins[key]
		if target == nil {
			r.pinsMu.Unlock()
			return nil
		}
		path := target.path
		r.pinsMu.Unlock()
		paths, err := r.expandPaths([]pathIntent{{path: path, write: true}})
		if err != nil {
			return err
		}
		release, err := r.enterPaths(ctx, paths)
		if err != nil {
			return err
		}
		r.pinsMu.Lock()
		target = r.pins[key]
		if target == nil {
			r.pinsMu.Unlock()
			release()
			return nil
		}
		if target.path != path {
			r.pinsMu.Unlock()
			release()
			continue
		}
		delete(r.pins, key)
		if r.physical[target.identity] == key {
			delete(r.physical, target.identity)
		}
		r.pinsMu.Unlock()
		err = s.closeFD(target.fd)
		release()
		return err
	}
}

func (s *Storage) target(fd int, path string) (locking.BackendKey, error) {
	if fd == -1 {
		return "", nil
	}
	identity, _, err := identityOf(fd)
	if err != nil {
		return "", err
	}
	r := s.runtime
	r.pinsMu.Lock()
	defer r.pinsMu.Unlock()
	key := r.physical[identity]
	if key != "" && r.pins[key].path != path {
		return "", locking.Wrap(locking.StaleResource, "native target path changed outside the namespace", nil)
	}
	return key, nil
}

// replaceTarget runs after the atomic namespace replacement, before releasing
// its path claim. The logical key follows the replacement inode.
func (s *Storage) replaceTarget(oldFD, newFD int, path string) error {
	if oldFD == -1 {
		return nil
	}
	previous, _, err := identityOf(oldFD)
	if err != nil {
		return err
	}
	next, stat, err := identityOf(newFD)
	if err != nil {
		return err
	}
	if err := supportedTarget(stat); err != nil {
		return err
	}
	r := s.runtime
	r.pinsMu.Lock()
	key := r.physical[previous]
	r.pinsMu.Unlock()
	if key == "" {
		return nil
	}
	pin, err := s.openPath(path, unix.O_PATH|unix.O_NOFOLLOW)
	if err != nil {
		return err
	}
	pinned, _, err := identityOf(pin)
	if err != nil || pinned != next {
		if err == nil {
			err = locking.Wrap(locking.Unavailable, "replacement inode changed before identity transfer", nil)
		}
		return errors.Join(err, s.closeFD(pin))
	}
	r.pinsMu.Lock()
	target := r.pins[key]
	if target == nil || !target.live || target.identity != previous {
		r.pinsMu.Unlock()
		return s.closeFD(pin)
	}
	if other := r.physical[next]; other != "" && other != key {
		r.pinsMu.Unlock()
		return errors.Join(locking.Wrap(locking.Unavailable, "replacement inode already has a native identity", nil), s.closeFD(pin))
	}
	oldPin := target.fd
	delete(r.physical, previous)
	target.fd, target.identity, target.path = pin, next, path
	r.physical[next] = key
	r.pinsMu.Unlock()
	return s.closeFD(oldPin)
}

func (s *Storage) removeTarget(fd int) ([]locking.BackendKey, error) {
	identity, _, err := identityOf(fd)
	if err != nil {
		return nil, err
	}
	r := s.runtime
	r.pinsMu.Lock()
	defer r.pinsMu.Unlock()
	key := r.physical[identity]
	if key == "" {
		return nil, nil
	}
	r.pins[key].live = false
	delete(r.physical, identity)
	return []locking.BackendKey{key}, nil
}

// validateRenameTargets runs under both rename path claims before any effect.
// Descendant paths remain bounded even when the directory itself is much shorter.
func (s *Storage) validateRenameTargets(from, to string) error {
	r := s.runtime
	r.pinsMu.Lock()
	defer r.pinsMu.Unlock()
	for _, target := range r.pins {
		if target.live && (target.path == from || strings.HasPrefix(target.path, from+"/")) &&
			len(to)+len(target.path)-len(from) > r.limits.MaxPathBytes {
			return syscall.ENAMETOOLONG
		}
	}
	return nil
}

func (s *Storage) renameTargets(from, to string, sourceFD, targetFD int) ([]locking.BackendKey, error) {
	source, _, err := identityOf(sourceFD)
	if err != nil {
		return nil, err
	}
	var replaced nativeIdentity
	if targetFD >= 0 {
		replaced, _, err = identityOf(targetFD)
		if err != nil {
			return nil, err
		}
	}
	r := s.runtime
	r.pinsMu.Lock()
	defer r.pinsMu.Unlock()
	var retired []locking.BackendKey
	if targetFD >= 0 && source != replaced {
		if key := r.physical[replaced]; key != "" {
			r.pins[key].live = false
			delete(r.physical, replaced)
			retired = append(retired, key)
		}
	}
	for _, target := range r.pins {
		if target.live && (target.path == from || strings.HasPrefix(target.path, from+"/")) {
			target.path = to + strings.TrimPrefix(target.path, from)
		}
	}
	return retired, nil
}

func (r *nativeRuntime) closePins() error {
	r.pinsMu.Lock()
	if r.pinsClosed {
		err := r.pinsErr
		r.pinsMu.Unlock()
		return err
	}
	r.pinsClosed = true
	pins := r.pins
	r.pins = make(map[locking.BackendKey]*nativeTarget)
	r.physical = make(map[nativeIdentity]locking.BackendKey)
	r.pinsMu.Unlock()
	var result error
	for _, target := range pins {
		result = errors.Join(result, unix.Close(target.fd))
	}
	r.pinsMu.Lock()
	r.pinsErr = result
	r.pinsMu.Unlock()
	return result
}
