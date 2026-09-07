package localdir

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func openStateDirectories(ctx context.Context, config Config) (*directoryState, error) {
	now := time.Now
	if config.Locks.Clock != nil {
		now = config.Locks.Clock.Now
	}
	s, err := openDirectoryState(ctx, config.Root, config.Limits, now)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*directoryState, error) { return nil, errors.Join(err, s.Close()) }
	s.statePath, err = canonicalDirectory(config.StateRoot, s.limits.MaxPathBytes)
	if err != nil {
		return fail(err)
	}
	if s.rootPath == s.statePath || strings.HasPrefix(s.rootPath, s.statePath+string(filepath.Separator)) ||
		strings.HasPrefix(s.statePath, s.rootPath+string(filepath.Separator)) || s.rootPath == "/" || s.statePath == "/" {
		return fail(fmt.Errorf("namespace and private state directories must be disjoint: %w", syscall.EINVAL))
	}
	s.stateFD, err = openControlledDirectory(s.statePath, true)
	if err != nil {
		return fail(fmt.Errorf("open private state directory: %w", err))
	}
	mount, err := directoryMount(s.stateFD)
	if err != nil {
		return fail(err)
	}
	if mount != s.mount {
		return fail(fmt.Errorf("namespace and state directories must share the same current mount: %w", syscall.EXDEV))
	}
	var rootStat, stateStat unix.Stat_t
	if err := errors.Join(unix.Fstat(s.rootFD, &rootStat), unix.Fstat(s.stateFD, &stateStat)); err != nil {
		return fail(err)
	}
	if rootStat.Dev == stateStat.Dev && rootStat.Ino == stateStat.Ino {
		return fail(fmt.Errorf("namespace and state directories refer to the same inode: %w", syscall.EINVAL))
	}
	// The state directory also excludes another namespace attempting to initialize
	// against the same private evidence before its first identity is installed.
	if err := unix.Flock(s.stateFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("acquire private state ownership: %w", err))
	}
	if err := s.health(); err != nil {
		return fail(err)
	}
	return s, nil
}

// Init durably binds an existing namespace to its existing private StateRoot.
// It can finish a matching interrupted initialization, but never replaces a
// binding or interprets lost READY evidence as a fresh namespace.
func Init(ctx context.Context, config Config) (result error) {
	s, err := openStateDirectories(ctx, config)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	return s.initialize(ctx)
}

func (s *directoryState) initialize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, bindingErr := s.readBinding()
	if bindingErr != nil && !errors.Is(bindingErr, unix.ENODATA) {
		return bindingErr
	}
	record, recordErr := s.readRecord()
	if recordErr != nil && !errors.Is(recordErr, unix.ENOENT) {
		return recordErr
	}
	if recordErr == nil && bindingErr == nil && record.Binding != binding {
		return stateFailure("initialization intent does not match the namespace binding")
	}
	if recordErr == nil && record.Phase == "READY" {
		return fmt.Errorf("namespace is already initialized; use Open: %w", syscall.EEXIST)
	}
	if recordErr != nil {
		if bindingErr == nil {
			return stateFailure("bound namespace is missing its recorded initialization")
		}
		if _, err := readStateAttribute(s.rootFD, witnessAttribute); !errors.Is(err, unix.ENODATA) {
			return errors.Join(stateFailure("unbound namespace contains a lease witness"), err)
		}
		if err := s.inspectStateDirectory(false); err != nil {
			return err
		}
		namespace, err := newStateID()
		if err != nil {
			return err
		}
		stateID, err := newStateID()
		if err != nil {
			return err
		}
		record = leaseRecord{Binding: directoryBinding{1, namespace, stateID, s.rootPath, s.statePath}, Phase: "INIT"}
		if err := s.openStaging(true); err != nil {
			return err
		}
		if err := s.cleanRecoveryFiles(); err != nil {
			return err
		}
		if err := s.probeCapabilities(); err != nil {
			return err
		}
		if err := s.syncRecord(record); err != nil {
			return err
		}
	} else {
		if err := s.inspectStateDirectory(true); err != nil {
			return err
		}
		if err := s.openStaging(false); err != nil {
			return err
		}
		if err := s.cleanRecoveryFiles(); err != nil {
			return err
		}
		if err := s.probeCapabilities(); err != nil {
			return err
		}
	}
	s.binding = record.Binding
	if bindingErr != nil {
		data, err := encodeState(s.binding)
		if err != nil {
			return err
		}
		if err := s.ops.setxattr(s.rootFD, bindingAttribute, data, unix.XATTR_CREATE); err != nil {
			return err
		}
		if err := s.ops.fsync(s.rootFD); err != nil {
			return err
		}
	}
	witness, err := s.readWitness()
	if errors.Is(err, unix.ENODATA) {
		if err := s.syncWitness(leasePoint{}, unix.XATTR_CREATE); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if witness != (leasePoint{}) {
		return stateFailure("initialization witness contains active lease evidence")
	}
	// Even a matching visible xattr may come from an interrupted unsynced write.
	if err := s.ops.fsync(s.rootFD); err != nil {
		return err
	}
	record.Phase = "READY"
	if err := s.syncRecord(record); err != nil {
		return err
	}
	return s.health()
}

func openBoundState(ctx context.Context, config Config) (*directoryState, error) {
	s, err := openStateDirectories(ctx, config)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*directoryState, error) { return nil, errors.Join(err, s.Close()) }
	s.binding, err = s.readBinding()
	if err != nil {
		return fail(fmt.Errorf("read required namespace binding: %w", err))
	}
	record, err := s.readRecord()
	if err != nil {
		return fail(fmt.Errorf("read required READY lease evidence: %w", err))
	}
	if record.Phase != "READY" || record.Binding != s.binding {
		return fail(stateFailure("matching READY lease evidence is required; finish a recorded initialization with Init"))
	}
	witness, err := s.readWitness()
	if err != nil {
		return fail(fmt.Errorf("read required namespace lease witness: %w", err))
	}
	if err := s.inspectStateDirectory(true); err != nil {
		return fail(err)
	}
	if err := s.openStaging(false); err != nil {
		return fail(err)
	}
	if err := s.cleanRecoveryFiles(); err != nil {
		return fail(err)
	}
	if err := s.probeCapabilities(); err != nil {
		return fail(err)
	}
	if err := s.reconcile(record, witness); err != nil {
		return fail(err)
	}
	s.bound = true
	if err := s.health(); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *directoryState) openStaging(create bool) error {
	if create {
		if err := unix.Mkdirat(s.stateFD, stagingDirectory, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return err
		}
	}
	fd, err := unix.Openat(s.stateFD, stagingDirectory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	s.stagingFD = fd
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Mode&0o777 != 0o700 || stat.Uid != uint32(unix.Geteuid()) {
		return stateFailure("staging must be an owner-only directory owned by the serving user")
	}
	mount, err := directoryMount(fd)
	if err != nil {
		return err
	}
	if mount != s.mount {
		return fmt.Errorf("staging directory is on another mount: %w", syscall.EXDEV)
	}
	if create {
		return s.ops.fsync(s.stateFD)
	}
	return nil
}
