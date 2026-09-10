package localstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const initializationBindingStage = ".LOCALSTORE.init.stage"

var initializationBindingMagic = [8]byte{'R', 'F', 'S', 'I', 'N', 'I', 'T', 0}

type initializationIntent uint8

const (
	initializationIntentMissing initializationIntent = iota + 1
	initializationIntentPristine
	initializationIntentBound
)

func (a *rootAnchor) InspectInitializationIntent(
	id localdisk.ID,
	volumeName string,
	complete bool,
) (initializationIntent, error) {
	if complete {
		if exists, err := a.RequireRegularEntry(initializationBindingStage); err != nil {
			return 0, err
		} else if exists {
			return 0, fmt.Errorf(
				"a completed local store has an interrupted initialization binding: %w",
				syscall.EIO,
			)
		}
	} else if err := a.removeInitializationBindingStage(); err != nil {
		return 0, err
	}

	path := filepath.Join(a.path, localdisk.InitializationMarkerName)
	fd, err := unix.Openat(a.fd, localdisk.InitializationMarkerName,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return initializationIntentMissing, nil
		}
		return 0, &os.PathError{Op: "open local store initialization intent", Path: path, Err: err}
	}
	stat, validateErr := validatePrivateFile(fd, path, a.device, a.mount, false, a.statx)
	var encoded []byte
	readErr := error(nil)
	if validateErr == nil && stat.Size > 0 {
		if stat.Size < completionMinBytes || stat.Size > completionMaxBytes {
			validateErr = fmt.Errorf(
				"the local store initialization intent is %d bytes, want zero or between %d and %d: %w",
				stat.Size, completionMinBytes, completionMaxBytes, syscall.EIO,
			)
		} else {
			encoded = make([]byte, int(stat.Size))
			readErr = preadFull(fd, encoded)
		}
	}
	closeErr := unix.Close(fd)
	if err := errors.Join(validateErr, readErr,
		pathFailure("close local store initialization intent", path, closeErr)); err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		if complete {
			return 0, fmt.Errorf(
				"a completed local store has an unbound initialization intent: %w",
				syscall.EIO,
			)
		}
		return initializationIntentPristine, nil
	}
	if err := validateBinding(encoded, initializationBindingMagic, "initialization intent", id, volumeName); err != nil {
		return 0, err
	}
	return initializationIntentBound, nil
}

func (a *rootAnchor) BindInitialization(id localdisk.ID, volumeName string, databaseExists bool) error {
	intent, err := a.InspectInitializationIntent(id, volumeName, false)
	if err != nil {
		return err
	}
	if intent == initializationIntentBound {
		return nil
	}
	if intent == initializationIntentMissing {
		return fmt.Errorf("the local store initialization intent disappeared: %w", syscall.EIO)
	}
	if databaseExists {
		return fmt.Errorf("an unbound initialization intent already has a metadata database: %w", syscall.EIO)
	}
	if err := a.publishInitializationBinding(id, volumeName); err != nil {
		return err
	}
	return nil
}

func (a *rootAnchor) removeInitializationBindingStage() error {
	path := filepath.Join(a.path, initializationBindingStage)
	if err := unix.Unlinkat(a.fd, initializationBindingStage, 0); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return &os.PathError{Op: "remove interrupted initialization binding", Path: path, Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync initialization binding cleanup", Path: a.path, Err: err}
	}
	return nil
}

func (a *rootAnchor) publishInitializationBinding(id localdisk.ID, volumeName string) error {
	stagePath := filepath.Join(a.path, initializationBindingStage)
	fd, err := unix.Openat(a.fd, initializationBindingStage,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return &os.PathError{Op: "create initialization binding stage", Path: stagePath, Err: err}
	}
	encoded := encodeBinding(initializationBindingMagic, id, volumeName)
	writeErr := unix.Fchmod(fd, 0o600)
	if writeErr == nil {
		writeErr = writeFull(fd, encoded)
	}
	if writeErr == nil {
		writeErr = unix.Fsync(fd)
	}
	closeErr := unix.Close(fd)
	if writeErr != nil || closeErr != nil {
		cleanupErr := unix.Unlinkat(a.fd, initializationBindingStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		return errors.Join(
			pathFailure("write initialization binding stage", stagePath, writeErr),
			pathFailure("close initialization binding stage", stagePath, closeErr),
			pathFailure("remove failed initialization binding stage", stagePath, cleanupErr),
		)
	}
	if err := unix.Renameat(a.fd, initializationBindingStage, a.fd, localdisk.InitializationMarkerName); err != nil {
		return &os.PathError{Op: "publish local store initialization binding",
			Path: filepath.Join(a.path, localdisk.InitializationMarkerName), Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync local store initialization binding", Path: a.path, Err: err}
	}
	return nil
}

func (a *rootAnchor) RemoveInitializationIntent() error {
	if err := a.removeInitializationBindingStage(); err != nil {
		return err
	}
	path := filepath.Join(a.path, localdisk.InitializationMarkerName)
	if err := unix.Unlinkat(a.fd, localdisk.InitializationMarkerName, 0); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return &os.PathError{Op: "remove local store initialization intent", Path: path, Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync local store initialization completion", Path: a.path, Err: err}
	}
	return nil
}
