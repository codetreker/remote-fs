package localdisk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func prepareCompositeInitialization(
	rootFD int,
	root string,
	initialized bool,
	requested bool,
	expected filesystemIdentity,
	ops fileOperations,
) (CompositeInitializationState, error) {
	if !requested {
		return NoCompositeInitialization, nil
	}
	exists, err := validateCompositeInitializationMarker(rootFD, root, expected, ops)
	if err != nil {
		return NoCompositeInitialization, err
	}
	if exists {
		return CompositeInitializationResumed, nil
	}
	if initialized {
		return NoCompositeInitialization, nil
	}
	if err := createCompositeInitializationMarker(rootFD, root, ops); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			exists, validationErr := validateCompositeInitializationMarker(rootFD, root, expected, ops)
			if validationErr == nil && exists {
				return CompositeInitializationResumed, nil
			}
			return NoCompositeInitialization, errors.Join(err, validationErr)
		}
		return NoCompositeInitialization, err
	}
	return CompositeInitializationStarted, nil
}

func validateCompositeInitializationMarker(
	rootFD int,
	root string,
	expected filesystemIdentity,
	ops fileOperations,
) (bool, error) {
	path := filepath.Join(root, InitializationMarkerName)
	fd, err := unix.Openat(rootFD, InitializationMarkerName,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, rootFailure(root, "validate composite initialization marker", err)
	}
	_, validationErr := requirePrivateRegular(fd, "composite initialization marker", path)
	if validationErr == nil {
		validationErr = requireFilesystemIdentity(fd, expected, ops)
	}
	closeErr := unix.Close(fd)
	if validationErr != nil || closeErr != nil {
		return false, rootFailure(root, "validate composite initialization marker",
			errors.Join(validationErr, closeErr))
	}
	return true, nil
}

func createCompositeInitializationMarker(rootFD int, root string, ops fileOperations) (returned error) {
	path := filepath.Join(root, InitializationMarkerName)
	fd, err := unix.Openat(rootFD, InitializationMarkerName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return &os.PathError{Op: "create composite initialization marker", Path: path, Err: err}
	}
	open := true
	remove := true
	defer func() {
		if open {
			returned = errors.Join(returned, unix.Close(fd))
		}
		if returned != nil && remove {
			if err := ops.unlinkat(rootFD, InitializationMarkerName, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
				returned = errors.Join(returned, fmt.Errorf("remove incomplete composite initialization marker: %w", err))
			} else if err := ops.fsync(rootFD); err != nil {
				returned = errors.Join(returned, fmt.Errorf("sync incomplete composite initialization marker removal: %w", err))
			}
		}
	}()
	if err := unix.Fchmod(fd, privateFileMode); err != nil {
		return &os.PathError{Op: "make composite initialization marker private", Path: path, Err: err}
	}
	if err := ops.fsync(fd); err != nil {
		return &os.PathError{Op: "sync composite initialization marker", Path: path, Err: err}
	}
	open = false
	if err := unix.Close(fd); err != nil {
		return &os.PathError{Op: "close composite initialization marker", Path: path, Err: err}
	}
	if err := ops.fsync(rootFD); err != nil {
		return &os.PathError{Op: "publish composite initialization marker", Path: root, Err: err}
	}
	remove = false
	return nil
}
