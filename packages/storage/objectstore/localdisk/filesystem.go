package localdisk

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	manifestName         = "FORMAT"
	manifestStageName    = ".FORMAT.stage"
	ownerLockName        = "OWNER.lock"
	objectsDirectory     = "objects"
	probeSourceName      = ".publish-probe-source"
	probeTargetName      = ".publish-probe-target"
	probeOccupiedName    = ".publish-probe-occupied"
	manifestBytes        = 64
	manifestVersion      = 1
	privateDirectoryMode = 0o700
	privateFileMode      = 0o600
)

var manifestMagic = [8]byte{'R', 'F', 'S', 'O', 'B', 'J', 'S', 0}

type fileOperations struct {
	fsync    func(int) error
	fstat    func(int, *unix.Stat_t) error
	fstatfs  func(int, *unix.Statfs_t) error
	statx    func(int, string, int, int, *unix.Statx_t) error
	linkat   func(int, string, int, string, int) error
	unlinkat func(int, string, int) error
}

var systemFileOperations = fileOperations{
	fsync:    unix.Fsync,
	fstat:    unix.Fstat,
	fstatfs:  unix.Fstatfs,
	statx:    unix.Statx,
	linkat:   unix.Linkat,
	unlinkat: unix.Unlinkat,
}

type objectLocation struct {
	shard   byte
	first   string
	final   string
	staging string
}

func locate(key string) (objectLocation, error) {
	if len(key) > MaxKeyBytes {
		return objectLocation{}, fmt.Errorf("key is %d bytes; the largest local-disk object key is %d: %w",
			len(key), MaxKeyBytes, syscall.ENAMETOOLONG)
	}
	digest := sha256.Sum256([]byte(key))
	encoded := "k" + base64.RawURLEncoding.EncodeToString([]byte(key))
	return objectLocation{
		shard:   digest[0],
		first:   fmt.Sprintf("%02x", digest[0]),
		final:   encoded,
		staging: ".stage-" + encoded,
	}, nil
}

func decodeEncodedKey(name string) (string, error) {
	if !strings.HasPrefix(name, "k") {
		return "", syscall.EIO
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(name, "k"))
	if err != nil || len(decoded) > MaxKeyBytes {
		return "", syscall.EIO
	}
	key := string(decoded)
	location, err := locate(key)
	if err != nil || location.final != name {
		return "", syscall.EIO
	}
	return key, nil
}

func hasManifest(rootFD int) (bool, error) {
	fd, err := unix.Openat(rootFD, manifestName, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, err
	}
	if err := unix.Close(fd); err != nil {
		return false, err
	}
	return true, nil
}

func requirePrivateDirectory(fd int, what, path string) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return &os.PathError{Op: "stat " + what, Path: path, Err: err}
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return &os.PathError{Op: "open " + what, Path: path, Err: syscall.ENOTDIR}
	}
	if st.Uid != uint32(unix.Geteuid()) {
		return &os.PathError{Op: "open " + what, Path: path, Err: syscall.EACCES}
	}
	if fs.FileMode(st.Mode).Perm() != privateDirectoryMode {
		return &os.PathError{Op: "open " + what, Path: path, Err: fmt.Errorf("mode is %04o, want %04o: %w",
			fs.FileMode(st.Mode).Perm(), privateDirectoryMode, syscall.EACCES)}
	}
	return nil
}

func requirePrivateRegular(fd int, what, path string) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return unix.Stat_t{}, &os.PathError{Op: "stat " + what, Path: path, Err: err}
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return unix.Stat_t{}, &os.PathError{Op: "open " + what, Path: path, Err: syscall.EIO}
	}
	if st.Uid != uint32(unix.Geteuid()) || fs.FileMode(st.Mode).Perm() != privateFileMode {
		return unix.Stat_t{}, &os.PathError{Op: "open " + what, Path: path, Err: fmt.Errorf("object-store file is not private: %w", syscall.EIO)}
	}
	return st, nil
}

func rootFailure(root, action string, err error) error {
	if errors.Is(err, syscall.EOVERFLOW) || errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.EBUSY) || errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EINTR) {
		return &os.PathError{Op: action, Path: root, Err: err}
	}
	return &os.PathError{Op: action, Path: root,
		Err: fmt.Errorf("the object-store root is incomplete or corrupt: %v: %w", err, syscall.EIO)}
}

func directoryEntries(fd, maximum int) ([]string, error) {
	readFD, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(readFD), "object-store directory")
	names := make([]string, 0, maximum)
	for {
		entries, err := file.ReadDir(maximum + 1)
		for _, entry := range entries {
			names = append(names, entry.Name())
			if len(names) > maximum {
				_ = file.Close()
				return nil, fmt.Errorf("directory contains more than %d initialization entries: %w",
					maximum, syscall.ENOTEMPTY)
			}
		}
		if errors.Is(err, io.EOF) {
			return names, file.Close()
		}
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
}

func requireRecognizedInitialRoot(
	rootFD int,
	root string,
	expected filesystemIdentity,
	ops fileOperations,
) error {
	entries, err := directoryEntries(rootFD, 4)
	if err != nil {
		return rootFailure(root, "list uninitialized root", err)
	}
	for _, name := range entries {
		switch name {
		case ownerLockName, manifestStageName:
			if _, exists, err := statPrivateEntryOnFilesystem(rootFD, name, expected, ops); err != nil {
				return rootFailure(root, "validate partial object-store control file", err)
			} else if !exists {
				return rootFailure(root, "validate partial object-store control file", syscall.EIO)
			}
		case objectsDirectory:
			if err := validatePartialObjectsDirectory(rootFD, root, expected, ops); err != nil {
				return err
			}
		case InitializationMarkerName:
			fd, err := unix.Openat(rootFD, name,
				unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				return rootFailure(root, "validate composite initialization marker", err)
			}
			_, validationErr := requirePrivateRegular(fd, "composite initialization marker", filepath.Join(root, name))
			if validationErr == nil {
				validationErr = requireFilesystemIdentity(fd, expected, ops)
			}
			closeErr := unix.Close(fd)
			if validationErr != nil || closeErr != nil {
				return rootFailure(root, "validate composite initialization marker", errors.Join(validationErr, closeErr))
			}
		default:
			return &os.PathError{Op: "initialize object-store root", Path: root,
				Err: fmt.Errorf("unrecognized entry %q in a root without FORMAT: %w", name, syscall.ENOTEMPTY)}
		}
	}
	return nil
}

func validatePartialObjectsDirectory(
	rootFD int,
	root string,
	expected filesystemIdentity,
	ops fileOperations,
) error {
	path := filepath.Join(root, objectsDirectory)
	fd, err := unix.Openat(rootFD, objectsDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return rootFailure(root, "validate partial objects directory", err)
	}
	defer unix.Close(fd)
	if err := requirePrivateDirectory(fd, "partial objects directory", path); err != nil {
		return rootFailure(root, "validate partial objects directory", err)
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		return rootFailure(root, "validate partial objects filesystem", err)
	}
	entries, err := directoryEntries(fd, 4)
	if err != nil {
		return rootFailure(root, "inspect partial objects directory", err)
	}
	for _, name := range entries {
		if name != storeIdentityName && name != probeSourceName && name != probeTargetName && name != probeOccupiedName {
			return &os.PathError{Op: "initialize object-store root", Path: root,
				Err: fmt.Errorf("objects contains %q but FORMAT is absent: %w", name, syscall.ENOTEMPTY)}
		}
		if name == storeIdentityName {
			_, kind, shard, err := readDirectoryMarker(fd, expected, ops)
			if err != nil || kind != directoryKindObjects || shard != 0 {
				if err == nil {
					err = syscall.EIO
				}
				return rootFailure(root, "validate partial objects identity", err)
			}
			continue
		}
		if _, exists, err := statPrivateEntryOnFilesystem(fd, name, expected, ops); err != nil {
			return rootFailure(root, "validate partial publication probe", err)
		} else if !exists {
			return rootFailure(root, "validate partial publication probe", syscall.EIO)
		}
	}
	return nil
}

func readManifest(rootFD int, expected filesystemIdentity, ops fileOperations) (ID, error) {
	fd, err := unix.Openat(rootFD, manifestName, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ID{}, err
	}
	defer unix.Close(fd)
	st, err := requirePrivateRegular(fd, "FORMAT", manifestName)
	if err != nil {
		return ID{}, err
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		return ID{}, err
	}
	if st.Size != manifestBytes {
		return ID{}, fmt.Errorf("FORMAT is %d bytes, want %d: %w", st.Size, manifestBytes, syscall.EIO)
	}
	var encoded [manifestBytes]byte
	if err := preadFull(fd, encoded[:], 0); err != nil {
		return ID{}, err
	}
	if string(encoded[:len(manifestMagic)]) != string(manifestMagic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != manifestVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != manifestBytes ||
		binary.BigEndian.Uint32(encoded[12:16]) != 0 {
		return ID{}, fmt.Errorf("FORMAT has an unknown header: %w", syscall.EIO)
	}
	var id ID
	copy(id[:], encoded[16:32])
	digest := sha256.Sum256(encoded[:32])
	if string(encoded[32:64]) != string(digest[:]) {
		return ID{}, fmt.Errorf("FORMAT checksum does not match its contents: %w", syscall.EIO)
	}
	if !validStoreID(id) {
		return ID{}, fmt.Errorf("FORMAT has an invalid store UUID: %w", syscall.EIO)
	}
	return id, nil
}

func encodeManifest(id ID) [manifestBytes]byte {
	var encoded [manifestBytes]byte
	copy(encoded[:8], manifestMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], manifestVersion)
	binary.BigEndian.PutUint16(encoded[10:12], manifestBytes)
	copy(encoded[16:32], id[:])
	digest := sha256.Sum256(encoded[:32])
	copy(encoded[32:64], digest[:])
	return encoded
}

func writeFull(fd int, content []byte) error {
	for len(content) != 0 {
		written, err := unix.Write(fd, content)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if written == 0 {
			return syscall.EIO
		}
		content = content[written:]
	}
	return nil
}

func preadFull(fd int, content []byte, offset int64) error {
	for len(content) != 0 {
		read, err := unix.Pread(fd, content, offset)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if read == 0 {
			return syscall.EIO
		}
		content = content[read:]
		offset += int64(read)
	}
	return nil
}

func isShardComponent(name string) bool {
	if len(name) != 2 {
		return false
	}
	for _, character := range []byte(name) {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func requireNameCapacity(fd int, ops fileOperations) error {
	var st unix.Statfs_t
	if err := ops.fstatfs(fd, &st); err != nil {
		return err
	}
	longest := len(deleteMarkerPrefix) + 1 + base64.RawURLEncoding.EncodedLen(MaxKeyBytes)
	if st.Namelen > 0 && uint64(st.Namelen) < uint64(longest) {
		return fmt.Errorf("filesystem accepts %d-byte names; object recovery needs %d: %w",
			st.Namelen, longest, syscall.ENAMETOOLONG)
	}
	return nil
}

func openOwnerLock(
	rootFD int,
	root string,
	initialized bool,
	expected filesystemIdentity,
	ops fileOperations,
) (int, error) {
	flags := unix.O_RDWR | unix.O_NONBLOCK | unix.O_NOFOLLOW | unix.O_CLOEXEC
	created := false
	fd := -1
	var err error
	if !initialized {
		fd, err = unix.Openat(rootFD, ownerLockName, flags|unix.O_CREAT|unix.O_EXCL, privateFileMode)
		if err == nil {
			created = true
		} else if errors.Is(err, syscall.EEXIST) {
			fd, err = unix.Openat(rootFD, ownerLockName, flags, privateFileMode)
		}
	} else {
		fd, err = unix.Openat(rootFD, ownerLockName, flags, privateFileMode)
	}
	if err != nil {
		if initialized && errors.Is(err, syscall.ENOENT) {
			return -1, rootFailure(root, "open OWNER.lock", err)
		}
		return -1, &os.PathError{Op: "open owner lock", Path: filepath.Join(root, ownerLockName), Err: err}
	}
	if created {
		if err := unix.Fchmod(fd, privateFileMode); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "make owner lock private", Path: filepath.Join(root, ownerLockName), Err: err}
		}
		if err := unix.Fsync(fd); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "sync owner lock", Path: filepath.Join(root, ownerLockName), Err: err}
		}
	}
	if _, err := requirePrivateRegular(fd, "owner lock", filepath.Join(root, ownerLockName)); err != nil {
		_ = unix.Close(fd)
		return -1, rootFailure(root, "validate OWNER.lock", err)
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, rootFailure(root, "validate OWNER.lock filesystem", err)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = unix.Close(fd)
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = syscall.EBUSY
		}
		return -1, &os.PathError{Op: "lock owner file", Path: filepath.Join(root, ownerLockName), Err: err}
	}
	if !initialized {
		if err := unix.Fsync(rootFD); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "sync owner lock", Path: root, Err: err}
		}
	}
	return fd, nil
}

func openObjectsDirectory(
	rootFD int,
	root string,
	initialized bool,
	id ID,
	expected filesystemIdentity,
	ops fileOperations,
) (int, error) {
	fd, err := unix.Openat(rootFD, objectsDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err == nil {
		if err := requirePrivateDirectory(fd, "objects directory", filepath.Join(root, objectsDirectory)); err != nil {
			_ = unix.Close(fd)
			return -1, rootFailure(root, "validate objects directory", err)
		}
		if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
			_ = unix.Close(fd)
			return -1, rootFailure(root, "validate objects filesystem", err)
		}
		if !initialized {
			entries, err := directoryEntries(fd, 4)
			if err != nil {
				_ = unix.Close(fd)
				return -1, rootFailure(root, "inspect partial objects directory", err)
			}
			for _, name := range entries {
				if name != storeIdentityName && name != probeSourceName && name != probeTargetName && name != probeOccupiedName {
					_ = unix.Close(fd)
					return -1, &os.PathError{Op: "initialize object-store root", Path: root,
						Err: fmt.Errorf("objects contains %q but FORMAT is absent: %w", name, syscall.ENOTEMPTY)}
				}
			}
		}
		if err := requireDirectoryMarker(fd, id, directoryKindObjects, 0, expected, ops); err != nil {
			if !initialized && errors.Is(err, syscall.ENOENT) {
				if err := writeDirectoryMarker(fd, id, directoryKindObjects, 0, expected, ops); err != nil {
					_ = unix.Close(fd)
					return -1, rootFailure(root, "create objects identity", err)
				}
			} else {
				_ = unix.Close(fd)
				return -1, rootFailure(root, "validate objects identity", err)
			}
		}
		return fd, nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return -1, rootFailure(root, "open objects directory", err)
	}
	if initialized {
		return -1, rootFailure(root, "open objects directory", err)
	}
	created := false
	if err := unix.Mkdirat(rootFD, objectsDirectory, privateDirectoryMode); err != nil && !errors.Is(err, syscall.EEXIST) {
		return -1, &os.PathError{Op: "create objects directory", Path: filepath.Join(root, objectsDirectory), Err: err}
	} else if err == nil {
		created = true
	}
	if err := unix.Fsync(rootFD); err != nil {
		return -1, &os.PathError{Op: "sync objects directory", Path: root, Err: err}
	}
	fd, err = unix.Openat(rootFD, objectsDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, rootFailure(root, "open created objects directory", err)
	}
	if created {
		if err := unix.Fchmod(fd, privateDirectoryMode); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "make objects directory private", Path: filepath.Join(root, objectsDirectory), Err: err}
		}
		if err := unix.Fsync(fd); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "sync objects directory mode", Path: filepath.Join(root, objectsDirectory), Err: err}
		}
		if err := unix.Fsync(rootFD); err != nil {
			_ = unix.Close(fd)
			return -1, &os.PathError{Op: "sync objects directory", Path: root, Err: err}
		}
	}
	if err := requirePrivateDirectory(fd, "objects directory", filepath.Join(root, objectsDirectory)); err != nil {
		_ = unix.Close(fd)
		return -1, rootFailure(root, "validate created objects directory", err)
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, rootFailure(root, "validate created objects filesystem", err)
	}
	if err := writeDirectoryMarker(fd, id, directoryKindObjects, 0, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, rootFailure(root, "create objects identity", err)
	}
	return fd, nil
}

func removePrivateRegularAt(
	dirFD int,
	name string,
	expected filesystemIdentity,
	ops fileOperations,
) (bool, error) {
	fd, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, err
	}
	_, validationErr := requirePrivateRegular(fd, name, name)
	if validationErr == nil {
		validationErr = requireFilesystemIdentity(fd, expected, ops)
	}
	closeErr := unix.Close(fd)
	if validationErr != nil {
		return false, validationErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if err := ops.unlinkat(dirFD, name, 0); err != nil {
		return false, err
	}
	return true, nil
}

// probePublication establishes at Open, rather than at the first write, that the backing
// filesystem implements hard-link publication and refuses an occupied destination. These
// are the two properties Put relies on for create-if-absent atomicity.
func probePublication(objectsFD int, expected filesystemIdentity, ops fileOperations) (returned error) {
	removed := false
	cleanup := false
	defer func() {
		if !cleanup {
			return
		}
		for _, name := range []string{probeSourceName, probeTargetName, probeOccupiedName} {
			if didRemove, err := removePrivateRegularAt(objectsFD, name, expected, ops); err != nil {
				returned = errors.Join(returned, fmt.Errorf("clean publication probe %q: %w", name, err))
			} else {
				removed = removed || didRemove
			}
		}
		if removed {
			returned = errors.Join(returned, ops.fsync(objectsFD))
		}
	}()

	sourceResidue, sourceExists, err := statPrivateEntryOnFilesystem(objectsFD, probeSourceName, expected, ops)
	if err != nil {
		return fmt.Errorf("validate stale publication probe source: %w", err)
	}
	targetResidue, targetExists, err := statPrivateEntryOnFilesystem(objectsFD, probeTargetName, expected, ops)
	if err != nil {
		return fmt.Errorf("validate stale publication probe target: %w", err)
	}
	if sourceExists && targetExists && (sourceResidue.Dev != targetResidue.Dev || sourceResidue.Ino != targetResidue.Ino) {
		return fmt.Errorf("stale publication probe source and target are different inodes: %w", syscall.EIO)
	}
	cleanup = true
	for _, name := range []string{probeSourceName, probeTargetName, probeOccupiedName} {
		if didRemove, err := removePrivateRegularAt(objectsFD, name, expected, ops); err != nil {
			return fmt.Errorf("remove stale publication probe %q: %w", name, err)
		} else {
			removed = removed || didRemove
		}
	}
	source, err := unix.Openat(objectsFD, probeSourceName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return fmt.Errorf("create publication probe: %w", err)
	}
	if err := unix.Fchmod(source, privateFileMode); err != nil {
		_ = unix.Close(source)
		return fmt.Errorf("make publication probe private: %w", err)
	}
	if err := requireFilesystemIdentity(source, expected, ops); err != nil {
		_ = unix.Close(source)
		return fmt.Errorf("validate publication probe filesystem: %w", err)
	}
	if err := unix.Close(source); err != nil {
		return fmt.Errorf("close publication probe: %w", err)
	}
	if err := ops.linkat(objectsFD, probeSourceName, objectsFD, probeTargetName, 0); err != nil {
		return publicationProbeError("publish a hard link", err)
	}
	sourceStat, sourceExists, err := statPrivateEntryOnFilesystem(objectsFD, probeSourceName, expected, ops)
	if err != nil || !sourceExists {
		if err == nil {
			err = syscall.EIO
		}
		return fmt.Errorf("validate publication probe source: %w", err)
	}
	targetStat, targetExists, err := statPrivateEntryOnFilesystem(objectsFD, probeTargetName, expected, ops)
	if err != nil || !targetExists {
		if err == nil {
			err = syscall.EIO
		}
		return fmt.Errorf("validate linked publication probe: %w", err)
	}
	if sourceStat.Dev != targetStat.Dev || sourceStat.Ino != targetStat.Ino {
		return fmt.Errorf("hard-link publication produced a different inode: %w", syscall.EOPNOTSUPP)
	}
	occupiedFD, err := unix.Openat(objectsFD, probeOccupiedName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return fmt.Errorf("create occupied publication probe: %w", err)
	}
	if err := unix.Fchmod(occupiedFD, privateFileMode); err != nil {
		_ = unix.Close(occupiedFD)
		return fmt.Errorf("make occupied publication probe private: %w", err)
	}
	if err := requireFilesystemIdentity(occupiedFD, expected, ops); err != nil {
		_ = unix.Close(occupiedFD)
		return fmt.Errorf("validate occupied publication probe filesystem: %w", err)
	}
	var occupiedBefore unix.Stat_t
	if err := unix.Fstat(occupiedFD, &occupiedBefore); err != nil {
		_ = unix.Close(occupiedFD)
		return fmt.Errorf("stat occupied publication probe: %w", err)
	}
	if err := ops.linkat(objectsFD, probeSourceName, objectsFD, probeOccupiedName, 0); !errors.Is(err, syscall.EEXIST) {
		_ = unix.Close(occupiedFD)
		if err == nil {
			return fmt.Errorf("hard-link publication replaced an occupied destination: %w", syscall.EOPNOTSUPP)
		}
		return publicationProbeError("refuse an occupied hard-link destination", err)
	}
	occupiedAfter, exists, err := statPrivateEntryOnFilesystem(objectsFD, probeOccupiedName, expected, ops)
	if err != nil || !exists {
		_ = unix.Close(occupiedFD)
		if err == nil {
			err = syscall.EIO
		}
		return fmt.Errorf("restat occupied publication probe: %w", err)
	}
	if occupiedBefore.Dev != occupiedAfter.Dev || occupiedBefore.Ino != occupiedAfter.Ino {
		_ = unix.Close(occupiedFD)
		return fmt.Errorf("hard-link publication changed an occupied destination: %w", syscall.EOPNOTSUPP)
	}
	if err := unix.Close(occupiedFD); err != nil {
		return fmt.Errorf("close occupied publication probe: %w", err)
	}
	return nil
}

func publicationProbeError(action string, err error) error {
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EPERM) {
		return fmt.Errorf("hard-link publication cannot %s: %v: %w", action, err, syscall.EOPNOTSUPP)
	}
	return fmt.Errorf("hard-link publication failed to %s: %w", action, err)
}

func writeManifest(rootFD int, id ID, expected filesystemIdentity, ops fileOperations) (returned error) {
	if removed, err := removePrivateRegularAt(rootFD, manifestStageName, expected, ops); err != nil {
		return fmt.Errorf("remove stale FORMAT staging file: %w", err)
	} else if removed {
		if err := ops.fsync(rootFD); err != nil {
			return fmt.Errorf("sync stale FORMAT removal: %w", err)
		}
	}

	stageFD, err := unix.Openat(rootFD, manifestStageName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return fmt.Errorf("create FORMAT staging file: %w", err)
	}
	if err := unix.Fchmod(stageFD, privateFileMode); err != nil {
		_ = unix.Close(stageFD)
		_ = ops.unlinkat(rootFD, manifestStageName, 0)
		_ = ops.fsync(rootFD)
		return fmt.Errorf("make FORMAT staging file private: %w", err)
	}
	if err := requireFilesystemIdentity(stageFD, expected, ops); err != nil {
		_ = unix.Close(stageFD)
		_ = ops.unlinkat(rootFD, manifestStageName, 0)
		_ = ops.fsync(rootFD)
		return fmt.Errorf("validate FORMAT staging filesystem: %w", err)
	}
	stageOpen := true
	stagePresent := true
	defer func() {
		if stageOpen {
			returned = errors.Join(returned, unix.Close(stageFD))
		}
		if stagePresent {
			if err := ops.unlinkat(rootFD, manifestStageName, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
				returned = errors.Join(returned, fmt.Errorf("remove FORMAT staging file: %w", err))
			} else if err := ops.fsync(rootFD); err != nil {
				returned = errors.Join(returned, fmt.Errorf("sync FORMAT staging removal: %w", err))
			}
		}
	}()

	encoded := encodeManifest(id)
	if err := writeFull(stageFD, encoded[:]); err != nil {
		return fmt.Errorf("write FORMAT: %w", err)
	}
	if err := ops.fsync(stageFD); err != nil {
		return fmt.Errorf("sync FORMAT: %w", err)
	}
	stageOpen = false
	if err := unix.Close(stageFD); err != nil {
		return fmt.Errorf("close FORMAT: %w", err)
	}
	if err := ops.linkat(rootFD, manifestStageName, rootFD, manifestName, 0); err != nil {
		return fmt.Errorf("publish FORMAT: %w", err)
	}
	if err := ops.fsync(rootFD); err != nil {
		return fmt.Errorf("sync FORMAT publication: %w", err)
	}
	if err := ops.unlinkat(rootFD, manifestStageName, 0); err != nil {
		return fmt.Errorf("remove published FORMAT staging file: %w", err)
	}
	stagePresent = false
	if err := ops.fsync(rootFD); err != nil {
		return fmt.Errorf("sync FORMAT staging removal: %w", err)
	}
	return nil
}

func cleanupManifestStage(rootFD int, expected filesystemIdentity, ops fileOperations) error {
	stage, exists, err := statPrivateEntryOnFilesystem(rootFD, manifestStageName, expected, ops)
	if err != nil || !exists {
		return err
	}
	format, exists, err := statPrivateEntryOnFilesystem(rootFD, manifestName, expected, ops)
	if err != nil {
		return err
	}
	if !exists || stage.Dev != format.Dev || stage.Ino != format.Ino {
		return fmt.Errorf("FORMAT staging residue is not the published FORMAT inode: %w", syscall.EIO)
	}
	if err := ops.unlinkat(rootFD, manifestStageName, 0); err != nil {
		return fmt.Errorf("remove stale FORMAT staging file: %w", err)
	}
	return ops.fsync(rootFD)
}

func (o *Objects) openShard(location objectLocation, create bool) (int, bool, error) {
	return openShardDirectory(o.objectsFD, location.first, location.shard, create, o.rootPath, o.id, o.filesystem, o.ops)
}

func openShardDirectory(
	parentFD int,
	name string,
	shard byte,
	create bool,
	root string,
	id ID,
	expected filesystemIdentity,
	ops fileOperations,
) (int, bool, error) {
	fd, err := unix.Openat(parentFD, name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err == nil {
		if err := requirePrivateDirectory(fd, "object shard", filepath.Join(root, objectsDirectory, name)); err != nil {
			_ = unix.Close(fd)
			return -1, false, rootFailure(root, "validate object shard", err)
		}
		if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
			_ = unix.Close(fd)
			return -1, false, rootFailure(root, "validate object shard filesystem", err)
		}
		if err := ensureShardMarker(fd, id, shard, expected, ops, create); err != nil {
			_ = unix.Close(fd)
			return -1, false, rootFailure(root, "validate object shard identity", err)
		}
		if create {
			if err := ops.fsync(fd); err != nil {
				_ = unix.Close(fd)
				return -1, false, fmt.Errorf("sync object shard %q: %w", name, err)
			}
			if err := ops.fsync(parentFD); err != nil {
				_ = unix.Close(fd)
				return -1, false, fmt.Errorf("sync parent of object shard %q: %w", name, err)
			}
		}
		return fd, false, nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return -1, false, rootFailure(root, "open object shard", err)
	}
	if !create {
		return -1, true, nil
	}
	created := false
	if err := unix.Mkdirat(parentFD, name, privateDirectoryMode); err != nil && !errors.Is(err, syscall.EEXIST) {
		return -1, false, &os.PathError{Op: "create object shard", Path: name, Err: err}
	} else if err == nil {
		created = true
	}
	// The shard must outlive any object whose publication is synced inside it. Syncing its
	// parent before returning establishes that ordering even when this call made the shard.
	if err := ops.fsync(parentFD); err != nil {
		return -1, false, fmt.Errorf("sync object shard %q: %w", name, err)
	}
	fd, err = unix.Openat(parentFD, name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, false, rootFailure(root, "open created object shard", err)
	}
	if created {
		if err := unix.Fchmod(fd, privateDirectoryMode); err != nil {
			_ = unix.Close(fd)
			return -1, false, fmt.Errorf("make object shard %q private: %w", name, err)
		}
		if err := ops.fsync(fd); err != nil {
			_ = unix.Close(fd)
			return -1, false, fmt.Errorf("sync object shard %q mode: %w", name, err)
		}
		if err := ops.fsync(parentFD); err != nil {
			_ = unix.Close(fd)
			return -1, false, fmt.Errorf("sync object shard %q after setting its mode: %w", name, err)
		}
	}
	if err := requirePrivateDirectory(fd, "object shard", name); err != nil {
		_ = unix.Close(fd)
		return -1, false, rootFailure(root, "validate created object shard", err)
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, false, rootFailure(root, "validate created object shard filesystem", err)
	}
	if err := ensureShardMarker(fd, id, shard, expected, ops, true); err != nil {
		_ = unix.Close(fd)
		return -1, false, rootFailure(root, "create object shard identity", err)
	}
	return fd, false, nil
}

func shardByte(name string) (byte, error) {
	value, err := strconv.ParseUint(name, 16, 8)
	if err != nil || fmt.Sprintf("%02x", value) != name {
		return 0, syscall.EIO
	}
	return byte(value), nil
}
