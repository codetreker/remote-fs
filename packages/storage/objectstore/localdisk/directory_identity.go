package localdisk

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	storeIdentityName      = ".store-identity"
	directoryMarkerBytes   = 64
	directoryMarkerVersion = 1
	directoryKindObjects   = 1
	directoryKindShard     = 2
)

var directoryMarkerMagic = [8]byte{'R', 'F', 'S', 'D', 'I', 'R', 0, 0}

func encodeDirectoryMarker(id ID, kind byte, shard byte) [directoryMarkerBytes]byte {
	var encoded [directoryMarkerBytes]byte
	copy(encoded[:8], directoryMarkerMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], directoryMarkerVersion)
	binary.BigEndian.PutUint16(encoded[10:12], directoryMarkerBytes)
	encoded[12] = kind
	encoded[13] = shard
	copy(encoded[16:32], id[:])
	digest := sha256.Sum256(encoded[:32])
	copy(encoded[32:], digest[:])
	return encoded
}

func readDirectoryMarker(
	dirFD int,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (ID, byte, byte, error) {
	fd, err := unix.Openat(dirFD, storeIdentityName,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ID{}, 0, 0, err
	}
	defer unix.Close(fd)
	st, err := requirePrivateRegular(fd, "object directory identity", storeIdentityName)
	if err != nil {
		return ID{}, 0, 0, err
	}
	if err := requireFilesystemIdentity(fd, expectedFilesystem, ops); err != nil {
		return ID{}, 0, 0, err
	}
	if st.Size != directoryMarkerBytes {
		return ID{}, 0, 0, fmt.Errorf("object directory identity is %d bytes, want %d: %w",
			st.Size, directoryMarkerBytes, syscall.EIO)
	}
	var encoded [directoryMarkerBytes]byte
	if err := preadFull(fd, encoded[:], 0); err != nil {
		return ID{}, 0, 0, err
	}
	digest := sha256.Sum256(encoded[:32])
	if string(encoded[:8]) != string(directoryMarkerMagic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != directoryMarkerVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != directoryMarkerBytes ||
		binary.BigEndian.Uint16(encoded[14:16]) != 0 ||
		string(encoded[32:]) != string(digest[:]) {
		return ID{}, 0, 0, fmt.Errorf("object directory identity is corrupt: %w", syscall.EIO)
	}
	var id ID
	copy(id[:], encoded[16:32])
	if !validStoreID(id) {
		return ID{}, 0, 0, fmt.Errorf("object directory identity has an invalid store UUID: %w", syscall.EIO)
	}
	return id, encoded[12], encoded[13], nil
}

func partialObjectsID(
	rootFD int,
	root string,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (ID, error) {
	fd, err := unix.Openat(rootFD, objectsDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ID{}, err
	}
	defer unix.Close(fd)
	if err := requirePrivateDirectory(fd, "partial objects directory", root); err != nil {
		return ID{}, err
	}
	if err := requireFilesystemIdentity(fd, expectedFilesystem, ops); err != nil {
		return ID{}, err
	}
	id, kind, shard, err := readDirectoryMarker(fd, expectedFilesystem, ops)
	if err != nil {
		return ID{}, err
	}
	if kind != directoryKindObjects || shard != 0 {
		return ID{}, fmt.Errorf("partial objects identity has the wrong directory kind: %w", syscall.EIO)
	}
	return id, nil
}

func writeDirectoryMarker(
	dirFD int,
	id ID,
	kind byte,
	shard byte,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (returned error) {
	fd, err := unix.Openat(dirFD, storeIdentityName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return err
	}
	open := true
	created := true
	defer func() {
		if open {
			returned = errors.Join(returned, unix.Close(fd))
		}
		if returned != nil && created {
			if err := ops.unlinkat(dirFD, storeIdentityName, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
				returned = errors.Join(returned, err)
			} else {
				returned = errors.Join(returned, ops.fsync(dirFD))
			}
		}
	}()
	if err := unix.Fchmod(fd, privateFileMode); err != nil {
		return err
	}
	if err := requireFilesystemIdentity(fd, expectedFilesystem, ops); err != nil {
		return err
	}
	encoded := encodeDirectoryMarker(id, kind, shard)
	if err := writeFull(fd, encoded[:]); err != nil {
		return err
	}
	if err := ops.fsync(fd); err != nil {
		return err
	}
	open = false
	if err := unix.Close(fd); err != nil {
		return err
	}
	if err := ops.fsync(dirFD); err != nil {
		return err
	}
	created = false
	return nil
}

func requireDirectoryMarker(
	dirFD int,
	id ID,
	kind byte,
	shard byte,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) error {
	actualID, actualKind, actualShard, err := readDirectoryMarker(dirFD, expectedFilesystem, ops)
	if err != nil {
		return err
	}
	if actualID != id || actualKind != kind || actualShard != shard {
		return fmt.Errorf("object directory identity belongs to another store or shard: %w", syscall.EIO)
	}
	return nil
}

func ensureShardMarker(
	dirFD int,
	id ID,
	shard byte,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
	allowCreate bool,
) error {
	err := requireDirectoryMarker(dirFD, id, directoryKindShard, shard, expectedFilesystem, ops)
	if err == nil || !errors.Is(err, syscall.ENOENT) || !allowCreate {
		return err
	}
	entries, listErr := directoryEntries(dirFD, 1)
	if listErr != nil {
		return listErr
	}
	if len(entries) != 0 {
		return fmt.Errorf("object shard has entries but no store identity: %w", syscall.EIO)
	}
	return writeDirectoryMarker(dirFD, id, directoryKindShard, shard, expectedFilesystem, ops)
}
