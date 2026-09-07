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
	storeIdentityStageName = ".store-identity.stage"
	directoryMarkerBytes   = 64
	directoryMarkerVersion = 1
	directoryKindObjects   = 1
	directoryKindShard     = 2
)

var directoryMarkerMagic = [8]byte{'R', 'F', 'S', 'D', 'I', 'R', 0, 0}

var errShardIdentityPublicationUncertain = fmt.Errorf("shard identity publication is uncertain: %w", syscall.EIO)

type directoryMarker struct {
	stat  unix.Stat_t
	id    ID
	kind  byte
	shard byte
}

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
	marker, err := readNamedDirectoryMarker(dirFD, storeIdentityName, expectedFilesystem, ops)
	return marker.id, marker.kind, marker.shard, err
}

func readNamedDirectoryMarker(
	dirFD int,
	name string,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (directoryMarker, error) {
	fd, err := unix.Openat(dirFD, name,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return directoryMarker{}, err
	}
	defer unix.Close(fd)
	st, err := requirePrivateRegular(fd, "object directory identity", name)
	if err != nil {
		return directoryMarker{}, err
	}
	if err := requireFilesystemIdentity(fd, expectedFilesystem, ops); err != nil {
		return directoryMarker{}, err
	}
	if st.Size != directoryMarkerBytes {
		return directoryMarker{}, fmt.Errorf("object directory identity %q is %d bytes, want %d: %w",
			name,
			st.Size, directoryMarkerBytes, syscall.EIO)
	}
	var encoded [directoryMarkerBytes]byte
	if err := preadFull(fd, encoded[:], 0); err != nil {
		return directoryMarker{}, err
	}
	digest := sha256.Sum256(encoded[:32])
	if string(encoded[:8]) != string(directoryMarkerMagic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != directoryMarkerVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != directoryMarkerBytes ||
		binary.BigEndian.Uint16(encoded[14:16]) != 0 ||
		string(encoded[32:]) != string(digest[:]) {
		return directoryMarker{}, fmt.Errorf("object directory identity %q is corrupt: %w", name, syscall.EIO)
	}
	var id ID
	copy(id[:], encoded[16:32])
	if !validStoreID(id) {
		return directoryMarker{}, fmt.Errorf("object directory identity %q has an invalid store UUID: %w", name, syscall.EIO)
	}
	return directoryMarker{stat: st, id: id, kind: encoded[12], shard: encoded[13]}, nil
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

func writeObjectsDirectoryMarker(
	dirFD int,
	id ID,
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
	encoded := encodeDirectoryMarker(id, directoryKindObjects, 0)
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
	final, finalExists, err := optionalDirectoryMarker(
		dirFD, storeIdentityName, expectedFilesystem, ops,
	)
	if err != nil {
		return err
	}
	stage, stageExists, err := optionalDirectoryMarker(
		dirFD, storeIdentityStageName, expectedFilesystem, ops,
	)
	if err != nil {
		return err
	}
	if finalExists {
		if err := requireExpectedShardMarker(final, id, shard, storeIdentityName); err != nil {
			return err
		}
		if !stageExists {
			return requireDirectoryMarkerLinks(final, 1, storeIdentityName)
		}
		if err := requireExpectedShardMarker(stage, id, shard, storeIdentityStageName); err != nil {
			return err
		}
		if final.stat.Dev != stage.stat.Dev || final.stat.Ino != stage.stat.Ino {
			return fmt.Errorf("shard identity final and staging names refer to different inodes: %w", syscall.EIO)
		}
		if err := requireOnlyPublishedShardIdentity(dirFD); err != nil {
			return err
		}
		if err := errors.Join(
			requireDirectoryMarkerLinks(final, 2, storeIdentityName),
			requireDirectoryMarkerLinks(stage, 2, storeIdentityStageName),
		); err != nil {
			return err
		}
		return removePublishedShardMarkerStage(dirFD, ops, true)
	}
	if stageExists {
		if err := requireExpectedShardMarker(stage, id, shard, storeIdentityStageName); err != nil {
			return err
		}
		if err := requireDirectoryMarkerLinks(stage, 1, storeIdentityStageName); err != nil {
			return err
		}
		if err := requireOnlyShardIdentityStage(dirFD); err != nil {
			return err
		}
		if !allowCreate {
			return fmt.Errorf("object shard has an unpublished identity marker: %w", syscall.EIO)
		}
		return publishShardMarkerStage(dirFD, id, shard, expectedFilesystem, ops)
	}
	if !allowCreate {
		return syscall.ENOENT
	}
	entries, listErr := directoryEntries(dirFD, 1)
	if listErr != nil {
		return listErr
	}
	if len(entries) != 0 {
		return fmt.Errorf("object shard has entries but no store identity: %w", syscall.EIO)
	}
	if err := createShardMarkerStage(dirFD, id, shard, expectedFilesystem, ops); err != nil {
		return err
	}
	return publishShardMarkerStage(dirFD, id, shard, expectedFilesystem, ops)
}

func optionalDirectoryMarker(
	dirFD int,
	name string,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (directoryMarker, bool, error) {
	marker, err := readNamedDirectoryMarker(dirFD, name, expectedFilesystem, ops)
	if errors.Is(err, syscall.ENOENT) {
		return directoryMarker{}, false, nil
	}
	if err != nil {
		return directoryMarker{}, false, err
	}
	return marker, true, nil
}

func requireExpectedShardMarker(marker directoryMarker, id ID, shard byte, name string) error {
	if marker.id != id || marker.kind != directoryKindShard || marker.shard != shard {
		return fmt.Errorf("object shard identity %q belongs to another store or shard: %w", name, syscall.EIO)
	}
	return nil
}

func requireDirectoryMarkerLinks(marker directoryMarker, want uint64, name string) error {
	if marker.stat.Nlink != want {
		return fmt.Errorf("object shard identity %q has %d links, want %d: %w",
			name, marker.stat.Nlink, want, syscall.EIO)
	}
	return nil
}

func requireOnlyShardIdentityStage(dirFD int) error {
	entries, err := directoryEntries(dirFD, 1)
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0] != storeIdentityStageName {
		return fmt.Errorf("object shard has entries beside an unpublished identity marker: %w", syscall.EIO)
	}
	return nil
}

func requireOnlyPublishedShardIdentity(dirFD int) error {
	entries, err := directoryEntries(dirFD, 2)
	if err != nil {
		return err
	}
	if len(entries) != 2 {
		return fmt.Errorf("published object shard identity has unexpected directory entries: %w", syscall.EIO)
	}
	foundFinal := false
	foundStage := false
	for _, name := range entries {
		switch name {
		case storeIdentityName:
			foundFinal = true
		case storeIdentityStageName:
			foundStage = true
		default:
			return fmt.Errorf("published object shard identity has unexpected entry %q: %w", name, syscall.EIO)
		}
	}
	if !foundFinal || !foundStage {
		return fmt.Errorf("published object shard identity names changed during validation: %w", syscall.EIO)
	}
	return nil
}

func createShardMarkerStage(
	dirFD int,
	id ID,
	shard byte,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) (returned error) {
	fd, err := unix.Openat(dirFD, storeIdentityStageName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return err
	}
	open := true
	defer func() {
		if open {
			returned = errors.Join(returned, unix.Close(fd))
		}
		if returned == nil {
			return
		}
		if cleanupErr := discardShardMarkerStage(dirFD, ops); cleanupErr != nil {
			returned = errors.Join(returned, cleanupErr, errShardIdentityPublicationUncertain)
		}
	}()
	if err := unix.Fchmod(fd, privateFileMode); err != nil {
		return err
	}
	if err := requireFilesystemIdentity(fd, expectedFilesystem, ops); err != nil {
		return err
	}
	encoded := encodeDirectoryMarker(id, directoryKindShard, shard)
	if err := writeFull(fd, encoded[:]); err != nil {
		return err
	}
	if err := ops.fsync(fd); err != nil {
		return err
	}
	open = false
	return unix.Close(fd)
}

func publishShardMarkerStage(
	dirFD int,
	id ID,
	shard byte,
	expectedFilesystem filesystemIdentity,
	ops fileOperations,
) error {
	if err := ops.linkat(dirFD, storeIdentityStageName, dirFD, storeIdentityName, 0); err != nil {
		final, finalExists, inspectFinalErr := optionalDirectoryMarker(
			dirFD, storeIdentityName, expectedFilesystem, ops,
		)
		stage, stageExists, inspectStageErr := optionalDirectoryMarker(
			dirFD, storeIdentityStageName, expectedFilesystem, ops,
		)
		if inspectFinalErr != nil || inspectStageErr != nil {
			return errors.Join(err, inspectFinalErr, inspectStageErr, errShardIdentityPublicationUncertain)
		}
		if finalExists && stageExists && final.stat.Dev == stage.stat.Dev && final.stat.Ino == stage.stat.Ino {
			return errors.Join(err, errShardIdentityPublicationUncertain)
		}
		if finalExists || !stageExists {
			return errors.Join(
				err,
				fmt.Errorf("shard identity changed during publication: %w", syscall.EIO),
				errShardIdentityPublicationUncertain,
			)
		}
		if validationErr := errors.Join(
			requireExpectedShardMarker(stage, id, shard, storeIdentityStageName),
			requireOnlyShardIdentityStage(dirFD),
		); validationErr != nil {
			return errors.Join(err, validationErr)
		}
		if cleanupErr := discardShardMarkerStage(dirFD, ops); cleanupErr != nil {
			return errors.Join(err, cleanupErr, errShardIdentityPublicationUncertain)
		}
		return err
	}
	final, finalExists, inspectFinalErr := optionalDirectoryMarker(
		dirFD, storeIdentityName, expectedFilesystem, ops,
	)
	stage, stageExists, inspectStageErr := optionalDirectoryMarker(
		dirFD, storeIdentityStageName, expectedFilesystem, ops,
	)
	if inspectFinalErr != nil || inspectStageErr != nil || !finalExists || !stageExists {
		return errors.Join(inspectFinalErr, inspectStageErr,
			fmt.Errorf("published shard identity names cannot be validated: %w", syscall.EIO),
			errShardIdentityPublicationUncertain)
	}
	if validationErr := errors.Join(
		requireExpectedShardMarker(final, id, shard, storeIdentityName),
		requireExpectedShardMarker(stage, id, shard, storeIdentityStageName),
		requireDirectoryMarkerLinks(final, 2, storeIdentityName),
		requireDirectoryMarkerLinks(stage, 2, storeIdentityStageName),
	); validationErr != nil || final.stat.Dev != stage.stat.Dev || final.stat.Ino != stage.stat.Ino {
		if validationErr == nil {
			validationErr = fmt.Errorf("published shard identity names refer to different inodes: %w", syscall.EIO)
		}
		return errors.Join(validationErr, errShardIdentityPublicationUncertain)
	}
	if err := ops.fsync(dirFD); err != nil {
		return errors.Join(err, errShardIdentityPublicationUncertain)
	}
	return removePublishedShardMarkerStage(dirFD, ops, false)
}

func removePublishedShardMarkerStage(dirFD int, ops fileOperations, syncPublication bool) error {
	// Sync before removing the staging name so final+stage always means a recoverable
	// publication, never an inode whose final name may still disappear after a crash.
	if syncPublication {
		if err := ops.fsync(dirFD); err != nil {
			return errors.Join(err, errShardIdentityPublicationUncertain)
		}
	}
	if err := ops.unlinkat(dirFD, storeIdentityStageName, 0); err != nil {
		return errors.Join(err, errShardIdentityPublicationUncertain)
	}
	if err := ops.fsync(dirFD); err != nil {
		return errors.Join(err, errShardIdentityPublicationUncertain)
	}
	return nil
}

func discardShardMarkerStage(dirFD int, ops fileOperations) error {
	if err := ops.unlinkat(dirFD, storeIdentityStageName, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
		return err
	}
	return ops.fsync(dirFD)
}
