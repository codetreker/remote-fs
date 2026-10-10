package localdisk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	putMarkerPrefix         = ".put-"
	deleteMarkerPrefix      = ".delete-"
	markerPreparationPrefix = ".prep"
	recoveryMarkerBytes     = 96
	recoveryMarkerVersion   = 1
	recoveryOperationPut    = 1
	recoveryOperationDelete = 2
)

var recoveryMarkerMagic = [8]byte{'R', 'F', 'S', 'R', 'E', 'C', 0, 0}

type recoveryMarker struct {
	name     string
	key      string
	deleting bool
}

var errRecoveryResidue = errors.New("recovery record may remain")

func markerName(key string, deleting bool) (string, error) {
	location, err := locate(key)
	if err != nil {
		return "", err
	}
	prefix := putMarkerPrefix
	if deleting {
		prefix = deleteMarkerPrefix
	}
	return prefix + location.final, nil
}

func encodeRecoveryMarker(id ID, key string, deleting bool) [recoveryMarkerBytes]byte {
	var encoded [recoveryMarkerBytes]byte
	copy(encoded[:8], recoveryMarkerMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], recoveryMarkerVersion)
	binary.BigEndian.PutUint16(encoded[10:12], recoveryMarkerBytes)
	encoded[12] = recoveryOperationPut
	if deleting {
		encoded[12] = recoveryOperationDelete
	}
	copy(encoded[16:32], id[:])
	keyHash := sha256.Sum256([]byte(key))
	copy(encoded[32:64], keyHash[:])
	digest := sha256.Sum256(encoded[:64])
	copy(encoded[64:], digest[:])
	return encoded
}

func createMarker(
	objectsFD int,
	name string,
	id ID,
	key string,
	deleting bool,
	expected filesystemIdentity,
	ops fileOperations,
) (returned error) {
	preparation := markerPreparationPrefix + name
	fd, err := unix.Openat(objectsFD, preparation,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return err
	}
	open := true
	prepared := true
	publicationAttempted := false
	defer func() {
		if open {
			returned = errors.Join(returned, unix.Close(fd))
		}
		if returned == nil {
			return
		}
		if publicationAttempted {
			returned = errors.Join(returned, errRecoveryResidue)
			return
		}
		if prepared {
			if err := ops.unlinkat(objectsFD, preparation, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
				returned = errors.Join(returned, errRecoveryResidue, err)
			} else if err := ops.fsync(objectsFD); err != nil {
				returned = errors.Join(returned, errRecoveryResidue, err)
			}
		}
	}()
	if err := unix.Fchmod(fd, privateFileMode); err != nil {
		return err
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		return err
	}
	encoded := encodeRecoveryMarker(id, key, deleting)
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
	// Only a complete, synced record may acquire recovery authority. The
	// preparation name cannot authorize an object effect after a crash.
	publicationAttempted = true
	if err := ops.linkat(objectsFD, preparation, objectsFD, name, 0); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			publicationAttempted = false
		}
		return err
	}
	if err := ops.fsync(objectsFD); err != nil {
		return err
	}
	if err := ops.unlinkat(objectsFD, preparation, 0); err != nil {
		return err
	}
	prepared = false
	if err := ops.fsync(objectsFD); err != nil {
		return err
	}
	return nil
}

type recoveryRecord struct {
	marker recoveryMarker
	stat   unix.Stat_t
	bytes  []byte
}

func readRecoveryRecord(
	objectsFD int,
	marker recoveryMarker,
	expected filesystemIdentity,
	ops fileOperations,
) (_ recoveryRecord, returned error) {
	fd, err := unix.Openat(objectsFD, marker.name,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return recoveryRecord{}, err
	}
	defer func() { returned = errors.Join(returned, unix.Close(fd)) }()
	st, err := requirePrivateRegular(fd, "recovery record", marker.name)
	if err != nil {
		return recoveryRecord{}, err
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		return recoveryRecord{}, err
	}
	if st.Size < 0 || st.Size > recoveryMarkerBytes {
		return recoveryRecord{}, fmt.Errorf("recovery record %q is %d bytes, above %d: %w",
			marker.name, st.Size, recoveryMarkerBytes, syscall.EIO)
	}
	encoded := make([]byte, int(st.Size))
	if err := preadFull(fd, encoded, 0); err != nil {
		return recoveryRecord{}, err
	}
	return recoveryRecord{marker: marker, stat: st, bytes: encoded}, nil
}

func recoveryRecordComplete(encoded []byte) bool {
	if len(encoded) != recoveryMarkerBytes {
		return false
	}
	digest := sha256.Sum256(encoded[:64])
	return string(encoded[:8]) == string(recoveryMarkerMagic[:]) &&
		binary.BigEndian.Uint16(encoded[8:10]) == recoveryMarkerVersion &&
		binary.BigEndian.Uint16(encoded[10:12]) == recoveryMarkerBytes &&
		(encoded[12] == recoveryOperationPut || encoded[12] == recoveryOperationDelete) &&
		encoded[13] == 0 && binary.BigEndian.Uint16(encoded[14:16]) == 0 &&
		string(encoded[64:]) == string(digest[:])
}

func validateRecoveryRecord(record recoveryRecord, id ID) error {
	encoded := encodeRecoveryMarker(id, record.marker.key, record.marker.deleting)
	if string(record.bytes) != string(encoded[:]) {
		return fmt.Errorf("recovery record %q is incomplete, corrupt or belongs to another store/key: %w",
			record.marker.name, syscall.EIO)
	}
	return nil
}

func removeMarker(objectsFD int, name string, ops fileOperations) error {
	if err := ops.unlinkat(objectsFD, name, 0); err != nil {
		return unexpectedAbsence(err)
	}
	return ops.fsync(objectsFD)
}

func recoverStaging(
	ctx context.Context,
	objectsFD int,
	id ID,
	limits limits,
	expected filesystemIdentity,
	ops fileOperations,
) error {
	type recoveryAction struct {
		final       *recoveryRecord
		preparation *recoveryRecord
	}
	actions := make(map[string]*recoveryAction)
	err := forEachRecoveryEntry(objectsFD, func(name string) error {
		if err := checkContext(ctx, "recover staged objects", ""); err != nil {
			return err
		}
		if name == storeIdentityName {
			return nil
		}
		if isShardComponent(name) {
			fd, err := openExistingShard(objectsFD, name, id, expected, ops)
			if err != nil {
				return err
			}
			return unix.Close(fd)
		}
		finalName := name
		preparation := strings.HasPrefix(name, markerPreparationPrefix)
		if preparation {
			finalName = strings.TrimPrefix(name, markerPreparationPrefix)
		}
		marker, ok, err := parseMarker(finalName)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("objects contains unrecognized entry %q: %w", name, syscall.EIO)
		}
		action := actions[finalName]
		if action == nil {
			if len(actions) == limits.maxRecoveryEntries {
				return fmt.Errorf("objects contains more than %d recovery actions: %w",
					limits.maxRecoveryEntries, syscall.EOVERFLOW)
			}
			action = new(recoveryAction)
			actions[finalName] = action
		}
		marker.name = name
		record, err := readRecoveryRecord(objectsFD, marker, expected, ops)
		if err != nil {
			return fmt.Errorf("read recovery record %q: %w", name, err)
		}
		if preparation {
			action.preparation = &record
		} else {
			action.final = &record
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Validate the complete set before removing even an unaccepted preparation.
	for _, action := range actions {
		if action.final != nil {
			if err := validateRecoveryRecord(*action.final, id); err != nil {
				return err
			}
		}
		links := uint64(1)
		if action.final != nil && action.preparation != nil {
			final, preparation := action.final.stat, action.preparation.stat
			if final.Dev != preparation.Dev || final.Ino != preparation.Ino {
				return fmt.Errorf("recovery final and preparation refer to different inodes: %w", syscall.EIO)
			}
			links = 2
		}
		for _, record := range []*recoveryRecord{action.final, action.preparation} {
			if record != nil && uint64(record.stat.Nlink) != links {
				return fmt.Errorf("recovery record %q has %d links, want %d: %w",
					record.marker.name, record.stat.Nlink, links, syscall.EIO)
			}
		}
		if action.preparation != nil && action.final == nil {
			if recoveryRecordComplete(action.preparation.bytes) {
				if err := validateRecoveryRecord(*action.preparation, id); err != nil {
					return err
				}
			}
			location, err := locate(action.preparation.marker.key)
			if err != nil {
				return err
			}
			shardFD, err := openExistingShard(objectsFD, location.first, id, expected, ops)
			if err != nil {
				return err
			}
			_, staged, stageErr := statPrivateEntryOnFilesystem(shardFD, location.staging, expected, ops)
			closeErr := unix.Close(shardFD)
			if stageErr != nil || closeErr != nil {
				return errors.Join(stageErr, closeErr)
			}
			if staged {
				return fmt.Errorf("unaccepted recovery preparation has object staging state: %w", syscall.EIO)
			}
		}
	}
	if len(actions) != 0 {
		if err := ops.fsync(objectsFD); err != nil {
			return err
		}
	}
	for _, action := range actions {
		if action.preparation != nil {
			if err := removeMarker(objectsFD, action.preparation.marker.name, ops); err != nil {
				return err
			}
		}
		if action.final != nil {
			if err := recoverMarker(ctx, objectsFD, id, limits, expected, action.final.marker, ops); err != nil {
				return err
			}
		}
	}
	return nil
}

func recoverMarker(
	ctx context.Context,
	objectsFD int,
	id ID,
	limits limits,
	expected filesystemIdentity,
	marker recoveryMarker,
	ops fileOperations,
) error {
	if err := checkContext(ctx, "recover staged object", marker.key); err != nil {
		return err
	}
	location, err := locate(marker.key)
	if err != nil {
		return err
	}
	shardFD, err := openExistingShard(objectsFD, location.first, id, expected, ops)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return fmt.Errorf("recovery record %q names a missing shard: %v: %w", marker.name, err, syscall.EIO)
		}
		return err
	}
	defer unix.Close(shardFD)

	_, stageExists, err := statPrivateEntryOnFilesystem(shardFD, location.staging, expected, ops)
	if err != nil {
		return err
	}
	if marker.deleting && stageExists {
		return fmt.Errorf("delete recovery record %q has impossible staging state: %w", marker.name, syscall.EIO)
	}
	if _, exists, err := statPrivateEntry(shardFD, location.final); err != nil {
		return err
	} else if exists {
		fd, opened, err := openPrivateObject(shardFD, location.final, expected, ops)
		if err != nil {
			return err
		}
		_, headerErr := readHeader(fd, opened, id, marker.key, limits.maxObjectBytes)
		closeErr := unix.Close(fd)
		if headerErr != nil || closeErr != nil {
			return errors.Join(headerErr, closeErr)
		}
		if marker.deleting {
			if err := ops.unlinkat(shardFD, location.final, 0); err != nil {
				return err
			}
		}
	}
	if stageExists {
		if err := ops.unlinkat(shardFD, location.staging, 0); err != nil {
			return err
		}
	}
	if err := ops.fsync(shardFD); err != nil {
		return err
	}
	return removeMarker(objectsFD, marker.name, ops)
}

func parseMarker(name string) (recoveryMarker, bool, error) {
	deleting := false
	encoded := ""
	switch {
	case strings.HasPrefix(name, putMarkerPrefix):
		encoded = strings.TrimPrefix(name, putMarkerPrefix)
	case strings.HasPrefix(name, deleteMarkerPrefix):
		encoded = strings.TrimPrefix(name, deleteMarkerPrefix)
		deleting = true
	default:
		return recoveryMarker{}, false, nil
	}
	key, err := decodeEncodedKey(encoded)
	if err != nil {
		return recoveryMarker{}, true, fmt.Errorf("recovery record %q has an invalid key: %w", name, err)
	}
	return recoveryMarker{name: name, key: key, deleting: deleting}, true, nil
}

func openExistingShard(
	objectsFD int,
	name string,
	id ID,
	expected filesystemIdentity,
	ops fileOperations,
) (int, error) {
	fd, err := unix.Openat(objectsFD, name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if err := requirePrivateDirectory(fd, "object shard", name); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	shard, err := shardByte(name)
	if err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	if err := ensureShardMarker(fd, id, shard, expected, ops, true); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func forEachRecoveryEntry(fd int, visit func(string) error) error {
	readFD, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(readFD), "object recovery directory")
	defer file.Close()
	for {
		entries, err := file.ReadDir(128)
		for _, entry := range entries {
			if err := visit(entry.Name()); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
