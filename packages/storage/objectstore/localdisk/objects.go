package localdisk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	fixedEnvelopeBytes = 72
	envelopeVersion    = 1
	writeChunkBytes    = 1024 * 1024
)

var envelopeMagic = [8]byte{'R', 'F', 'S', 'O', 'B', 'J', 0, 0}

type objectHeader struct {
	keyLength     int
	payloadLength int64
	digest        [sha256.Size]byte
}

func encodeHeader(id ID, key string, payloadLength int, digest [sha256.Size]byte) [fixedEnvelopeBytes]byte {
	var encoded [fixedEnvelopeBytes]byte
	copy(encoded[:8], envelopeMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], envelopeVersion)
	binary.BigEndian.PutUint16(encoded[10:12], fixedEnvelopeBytes)
	copy(encoded[12:28], id[:])
	binary.BigEndian.PutUint16(encoded[28:30], uint16(len(key)))
	binary.BigEndian.PutUint64(encoded[32:40], uint64(payloadLength))
	copy(encoded[40:72], digest[:])
	return encoded
}

func readHeader(fd int, st unix.Stat_t, expectedID ID, expectedKey string, maxObjectBytes int64) (objectHeader, error) {
	if st.Size < fixedEnvelopeBytes {
		return objectHeader{}, fmt.Errorf("object is %d bytes, shorter than its envelope: %w", st.Size, syscall.EIO)
	}
	var encoded [fixedEnvelopeBytes]byte
	if err := preadFull(fd, encoded[:], 0); err != nil {
		return objectHeader{}, fmt.Errorf("read object envelope: %w", err)
	}
	if string(encoded[:8]) != string(envelopeMagic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != envelopeVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != fixedEnvelopeBytes ||
		binary.BigEndian.Uint16(encoded[30:32]) != 0 {
		return objectHeader{}, fmt.Errorf("object has an unknown envelope: %w", syscall.EIO)
	}
	if string(encoded[12:28]) != string(expectedID[:]) {
		return objectHeader{}, fmt.Errorf("object belongs to another store: %w", syscall.EIO)
	}
	keyLength := int(binary.BigEndian.Uint16(encoded[28:30]))
	if keyLength > MaxKeyBytes || keyLength != len(expectedKey) {
		return objectHeader{}, fmt.Errorf("object envelope names a %d-byte key, want %d bytes: %w",
			keyLength, len(expectedKey), syscall.EIO)
	}
	payloadLength := binary.BigEndian.Uint64(encoded[32:40])
	if payloadLength > uint64(maxObjectBytes) {
		return objectHeader{}, fmt.Errorf("object envelope declares %d bytes, above the configured maximum %d: %w",
			payloadLength, maxObjectBytes, syscall.EIO)
	}
	expectedSize := uint64(fixedEnvelopeBytes+keyLength) + payloadLength
	if expectedSize > uint64(^uint64(0)>>1) || st.Size != int64(expectedSize) {
		return objectHeader{}, fmt.Errorf("object is %d bytes; its envelope describes %d: %w",
			st.Size, expectedSize, syscall.EIO)
	}
	keyBytes := make([]byte, keyLength)
	if err := preadFull(fd, keyBytes, fixedEnvelopeBytes); err != nil {
		return objectHeader{}, fmt.Errorf("read object key: %w", err)
	}
	if string(keyBytes) != expectedKey {
		return objectHeader{}, fmt.Errorf("object envelope names a different key: %w", syscall.EIO)
	}
	var digest [sha256.Size]byte
	copy(digest[:], encoded[40:72])
	return objectHeader{keyLength: keyLength, payloadLength: int64(payloadLength), digest: digest}, nil
}

func statPrivateEntry(dirFD int, name string) (unix.Stat_t, bool, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(dirFD, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return unix.Stat_t{}, false, nil
		}
		return unix.Stat_t{}, false, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(unix.Geteuid()) ||
		fs.FileMode(st.Mode).Perm() != privateFileMode {
		return unix.Stat_t{}, false, fmt.Errorf("%q is not a private regular object-store file: %w", name, syscall.EIO)
	}
	return st, true, nil
}

func statPrivateEntryOnFilesystem(
	dirFD int,
	name string,
	expected filesystemIdentity,
	ops fileOperations,
) (unix.Stat_t, bool, error) {
	st, exists, err := statPrivateEntry(dirFD, name)
	if err != nil || !exists {
		return st, exists, err
	}
	fd, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return unix.Stat_t{}, false, err
	}
	opened, validationErr := requirePrivateRegular(fd, "object-store entry", name)
	if validationErr == nil {
		validationErr = requireFilesystemIdentity(fd, expected, ops)
	}
	closeErr := unix.Close(fd)
	if validationErr != nil || closeErr != nil {
		return unix.Stat_t{}, false, errors.Join(validationErr, closeErr)
	}
	if opened.Dev != st.Dev || opened.Ino != st.Ino {
		return unix.Stat_t{}, false, fmt.Errorf("object-store entry %q changed while it was validated: %w", name, syscall.EIO)
	}
	return opened, true, nil
}

func openPrivateObject(
	dirFD int,
	name string,
	expected filesystemIdentity,
	ops fileOperations,
) (int, unix.Stat_t, error) {
	fd, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, unix.Stat_t{}, err
	}
	st, err := requirePrivateRegular(fd, "object", name)
	if err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, err
	}
	if err := requireFilesystemIdentity(fd, expected, ops); err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, err
	}
	return fd, st, nil
}

func inspectObject(
	ctx context.Context,
	shardFD int,
	location objectLocation,
	id ID,
	key string,
	maxObjectBytes int64,
	expected filesystemIdentity,
	ops fileOperations,
) (unix.Stat_t, bool, error) {
	fd, st, err := openPrivateObject(shardFD, location.final, expected, ops)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return unix.Stat_t{}, false, nil
		}
		return unix.Stat_t{}, false, fmt.Errorf("existing object cannot be validated: %v: %w", err, syscall.EIO)
	}
	_, headerErr := readHeader(fd, st, id, key, maxObjectBytes)
	closeErr := unix.Close(fd)
	if headerErr != nil || closeErr != nil {
		return unix.Stat_t{}, false, errors.Join(headerErr, closeErr)
	}
	if err := checkContext(ctx, "inspect object", key); err != nil {
		return unix.Stat_t{}, false, err
	}
	return st, true, nil
}

func contextFailure(op, key string, err error) error {
	errno := syscall.EIO
	if errors.Is(err, context.Canceled) {
		errno = syscall.EINTR
	}
	if key == "" {
		return fmt.Errorf("%s: %v: %w", op, err, errno)
	}
	return fmt.Errorf("%s %q: %v: %w", op, key, err, errno)
}

func checkContext(ctx context.Context, op, key string) error {
	if err := ctx.Err(); err != nil {
		return contextFailure(op, key, err)
	}
	return nil
}

func unexpectedAbsence(err error) error {
	if errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("a pinned object-store path disappeared: %v: %w", err, syscall.EIO)
	}
	return err
}

func (o *Objects) acquireKey(ctx context.Context, op, key string) (func(), error) {
	unlock, err := o.keys.acquire(ctx, key)
	if err != nil {
		return nil, contextFailure(op, key, err)
	}
	return unlock, nil
}

func writeContent(ctx context.Context, fd int, content []byte, op, key string) error {
	for len(content) != 0 {
		if err := checkContext(ctx, op, key); err != nil {
			return err
		}
		chunk := content
		if len(chunk) > writeChunkBytes {
			chunk = chunk[:writeChunkBytes]
		}
		if err := writeFull(fd, chunk); err != nil {
			return err
		}
		content = content[len(chunk):]
	}
	return nil
}

func digestContent(ctx context.Context, key string, content []byte) ([sha256.Size]byte, error) {
	hash := sha256.New()
	for len(content) != 0 {
		if err := checkContext(ctx, "put", key); err != nil {
			return [sha256.Size]byte{}, err
		}
		chunk := content
		if len(chunk) > writeChunkBytes {
			chunk = chunk[:writeChunkBytes]
		}
		_, _ = hash.Write(chunk)
		content = content[len(chunk):]
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func readPayload(ctx context.Context, fd int, payload []byte, offset int64, key string, expected [sha256.Size]byte) error {
	hash := sha256.New()
	for len(payload) != 0 {
		if err := checkContext(ctx, "get", key); err != nil {
			return err
		}
		chunk := payload
		if len(chunk) > writeChunkBytes {
			chunk = chunk[:writeChunkBytes]
		}
		if err := preadFull(fd, chunk, offset); err != nil {
			return err
		}
		_, _ = hash.Write(chunk)
		payload = payload[len(chunk):]
		offset += int64(len(chunk))
	}
	if !bytes.Equal(hash.Sum(nil), expected[:]) {
		return fmt.Errorf("payload checksum does not match its envelope: %w", syscall.EIO)
	}
	return nil
}

// Put stores an immutable object by publishing a synced staging inode under its final
// name with linkat(2). The link cannot replace an existing name, and both names are in the
// same shard, so publication is one atomic directory operation.
func (o *Objects) Put(ctx context.Context, key string, content []byte) (_ []byte, returned error) {
	if err := checkContext(ctx, "put", key); err != nil {
		return nil, err
	}
	location, err := locate(key)
	if err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	if int64(len(content)) > o.limits.maxObjectBytes {
		return nil, fmt.Errorf("put %q: %d bytes is above the configured maximum %d: %w",
			key, len(content), o.limits.maxObjectBytes, syscall.EFBIG)
	}
	objectBytes := int64(fixedEnvelopeBytes) + int64(len(key)) + int64(len(content))
	waiting, err := o.gate.acquireWaiting(ctx)
	if err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	var ticket *ticket
	defer func() {
		if ticket != nil {
			ticket.release()
		}
		waiting.release()
	}()
	if err := o.health.failure(); err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	defer func() { returned = o.health.finish(ctx, returned) }()
	unlock, err := o.acquireKey(ctx, "put", key)
	if err != nil {
		return nil, err
	}
	defer unlock()

	shardFD, _, err := o.openShard(ctx, location, true)
	if err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	defer unix.Close(shardFD)
	ticket, err = waiting.promote(ctx, objectBytes)
	if err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	if err := o.health.failure(); err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	if _, exists, err := inspectObject(
		ctx, shardFD, location, o.id, key, o.limits.maxObjectBytes, o.filesystem, o.ops,
	); err != nil {
		return nil, fmt.Errorf("put %q: inspect existing object: %w", key, err)
	} else if exists {
		return nil, fmt.Errorf("put %q: an object is already stored under this key: %w", key, syscall.EEXIST)
	}
	if _, exists, err := statPrivateEntryOnFilesystem(shardFD, location.staging, o.filesystem, o.ops); err != nil {
		return nil, fmt.Errorf("put %q: inspect staging object: %w", key, err)
	} else if exists {
		return nil, o.health.poison(fmt.Errorf("put %q found staging state without a recovery record", key))
	}
	if err := o.capacity.reserve(o.rootFD, objectBytes, o.limits.maintenanceReserveBytes, o.ops); err != nil {
		return nil, fmt.Errorf("put %q: reserve physical capacity: %w", key, err)
	}
	defer o.capacity.release(objectBytes)
	marker, err := markerName(key, false)
	if err != nil {
		return nil, fmt.Errorf("put %q: name recovery record: %w", key, err)
	}
	if err := createMarker(o.objectsFD, marker, o.id, key, false, o.filesystem, o.ops); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return nil, o.health.poison(fmt.Errorf("put %q found an unexpected recovery record", key))
		}
		if errors.Is(err, errRecoveryResidue) {
			return nil, errors.Join(fmt.Errorf("put %q: create recovery record: %w", key, err),
				o.health.poison(fmt.Errorf("put %q may have left an untracked recovery record", key)))
		}
		return nil, fmt.Errorf("put %q: create recovery record: %w", key, err)
	}
	o.recoveryRecords.Add(1)
	markerPresent := true
	published := false
	stagePresent := false
	stageFD := -1
	defer func() {
		if stageFD >= 0 {
			if err := unix.Close(stageFD); err != nil {
				returned = errors.Join(returned, fmt.Errorf("put %q: close staging object: %w", key, err))
			}
		}
		cleanupComplete := true
		if stagePresent {
			if err := o.ops.unlinkat(shardFD, location.staging, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
				returned = errors.Join(returned, fmt.Errorf("put %q: remove staging object: %w", key, err))
				cleanupComplete = false
			} else if err := o.ops.fsync(shardFD); err != nil {
				returned = errors.Join(returned, fmt.Errorf("put %q: sync staging removal: %w", key, err))
				cleanupComplete = false
			}
		}
		if markerPresent && cleanupComplete {
			if err := removeMarker(o.objectsFD, marker, o.ops); err != nil {
				returned = errors.Join(returned, fmt.Errorf("put %q: remove recovery record: %w", key, err))
				cleanupComplete = false
			} else {
				markerPresent = false
				o.recoveryRecords.Add(-1)
			}
		}
		if returned != nil && (!cleanupComplete || published) {
			returned = errors.Join(returned, o.health.poison(fmt.Errorf("put %q left object durability or staging state uncertain", key)))
		}
	}()

	stageFD, err = unix.Openat(shardFD, location.staging,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
	if err != nil {
		return nil, fmt.Errorf("put %q: create staging object: %w", key, unexpectedAbsence(err))
	}
	stagePresent = true
	if err := unix.Fchmod(stageFD, privateFileMode); err != nil {
		return nil, fmt.Errorf("put %q: make staging object private: %w", key, err)
	}
	if err := requireFilesystemIdentity(stageFD, o.filesystem, o.ops); err != nil {
		return nil, fmt.Errorf("put %q: validate staging object filesystem: %w", key, err)
	}
	digest, err := digestContent(ctx, key, content)
	if err != nil {
		return nil, err
	}
	header := encodeHeader(o.id, key, len(content), digest)
	if err := writeContent(ctx, stageFD, header[:], "put", key); err != nil {
		return nil, fmt.Errorf("put %q: write envelope: %w", key, err)
	}
	if err := writeContent(ctx, stageFD, []byte(key), "put", key); err != nil {
		return nil, fmt.Errorf("put %q: write key: %w", key, err)
	}
	if err := writeContent(ctx, stageFD, content, "put", key); err != nil {
		return nil, fmt.Errorf("put %q: write payload: %w", key, err)
	}
	if err := o.ops.fsync(stageFD); err != nil {
		return nil, fmt.Errorf("put %q: sync staging object: %w", key, err)
	}
	closeErr := unix.Close(stageFD)
	stageFD = -1
	if closeErr != nil {
		return nil, fmt.Errorf("put %q: close staging object: %w", key, closeErr)
	}
	if err := checkContext(ctx, "put", key); err != nil {
		return nil, err
	}
	if _, exists, err := statPrivateEntryOnFilesystem(shardFD, location.staging, o.filesystem, o.ops); err != nil {
		return nil, fmt.Errorf("put %q: validate staged object filesystem: %w", key, err)
	} else if !exists {
		return nil, fmt.Errorf("put %q: staged object disappeared before publication: %w", key, syscall.EIO)
	}
	if err := o.ops.linkat(shardFD, location.staging, shardFD, location.final, 0); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return nil, fmt.Errorf("put %q: an object is already stored under this key: %w", key, syscall.EEXIST)
		}
		return nil, fmt.Errorf("put %q: publish object: %w", key, unexpectedAbsence(err))
	}
	published = true
	if err := o.ops.fsync(shardFD); err != nil {
		return nil, fmt.Errorf("put %q: sync object publication: %w", key, err)
	}
	if err := o.ops.unlinkat(shardFD, location.staging, 0); err != nil {
		return nil, fmt.Errorf("put %q: remove staging name: %w", key, unexpectedAbsence(err))
	}
	if err := o.ops.fsync(shardFD); err != nil {
		return nil, fmt.Errorf("put %q: sync staging cleanup: %w", key, err)
	}
	stagePresent = false
	if err := removeMarker(o.objectsFD, marker, o.ops); err != nil {
		return nil, fmt.Errorf("put %q: remove recovery record: %w", key, err)
	}
	markerPresent = false
	o.recoveryRecords.Add(-1)
	// There is no lower service able to report a digest independently of the buffer sent,
	// so nil is the only digest that satisfies objectstore.Objects' contract here.
	return nil, nil
}

// Get verifies the file type, ownership, mode, format, store identity, original key,
// declared length, and SHA-256 before returning any bytes.
func (o *Objects) Get(ctx context.Context, key string) ([]byte, error) {
	return o.get(ctx, key, 0, false)
}

// GetBounded applies a caller-provided payload limit after validating the envelope and
// before reserving or allocating payload bytes. maxBytes must be positive; an object above
// it fails with EFBIG without returning a prefix.
func (o *Objects) GetBounded(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("get %q: payload limit %d is not positive: %w", key, maxBytes, syscall.EINVAL)
	}
	return o.get(ctx, key, maxBytes, true)
}

func (o *Objects) get(ctx context.Context, key string, maxBytes int64, bounded bool) (returnedPayload []byte, returned error) {
	if err := checkContext(ctx, "get", key); err != nil {
		return nil, err
	}
	location, err := locate(key)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	waiting, err := o.gate.acquireWaiting(ctx)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	var ticket *ticket
	defer func() {
		if ticket != nil {
			ticket.release()
		}
		waiting.release()
	}()
	if err := o.health.failure(); err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	defer func() {
		returned = o.health.finish(ctx, returned)
		if returned != nil {
			returnedPayload = nil
		}
	}()
	shardFD, absent, err := o.openShard(ctx, location, false)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if !absent {
		defer unix.Close(shardFD)
	}
	ticket, err = waiting.promote(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if err := o.health.failure(); err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if absent {
		return nil, fmt.Errorf("get %q: nothing is stored under this key: %w", key, syscall.ENOENT)
	}
	fd, st, err := openPrivateObject(shardFD, location.final, o.filesystem, o.ops)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil, fmt.Errorf("get %q: nothing is stored under this key: %w", key, syscall.ENOENT)
		}
		return nil, fmt.Errorf("get %q: open object: %v: %w", key, err, syscall.EIO)
	}
	defer unix.Close(fd)
	header, err := readHeader(fd, st, o.id, key, o.limits.maxObjectBytes)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if bounded && header.payloadLength > maxBytes {
		return nil, fmt.Errorf(
			"get %q: payload is %d bytes, above the %d-byte limit: %w",
			key, header.payloadLength, maxBytes, syscall.EFBIG,
		)
	}
	if err := ticket.addBytes(ctx, header.payloadLength); err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	if int64(int(header.payloadLength)) != header.payloadLength {
		return nil, fmt.Errorf("get %q: object is too large for this process: %w", key, syscall.EFBIG)
	}
	payload := make([]byte, int(header.payloadLength))
	if err := readPayload(ctx, fd, payload, int64(fixedEnvelopeBytes+header.keyLength), key, header.digest); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("get %q: %w", key, err)
		}
		return nil, fmt.Errorf("get %q: read payload: %v: %w", key, err, syscall.EIO)
	}
	if err := checkContext(ctx, "get", key); err != nil {
		return nil, err
	}
	return payload, nil
}

// Delete validates and removes the published name, then syncs the shard. A missing object
// is already the requested state, but its existing shard is still synced so a retry can
// finish an unlink whose earlier directory barrier failed.
func (o *Objects) Delete(ctx context.Context, key string) (returned error) {
	if err := checkContext(ctx, "delete", key); err != nil {
		return err
	}
	location, err := locate(key)
	if err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	waiting, err := o.gate.acquireWaiting(ctx)
	if err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	var ticket *ticket
	defer func() {
		if ticket != nil {
			ticket.release()
		}
		waiting.release()
	}()
	if err := o.health.failure(); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	defer func() { returned = o.health.finish(ctx, returned) }()
	unlock, err := o.acquireKey(ctx, "delete", key)
	if err != nil {
		return err
	}
	defer unlock()

	shardFD, absent, err := o.openShard(ctx, location, false)
	if err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	if !absent {
		defer unix.Close(shardFD)
	}
	ticket, err = waiting.promote(ctx, 0)
	if err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	if err := o.health.failure(); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	if absent {
		return nil
	}
	_, finalExists, err := inspectObject(
		ctx, shardFD, location, o.id, key, o.limits.maxObjectBytes, o.filesystem, o.ops,
	)
	if err != nil {
		return fmt.Errorf("delete %q: inspect object: %w", key, err)
	}
	_, stageExists, err := statPrivateEntryOnFilesystem(shardFD, location.staging, o.filesystem, o.ops)
	if err != nil {
		return fmt.Errorf("delete %q: inspect staging object: %w", key, err)
	}
	if !finalExists && !stageExists {
		if err := o.ops.fsync(shardFD); err != nil {
			return errors.Join(
				fmt.Errorf("delete %q: sync already-absent object: %w", key, err),
				o.health.poison(fmt.Errorf("delete %q could not make an already-absent name durable", key)),
			)
		}
		return nil
	}
	if stageExists {
		return o.health.poison(fmt.Errorf("delete %q found staging state without a recovery record", key))
	}
	if err := checkContext(ctx, "delete", key); err != nil {
		return err
	}
	marker, err := markerName(key, true)
	if err != nil {
		return fmt.Errorf("delete %q: name recovery record: %w", key, err)
	}
	if err := createMarker(o.objectsFD, marker, o.id, key, true, o.filesystem, o.ops); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return o.health.poison(fmt.Errorf("delete %q found an unexpected recovery record", key))
		}
		if errors.Is(err, errRecoveryResidue) {
			return errors.Join(fmt.Errorf("delete %q: create recovery record: %w", key, err),
				o.health.poison(fmt.Errorf("delete %q may have left an untracked recovery record", key)))
		}
		return fmt.Errorf("delete %q: create recovery record: %w", key, err)
	}
	o.recoveryRecords.Add(1)
	markerPresent := true
	mutated := false
	defer func() {
		if markerPresent && !mutated {
			if err := removeMarker(o.objectsFD, marker, o.ops); err != nil {
				o.health.poison(fmt.Errorf("delete %q could not clean an unused recovery record: %v", key, err))
			} else {
				o.recoveryRecords.Add(-1)
			}
		}
	}()
	if finalExists {
		if err := o.ops.unlinkat(shardFD, location.final, 0); err != nil {
			return fmt.Errorf("delete %q: unlink object: %w", key, unexpectedAbsence(err))
		}
		mutated = true
	}
	if err := o.ops.fsync(shardFD); err != nil {
		return errors.Join(
			fmt.Errorf("delete %q: sync object removal: %w", key, err),
			o.health.poison(fmt.Errorf("delete %q could not establish whether removal is durable", key)),
		)
	}
	if err := removeMarker(o.objectsFD, marker, o.ops); err != nil {
		return errors.Join(
			fmt.Errorf("delete %q: remove recovery record: %w", key, err),
			o.health.poison(fmt.Errorf("delete %q left completed recovery state behind", key)),
		)
	}
	markerPresent = false
	o.recoveryRecords.Add(-1)
	return nil
}
