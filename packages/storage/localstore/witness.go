package localstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	metastoreWitnessFilename    = "METASTORE"
	metastoreWitnessStage       = ".METASTORE.stage"
	metastoreWitnessVersion     = 1
	metastoreWitnessHeaderBytes = 80
	metastoreWitnessDigestBytes = sha256.Size
	durableDatabaseIDBytes      = 32
	metastoreWitnessMinBytes    = metastoreWitnessHeaderBytes + metastoreWitnessDigestBytes
	metastoreWitnessMaxBytes    = metastoreWitnessMinBytes + MaxVolumeBytes + durableDatabaseIDBytes
)

var metastoreWitnessMagic = [8]byte{'R', 'F', 'S', 'M', 'E', 'T', 'A', 0}

type metastoreWitnessRecord struct {
	StoreID                localdisk.ID
	Volume                 string
	State                  sqlite.DurableState
	CheckpointedGeneration int64
}

type metastoreWitness struct {
	mu         sync.Mutex
	anchor     *rootAnchor
	record     metastoreWitnessRecord
	exists     bool
	checkpoint chan struct{}
}

var _ sqlite.CommitWitness = (*metastoreWitness)(nil)

func (a *rootAnchor) InspectMetastoreWitness(
	id localdisk.ID,
	volumeName string,
) (*metastoreWitness, bool, bool, error) {
	final, finalExists, err := a.readMetastoreWitnessEntry(metastoreWitnessFilename, id, volumeName)
	if err != nil {
		return nil, false, false, err
	}
	stageExists, err := a.inspectMetastoreWitnessStage(id, volumeName, final.State.DatabaseID, finalExists)
	if err != nil {
		return nil, false, false, err
	}
	if !finalExists {
		final = metastoreWitnessRecord{StoreID: id, Volume: volumeName}
	}
	return &metastoreWitness{
		anchor: a, record: final, exists: finalExists, checkpoint: make(chan struct{}, 1),
	}, finalExists, stageExists, nil
}

func (w *metastoreWitness) RemoveInterruptedStage() error {
	if err := w.anchor.witnessOps.unlinkat(w.anchor.fd, metastoreWitnessStage, 0); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return witnessPathFailure("remove interrupted local store metastore witness stage", filepath.Join(w.anchor.path, metastoreWitnessStage), err)
	}
	if err := w.anchor.witnessOps.fsync(w.anchor.fd); err != nil {
		return witnessPathFailure("sync interrupted local store metastore witness cleanup", w.anchor.path, err)
	}
	return nil
}

func (a *rootAnchor) readMetastoreWitnessEntry(
	name string,
	id localdisk.ID,
	volumeName string,
) (metastoreWitnessRecord, bool, error) {
	encoded, exists, err := a.readMetastoreWitnessBytes(name)
	if err != nil || !exists {
		return metastoreWitnessRecord{}, exists, err
	}
	record, err := decodeMetastoreWitness(encoded, id, volumeName)
	return record, true, err
}

// File structure and read/close outcomes must be known before stage contents
// can be classified as an interrupted write. Only the byte parser is relaxable.
func (a *rootAnchor) readMetastoreWitnessBytes(name string) ([]byte, bool, error) {
	path := filepath.Join(a.path, name)
	fd, err := a.witnessOps.openat(a.fd, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil, false, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			err = fmt.Errorf("the entry is a symbolic link: %w", err)
		}
		return nil, false, witnessPathFailure("open local store metastore witness", path, err)
	}
	stat, validateErr := validatePrivateFile(fd, path, a.device, a.mount, false, a.statx)
	var encoded []byte
	readErr := error(nil)
	if validateErr == nil {
		if stat.Size < 0 || stat.Size > metastoreWitnessMaxBytes {
			validateErr = fmt.Errorf("the metastore witness is %d bytes, want at most %d: %w",
				stat.Size, metastoreWitnessMaxBytes, syscall.EIO)
		} else {
			encoded = make([]byte, int(stat.Size))
			readErr = a.witnessOps.readFull(fd, encoded)
		}
	}
	closeErr := a.witnessOps.close(fd)
	if err := errors.Join(validateErr,
		witnessPathFailure("read local store metastore witness", path, readErr),
		witnessPathFailure("close local store metastore witness", path, closeErr)); err != nil {
		return nil, false, errors.Join(err, syscall.EIO)
	}
	return encoded, true, nil
}

func decodeMetastoreWitness(
	encoded []byte,
	id localdisk.ID,
	volumeName string,
) (metastoreWitnessRecord, error) {
	record, err := parseMetastoreWitness(encoded)
	if err != nil {
		return metastoreWitnessRecord{}, err
	}
	if err := validateWitnessBinding(record, id, volumeName, ""); err != nil {
		return metastoreWitnessRecord{}, err
	}
	return record, nil
}

func parseMetastoreWitness(encoded []byte) (metastoreWitnessRecord, error) {
	if len(encoded) < metastoreWitnessMinBytes || len(encoded) > metastoreWitnessMaxBytes {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has an impossible length: %w", syscall.EIO)
	}
	if !bytes.Equal(encoded[:8], metastoreWitnessMagic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != metastoreWitnessVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != metastoreWitnessHeaderBytes ||
		binary.BigEndian.Uint32(encoded[12:16]) != uint32(len(encoded)) ||
		binary.BigEndian.Uint32(encoded[56:60]) != 0 ||
		binary.BigEndian.Uint32(encoded[60:64]) != 0 {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has an unknown header: %w", syscall.EIO)
	}
	volumeLength := binary.BigEndian.Uint32(encoded[16:20])
	databaseIDLength := binary.BigEndian.Uint32(encoded[20:24])
	if volumeLength == 0 || volumeLength > MaxVolumeBytes || databaseIDLength != durableDatabaseIDBytes ||
		len(encoded) != metastoreWitnessMinBytes+int(volumeLength)+int(databaseIDLength) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has invalid identity lengths: %w", syscall.EIO)
	}
	volumeBytes, databaseIDBytes := int(volumeLength), int(databaseIDLength)
	var id localdisk.ID
	copy(id[:], encoded[64:80])
	variableOffset := metastoreWitnessHeaderBytes
	storedVolume := string(encoded[variableOffset : variableOffset+volumeBytes])
	databaseOffset := variableOffset + volumeBytes
	databaseID := string(encoded[databaseOffset : databaseOffset+databaseIDBytes])
	digestOffset := databaseOffset + databaseIDBytes
	digest := sha256.Sum256(encoded[:digestOffset])
	if !bytes.Equal(encoded[digestOffset:], digest[:]) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness checksum does not match its contents: %w", syscall.EIO)
	}
	if id == (localdisk.ID{}) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has an invalid object-store identity: %w", syscall.EIO)
	}
	if !validDatabaseID(databaseID) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has an invalid database identity: %w", syscall.EIO)
	}
	state := sqlite.DurableState{
		DatabaseID:      databaseID,
		Generation:      checkedInt64(encoded[24:32]),
		NodeHighWater:   checkedInt64(encoded[40:48]),
		ChangeHighWater: checkedInt64(encoded[48:56]),
	}
	checkpointed := checkedInt64(encoded[32:40])
	if state.Generation < 0 || state.NodeHighWater < 0 || state.ChangeHighWater < 0 ||
		checkpointed < 0 || checkpointed > state.Generation {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has impossible durability counters: %w", syscall.EIO)
	}
	return metastoreWitnessRecord{
		StoreID: id, Volume: storedVolume, State: state, CheckpointedGeneration: checkpointed,
	}, nil
}

func validateWitnessBinding(record metastoreWitnessRecord, id localdisk.ID, volumeName, databaseID string) error {
	if record.StoreID != id {
		return fmt.Errorf("the local store metastore witness belongs to another object store: %w", syscall.EIO)
	}
	if record.Volume != volumeName {
		return fmt.Errorf("the local store metastore witness binds volume %q, not %q: %w", record.Volume, volumeName, syscall.EIO)
	}
	if databaseID != "" && record.State.DatabaseID != databaseID {
		return fmt.Errorf("the interrupted metastore witness stage belongs to another database: %w", syscall.EIO)
	}
	return nil
}

// A validated final is the accepted/checkpointed authority. Torn bytes in a
// private stage carry no authority; a complete foreign record remains conflicting
// evidence and cannot be discarded as an interrupted write.
func (a *rootAnchor) inspectMetastoreWitnessStage(id localdisk.ID, volumeName, databaseID string, finalExists bool) (bool, error) {
	encoded, exists, err := a.readMetastoreWitnessBytes(metastoreWitnessStage)
	if err != nil || !exists {
		return exists, err
	}
	record, err := parseMetastoreWitness(encoded)
	if err != nil {
		if finalExists {
			return true, nil
		}
		return true, err
	}
	return true, validateWitnessBinding(record, id, volumeName, databaseID)
}

func checkedInt64(encoded []byte) int64 {
	value := binary.BigEndian.Uint64(encoded)
	if value > math.MaxInt64 {
		return -1
	}
	return int64(value)
}

func encodeMetastoreWitness(record metastoreWitnessRecord) ([]byte, error) {
	if record.StoreID == (localdisk.ID{}) || record.Volume == "" ||
		len(record.Volume) > MaxVolumeBytes || !validDatabaseID(record.State.DatabaseID) ||
		record.State.Generation < 0 ||
		record.State.NodeHighWater < 0 || record.State.ChangeHighWater < 0 ||
		record.CheckpointedGeneration < 0 || record.CheckpointedGeneration > record.State.Generation {
		return nil, fmt.Errorf("the local store metastore witness state is invalid: %w", syscall.EIO)
	}
	encoded := make([]byte, metastoreWitnessMinBytes+len(record.Volume)+len(record.State.DatabaseID))
	copy(encoded[:8], metastoreWitnessMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], metastoreWitnessVersion)
	binary.BigEndian.PutUint16(encoded[10:12], metastoreWitnessHeaderBytes)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(encoded)))
	binary.BigEndian.PutUint32(encoded[16:20], uint32(len(record.Volume)))
	binary.BigEndian.PutUint32(encoded[20:24], uint32(len(record.State.DatabaseID)))
	binary.BigEndian.PutUint64(encoded[24:32], uint64(record.State.Generation))
	binary.BigEndian.PutUint64(encoded[32:40], uint64(record.CheckpointedGeneration))
	binary.BigEndian.PutUint64(encoded[40:48], uint64(record.State.NodeHighWater))
	binary.BigEndian.PutUint64(encoded[48:56], uint64(record.State.ChangeHighWater))
	copy(encoded[64:80], record.StoreID[:])
	variableOffset := metastoreWitnessHeaderBytes
	copy(encoded[variableOffset:], record.Volume)
	databaseOffset := variableOffset + len(record.Volume)
	copy(encoded[databaseOffset:], record.State.DatabaseID)
	digestOffset := databaseOffset + len(record.State.DatabaseID)
	digest := sha256.Sum256(encoded[:digestOffset])
	copy(encoded[digestOffset:], digest[:])
	return encoded, nil
}

func validDatabaseID(id string) bool {
	return len(id) == durableDatabaseIDBytes && strings.Trim(id, "0123456789abcdef") == ""
}

func (w *metastoreWitness) Startup(walPresent, walNonEmpty bool) sqlite.DurableStartup {
	w.mu.Lock()
	defer w.mu.Unlock()
	return sqlite.DurableStartup{
		Accepted:               w.record.State,
		CheckpointedGeneration: w.record.CheckpointedGeneration,
		WALPresent:             walPresent,
		WALNonEmpty:            walNonEmpty,
	}
}

func (w *metastoreWitness) Exists() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.exists
}

func (w *metastoreWitness) generations() (accepted, checkpointed int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.record.State.Generation, w.record.CheckpointedGeneration
}

func (w *metastoreWitness) Accept(state sqlite.DurableState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := validateAcceptedTransition(w.record.State, state, w.exists); err != nil {
		return err
	}
	next := w.record
	next.State = state
	if err := w.publish(next); err != nil {
		return fmt.Errorf("publish accepted metastore state: %w", errors.Join(err, syscall.EIO))
	}
	w.record = next
	w.exists = true
	select {
	case w.checkpoint <- struct{}{}:
	default:
	}
	return nil
}

func validateAcceptedTransition(previous, next sqlite.DurableState, exists bool) error {
	if !validDatabaseID(next.DatabaseID) || next.Generation < 0 || next.NodeHighWater < 0 || next.ChangeHighWater < 0 {
		return fmt.Errorf("SQLite returned an invalid durable state: %w", syscall.EIO)
	}
	if !exists {
		return nil
	}
	if previous.DatabaseID != next.DatabaseID || next.Generation < previous.Generation ||
		next.NodeHighWater < previous.NodeHighWater || next.ChangeHighWater < previous.ChangeHighWater {
		return fmt.Errorf("SQLite durable state moved backwards or changed identity: %w", syscall.EIO)
	}
	if next.Generation == previous.Generation && next != previous {
		return fmt.Errorf("SQLite durable state changed without advancing its generation: %w", syscall.EIO)
	}
	return nil
}

func (w *metastoreWitness) Checkpoint(state sqlite.DurableState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.exists || state != w.record.State {
		return fmt.Errorf("SQLite checkpointed a state other than the accepted state: %w", syscall.EIO)
	}
	if w.record.CheckpointedGeneration == state.Generation {
		return nil
	}
	next := w.record
	next.CheckpointedGeneration = state.Generation
	if err := w.publish(next); err != nil {
		return fmt.Errorf("publish checkpointed metastore state: %w", errors.Join(err, syscall.EIO))
	}
	w.record = next
	return nil
}

func (w *metastoreWitness) publish(record metastoreWitnessRecord) error {
	if err := w.anchor.VerifyPath(); err != nil {
		return err
	}
	stageExists, err := w.anchor.inspectMetastoreWitnessStage(
		record.StoreID, record.Volume, record.State.DatabaseID, w.exists,
	)
	if err != nil {
		return err
	}
	if stageExists {
		if err := w.RemoveInterruptedStage(); err != nil {
			return err
		}
	}
	encoded, err := encodeMetastoreWitness(record)
	if err != nil {
		return err
	}
	stagePath := filepath.Join(w.anchor.path, metastoreWitnessStage)
	fd, err := w.anchor.witnessOps.openat(w.anchor.fd, metastoreWitnessStage,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return witnessPathFailure("create local store metastore witness stage", stagePath, err)
	}
	writeErr := unix.Fchmod(fd, 0o600)
	if writeErr == nil {
		writeErr = w.anchor.witnessOps.writeFull(fd, encoded)
	}
	if writeErr == nil {
		writeErr = w.anchor.witnessOps.fsync(fd)
	}
	closeErr := w.anchor.witnessOps.close(fd)
	if writeErr != nil || closeErr != nil {
		cleanupErr := w.anchor.witnessOps.unlinkat(w.anchor.fd, metastoreWitnessStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		if cleanupErr == nil {
			cleanupErr = w.anchor.witnessOps.fsync(w.anchor.fd)
		}
		return errors.Join(
			witnessPathFailure("write local store metastore witness stage", stagePath, writeErr),
			witnessPathFailure("close local store metastore witness stage", stagePath, closeErr),
			witnessPathFailure("clean failed local store metastore witness stage", stagePath, cleanupErr),
		)
	}
	if err := w.anchor.witnessOps.renameat(w.anchor.fd, metastoreWitnessStage, w.anchor.fd, metastoreWitnessFilename); err != nil {
		renameErr := witnessPathFailure("publish local store metastore witness", filepath.Join(w.anchor.path, metastoreWitnessFilename), err)
		cleanupErr := w.anchor.witnessOps.unlinkat(w.anchor.fd, metastoreWitnessStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		if cleanupErr == nil {
			cleanupErr = w.anchor.witnessOps.fsync(w.anchor.fd)
		}
		return errors.Join(renameErr,
			witnessPathFailure("clean failed local store metastore witness stage", stagePath, cleanupErr))
	}
	if err := w.anchor.witnessOps.fsync(w.anchor.fd); err != nil {
		return witnessPathFailure("sync local store metastore witness", w.anchor.path, err)
	}
	return w.anchor.VerifyPath()
}
