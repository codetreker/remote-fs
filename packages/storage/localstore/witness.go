package localstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
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
	metastoreWitnessMaxBytes    = metastoreWitnessMinBytes + MaxWorkspaceBytes + durableDatabaseIDBytes
)

var metastoreWitnessMagic = [8]byte{'R', 'F', 'S', 'M', 'E', 'T', 'A', 0}

type metastoreWitnessRecord struct {
	StoreID                localdisk.ID
	Workspace              string
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
	workspace string,
) (*metastoreWitness, bool, bool, error) {
	final, finalExists, err := a.readMetastoreWitnessEntry(metastoreWitnessFilename, id, workspace)
	if err != nil {
		return nil, false, false, err
	}
	stage, stageExists, err := a.readMetastoreWitnessEntry(metastoreWitnessStage, id, workspace)
	if err != nil {
		return nil, false, false, err
	}
	if stageExists {
		if finalExists && (final.StoreID != stage.StoreID || final.Workspace != stage.Workspace ||
			final.State.DatabaseID != stage.State.DatabaseID) {
			return nil, false, false, fmt.Errorf(
				"the local store metastore witness stage does not belong to the published witness: %w",
				syscall.EIO,
			)
		}
	}
	if !finalExists {
		final = metastoreWitnessRecord{StoreID: id, Workspace: workspace}
	}
	return &metastoreWitness{
		anchor: a, record: final, exists: finalExists, checkpoint: make(chan struct{}, 1),
	}, finalExists, stageExists, nil
}

func (w *metastoreWitness) RemoveInterruptedStage() error {
	if err := unix.Unlinkat(w.anchor.fd, metastoreWitnessStage, 0); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return &os.PathError{
			Op:   "remove interrupted local store metastore witness stage",
			Path: filepath.Join(w.anchor.path, metastoreWitnessStage),
			Err:  err,
		}
	}
	if err := unix.Fsync(w.anchor.fd); err != nil {
		return &os.PathError{
			Op:   "sync interrupted local store metastore witness cleanup",
			Path: w.anchor.path,
			Err:  err,
		}
	}
	return nil
}

func (a *rootAnchor) readMetastoreWitnessEntry(
	name string,
	id localdisk.ID,
	workspace string,
) (metastoreWitnessRecord, bool, error) {
	path := filepath.Join(a.path, name)
	fd, err := unix.Openat(a.fd, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return metastoreWitnessRecord{}, false, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			err = fmt.Errorf("the entry is a symbolic link: %w", syscall.EIO)
		}
		return metastoreWitnessRecord{}, false, &os.PathError{Op: "open local store metastore witness", Path: path, Err: err}
	}
	stat, validateErr := validatePrivateFile(fd, path, a.device, a.mount, false, a.statx)
	var encoded []byte
	readErr := error(nil)
	if validateErr == nil {
		if stat.Size < metastoreWitnessMinBytes || stat.Size > metastoreWitnessMaxBytes {
			validateErr = fmt.Errorf("the metastore witness is %d bytes, want between %d and %d: %w",
				stat.Size, metastoreWitnessMinBytes, metastoreWitnessMaxBytes, syscall.EIO)
		} else {
			encoded = make([]byte, int(stat.Size))
			readErr = preadFull(fd, encoded)
		}
	}
	closeErr := unix.Close(fd)
	if err := errors.Join(validateErr, readErr, pathFailure("close local store metastore witness", path, closeErr)); err != nil {
		return metastoreWitnessRecord{}, false, err
	}
	record, err := decodeMetastoreWitness(encoded, id, workspace)
	if err != nil {
		return metastoreWitnessRecord{}, false, err
	}
	return record, true, nil
}

func decodeMetastoreWitness(
	encoded []byte,
	id localdisk.ID,
	workspace string,
) (metastoreWitnessRecord, error) {
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
	workspaceBytes := int(binary.BigEndian.Uint32(encoded[16:20]))
	databaseIDBytes := int(binary.BigEndian.Uint32(encoded[20:24]))
	if workspaceBytes > MaxWorkspaceBytes || databaseIDBytes != durableDatabaseIDBytes ||
		len(encoded) != metastoreWitnessMinBytes+workspaceBytes+databaseIDBytes {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness has invalid identity lengths: %w", syscall.EIO)
	}
	if !bytes.Equal(encoded[64:80], id[:]) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness belongs to another object store: %w", syscall.EIO)
	}
	variableOffset := metastoreWitnessHeaderBytes
	storedWorkspace := string(encoded[variableOffset : variableOffset+workspaceBytes])
	databaseOffset := variableOffset + workspaceBytes
	databaseID := string(encoded[databaseOffset : databaseOffset+databaseIDBytes])
	digestOffset := databaseOffset + databaseIDBytes
	digest := sha256.Sum256(encoded[:digestOffset])
	if !bytes.Equal(encoded[digestOffset:], digest[:]) {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness checksum does not match its contents: %w", syscall.EIO)
	}
	if storedWorkspace != workspace {
		return metastoreWitnessRecord{}, fmt.Errorf("the local store metastore witness binds workspace %q, not %q: %w",
			storedWorkspace, workspace, syscall.EIO)
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
		StoreID: id, Workspace: workspace, State: state, CheckpointedGeneration: checkpointed,
	}, nil
}

func checkedInt64(encoded []byte) int64 {
	value := binary.BigEndian.Uint64(encoded)
	if value > math.MaxInt64 {
		return -1
	}
	return int64(value)
}

func encodeMetastoreWitness(record metastoreWitnessRecord) ([]byte, error) {
	if record.StoreID == (localdisk.ID{}) || record.Workspace == "" ||
		len(record.Workspace) > MaxWorkspaceBytes || !validDatabaseID(record.State.DatabaseID) ||
		record.State.Generation < 0 ||
		record.State.NodeHighWater < 0 || record.State.ChangeHighWater < 0 ||
		record.CheckpointedGeneration < 0 || record.CheckpointedGeneration > record.State.Generation {
		return nil, fmt.Errorf("the local store metastore witness state is invalid: %w", syscall.EIO)
	}
	encoded := make([]byte, metastoreWitnessMinBytes+len(record.Workspace)+len(record.State.DatabaseID))
	copy(encoded[:8], metastoreWitnessMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], metastoreWitnessVersion)
	binary.BigEndian.PutUint16(encoded[10:12], metastoreWitnessHeaderBytes)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(encoded)))
	binary.BigEndian.PutUint32(encoded[16:20], uint32(len(record.Workspace)))
	binary.BigEndian.PutUint32(encoded[20:24], uint32(len(record.State.DatabaseID)))
	binary.BigEndian.PutUint64(encoded[24:32], uint64(record.State.Generation))
	binary.BigEndian.PutUint64(encoded[32:40], uint64(record.CheckpointedGeneration))
	binary.BigEndian.PutUint64(encoded[40:48], uint64(record.State.NodeHighWater))
	binary.BigEndian.PutUint64(encoded[48:56], uint64(record.State.ChangeHighWater))
	copy(encoded[64:80], record.StoreID[:])
	variableOffset := metastoreWitnessHeaderBytes
	copy(encoded[variableOffset:], record.Workspace)
	databaseOffset := variableOffset + len(record.Workspace)
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
		return fmt.Errorf("publish accepted metastore state: %v: %w", err, syscall.EIO)
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
		return fmt.Errorf("publish checkpointed metastore state: %v: %w", err, syscall.EIO)
	}
	w.record = next
	return nil
}

func (w *metastoreWitness) publish(record metastoreWitnessRecord) error {
	if err := w.anchor.VerifyPath(); err != nil {
		return err
	}
	staged, stageExists, err := w.anchor.readMetastoreWitnessEntry(
		metastoreWitnessStage, record.StoreID, record.Workspace,
	)
	if err != nil {
		return err
	}
	if stageExists {
		if staged.State.DatabaseID != record.State.DatabaseID {
			return fmt.Errorf("the interrupted metastore witness stage belongs to another database: %w", syscall.EIO)
		}
		if err := w.RemoveInterruptedStage(); err != nil {
			return err
		}
	}
	encoded, err := encodeMetastoreWitness(record)
	if err != nil {
		return err
	}
	stagePath := filepath.Join(w.anchor.path, metastoreWitnessStage)
	fd, err := unix.Openat(w.anchor.fd, metastoreWitnessStage,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return &os.PathError{Op: "create local store metastore witness stage", Path: stagePath, Err: err}
	}
	writeErr := unix.Fchmod(fd, 0o600)
	if writeErr == nil {
		writeErr = writeFull(fd, encoded)
	}
	if writeErr == nil {
		writeErr = unix.Fsync(fd)
	}
	closeErr := unix.Close(fd)
	if writeErr != nil || closeErr != nil {
		cleanupErr := unix.Unlinkat(w.anchor.fd, metastoreWitnessStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		if cleanupErr == nil {
			cleanupErr = unix.Fsync(w.anchor.fd)
		}
		return errors.Join(
			pathFailure("write local store metastore witness stage", stagePath, writeErr),
			pathFailure("close local store metastore witness stage", stagePath, closeErr),
			pathFailure("clean failed local store metastore witness stage", stagePath, cleanupErr),
		)
	}
	if err := unix.Renameat(w.anchor.fd, metastoreWitnessStage, w.anchor.fd, metastoreWitnessFilename); err != nil {
		renameErr := &os.PathError{
			Op: "publish local store metastore witness", Path: filepath.Join(w.anchor.path, metastoreWitnessFilename), Err: err,
		}
		cleanupErr := unix.Unlinkat(w.anchor.fd, metastoreWitnessStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		if cleanupErr == nil {
			cleanupErr = unix.Fsync(w.anchor.fd)
		}
		return errors.Join(renameErr,
			pathFailure("clean failed local store metastore witness stage", stagePath, cleanupErr))
	}
	if err := unix.Fsync(w.anchor.fd); err != nil {
		return &os.PathError{Op: "sync local store metastore witness", Path: w.anchor.path, Err: err}
	}
	return w.anchor.VerifyPath()
}
