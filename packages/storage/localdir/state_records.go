package localdir

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type leasePoint struct {
	Generation uint64 `json:"generation"`
	Nanos      int64  `json:"nanos"`
}

type directoryBinding struct {
	Version   int    `json:"version"`
	Namespace string `json:"namespace"`
	State     string `json:"state"`
	RootPath  string `json:"rootPath"`
	StatePath string `json:"statePath"`
}

type leaseRecord struct {
	Binding  directoryBinding `json:"binding"`
	Phase    string           `json:"phase"`
	Accepted leasePoint       `json:"accepted"`
	Prepared *leasePoint      `json:"prepared"`
}

type leaseWitness struct {
	Binding directoryBinding `json:"binding"`
	Point   leasePoint       `json:"point"`
}

type stateEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func newStateID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func validStateID(id string) bool {
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == id
}

func encodeState(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payload)
	data, err := json.Marshal(stateEnvelope{payload, hex.EncodeToString(digest[:])})
	if err == nil && len(data) > stateRecordLimit {
		err = fmt.Errorf("lease evidence exceeds format bound: %w", syscall.E2BIG)
	}
	return data, err
}

func strictStateJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return stateFailure("lease evidence contains trailing JSON")
	}
	return nil
}

func decodeState(data []byte, value any) error {
	if len(data) == 0 || len(data) > stateRecordLimit {
		return stateFailure("lease evidence has invalid length")
	}
	var envelope stateEnvelope
	if err := strictStateJSON(data, &envelope); err != nil {
		return errors.Join(stateFailure("lease evidence envelope is invalid"), err)
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.SHA256 != hex.EncodeToString(digest[:]) {
		return stateFailure("lease evidence checksum does not match")
	}
	if err := strictStateJSON(envelope.Payload, value); err != nil {
		return errors.Join(stateFailure("lease evidence payload is invalid"), err)
	}
	// Canonical encoding rejects duplicate fields and noncanonical numeric forms.
	canonical, err := encodeState(value)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return stateFailure("lease evidence is not canonical")
	}
	return nil
}

func readStateAttribute(fd int, name string) ([]byte, error) {
	data := make([]byte, stateRecordLimit)
	n, err := unix.Fgetxattr(fd, name, data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (s *directoryState) readBinding() (directoryBinding, error) {
	data, err := readStateAttribute(s.rootFD, bindingAttribute)
	if err != nil {
		return directoryBinding{}, err
	}
	var binding directoryBinding
	if err := decodeState(data, &binding); err != nil {
		return directoryBinding{}, err
	}
	if binding.Version != 1 || !validStateID(binding.Namespace) || !validStateID(binding.State) ||
		binding.RootPath != s.rootPath || binding.StatePath != s.statePath {
		return directoryBinding{}, stateFailure("namespace binding does not match the configured directories")
	}
	return binding, nil
}

func (s *directoryState) readWitness() (leasePoint, error) {
	data, err := readStateAttribute(s.rootFD, witnessAttribute)
	if err != nil {
		return leasePoint{}, err
	}
	var witness leaseWitness
	if err := decodeState(data, &witness); err != nil {
		return leasePoint{}, err
	}
	if witness.Binding != s.binding || witness.Point.Nanos < 0 {
		return leasePoint{}, stateFailure("lease witness identity or duration is invalid")
	}
	return witness.Point, nil
}

func (s *directoryState) validatePrivateFile(fd int, maxBytes int64) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stat, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Size < 0 || stat.Size > maxBytes {
		return stat, stateFailure("private state file has invalid type, ownership, links, mode, or length")
	}
	mount, err := directoryMount(fd)
	if err != nil {
		return stat, err
	}
	if mount != s.mount {
		return stat, fmt.Errorf("private state file is on another mount: %w", syscall.EXDEV)
	}
	return stat, nil
}

func (s *directoryState) consumeFD(fd int) error {
	err := s.ops.close(fd)
	if err != nil {
		s.mu.Lock()
		s.closeErr = errors.Join(s.closeErr, fmt.Errorf("close private state file: %w", err))
		s.mu.Unlock()
	}
	return err
}

func (s *directoryState) readRecord() (leaseRecord, error) {
	fd, err := unix.Openat(s.stateFD, stateFilename, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return leaseRecord{}, err
	}
	stat, err := s.validatePrivateFile(fd, stateRecordLimit)
	var data []byte
	if err == nil {
		data = make([]byte, stat.Size+1)
		total := 0
		for total < len(data) {
			n, readErr := unix.Read(fd, data[total:])
			if n > 0 {
				total += n
			}
			if readErr != nil {
				err = readErr
				break
			}
			if n == 0 {
				break
			}
		}
		data = data[:total]
		if err == nil && int64(total) != stat.Size {
			err = stateFailure("private state file changed while being read")
		}
	}
	err = errors.Join(err, s.consumeFD(fd))
	if err != nil {
		return leaseRecord{}, err
	}
	var record leaseRecord
	if err := decodeState(data, &record); err != nil {
		return leaseRecord{}, err
	}
	if record.Binding.Version != 1 || !validStateID(record.Binding.Namespace) || !validStateID(record.Binding.State) ||
		record.Binding.RootPath != s.rootPath || record.Binding.StatePath != s.statePath ||
		record.Accepted.Nanos < 0 || record.Phase != "INIT" && record.Phase != "READY" {
		return leaseRecord{}, stateFailure("private lease record is invalid or belongs to different directories")
	}
	if record.Prepared != nil && (record.Accepted.Generation == math.MaxUint64 ||
		record.Prepared.Generation != record.Accepted.Generation+1 || record.Prepared.Nanos < record.Accepted.Nanos) {
		return leaseRecord{}, stateFailure("prepared lease generation or duration is invalid")
	}
	if record.Phase == "INIT" && (record.Accepted != (leasePoint{}) || record.Prepared != nil) {
		return leaseRecord{}, stateFailure("initialization intent contains active lease evidence")
	}
	return record, nil
}

func (s *directoryState) syncRecord(record leaseRecord) error {
	data, err := encodeState(record)
	if err != nil {
		return err
	}
	name := stateFilename + ".next"
	fd, err := unix.Openat(s.stateFD, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	err = unix.Fchmod(fd, 0o600)
	for offset := 0; err == nil && offset < len(data); {
		var n int
		n, err = s.ops.write(fd, data[offset:])
		if n < 0 || n > len(data)-offset {
			err = errors.Join(err, stateFailure("private state write returned an invalid count"))
			break
		}
		offset += n
		if err == nil && n == 0 {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = s.ops.fsync(fd)
	}
	err = errors.Join(err, s.consumeFD(fd))
	if err == nil {
		err = s.ops.rename(s.stateFD, name, s.stateFD, stateFilename)
	}
	if err == nil {
		err = s.ops.fsync(s.stateFD)
	}
	if err != nil {
		return fmt.Errorf("persist lease record: %w", err)
	}
	return nil
}

func (s *directoryState) syncWitness(point leasePoint, flags int) error {
	data, err := encodeState(leaseWitness{s.binding, point})
	if err != nil {
		return err
	}
	if err := s.ops.setxattr(s.rootFD, witnessAttribute, data, flags); err != nil {
		return err
	}
	return s.ops.fsync(s.rootFD)
}

func (s *directoryState) reconcile(record leaseRecord, witness leasePoint) error {
	if record.Binding != s.binding || record.Phase != "READY" {
		return stateFailure("matching READY lease evidence is required")
	}
	if record.Prepared == nil {
		if witness != record.Accepted {
			return stateFailure("accepted lease record and witness disagree")
		}
		if err := errors.Join(s.ops.fsync(s.rootFD), s.ops.fsync(s.stateFD)); err != nil {
			return err
		}
		s.record = record
		return nil
	}
	if witness != record.Accepted && witness != *record.Prepared {
		return stateFailure("prepared lease record and witness disagree")
	}
	if witness == record.Accepted {
		if err := s.syncWitness(*record.Prepared, unix.XATTR_REPLACE); err != nil {
			return err
		}
	}
	record.Accepted = *record.Prepared
	record.Prepared = nil
	if err := s.syncRecord(record); err != nil {
		return err
	}
	s.record = record
	return nil
}

func (s *directoryState) MaxLease(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.stateError(); err != nil {
		return 0, err
	}
	s.lifetime.RLock()
	defer s.lifetime.RUnlock()
	if err := s.healthActive(); err != nil {
		return 0, err
	}
	if !s.bound {
		return 0, stateFailure("unbound namespace has no lease persistence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stateErrorLocked(); err != nil {
		return 0, err
	}
	return time.Duration(s.record.Accepted.Nanos), nil
}

func (s *directoryState) RaiseMaxLease(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.stateError(); err != nil {
		return err
	}
	s.raiseMu.Lock()
	defer s.raiseMu.Unlock()
	s.lifetime.RLock()
	defer s.lifetime.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.healthActive(); err != nil {
		return err
	}
	if !s.bound {
		return stateFailure("unbound namespace has no lease persistence")
	}
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive: %w", syscall.EINVAL)
	}
	s.mu.Lock()
	record := s.record
	s.mu.Unlock()
	if duration <= time.Duration(record.Accepted.Nanos) {
		return s.stateError()
	}
	if record.Accepted.Generation == math.MaxUint64 {
		return stateFailure("lease generation exhausted")
	}
	next := leasePoint{Generation: record.Accepted.Generation + 1, Nanos: int64(duration)}
	record.Prepared = &next
	if err := s.syncRecord(record); err != nil {
		return s.fence(err)
	}
	if err := s.syncWitness(next, unix.XATTR_REPLACE); err != nil {
		return s.fence(err)
	}
	record.Accepted = next
	record.Prepared = nil
	if err := s.syncRecord(record); err != nil {
		return s.fence(err)
	}
	s.mu.Lock()
	s.record = record
	err := s.stateErrorLocked()
	s.mu.Unlock()
	return err
}
