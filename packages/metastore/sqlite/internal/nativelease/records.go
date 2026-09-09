package nativelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	leaseBindingAttribute = "user.remote-fs.lease-state"
	leaseAnchorMaxBytes   = 16 << 10
	leaseAnchorVersion    = 1
)

func (a *Anchor) readBinding() ([]byte, bool, error) {
	encoded := make([]byte, leaseAnchorMaxBytes)
	n, err := unix.Fgetxattr(a.bindingFD, leaseBindingAttribute, encoded)
	if errors.Is(err, unix.ENODATA) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, leaseAnchorFailure("read native lease binding", err)
	}
	return encoded[:n], true, nil
}

func (a *Anchor) readRecord(name string) ([]byte, bool, error) {
	fd, err := unix.Openat(a.directoryFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, leaseAnchorFailure("open lease state record", err)
	}
	var stat unix.Stat_t
	readErr := unix.Fstat(fd, &stat)
	if readErr == nil && (stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Dev != a.directoryStat.Dev ||
		stat.Size < 48 || stat.Size > leaseAnchorMaxBytes) {
		readErr = leaseAnchorFailure("lease state record must be bounded, private, regular, and single-linked", syscall.EIO)
	}
	if readErr == nil {
		mount, err := leaseMountID(fd)
		readErr = err
		if err == nil && mount != a.mount {
			readErr = leaseAnchorFailure("lease state record is on another mount", syscall.EIO)
		}
	}
	var encoded []byte
	if readErr == nil {
		encoded = make([]byte, int(stat.Size))
		for offset := 0; offset < len(encoded); {
			n, err := unix.Pread(fd, encoded[offset:], int64(offset))
			if err != nil {
				readErr = err
				break
			}
			if n == 0 {
				readErr = io.ErrUnexpectedEOF
				break
			}
			offset += n
		}
	}
	if err := errors.Join(readErr, unix.Close(fd)); err != nil {
		return nil, false, leaseAnchorFailure("read lease state record", err)
	}
	return encoded, true, nil
}

func (a *Anchor) publish(name string, encoded []byte, createOnly bool) error {
	if err := a.verifyDirectory(); err != nil {
		return err
	}
	stage := name + ".stage"
	if err := a.removeStage(stage); err != nil {
		return err
	}
	fd, err := unix.Openat(a.directoryFD, stage, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return leaseAnchorFailure("create lease state stage", err)
	}
	writeErr := unix.Fchmod(fd, 0o600)
	for offset := 0; writeErr == nil && offset < len(encoded); {
		var written int
		written, writeErr = unix.Write(fd, encoded[offset:])
		if written == 0 && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		offset += written
	}
	if writeErr == nil {
		writeErr = a.syncFile(fd)
	}
	if err := errors.Join(writeErr, unix.Close(fd)); err != nil {
		return errors.Join(leaseAnchorFailure("write lease state stage", err), a.removeStage(stage))
	}
	if createOnly {
		err = unix.Renameat2(a.directoryFD, stage, a.directoryFD, name, unix.RENAME_NOREPLACE)
	} else {
		err = unix.Renameat(a.directoryFD, stage, a.directoryFD, name)
	}
	if err != nil {
		return errors.Join(leaseAnchorFailure("publish lease state record", err), a.removeStage(stage))
	}
	return leaseAnchorFailure("sync lease state publication", a.syncFile(a.directoryFD))
}

func (a *Anchor) removeStage(name string) error {
	// The published record is the only acknowledgment evidence. An unfinished stage can
	// contain zero bytes after a crash and is never used to repair or replace that record.
	err := unix.Unlinkat(a.directoryFD, name, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		return leaseAnchorFailure("remove interrupted lease state stage", err)
	}
	return leaseAnchorFailure("sync lease state stage removal", a.syncFile(a.directoryFD))
}

func encodeLeaseRecord(kind string, value any) ([]byte, error) {
	payload, err := json.Marshal(struct {
		Kind  string
		Value any
	}{kind, value})
	if err != nil {
		return nil, leaseAnchorFailure("encode lease state", err)
	}
	if len(payload)+48 > leaseAnchorMaxBytes {
		return nil, leaseAnchorFailure("lease state encoding exceeds its bound", syscall.EIO)
	}
	encoded := make([]byte, 16+len(payload)+sha256.Size)
	copy(encoded[:8], "RFSLEASE")
	binary.BigEndian.PutUint32(encoded[8:12], leaseAnchorVersion)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(encoded)))
	copy(encoded[16:], payload)
	digest := sha256.Sum256(encoded[:len(encoded)-sha256.Size])
	copy(encoded[len(encoded)-sha256.Size:], digest[:])
	return encoded, nil
}

func decodeLeaseRecord(encoded []byte, kind string, target any) error {
	if len(encoded) < 48 || len(encoded) > leaseAnchorMaxBytes || string(encoded[:8]) != "RFSLEASE" ||
		binary.BigEndian.Uint32(encoded[8:12]) != leaseAnchorVersion ||
		binary.BigEndian.Uint32(encoded[12:16]) != uint32(len(encoded)) {
		return leaseAnchorFailure("lease state has an unknown header or length", syscall.EIO)
	}
	digest := sha256.Sum256(encoded[:len(encoded)-sha256.Size])
	if !bytes.Equal(encoded[len(encoded)-sha256.Size:], digest[:]) {
		return leaseAnchorFailure("lease state checksum does not match its contents", syscall.EIO)
	}
	envelope := struct {
		Kind  string
		Value json.RawMessage
	}{}
	if err := json.Unmarshal(encoded[16:len(encoded)-sha256.Size], &envelope); err != nil {
		return leaseAnchorFailure("decode lease state envelope", err)
	}
	if envelope.Kind != kind {
		return leaseAnchorFailure("lease state record has the wrong kind", syscall.EIO)
	}
	if err := json.Unmarshal(envelope.Value, target); err != nil {
		return leaseAnchorFailure("decode lease state value", err)
	}
	canonical, err := encodeLeaseRecord(kind, target)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, encoded) {
		return leaseAnchorFailure("lease state has unknown or noncanonical fields", syscall.EIO)
	}
	return nil
}

func leaseAnchorFailure(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, errors.Join(cause, syscall.EIO))
}
