package sqlite

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	leaseBindingAttribute = "user.remote-fs.lease-state"
	leaseAnchorMaxBytes   = 16 << 10
	leaseAnchorVersion    = 1
	leaseProbeAttribute   = "user.remote-fs.lease-probe"
	leaseProbeContents    = "lease persistence capability probe"
)

type leaseAnchorOperations struct {
	fstatfs   func(int, *unix.Statfs_t) error
	flock     func(int, int) error
	renameat  func(int, string, int, string) error
	renameat2 func(int, string, int, string, uint) error
	setxattr  func(int, string, []byte, int) error
	getxattr  func(int, string, []byte) (int, error)
	fsync     func(int) error
}

var leaseAnchorSystemOperations = leaseAnchorOperations{
	fstatfs: unix.Fstatfs, flock: unix.Flock, renameat: unix.Renameat, renameat2: unix.Renameat2,
	setxattr: unix.Fsetxattr, getxattr: unix.Fgetxattr, fsync: unix.Fsync,
}

// LeaseAnchorConfig binds one witness location to an already exclusively owned native
// file or directory. BindingFD must remain owned until all SQLite handles close cleanly.
// RecoveryStart is captured after that ownership was acquired. Initialize permits creating
// a new durable intent; an existing READY intent still requires its accepted witness.
type LeaseAnchorConfig struct {
	Directory     string
	Name          string
	Identity      string
	BindingFD     int
	RecoveryStart time.Time
	Initialize    bool
}

type leaseAnchorBinding struct {
	Directory string
	Name      string
	Identity  string
	StateID   string
}

type leaseAnchorIntent struct {
	Binding leaseAnchorBinding
	Ready   bool
}

// LeaseAnchor holds a durable initialization intent and an independent accepted witness.
// Its descriptors pin the native binding, but it does not acquire or release flock itself.
// Close must be called only after the caller has proved every writable database handle closed.
type LeaseAnchor struct {
	mu            sync.Mutex
	directoryFD   int
	bindingFD     int
	directoryStat unix.Stat_t
	filesystem    int64
	mount         uint64
	intent        leaseAnchorIntent
	start         time.Time
	closed        bool
	closeErr      error
	syncFile      func(int) error
	operations    leaseAnchorOperations
}

func (a *LeaseAnchor) verifyDatabaseOwner(owner *leaseDatabaseFile) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return err
	}
	var parent, database, binding unix.Stat_t
	if err := unix.Stat(filepath.Dir(owner.path), &parent); err != nil {
		return err
	}
	if err := unix.Fstat(owner.fd, &database); err != nil {
		return err
	}
	if err := unix.Fstat(a.bindingFD, &binding); err != nil {
		return err
	}
	if parent.Dev != a.directoryStat.Dev || parent.Ino != a.directoryStat.Ino {
		return leaseAnchorFailure("lease state is not anchored beside the owned database", syscall.EIO)
	}
	boundDatabase := binding.Dev == database.Dev && binding.Ino == database.Ino
	boundRoot := binding.Dev == parent.Dev && binding.Ino == parent.Ino
	if !boundDatabase && !boundRoot {
		return leaseAnchorFailure("lease binding does not identify the owned database or its root", syscall.EIO)
	}
	return nil
}

// OpenLeaseAnchor validates existing evidence before making it available to SQLite.
// Missing evidence on an ordinary Open, malformed records, and changed native bindings
// return EIO. Only a matching durable initialization intent can resume an interrupted setup.
// Every open rejects known remote filesystems and probes local xattr, exclusive flock,
// atomic rename, and file/directory fsync support before creating or accepting lease state.
func OpenLeaseAnchor(config LeaseAnchorConfig) (*LeaseAnchor, error) {
	return openLeaseAnchor(config, leaseAnchorSystemOperations)
}

func openLeaseAnchor(config LeaseAnchorConfig, operations leaseAnchorOperations) (_ *LeaseAnchor, resultErr error) {
	if !filepath.IsAbs(config.Directory) || filepath.Clean(config.Directory) != config.Directory ||
		config.Name == "" || config.Name == "." || config.Name == ".." ||
		strings.ContainsAny(config.Name, "/\x00") || len(config.Name) > 128 ||
		config.Identity == "" || len(config.Identity) > 4096 || len(config.Directory) > 4096 ||
		config.BindingFD < 0 || config.RecoveryStart.IsZero() {
		return nil, fmt.Errorf("invalid lease anchor configuration: %w", syscall.EINVAL)
	}
	directoryFD, err := openLeaseDirectory(config.Directory)
	if err != nil {
		return nil, err
	}
	a := &LeaseAnchor{
		directoryFD: directoryFD, bindingFD: -1, start: config.RecoveryStart,
		syncFile: operations.fsync, operations: operations,
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, a.Close())
		}
	}()
	if err := unix.Fstat(directoryFD, &a.directoryStat); err != nil {
		return nil, leaseAnchorFailure("stat lease state directory", err)
	}
	if a.directoryStat.Uid != uint32(unix.Geteuid()) || a.directoryStat.Mode&0o022 != 0 {
		return nil, leaseAnchorFailure("lease state directory must be deployment-owned and protected from other writers", syscall.EACCES)
	}
	a.mount, err = leaseMountID(directoryFD)
	if err != nil {
		return nil, err
	}
	a.filesystem, err = leaseFilesystemType(directoryFD, operations.fstatfs)
	if err != nil {
		return nil, err
	}
	a.bindingFD, err = unix.FcntlInt(uintptr(config.BindingFD), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, leaseAnchorFailure("pin lease binding", err)
	}
	if err := a.validateBindingFD(); err != nil {
		return nil, err
	}
	if err := a.probeCapabilities(config.Name); err != nil {
		return nil, err
	}
	intentBytes, intentExists, err := a.readRecord(config.Name + ".intent")
	if err != nil {
		return nil, err
	}
	bindingBytes, bindingExists, err := a.readBinding()
	if err != nil {
		return nil, err
	}
	if !intentExists {
		if bindingExists || !config.Initialize {
			return nil, leaseAnchorFailure("lease initialization intent is missing", syscall.EIO)
		}
		if _, witnessExists, err := a.readRecord(config.Name + ".witness"); err != nil || witnessExists {
			return nil, errors.Join(err, leaseAnchorFailure("accepted lease witness exists without its initialization intent", syscall.EIO))
		}
		var stateID [16]byte
		if _, err := rand.Read(stateID[:]); err != nil {
			return nil, leaseAnchorFailure("generate lease state identity", err)
		}
		a.intent.Binding = leaseAnchorBinding{
			Directory: config.Directory, Name: config.Name, Identity: config.Identity,
			StateID: hex.EncodeToString(stateID[:]),
		}
		encoded, err := encodeLeaseRecord("intent", a.intent)
		if err != nil {
			return nil, err
		}
		if err := a.publish(config.Name+".intent", encoded, true); err != nil {
			return nil, err
		}
	} else {
		if err := decodeLeaseRecord(intentBytes, "intent", &a.intent); err != nil {
			return nil, err
		}
		if a.intent.Binding.Directory != config.Directory || a.intent.Binding.Name != config.Name ||
			a.intent.Binding.Identity != config.Identity || !validLeaseAnchorID(a.intent.Binding.StateID) {
			return nil, leaseAnchorFailure("lease initialization intent belongs to another binding", syscall.EIO)
		}
	}
	expectedBinding, err := encodeLeaseRecord("binding", a.intent.Binding)
	if err != nil {
		return nil, err
	}
	if !bindingExists {
		if a.intent.Ready || !config.Initialize {
			return nil, leaseAnchorFailure("native lease binding is missing", syscall.EIO)
		}
		if err := unix.Fsetxattr(a.bindingFD, leaseBindingAttribute, expectedBinding, unix.XATTR_CREATE); err != nil {
			return nil, leaseAnchorFailure("create native lease binding", err)
		}
		if err := a.syncFile(a.bindingFD); err != nil {
			return nil, leaseAnchorFailure("sync native lease binding", err)
		}
	} else if !bytes.Equal(bindingBytes, expectedBinding) {
		return nil, leaseAnchorFailure("native lease binding does not match its initialization intent", syscall.EIO)
	}
	if err := errors.Join(a.syncFile(a.bindingFD), a.syncFile(a.directoryFD)); err != nil {
		return nil, leaseAnchorFailure("sync lease anchor binding", err)
	}
	if err := a.verify(); err != nil {
		return nil, err
	}
	if _, exists, err := a.load(); err != nil || a.intent.Ready && !exists {
		return nil, errors.Join(err, leaseAnchorFailure("accepted lease witness is missing or invalid", syscall.EIO))
	}
	for _, name := range []string{config.Name + ".intent.stage", config.Name + ".witness.stage"} {
		if err := a.removeStage(name); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *LeaseAnchor) StateID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.intent.Binding.StateID
}

func (a *LeaseAnchor) RecoveryStart() time.Time { return a.start }

func (a *LeaseAnchor) Initializing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.intent.Ready
}

// Load reads the published witness, including after a write whose durability was uncertain.
// Absence is reported only while the anchor has a durable initialization intent.
func (a *LeaseAnchor) Load() (LeaseEvidence, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return LeaseEvidence{}, false, err
	}
	return a.load()
}

func (a *LeaseAnchor) load() (LeaseEvidence, bool, error) {
	encoded, exists, err := a.readRecord(a.intent.Binding.Name + ".witness")
	if err != nil {
		return LeaseEvidence{}, false, err
	}
	if !exists {
		if a.intent.Ready {
			return LeaseEvidence{}, false, leaseAnchorFailure("accepted lease witness is missing", syscall.EIO)
		}
		return LeaseEvidence{}, false, nil
	}
	var evidence LeaseEvidence
	if err := decodeLeaseRecord(encoded, "witness", &evidence); err != nil {
		return LeaseEvidence{}, false, err
	}
	if err := a.validateEvidence(evidence); err != nil {
		return LeaseEvidence{}, false, err
	}
	return evidence, true, nil
}

// Advance durably replaces the witness with exactly the next generation, or reconciles
// an identical published result. A decrease or identity change always fails with EIO.
func (a *LeaseAnchor) Advance(next LeaseEvidence) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return err
	}
	if err := a.validateEvidence(next); err != nil {
		return err
	}
	previous, exists, err := a.load()
	if err != nil {
		return err
	}
	if exists {
		if previous == next {
			return leaseAnchorFailure("sync reconciled lease witness", a.syncFile(a.directoryFD))
		}
		if next.DatabaseID != previous.DatabaseID ||
			next.Generation <= previous.Generation || next.Generation-previous.Generation != 1 ||
			next.MaxLease < previous.MaxLease {
			return leaseAnchorFailure("lease witness changed identity or did not advance by one nondecreasing generation", syscall.EIO)
		}
	}
	encoded, err := encodeLeaseRecord("witness", next)
	if err != nil {
		return err
	}
	if err := a.publish(a.intent.Binding.Name+".witness", encoded, !exists); err != nil {
		return err
	}
	return a.verify()
}

// Complete marks initialization READY after SQLite has durably accepted the same witness.
// The caller must establish that database acceptance before invoking Complete.
func (a *LeaseAnchor) Complete() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return err
	}
	if _, exists, err := a.load(); err != nil || !exists {
		return errors.Join(err, leaseAnchorFailure("lease initialization requires an accepted witness", syscall.EIO))
	}
	if a.intent.Ready {
		return leaseAnchorFailure("sync completed lease initialization", a.syncFile(a.directoryFD))
	}
	next := a.intent
	next.Ready = true
	encoded, err := encodeLeaseRecord("intent", next)
	if err != nil {
		return err
	}
	if err := a.publish(a.intent.Binding.Name+".intent", encoded, false); err != nil {
		return err
	}
	a.intent = next
	return a.verify()
}

func (a *LeaseAnchor) validateEvidence(evidence LeaseEvidence) error {
	if !validLeaseAnchorID(evidence.DatabaseID) || evidence.StateID != a.intent.Binding.StateID ||
		evidence.Generation < 0 || evidence.MaxLease < 0 {
		return leaseAnchorFailure("lease witness contains invalid identity or counters", syscall.EIO)
	}
	return nil
}

func validLeaseAnchorID(value string) bool {
	return len(value) == 32 && strings.Trim(value, "0123456789abcdef") == ""
}

func (a *LeaseAnchor) readBinding() ([]byte, bool, error) {
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

func (a *LeaseAnchor) readRecord(name string) ([]byte, bool, error) {
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

func (a *LeaseAnchor) publish(name string, encoded []byte, createOnly bool) error {
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

func (a *LeaseAnchor) removeStage(name string) error {
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

func (a *LeaseAnchor) verify() error {
	if err := a.verifyDirectory(); err != nil {
		return err
	}
	if err := a.validateBindingFD(); err != nil {
		return err
	}
	expected, err := encodeLeaseRecord("binding", a.intent.Binding)
	if err != nil {
		return err
	}
	actual, exists, err := a.readBinding()
	if err != nil {
		return err
	}
	if !exists || !bytes.Equal(actual, expected) {
		return leaseAnchorFailure("native lease binding changed while open", syscall.EIO)
	}
	return nil
}

func (a *LeaseAnchor) verifyDirectory() error {
	if a.closed {
		return leaseAnchorFailure("lease anchor is closed", syscall.EBADF)
	}
	fd, err := openLeaseDirectory(a.intent.Binding.Directory)
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	if statErr == nil && (stat.Dev != a.directoryStat.Dev || stat.Ino != a.directoryStat.Ino ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Mode&0o022 != 0) {
		statErr = leaseAnchorFailure("configured lease state directory no longer names the owned directory", syscall.EIO)
	}
	if statErr == nil {
		mount, err := leaseMountID(fd)
		statErr = err
		if err == nil && mount != a.mount {
			statErr = leaseAnchorFailure("configured lease state directory changed mount", syscall.EIO)
		}
	}
	return leaseAnchorFailure("verify lease state directory", errors.Join(statErr, unix.Close(fd)))
}

func (a *LeaseAnchor) validateBindingFD() error {
	var stat unix.Stat_t
	if err := unix.Fstat(a.bindingFD, &stat); err != nil {
		return leaseAnchorFailure("stat native lease binding", err)
	}
	kind := stat.Mode & unix.S_IFMT
	if kind != unix.S_IFREG && kind != unix.S_IFDIR || stat.Uid != uint32(unix.Geteuid()) ||
		stat.Mode&0o022 != 0 || stat.Nlink == 0 || kind == unix.S_IFREG && stat.Nlink != 1 ||
		stat.Dev != a.directoryStat.Dev {
		return leaseAnchorFailure("native lease binding must be owned, protected, and on the state filesystem", syscall.EIO)
	}
	mount, err := leaseMountID(a.bindingFD)
	if err != nil {
		return err
	}
	if mount != a.mount {
		return leaseAnchorFailure("native lease binding is on another mount", syscall.EOPNOTSUPP)
	}
	filesystem, err := leaseFilesystemType(a.bindingFD, a.operations.fstatfs)
	if err != nil {
		return err
	}
	if filesystem != a.filesystem {
		return leaseAnchorFailure("native lease binding is on another filesystem", syscall.EOPNOTSUPP)
	}
	return nil
}

func leaseFilesystemType(fd int, statfs func(int, *unix.Statfs_t) error) (int64, error) {
	var stat unix.Statfs_t
	if err := statfs(fd, &stat); err != nil {
		return 0, leaseAnchorFailure("identify lease state filesystem", err)
	}
	switch stat.Type {
	case unix.NFS_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC, unix.AFS_FS_MAGIC, unix.AFS_SUPER_MAGIC, unix.CEPH_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC, unix.NCP_SUPER_MAGIC:
		return 0, fmt.Errorf("lease state filesystem type %#x is remote and cannot provide local crash semantics: %w", stat.Type, syscall.EOPNOTSUPP)
	}
	return stat.Type, nil
}

func (a *LeaseAnchor) validateProbeFD(fd int) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stat, leaseAnchorFailure("stat lease capability probe", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Dev != a.directoryStat.Dev ||
		stat.Size < 0 || stat.Size > int64(len(leaseProbeContents)) {
		return stat, leaseAnchorFailure("lease capability probe must be bounded, private, regular, and single-linked", syscall.EIO)
	}
	mount, err := leaseMountID(fd)
	if err != nil {
		return stat, err
	}
	filesystem, err := leaseFilesystemType(fd, a.operations.fstatfs)
	if err != nil {
		return stat, err
	}
	if mount != a.mount || filesystem != a.filesystem {
		return stat, leaseAnchorFailure("lease capability probe is on another filesystem or mount", syscall.EOPNOTSUPP)
	}
	return stat, nil
}

func (a *LeaseAnchor) inspectProbe(name string) (unix.Stat_t, bool, error) {
	fd, err := unix.Openat(a.directoryFD, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return unix.Stat_t{}, false, nil
	}
	if err != nil {
		return unix.Stat_t{}, false, leaseAnchorFailure("open lease capability probe", err)
	}
	stat, validateErr := a.validateProbeFD(fd)
	return stat, true, errors.Join(validateErr, leaseAnchorFailure("close lease capability probe inspection", unix.Close(fd)))
}

func (a *LeaseAnchor) removeProbe(name string) error {
	stat, exists, err := a.inspectProbe(name)
	if err != nil || !exists {
		return err
	}
	var named unix.Stat_t
	if err := unix.Fstatat(a.directoryFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return leaseAnchorFailure("verify lease capability probe before removal", err)
	}
	if named.Dev != stat.Dev || named.Ino != stat.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG || named.Nlink != 1 {
		return leaseAnchorFailure("lease capability probe changed before removal", syscall.EIO)
	}
	if err := unix.Unlinkat(a.directoryFD, name, 0); err != nil {
		return leaseAnchorFailure("remove lease capability probe", err)
	}
	return leaseAnchorFailure("sync lease capability probe removal", a.syncFile(a.directoryFD))
}

func (a *LeaseAnchor) createProbe(name string) (int, unix.Stat_t, error) {
	fd, err := unix.Openat(a.directoryFD, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return -1, unix.Stat_t{}, leaseAnchorFailure("create lease capability probe", err)
	}
	if err := unix.Fchmod(fd, 0o600); err != nil {
		return -1, unix.Stat_t{}, errors.Join(leaseAnchorFailure("make lease capability probe private", err), unix.Close(fd))
	}
	stat, err := a.validateProbeFD(fd)
	if err != nil {
		return -1, unix.Stat_t{}, errors.Join(err, unix.Close(fd))
	}
	return fd, stat, nil
}

func (a *LeaseAnchor) probeCapabilities(prefix string) (returned error) {
	source, target := prefix+".probe-source", prefix+".probe-target"
	for _, name := range []string{source, target} {
		if _, _, err := a.inspectProbe(name); err != nil {
			return err
		}
	}
	defer func() {
		for _, name := range []string{source, target} {
			returned = errors.Join(returned, a.removeProbe(name))
		}
	}()
	for _, name := range []string{source, target} {
		if err := a.removeProbe(name); err != nil {
			return err
		}
	}
	fd, original, err := a.createProbe(source)
	if err != nil {
		return err
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close lease capability probe", unix.Close(fd)))
	}()
	if n, err := unix.Write(fd, []byte(leaseProbeContents)); err != nil || n != len(leaseProbeContents) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return leaseAnchorFailure("write lease capability probe", err)
	}
	original.Size = int64(len(leaseProbeContents))
	if err := a.probeXattr(fd); err != nil {
		return err
	}
	if err := a.probeFlock(fd, source, original); err != nil {
		return err
	}
	if err := a.syncFile(fd); err != nil {
		return leaseAnchorFailure("sync lease capability probe file", err)
	}
	if err := a.operations.renameat2(a.directoryFD, source, a.directoryFD, target, unix.RENAME_NOREPLACE); err != nil {
		return leaseAnchorFailure("probe create-only lease state rename", err)
	}
	if err := a.requireProbeIdentity(target, original); err != nil {
		return err
	}
	if err := a.requireProbeAbsent(source); err != nil {
		return err
	}
	if err := a.syncFile(a.directoryFD); err != nil {
		return leaseAnchorFailure("sync lease capability probe rename", err)
	}
	occupiedFD, occupied, err := a.createProbe(source)
	if err != nil {
		return err
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close occupied lease capability probe", unix.Close(occupiedFD)))
	}()
	if err := a.operations.renameat2(a.directoryFD, target, a.directoryFD, source, unix.RENAME_NOREPLACE); !errors.Is(err, syscall.EEXIST) {
		return leaseAnchorFailure("lease state create-only rename did not reject an occupied destination", errors.Join(err, syscall.EOPNOTSUPP))
	}
	if err := a.requireProbeIdentity(source, occupied); err != nil {
		return err
	}
	if err := a.requireProbeIdentity(target, original); err != nil {
		return err
	}
	if err := a.operations.renameat(a.directoryFD, target, a.directoryFD, source); err != nil {
		return leaseAnchorFailure("probe atomic lease state replacement", err)
	}
	if err := a.requireProbeIdentity(source, original); err != nil {
		return err
	}
	if err := a.requireProbeAbsent(target); err != nil {
		return err
	}
	return leaseAnchorFailure("sync lease capability probe replacement", a.syncFile(a.directoryFD))
}

func (a *LeaseAnchor) probeXattr(fd int) error {
	value := []byte(leaseProbeContents)
	if err := a.operations.setxattr(fd, leaseProbeAttribute, value, unix.XATTR_CREATE); err != nil {
		return leaseAnchorFailure("probe lease binding xattr creation", err)
	}
	if err := a.operations.setxattr(fd, leaseProbeAttribute, []byte("replacement"), unix.XATTR_CREATE); !errors.Is(err, syscall.EEXIST) {
		return leaseAnchorFailure("create-only lease binding xattr did not reject replacement", errors.Join(err, syscall.EOPNOTSUPP))
	}
	actual := make([]byte, len(value)+1)
	n, err := a.operations.getxattr(fd, leaseProbeAttribute, actual)
	if err != nil {
		return leaseAnchorFailure("probe lease binding xattr read", err)
	}
	if !bytes.Equal(actual[:n], value) {
		return leaseAnchorFailure("lease binding xattr did not preserve its value", syscall.EOPNOTSUPP)
	}
	return nil
}

func (a *LeaseAnchor) probeFlock(fd int, name string, expected unix.Stat_t) (returned error) {
	if err := a.operations.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return leaseAnchorFailure("probe exclusive lease state flock", err)
	}
	other, err := unix.Openat(a.directoryFD, name, unix.O_RDWR|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return leaseAnchorFailure("open independent lease flock probe", err)
	}
	defer func() {
		returned = errors.Join(returned, leaseAnchorFailure("close independent lease flock probe", unix.Close(other)))
	}()
	actual, err := a.validateProbeFD(other)
	if err != nil {
		return err
	}
	if actual.Dev != expected.Dev || actual.Ino != expected.Ino {
		return leaseAnchorFailure("independent lease flock probe opened another inode", syscall.EIO)
	}
	if err := a.operations.flock(other, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		return leaseAnchorFailure("lease state flock admitted a conflicting independent owner", errors.Join(err, syscall.EOPNOTSUPP))
	}
	return nil
}

func (a *LeaseAnchor) requireProbeIdentity(name string, expected unix.Stat_t) error {
	actual, exists, err := a.inspectProbe(name)
	if err != nil {
		return err
	}
	if !exists || actual.Dev != expected.Dev || actual.Ino != expected.Ino || actual.Size != expected.Size {
		return leaseAnchorFailure("lease state rename did not preserve the source inode and size", syscall.EOPNOTSUPP)
	}
	return nil
}

func (a *LeaseAnchor) requireProbeAbsent(name string) error {
	_, exists, err := a.inspectProbe(name)
	if err != nil {
		return err
	}
	if exists {
		return leaseAnchorFailure("lease state rename retained its source name", syscall.EOPNOTSUPP)
	}
	return nil
}

func leaseMountID(fd int) (uint64, error) {
	var stat unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT, unix.STATX_MNT_ID, &stat); err != nil {
		return 0, leaseAnchorFailure("read lease state mount identity", errors.Join(err, syscall.EOPNOTSUPP))
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return 0, leaseAnchorFailure("lease state filesystem does not report mount identity", syscall.EOPNOTSUPP)
	}
	return stat.Mnt_id, nil
}

func openLeaseDirectory(path string) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, leaseAnchorFailure("open lease path root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" {
			continue
		}
		var parent unix.Stat_t
		statErr := unix.Fstat(fd, &parent)
		if statErr != nil || parent.Uid != 0 && parent.Uid != uint32(unix.Geteuid()) ||
			parent.Mode&0o022 != 0 && parent.Mode&unix.S_ISVTX == 0 {
			return -1, errors.Join(leaseAnchorFailure("lease path ancestor permits replacement by another user", errors.Join(statErr, syscall.EACCES)), unix.Close(fd))
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if err := errors.Join(openErr, closeErr); err != nil {
			if next >= 0 {
				err = errors.Join(err, unix.Close(next))
			}
			return -1, leaseAnchorFailure("open lease path component", err)
		}
		var child unix.Stat_t
		if err := unix.Fstat(next, &child); err != nil ||
			parent.Mode&0o022 != 0 && child.Uid != 0 && child.Uid != uint32(unix.Geteuid()) {
			return -1, errors.Join(leaseAnchorFailure("lease path entry is not protected by its sticky parent", errors.Join(err, syscall.EACCES)), unix.Close(next))
		}
		fd = next
	}
	return fd, nil
}

func leaseAnchorFailure(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, errors.Join(cause, syscall.EIO))
}

func (a *LeaseAnchor) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return a.closeErr
	}
	a.closed = true
	var bindingErr error
	if a.bindingFD >= 0 {
		bindingErr = unix.Close(a.bindingFD)
	}
	a.closeErr = leaseAnchorFailure("close lease anchor", errors.Join(bindingErr, unix.Close(a.directoryFD)))
	return a.closeErr
}
