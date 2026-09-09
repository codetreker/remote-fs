package nativelease

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Config binds one witness location to an already exclusively owned native
// file or directory. BindingFD must remain owned until all SQLite handles close cleanly.
// RecoveryStart is captured after that ownership was acquired. Initialize permits creating
// a new durable intent; an existing READY intent still requires its accepted witness.
type Config struct {
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

// Anchor holds a durable initialization intent and an independent accepted witness.
// Its descriptors pin the native binding, but it does not acquire or release flock itself.
// Close must be called only after the caller has proved every writable database handle closed.
type Anchor struct {
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

// Open validates existing evidence before making it available to SQLite.
// Missing evidence on an ordinary Open, malformed records, and changed native bindings
// return EIO. Only a matching durable initialization intent can resume an interrupted setup.
// Every open rejects known remote filesystems and probes local xattr, exclusive flock,
// atomic rename, and file/directory fsync support before creating or accepting lease state.
func Open(config Config) (*Anchor, error) {
	return openAnchor(config, leaseAnchorSystemOperations)
}

func openAnchor(config Config, operations leaseAnchorOperations) (_ *Anchor, resultErr error) {
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
	a := &Anchor{
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

func (a *Anchor) StateID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.intent.Binding.StateID
}

func (a *Anchor) RecoveryStart() time.Time { return a.start }

func (a *Anchor) Initializing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.intent.Ready
}

// Load reads the published witness, including after a write whose durability was uncertain.
// Absence is reported only while the anchor has a durable initialization intent.
func (a *Anchor) Load() (Evidence, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.verify(); err != nil {
		return Evidence{}, false, err
	}
	return a.load()
}

func (a *Anchor) load() (Evidence, bool, error) {
	encoded, exists, err := a.readRecord(a.intent.Binding.Name + ".witness")
	if err != nil {
		return Evidence{}, false, err
	}
	if !exists {
		if a.intent.Ready {
			return Evidence{}, false, leaseAnchorFailure("accepted lease witness is missing", syscall.EIO)
		}
		return Evidence{}, false, nil
	}
	var evidence Evidence
	if err := decodeLeaseRecord(encoded, "witness", &evidence); err != nil {
		return Evidence{}, false, err
	}
	if err := a.validateEvidence(evidence); err != nil {
		return Evidence{}, false, err
	}
	return evidence, true, nil
}

// Advance durably replaces the witness with exactly the next generation, or reconciles
// an identical published result. A decrease or identity change always fails with EIO.
func (a *Anchor) Advance(next Evidence) error {
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
func (a *Anchor) Complete() error {
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

func (a *Anchor) validateEvidence(evidence Evidence) error {
	if !validLeaseAnchorID(evidence.DatabaseID) || evidence.StateID != a.intent.Binding.StateID ||
		evidence.Generation < 0 || evidence.MaxLease < 0 {
		return leaseAnchorFailure("lease witness contains invalid identity or counters", syscall.EIO)
	}
	return nil
}

func validLeaseAnchorID(value string) bool {
	return len(value) == 32 && strings.Trim(value, "0123456789abcdef") == ""
}

func (a *Anchor) Close() error {
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
