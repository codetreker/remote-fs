package windows

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidMapping      = errors.New("invalid Windows SMB mapping options")
	ErrMappingBusy         = errors.New("Windows SMB mapping is busy")
	ErrMappingOwnership    = errors.New("Windows SMB mapping no longer matches its owner")
	ErrMappingVerification = errors.New("Windows cannot verify the SMB mapping target")
	ErrMappingUncertain    = errors.New("Windows SMB mapping creation outcome is unknown")
)

// MappingOptions selects a drive and a share on this machine's IPv4 loopback
// endpoint. Required signing, write-through, and non-persistence cannot be disabled.
// No password or other credential is accepted by this helper.
type MappingOptions struct {
	LocalPath string
	Share     string
	TCPPort   uint16
}

func (o MappingOptions) validated() (MappingOptions, error) {
	if len(o.LocalPath) != 2 || o.LocalPath[1] != ':' ||
		(o.LocalPath[0] < 'A' || o.LocalPath[0] > 'Z') && (o.LocalPath[0] < 'a' || o.LocalPath[0] > 'z') ||
		o.TCPPort == 0 || o.Share == "" || len(o.Share) > 80 || !utf8.ValidString(o.Share) ||
		strings.TrimSpace(o.Share) != o.Share || strings.ContainsAny(o.Share, "\\/\x00\"[]:|<>+=;,?*") {
		return MappingOptions{}, ErrInvalidMapping
	}
	for _, r := range o.Share {
		if r < 32 {
			return MappingOptions{}, ErrInvalidMapping
		}
	}
	o.LocalPath = strings.ToUpper(o.LocalPath)
	return o, nil
}

func (o MappingOptions) remotePath() string { return `\\127.0.0.1\` + o.Share }

type logonIdentity struct {
	SID              string
	AuthenticationID uint64
	SessionID        uint32
}

type mappingRecord struct {
	LocalPath  string  `json:"localPath"`
	RemotePath string  `json:"remotePath"`
	Status     *uint32 `json:"status"`
	Device     string  `json:"-"`
}

func (r mappingRecord) verifies(o MappingOptions) bool {
	return strings.EqualFold(r.LocalPath, o.LocalPath) && r.RemotePath == o.remotePath() &&
		r.Device != ""
}

func (r mappingRecord) sameMapping(other mappingRecord) bool {
	return strings.EqualFold(r.LocalPath, other.LocalPath) && r.RemotePath == other.RemotePath &&
		r.Device != "" && r.Device == other.Device
}

// MappingStatus distinguishes accepted creation parameters from the most recent
// observed mapping identity. The helper does not read or verify provider-specific
// port or policy properties; it does not assume a cross-provider query contract.
type MappingStatus struct {
	ParametersAccepted                                  bool
	Options                                             MappingOptions
	OwnerSID                                            string
	AuthenticationID                                    uint64
	SessionID                                           uint32
	ObservedLocalPath, ObservedRemotePath, DeviceTarget string
	ConnectionStatus                                    *uint32
	Closed                                              bool
}

type mappingSystem interface {
	identity(context.Context) (logonIdentity, error)
	query(context.Context, logonIdentity, string) (mappingRecord, bool, error)
	create(context.Context, logonIdentity, MappingOptions) (mappingRecord, bool, error)
	remove(context.Context, logonIdentity, string, bool) error
}

// Mapping owns only the OS mapping created by Map, never an SMB Server or Export.
// Operations verify the recorded login, drive/UNC and DOS-device target.
// Ownership does not rely on a provider-specific mapping generation. Callers must
// not externally replace an owned drive, including an indistinguishable mapping.
// A zero Mapping owns nothing; callers obtain a valid instance from Map.
type Mapping struct {
	mu       sync.Mutex
	system   mappingSystem
	owner    logonIdentity
	options  MappingOptions
	record   mappingRecord
	accepted bool
	closed   bool
}

// Map creates a mapping in the current process user's login session on Windows 11
// 24H2 or later. It never elevates, takes an occupied drive, or modifies machine
// policy. Required typed parameters must be supported and accepted by Windows;
// the created drive, UNC and device identity are then observed separately.
// A non-nil Mapping with an error means creation was confirmed but rollback could
// not finish; the caller retains that object to retry cleanup. An uncertain
// creation is reported explicitly and does not authorize deleting an observed drive.
func Map(ctx context.Context, options MappingOptions) (*Mapping, error) {
	system, err := newMappingSystem()
	if err != nil {
		return nil, err
	}
	return mapWithSystem(ctx, options, system)
}

func mapWithSystem(ctx context.Context, options MappingOptions, system mappingSystem) (*Mapping, error) {
	options, err := options.validated()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner, err := system.identity(ctx)
	if err != nil {
		return nil, err
	}
	if _, exists, err := system.query(ctx, owner, options.LocalPath); err != nil {
		return nil, err
	} else if exists {
		return nil, ErrMappingBusy
	}
	record, created, err := system.create(ctx, owner, options)
	if !created {
		if err == nil {
			err = ErrMappingUncertain
		}
		return nil, err
	}
	m := &Mapping{system: system, owner: owner, options: options, record: record, accepted: true}
	if err == nil && (!record.verifies(options) || record.Status == nil || *record.Status != 0) {
		err = ErrMappingVerification
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		return m, nil
	}
	// Cleanup has its own finite budget after the create operation was confirmed.
	// It still checks ownership and does not force other processes' handles closed.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if cleanupErr := m.Unmount(cleanup); cleanupErr != nil {
		return m, errors.Join(err, cleanupErr)
	}
	return nil, err
}

// Options returns the immutable requested mapping configuration.
func (m *Mapping) Options() MappingOptions { return m.options }

// Status reports the last OS observation and accepted creation parameters. It makes
// no fresh system query. Closed indicates that this owner completed removal or
// subsequently observed its mapping absent.
func (m *Mapping) Status() MappingStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := MappingStatus{ParametersAccepted: m.accepted, Options: m.options, OwnerSID: m.owner.SID,
		AuthenticationID: m.owner.AuthenticationID, SessionID: m.owner.SessionID,
		ObservedLocalPath: m.record.LocalPath, ObservedRemotePath: m.record.RemotePath,
		DeviceTarget: m.record.Device, Closed: m.closed}
	if m.record.Status != nil {
		value := *m.record.Status
		status.ConnectionStatus = &value
	}
	return status
}

// Unmount removes this mapping only while its login and target still match.
// Open files cause ErrMappingBusy and leave the mapping usable. A successful
// removal does not close the separately owned SMB Server or Export.
func (m *Mapping) Unmount(ctx context.Context) error { return m.unmount(ctx, false) }

// ForceUnmount explicitly disconnects open files. Their subsequent I/O can fail;
// this is not a lossless unmount and does not confirm remote application outcomes.
func (m *Mapping) ForceUnmount(ctx context.Context) error { return m.unmount(ctx, true) }

func (m *Mapping) unmount(ctx context.Context, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if !m.accepted {
		return ErrMappingOwnership
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owner, err := m.system.identity(ctx)
	if err != nil {
		return err
	}
	if owner != m.owner {
		return ErrMappingOwnership
	}
	record, exists, err := m.system.query(ctx, owner, m.options.LocalPath)
	if err != nil {
		return err
	}
	if !exists {
		m.closed = true
		return nil
	}
	if !record.sameMapping(m.record) {
		return ErrMappingOwnership
	}
	m.record = record
	if err := m.system.remove(ctx, m.owner, m.options.LocalPath, force); err != nil {
		return err
	}
	// WNet success confirms that this redirection ended. Retire the owner now;
	// a later query could describe a newly assigned drive, not this mapping.
	m.closed = true
	return nil
}
