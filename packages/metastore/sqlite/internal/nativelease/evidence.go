package nativelease

import (
	"fmt"
	"strings"
	"syscall"
	"time"
)

// Evidence binds a monotonic maximum lease duration to one database. It contains no
// owner or grant capabilities. Generation and duration are validated before use.
type Evidence struct {
	DatabaseID string
	StateID    string
	Generation int64
	MaxLease   time.Duration
	Quiescent  bool
}

type witnessRecord struct {
	DatabaseID string
	StateID    string
	Generation int64
	MaxLease   time.Duration
}

type fileWitnessRecord struct {
	witnessRecord
	Quiescent bool
}

func (a *Anchor) encodeWitness(e Evidence) ([]byte, error) {
	record := witnessRecord{e.DatabaseID, e.StateID, e.Generation, e.MaxLease}
	if a.domain == DomainFile {
		return encodeLeaseRecord(a.recordKind("witness"), fileWitnessRecord{record, e.Quiescent})
	}
	return encodeLeaseRecord(a.recordKind("witness"), record)
}

func (a *Anchor) decodeWitness(encoded []byte) (Evidence, error) {
	var record fileWitnessRecord
	var err error
	if a.domain == DomainFile {
		err = decodeLeaseRecord(encoded, a.recordKind("witness"), &record)
	} else {
		err = decodeLeaseRecord(encoded, a.recordKind("witness"), &record.witnessRecord)
	}
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{DatabaseID: record.DatabaseID, StateID: record.StateID,
		Generation: record.Generation, MaxLease: record.MaxLease, Quiescent: record.Quiescent}, nil
}

// Witness stores independent, volume-anchored evidence. Advance must durably publish
// the exact next generation before returning. Repeating the same record is idempotent.
type Witness interface {
	Load() (Evidence, bool, error)
	Advance(Evidence) error
}

func ValidID(id string) bool {
	return len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

func ValidateEvidence(e Evidence) error {
	if !ValidID(e.DatabaseID) || !ValidID(e.StateID) ||
		e.Generation < 0 || e.MaxLease < 0 {
		return fmt.Errorf("lease recovery evidence has an invalid identity or counter: %w", syscall.EIO)
	}
	return nil
}
