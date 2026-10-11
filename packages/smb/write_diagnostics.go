package smb

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type WriteOwnerID struct {
	Incarnation [16]byte
	Generation  uint64
}

type WriteExecutionState uint8

const (
	WriteExecutionUnknown WriteExecutionState = iota + 1
	WriteExecutionCompleted
	WriteExecutionNotExecuted
)

type WriteDurabilityState uint8

const (
	WriteDurabilityNotRequested WriteDurabilityState = iota + 1
	WriteDurabilityUnknown
	WriteDurabilityConfirmed
)

type ResponseDisposition uint8

const (
	ResponseNotSent ResponseDisposition = iota + 1
	ResponseSent
	ResponseSendFailed
)

type WriteErrorCategory uint8

const (
	WriteErrorNone WriteErrorCategory = iota
	WriteErrorIO
	WriteErrorDenied
	WriteErrorCondition
	WriteErrorCapacity
	WriteErrorRetired
)

// ResponseSent records local transmission, not application acknowledgment.
type WriteFailure struct {
	Owner             WriteOwnerID
	ExportGeneration  uint64
	FileGeneration    uint64
	NodeID            uint64
	Action            storage.FileActionID
	Operation         storage.Operation
	Bytes             uint32
	Digest            [32]byte
	Execution         WriteExecutionState
	Durability        WriteDurabilityState
	Response          ResponseDisposition
	ResponseFinal     bool
	Terminal          bool
	PayloadReleased   bool
	ReferenceReleased bool
	ChainSettled      bool
	ErrorCategory     WriteErrorCategory
}

type writeRecord struct {
	fact         WriteFailure
	export       *Export
	payloadBytes int
	failed       bool
	terminal     bool
	flush        bool
}

type writeLedger struct {
	mu          sync.Mutex
	incarnation [16]byte
	next        uint64
	nextExport  uint64
	records     map[WriteOwnerID]*writeRecord
	bytes       int
}

func (s *Server) reserveIORecord(t *tree, h *fileHandle, bytes int, flush bool) (WriteOwnerID, error) {
	if t == nil || t.export == nil || h == nil || h.file == nil || bytes < 0 {
		return WriteOwnerID{}, syscall.EINVAL
	}
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	l := &s.writes
	l.mu.Lock()
	defer l.mu.Unlock()
	limits := s.config.Limits
	if len(l.records) >= limits.MaxWriteOwners || len(l.records)+len(s.handleOwners) >= limits.MaxDiagnosticBytes/diagnosticOwnerBytes || bytes > limits.MaxRetainedWriteBytes-l.bytes {
		return WriteOwnerID{}, syscall.ENOMEM
	}
	exportCount, exportBytes := 0, 0
	for _, record := range l.records {
		if record.export == t.export {
			exportCount++
			exportBytes += record.payloadBytes
		}
	}
	if exportCount >= limits.MaxWriteOwners || bytes > limits.MaxRetainedWriteBytes-exportBytes || l.next == math.MaxUint64 {
		return WriteOwnerID{}, syscall.ENOMEM
	}
	if l.incarnation == ([16]byte{}) {
		if _, err := rand.Read(l.incarnation[:]); err != nil {
			return WriteOwnerID{}, err
		}
	}
	if l.records == nil {
		l.records = make(map[WriteOwnerID]*writeRecord)
	}
	exportGeneration := t.export.ioGeneration
	if exportGeneration == 0 {
		if l.nextExport == math.MaxUint64 {
			return WriteOwnerID{}, syscall.ENOMEM
		}
		l.nextExport++
		exportGeneration = l.nextExport
		t.export.ioGeneration = exportGeneration
	}
	l.next++
	id := WriteOwnerID{Incarnation: l.incarnation, Generation: l.next}
	durability := WriteDurabilityNotRequested
	if flush {
		durability = WriteDurabilityUnknown
	}
	l.records[id] = &writeRecord{export: t.export, payloadBytes: bytes, flush: flush, fact: WriteFailure{
		Owner: id, ExportGeneration: exportGeneration, FileGeneration: binary.LittleEndian.Uint64(h.id[8:]), NodeID: h.nodeID,
		Bytes: uint32(bytes), Execution: WriteExecutionUnknown, Durability: durability, Response: ResponseNotSent,
	}}
	l.bytes += bytes
	return id, nil
}

func (l *writeLedger) update(id WriteOwnerID, fn func(*writeRecord)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.records[id]; record != nil {
		if record.fact.Terminal {
			return
		}
		fn(record)
		record.fact.Terminal = record.terminal && record.fact.ResponseFinal && record.fact.PayloadReleased
	}
}

func (l *writeLedger) releasePayload(id WriteOwnerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.records[id]; record != nil {
		l.bytes -= record.payloadBytes
		record.payloadBytes = 0
		record.fact.PayloadReleased = true
		record.fact.Terminal = record.terminal && record.fact.ResponseFinal
	}
}

func (l *writeLedger) removeSuccess(id WriteOwnerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.records[id]; record != nil && !record.failed && record.terminal && record.fact.Response == ResponseSent {
		l.bytes -= record.payloadBytes
		delete(l.records, id)
	}
}

func (l *writeLedger) usedDiagnosticSlots() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

func (l *writeLedger) addStatus(out *Status) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out.RetainedWriteBytes = l.bytes
	for _, record := range l.records {
		if record.flush {
			if !record.terminal {
				out.PendingFlushes++
			}
		} else {
			out.WriteOwners++
			if record.fact.Execution == WriteExecutionUnknown {
				out.UnknownWrites++
			}
		}
		if record.failed {
			out.WriteFailures = append(out.WriteFailures, record.fact)
		}
	}
}

func (l *writeLedger) pendingError(export *Export) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.records {
		if export == nil || record.export == export {
			return syscall.EIO
		}
	}
	return nil
}

// Acknowledgment transfers reporting responsibility for terminal immutable facts.
// The whole batch is validated before any record or charge is removed.
func (s *Server) AcknowledgeWriteFailures(ids []WriteOwnerID) error {
	l := &s.writes
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := make(map[WriteOwnerID]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return syscall.EINVAL
		}
		seen[id] = struct{}{}
		record := l.records[id]
		if record == nil || !record.failed || !record.fact.Terminal || !record.fact.PayloadReleased || !record.fact.ChainSettled {
			return syscall.EINVAL
		}
	}
	for _, id := range ids {
		delete(l.records, id)
	}
	return nil
}

func writeErrorCategory(err error) WriteErrorCategory {
	if err == nil {
		return WriteErrorNone
	}
	if errors.Is(err, storage.ErrConditionConflict) {
		return WriteErrorCondition
	}
	if errors.Is(err, authz.ErrDenied) || errors.Is(err, ErrIdentityDenied) {
		return WriteErrorDenied
	}
	switch storage.ErrnoOf(err) {
	case syscall.EACCES, syscall.EPERM:
		return WriteErrorDenied
	case syscall.ENOMEM, syscall.ENOSPC, syscall.EMFILE, syscall.EFBIG, syscall.EDQUOT:
		return WriteErrorCapacity
	case syscall.ESTALE, syscall.EBADF:
		return WriteErrorRetired
	default:
		return WriteErrorIO
	}
}
