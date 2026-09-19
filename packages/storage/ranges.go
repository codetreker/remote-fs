package storage

import (
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ExtentKind uint8

const (
	Bytes ExtentKind = iota + 1
	Boundary
)

type Range struct {
	Kind                 ExtentKind
	Start, Length, CutAt uint64
}
type ConflictDomain uint8

const (
	DomainRecord ConflictDomain = iota + 1
	DomainWholeFile
	DomainEnforced
)

type RangeMode uint8

const (
	RangeShared RangeMode = iota + 1
	RangeExclusive
)

type RangeEdit uint8

const (
	Replace RangeEdit = iota + 1
	Subtract
	AddExact
	RemoveExact
)

type ConversionRule uint8

const (
	PreserveBeforeAcquire ConversionRule = iota
	DropBeforeAcquire
)

type RangePolicy struct{ DenySelf, DenyOthers Uses }
type RangeCommand struct {
	Domain     ConflictDomain
	Mode       RangeMode
	Range      Range
	Edit       RangeEdit
	Claim      ClaimID
	Wait       bool
	Conversion ConversionRule
	Policy     RangePolicy
}
type ClaimID string

func NewClaimID(request LockRequestID, index int) (ClaimID, error) {
	if _, err := request.Epoch(); err != nil {
		return "", err
	}
	if index < 0 || index >= MaxRangeCommands {
		return "", syscall.EINVAL
	}
	return ClaimID(string(request) + ":" + strconv.Itoa(index)), nil
}
func (c ClaimID) Check() error {
	// The request identity uses at most 53 bytes; the command suffix adds three.
	if len(c) > 56 {
		return syscall.EINVAL
	}
	at := strings.LastIndexByte(string(c), ':')
	if at < 0 {
		return syscall.EINVAL
	}
	request := LockRequestID(c[:at])
	index, err := strconv.Atoi(string(c[at+1:]))
	if err != nil {
		return syscall.EINVAL
	}
	want, err := NewClaimID(request, index)
	if err != nil || want != c {
		return syscall.EINVAL
	}
	return nil
}

type AttemptState uint8

const (
	Pending AttemptState = iota + 1
	Granted
	Rejected
	Cancelled
	Released
)

type RejectionCode string

const (
	RangeBlocked     RejectionCode = "blocked"
	RangeNotHeld     RejectionCode = "not-held"
	RangeExhausted   RejectionCode = "exhausted"
	RangeDeadlock    RejectionCode = "deadlock"
	RangeInvalid     RejectionCode = "invalid"
	RangeUnsupported RejectionCode = "unsupported"
	RangeExpired     RejectionCode = "expired"
	RangeTooLarge    RejectionCode = "too-large"
)

type RangeEffect struct {
	Claim    ClaimID
	Command  RangeCommand
	Released bool
}
type RangeConflict struct {
	Found bool
	Owner OwnerDiagnostic
	Range Range
	Mode  RangeMode
}
type RangeAttempt struct {
	Request          LockRequestID
	State            AttemptState
	Commands         []RangeCommand `json:",omitempty"`
	Claims           []ClaimID      `json:",omitempty"`
	Conflict         RangeConflict
	Rejection        RejectionCode
	Effects          []RangeEffect `json:",omitempty"`
	EverGranted      bool
	HistoryRemaining time.Duration
}

const (
	MaxRangeCommands = 64
	MaxRangeClaims   = 64
	MaxRangeEffects  = 64
)

func (r Range) Check() error {
	switch r.Kind {
	case Bytes:
		if r.Length == 0 || r.CutAt != 0 || r.Length-1 > ^uint64(0)-r.Start {
			return syscall.EINVAL
		}
	case Boundary:
		if r.Start != 0 || r.Length != 0 {
			return syscall.EINVAL
		}
	default:
		return syscall.EINVAL
	}
	return nil
}
func (c RangeCommand) Check() error {
	if err := c.Range.Check(); err != nil {
		return err
	}
	if c.Range.Kind == Boundary && (c.Edit == Replace || c.Edit == Subtract) {
		return syscall.EINVAL
	}
	if c.Domain < DomainRecord || c.Domain > DomainEnforced || c.Mode < RangeShared || c.Mode > RangeExclusive || c.Edit < Replace || c.Edit > RemoveExact || c.Conversion > DropBeforeAcquire {
		return syscall.EINVAL
	}
	if c.Edit == RemoveExact {
		if c.Wait || c.Conversion != PreserveBeforeAcquire {
			return syscall.EINVAL
		}
		if err := c.Claim.Check(); err != nil {
			return err
		}
	} else if c.Claim != "" {
		return syscall.EINVAL
	}
	if c.Edit == Subtract && (c.Wait || c.Conversion != PreserveBeforeAcquire) {
		return syscall.EINVAL
	}
	if (c.Policy.DenySelf|c.Policy.DenyOthers)&^(ReadData|WriteData) != 0 {
		return syscall.EINVAL
	}
	if c.Domain == DomainEnforced {
		if c.Edit != AddExact && c.Edit != RemoveExact {
			return syscall.EOPNOTSUPP
		}
		if c.Policy.DenyOthers == 0 {
			return fmt.Errorf("enforced range requires explicit I/O restrictions: %w", syscall.EINVAL)
		}
	} else {
		if c.Policy != (RangePolicy{}) || c.Edit == AddExact || c.Edit == RemoveExact {
			return syscall.EOPNOTSUPP
		}
	}
	if c.Conversion == DropBeforeAcquire && (c.Domain != DomainWholeFile || c.Edit != Replace) {
		return syscall.EINVAL
	}
	return nil
}
func (a RangeAttempt) Clone() RangeAttempt {
	a.Commands = append([]RangeCommand(nil), a.Commands...)
	a.Claims = append([]ClaimID(nil), a.Claims...)
	a.Effects = append([]RangeEffect(nil), a.Effects...)
	return a
}
