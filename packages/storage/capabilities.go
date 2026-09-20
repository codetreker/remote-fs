package storage

import (
	"context"
	"syscall"
)

// ScopedReference returns an opaque capability for one exact live File reference.
// The token carries no authority until the owning session validates it against the
// target node identity.
type ScopedReference interface {
	CheckScopedReference() error
	Scope(context.Context) (UseScope, error)
}

// MetadataAccess compares and replaces exactly one metadata namespace atomically.
// An empty expected version requires absence; an empty payload stores a present empty
// value. The authority assigns the returned version and advances ChangeTime.
type MetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, uint64, string, []byte, []byte) (OpaquePayload, error)
}

// ReferenceMetadataAccess applies the same metadata namespace operation through a
// retained File identity.
type ReferenceMetadataAccess interface {
	CheckMetadataAccess() error
	SetMetadata(context.Context, string, []byte, []byte) (OpaquePayload, error)
}

// UseOwners registers range-control owners against a validated live reference scope.
// Numeric owner values alone convey no authority.
type UseOwners interface {
	CheckUseOwners() error
	NewUseOwner(context.Context, uint64, UseScope, OwnerOptions) (UseOwner, error)
	RetireUseOwner(context.Context, UseOwner) error
}

// RangeControl keeps finite, replayable request receipts. A repeated request identity
// returns its retained outcome; changed intent is rejected. Cancellation does not prove
// that a concurrent grant failed, so callers reconcile the returned attempt.
type RangeControl interface {
	CheckRangeControl() error
	GetConflict(context.Context, UseOwner, RangeCommand) (RangeConflict, error)
	Apply(context.Context, UseOwner, []RangeCommand, LockRequestID) (RangeAttempt, error)
	Query(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Cancel(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
	Drop(context.Context, UseOwner, ConflictDomain) error
}

const MaxScopeBytes = 128

type UseScope struct{ Token string }

func (s UseScope) Check() error {
	if len(s.Token) == 0 || len(s.Token) > MaxScopeBytes {
		return syscall.EINVAL
	}
	for i := range s.Token {
		if s.Token[i] == 0 {
			return syscall.EINVAL
		}
	}
	return nil
}

type Uses uint8

const (
	ReadData Uses = 1 << iota
	WriteData
	ReadEntries
	DeleteName
)

const AllUses = ReadData | WriteData | ReadEntries | DeleteName

// UseClaim declares what one reference may do and what uses it denies to every
// other reference. Conflicts are symmetric: either claim's Deny set may reject
// the other claim's Uses set.
type UseClaim struct{ Uses, Deny Uses }

func (c UseClaim) Check() error {
	if (c.Uses|c.Deny)&^AllUses != 0 {
		return syscall.EINVAL
	}
	return nil
}

type UseOwner uint64
type OwnerDiagnostic uint64

type OwnerOptions struct {
	Lifetime OwnerLifetime
	// Group identifies a session-local deadlock participant. Zero keeps the
	// owner independent; it never shares claims or use exemptions.
	Group uint64
}

type OwnerLifetime uint8

const (
	OwnerReference OwnerLifetime = iota + 1
	OwnerExplicit
)

func (o OwnerOptions) Check() error {
	if o.Lifetime != OwnerReference && o.Lifetime != OwnerExplicit {
		return syscall.EINVAL
	}
	return nil
}

type capabilityError struct {
	message string
	errno   syscall.Errno
}

func (e *capabilityError) Error() string { return e.message }
func (e *capabilityError) Unwrap() error { return e.errno }

var (
	ErrUseConflict   error = &capabilityError{"conflicting reference use", syscall.EAGAIN}
	ErrRangeConflict error = &capabilityError{"conflicting enforced range", syscall.EAGAIN}
	ErrInvalidScope  error = &capabilityError{"reference scope invalid", syscall.ESTALE}
)

func CheckMetadataUpdate(namespace string, expectedVersion, payload []byte) error {
	if err := CheckMetadataNamespace(namespace); err != nil {
		return err
	}
	if len(expectedVersion) > MaxObservationTokenBytes {
		return syscall.EINVAL
	}
	if len(payload) > MaxMetadataValueBytes {
		return syscall.EFBIG
	}
	return nil
}
