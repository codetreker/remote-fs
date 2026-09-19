package storage

import (
	"bytes"
	"context"
	"fmt"
	"syscall"
)

type NameBindingState uint8

const (
	NameRoot NameBindingState = iota + 1
	NameLinked
	NameDetached
)

// NameObservation captures the current binding of one immutable NodeID. Root
// and Detached have no parent or leaf; Linked owns its exact raw leaf bytes.
// Root identifies the selected volume root, not an unknown or absent binding.
type NameObservation struct {
	NodeID   uint64
	State    NameBindingState
	ParentID uint64
	RawLeaf  []byte `json:",omitempty"`
}

// ReferenceIdentity exposes only the immutable NodeID already owned by a
// reference. It performs no I/O and grants no access or lifetime. A closed
// reference may still report this identity; operations keep their normal
// liveness checks. Unsupported getters return EOPNOTSUPP, never a guessed zero.
// This optional facet does not change File or NodeReference requirements.
type ReferenceIdentity interface {
	ReferenceNodeID() (uint64, error)
}

// ReferenceNodeID verifies optional identity support without probing attributes
// or names. A getter failure is preserved; successful zero is a contract error.
func ReferenceNodeID(reference any) (uint64, error) {
	identity, ok := reference.(ReferenceIdentity)
	if !ok {
		return 0, syscall.EOPNOTSUPP
	}
	id, err := identity.ReferenceNodeID()
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, fmt.Errorf("reference identity getter returned zero: %w", syscall.EIO)
	}
	return id, nil
}

// ReferenceNameObserver is optional on File and NodeReference. It uses their
// existing lifetime and owning session, and checks guards with the binding in
// one authoritative capture. It grants no data, metadata-attribute or directory
// enumeration permission. HTTP authorizes this disclosure as a metadata snapshot.
// Unsupported checks and calls return EOPNOTSUPP. Every call error returns the
// zero observation. Its capability check requires a successful ReferenceIdentity
// getter, and callers verify observations against that immutable NodeID.
type ReferenceNameObserver interface {
	CheckReferenceNameObservation() error
	ObserveName(context.Context, *NamespaceGuards) (NameObservation, error)
}

// Check rejects contradictory state fields without interpreting platform names.
// The producer establishes Root identity and linkedness; this check validates
// their representation, not their truth against a later namespace state.
func (o NameObservation) Check() error {
	scalar := o
	scalar.RawLeaf = nil
	if err := checkNameObservationHeader(scalar, int64(len(o.RawLeaf))); err != nil {
		return err
	}
	if o.State != NameLinked {
		if o.RawLeaf != nil {
			return fmt.Errorf("unbound name observation carries a leaf: %w", syscall.EIO)
		}
		return nil
	}
	return CheckLeaf(o.RawLeaf)
}

func (o NameObservation) Clone() NameObservation {
	o.RawLeaf = bytes.Clone(o.RawLeaf)
	return o
}

func checkNameObservationHeader(scalar NameObservation, leafBytes int64) error {
	if scalar.NodeID == 0 || scalar.RawLeaf != nil {
		return fmt.Errorf("name observation requires identity and unloaded leaf: %w", syscall.EIO)
	}
	switch scalar.State {
	case NameRoot, NameDetached:
		if scalar.ParentID != 0 || leafBytes != 0 {
			return fmt.Errorf("unbound name observation carries parent or leaf length: %w", syscall.EIO)
		}
	case NameLinked:
		if scalar.ParentID == 0 || scalar.ParentID == scalar.NodeID || leafBytes <= 0 {
			return fmt.Errorf("linked name observation has invalid parent or leaf length: %w", syscall.EIO)
		}
		if leafBytes > MaxLeafBytes {
			return fmt.Errorf("observed leaf exceeds name bound: %w", syscall.ENAMETOOLONG)
		}
	default:
		return fmt.Errorf("unknown name binding state: %w", syscall.EIO)
	}
	return nil
}

// NameObservationBudget sizes the complete output prefix from copied scalar
// facts and the actual leaf length, before the leaf is loaded. Its charge must
// include at least NameObservationRetentionBytes plus any boundary encoding.
// Callbacks perform bounded sizing only: no I/O, reentry or retained inputs.
// They can return a known refusal when the configured result cannot fit.
type NameObservationBudget func(scalar NameObservation, leafBytes int64) (int64, error)

type nameObservationBudgetKey struct{}

func WithNameObservationBudget(ctx context.Context, budget NameObservationBudget) context.Context {
	if budget == nil {
		panic("storage: nil name observation budget")
	}
	return context.WithValue(ctx, nameObservationBudgetKey{}, budget)
}

// NameObservationRetentionBytes covers fixed bookkeeping and the retained raw
// name representations. It charges actual bytes, including zero for unbound names.
func NameObservationRetentionBytes(leafBytes int64) (int64, error) {
	if leafBytes < 0 {
		return 0, syscall.EINVAL
	}
	if leafBytes > MaxLeafBytes {
		return 0, syscall.ENAMETOOLONG
	}
	return 512 + 4*leafBytes, nil
}

// CheckNameObservationBudget validates the unloaded header before invoking any
// caller budget. Without a callback it returns the native retention charge. A
// callback cannot reduce that charge or return a negative charge as success.
func CheckNameObservationBudget(ctx context.Context, scalar NameObservation, leafBytes int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := checkNameObservationHeader(scalar, leafBytes); err != nil {
		return 0, err
	}
	minimum, err := NameObservationRetentionBytes(leafBytes)
	if err != nil {
		return 0, err
	}
	budget, _ := ctx.Value(nameObservationBudgetKey{}).(NameObservationBudget)
	if budget == nil {
		return minimum, nil
	}
	charge, err := budget(scalar, leafBytes)
	if err != nil {
		return 0, err
	}
	if charge < minimum {
		return 0, fmt.Errorf("name observation charge is below required residency: %w", syscall.EINVAL)
	}
	return charge, nil
}
