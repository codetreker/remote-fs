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
// Root identifies the selected volume root, not an unknown binding.
type NameObservation struct {
	NodeID   uint64
	State    NameBindingState
	ParentID uint64
	RawLeaf  []byte `json:",omitempty"`
}

// ReferenceIdentity exposes only the immutable NodeID already owned by a
// retained reference. It performs no I/O and grants no access or lifetime.
type ReferenceIdentity interface {
	ReferenceNodeID() (uint64, error)
}

// ReferenceNodeID verifies optional identity support without probing mutable
// attributes or names. A successful zero identity is a contract failure.
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
// one authoritative capture. It grants no attribute, directory enumeration or
// name-mutation permission. Every failed call returns a zero observation.
type ReferenceNameObserver interface {
	CheckReferenceNameObservation() error
	ObserveName(context.Context, *NamespaceGuards) (NameObservation, error)
}

// Check rejects contradictory state fields without interpreting platform names.
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

// NameObservationBudget sizes the complete output from scalar facts and the
// actual raw leaf length before the leaf is loaded. Callbacks perform bounded
// accounting only and may refuse a result that cannot fit.
type NameObservationBudget func(scalar NameObservation, leafBytes int64) (int64, error)

type nameObservationBudgetKey struct{}

func WithNameObservationBudget(ctx context.Context, budget NameObservationBudget) context.Context {
	if budget == nil {
		panic("storage: nil name observation budget")
	}
	return context.WithValue(ctx, nameObservationBudgetKey{}, budget)
}

// NameObservationRetentionBytes covers fixed bookkeeping and every retained
// raw-name representation.
func NameObservationRetentionBytes(leafBytes int64) (int64, error) {
	if leafBytes < 0 {
		return 0, syscall.EINVAL
	}
	if leafBytes > MaxLeafBytes {
		return 0, syscall.ENAMETOOLONG
	}
	return 512 + 4*leafBytes, nil
}

// CheckNameObservationBudget validates an unloaded header before invoking a
// caller budget. A callback cannot undercharge the native retention bound.
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
