package storage_test

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNameObservationDistinguishesBindingsAndOwnsRawBytes(t *testing.T) {
	for _, observation := range []storage.NameObservation{
		{NodeID: 1, State: storage.NameRoot},
		{NodeID: 2, State: storage.NameDetached},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff, ':', '\\'}},
	} {
		if err := observation.Check(); err != nil {
			t.Fatalf("valid name observation %+v: %v", observation, err)
		}
	}
	backing := make([]byte, 1<<20)
	backing[len(backing)-1] = 0xff
	original := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: backing[len(backing)-1:]}
	copy := original.Clone()
	if unsafe.SliceData(copy.RawLeaf) == unsafe.SliceData(original.RawLeaf) {
		t.Fatal("cloned leaf retains caller backing allocation")
	}
	original.RawLeaf[0] = 'x'
	if copy.RawLeaf[0] != 0xff {
		t.Fatal("cloned raw name changed with caller bytes")
	}
	if got := (storage.NameObservation{NodeID: 1, State: storage.NameRoot}).Clone(); got.RawLeaf != nil {
		t.Fatal("root clone invented name payload")
	}
}

func TestNameObservationRejectsContradictoryFields(t *testing.T) {
	for _, observation := range []storage.NameObservation{
		{}, {NodeID: 1}, {NodeID: 1, State: 255},
		{NodeID: 1, State: storage.NameRoot, ParentID: 2},
		{NodeID: 1, State: storage.NameRoot, RawLeaf: []byte{}},
		{NodeID: 1, State: storage.NameDetached, RawLeaf: []byte("old")},
		{NodeID: 1, State: storage.NameDetached, ParentID: 2},
		{NodeID: 2, State: storage.NameLinked, RawLeaf: []byte("x")},
		{NodeID: 2, State: storage.NameLinked, ParentID: 2, RawLeaf: []byte("x")},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte("..")},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte("a/b")},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{'a', 0}},
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: make([]byte, storage.MaxLeafBytes+1)},
	} {
		if err := observation.Check(); err == nil {
			t.Fatalf("contradictory name observation accepted: %+v", observation)
		}
	}
}

func TestNameObservationBudgetUsesActualUnloadedName(t *testing.T) {
	scalar := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}
	for _, length := range []int64{1, storage.MaxLeafBytes} {
		charge, err := storage.CheckNameObservationBudget(t.Context(), scalar, length)
		if err != nil || charge != 512+4*length {
			t.Fatalf("default name charge for %d: %d %v", length, charge, err)
		}
	}
	for _, state := range []storage.NameBindingState{storage.NameRoot, storage.NameDetached} {
		charge, err := storage.CheckNameObservationBudget(t.Context(), storage.NameObservation{NodeID: 2, State: state}, 0)
		if err != nil || charge != 512 {
			t.Fatalf("unbound name charge: %d %v", charge, err)
		}
	}
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(got storage.NameObservation, length int64) (int64, error) {
		calls++
		if got.RawLeaf != nil || got.NodeID != scalar.NodeID || got.ParentID != scalar.ParentID || length != 1 {
			t.Fatalf("budget received wrong or loaded header: %+v %d", got, length)
		}
		got.NodeID = 99
		return 700, nil
	})
	charge, err := storage.CheckNameObservationBudget(ctx, scalar, 1)
	if err != nil || charge != 700 || calls != 1 || scalar.NodeID != 2 {
		t.Fatalf("custom name charge: %d %v calls=%d", charge, err, calls)
	}
	if charge, err := storage.CheckNameObservationBudget(t.Context(), scalar, 1); err != nil || charge != 516 || calls != 1 {
		t.Fatal("name budget escaped its request context")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if charge, err := storage.CheckNameObservationBudget(cancelled, scalar, 1); charge != 0 || !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled name admission: %d %v", charge, err)
	}
}

func TestNameObservationBudgetRejectsInvalidHeadersAndUndercharging(t *testing.T) {
	scalar := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) { calls++; return math.MaxInt64, nil })
	for _, test := range []struct {
		header storage.NameObservation
		length int64
	}{
		{storage.NameObservation{}, 0},
		{storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{}}, 1},
		{scalar, 0}, {scalar, -1}, {scalar, storage.MaxLeafBytes + 1}, {scalar, math.MaxInt64},
		{storage.NameObservation{NodeID: 2, State: storage.NameRoot}, 1},
		{storage.NameObservation{NodeID: 2, State: storage.NameDetached, ParentID: 1}, 0},
	} {
		if charge, err := storage.CheckNameObservationBudget(ctx, test.header, test.length); charge != 0 || err == nil {
			t.Fatalf("invalid name header admitted: %d %v", charge, err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid header reached name budget")
	}
	for _, charge := range []int64{-1, 0, 515} {
		bounded := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) { return charge, nil })
		if got, err := storage.CheckNameObservationBudget(bounded, scalar, 1); got != 0 || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("undercharged name result: %d %v", got, err)
		}
	}
	refusal := errors.New("name response budget exhausted")
	bounded := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) { return 900, refusal })
	if got, err := storage.CheckNameObservationBudget(bounded, scalar, 1); got != 0 || err != refusal {
		t.Fatalf("callback failure lost: %d %v", got, err)
	}
	if _, err := storage.NameObservationRetentionBytes(-1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative name size: %v", err)
	}
	if _, err := storage.NameObservationRetentionBytes(math.MaxInt64); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("overflowing name size: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("nil name budget accepted")
		}
	}()
	_ = storage.WithNameObservationBudget(t.Context(), nil)
}

type referenceIdentityFunc func() (uint64, error)

func (f referenceIdentityFunc) ReferenceNodeID() (uint64, error) { return f() }

func TestReferenceNodeIDUsesOnlyTheOptionalImmutableGetter(t *testing.T) {
	calls := 0
	identity := referenceIdentityFunc(func() (uint64, error) { calls++; return 42, nil })
	for range 2 {
		id, err := storage.ReferenceNodeID(identity)
		if err != nil || id != 42 {
			t.Fatalf("immutable identity=%d %v", id, err)
		}
	}
	if calls != 2 {
		t.Fatalf("identity getter calls=%d", calls)
	}
	for _, missing := range []any{nil, struct{}{}} {
		if id, err := storage.ReferenceNodeID(missing); id != 0 || !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported identity=%d %v", id, err)
		}
	}
	cause := errors.New("identity capability unavailable")
	if id, err := storage.ReferenceNodeID(referenceIdentityFunc(func() (uint64, error) { return 42, cause })); id != 0 || err != cause {
		t.Fatalf("failed getter disclosed identity=%d %v", id, err)
	}
	if id, err := storage.ReferenceNodeID(referenceIdentityFunc(func() (uint64, error) { return 0, nil })); id != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero identity accepted=%d %v", id, err)
	}
}
