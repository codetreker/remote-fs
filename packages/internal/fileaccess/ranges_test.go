package fileaccess

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

var ownerA = Owner{Session: 1, ID: 0}
var ownerB = Owner{Session: 2, ID: 0}
var ownerC = Owner{Session: 3, ID: 0}

func replace(t *testing.T, c *Coordinator, owner Owner, scope Scope, ranges ...Acquisition) uint64 {
	t.Helper()
	revision, err := c.Revision()
	if err != nil {
		t.Fatal(err)
	}
	revision, err = c.ReplaceOwned(owner, scope, revision, ranges, nil)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func snapshot(t *testing.T, c *Coordinator, owner Owner, scope Scope) Snapshot {
	t.Helper()
	result, err := c.Snapshot(owner, scope)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestAcquisitionMultiplicityAndSnapshotsRemainIndependent(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	scope := Scope{Resource: 10, Enforced: true}
	first := Acquisition{ID: 1, Start: 10, End: 19}
	second := first
	second.ID = 2
	input := []Acquisition{second, first}
	replace(t, c, ownerA, scope, input...)
	input[0].Start = 0
	view := snapshot(t, c, ownerA, scope)
	if !reflect.DeepEqual(view.Own, []Acquisition{first, second}) {
		t.Fatalf("acquisitions merged or aliased: %+v", view.Own)
	}
	view.Own[0].End = 100
	other := snapshot(t, c, ownerB, scope)
	other.Other[0].Acquisition.End = 100
	if got := snapshot(t, c, ownerA, scope).Own; !reflect.DeepEqual(got, []Acquisition{first, second}) {
		t.Fatalf("snapshot changed authority: %+v", got)
	}
	replace(t, c, ownerA, scope, second)
	if err := c.CheckIO(10, &ownerB, Span{Start: 10, End: 19}, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("removing one duplicate released both: %v", err)
	}
	if err := c.CheckIO(10, &ownerA, Span{Start: 10, End: 19}, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("shared acquisition allowed holder write: %v", err)
	}
	if err := c.CheckIO(10, nil, Span{Start: 10, End: 19}, false); err != nil {
		t.Fatal(err)
	}
	replace(t, c, ownerA, scope)
	if err := c.CheckIO(10, nil, Span{Start: 10, End: 19}, true); err != nil {
		t.Fatal(err)
	}
}

func TestEnforcedOwnerZeroAndAdvisoryDomainsRemainDistinct(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	exclusive := Acquisition{ID: 1, Start: 0, End: math.MaxUint64, Exclusive: true}
	replace(t, c, ownerA, Scope{Resource: 10, Enforced: true}, exclusive)
	for _, actor := range []*Owner{nil, &ownerB} {
		err := c.CheckIO(10, actor, Span{Start: math.MaxUint64, End: math.MaxUint64}, false)
		var conflict *Conflict
		if !errors.As(err, &conflict) || conflict.Held.Owner != ownerA || conflict.Error() != ErrConflict.Error() {
			t.Fatalf("anonymous/foreign owner borrowed owner0: %v", err)
		}
	}
	if err := c.CheckIO(10, &ownerA, Span{Start: 0, End: 1}, true); err != nil {
		t.Fatal(err)
	}
	replace(t, c, ownerA, Scope{Resource: 10, Enforced: true})
	replace(t, c, ownerA, Scope{Resource: 10, Domain: 0}, exclusive)
	replace(t, c, ownerB, Scope{Resource: 10, Domain: 1}, exclusive)
	view := snapshot(t, c, ownerB, Scope{Resource: 10, Domain: 0})
	if _, err := c.ReplaceOwned(ownerB, Scope{Resource: 10, Domain: 0}, view.Revision, []Acquisition{exclusive}, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("same advisory domain did not conflict: %v", err)
	}
	if err := c.CheckIO(10, nil, Span{Start: 0, End: math.MaxUint64}, true); err != nil {
		t.Fatalf("advisory ranges constrained ordinary I/O: %v", err)
	}
}

func TestBoundaryAcquisitionsPreserveAnchorsAndCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRanges = 2
	limits.MaxOwnerRanges = 2
	limits.MaxSetRanges = 2
	c := coordinator(t, limits)
	scope := Scope{Resource: 10, Enforced: true}
	boundary := Acquisition{ID: 1, Start: 42, End: 42, Boundary: true, Exclusive: true}
	replace(t, c, ownerA, scope, boundary)
	other := boundary
	other.ID = 2
	replace(t, c, ownerB, scope, other)
	if got := snapshot(t, c, ownerA, scope).Own; len(got) != 1 || got[0] != boundary {
		t.Fatalf("boundary lost anchor: %+v", got)
	}
	if err := c.CheckIO(10, nil, Span{Boundary: true}, true); err != nil {
		t.Fatalf("zero boundary intersected: %v", err)
	}
	replace(t, c, ownerB, scope)
	if err := c.CheckIO(10, nil, Span{Start: 0, End: math.MaxUint64}, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("crossing boundary was allowed: %v", err)
	}
	if err := c.CheckIO(10, nil, Span{Start: 42, End: 42}, true); err != nil {
		t.Fatalf("starting at boundary was rejected: %v", err)
	}
	replace(t, c, ownerA, scope, boundary, other)
	if available := snapshot(t, c, ownerB, scope).Available; available != 0 {
		t.Fatalf("boundary acquisitions were not charged: %d", available)
	}
}

func TestGuardRevisionCoversOtherResourcesCapacityAndExpiryFacts(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRanges = 3
	limits.MaxOwnerRanges = 3
	limits.MaxSetRanges = 3
	c := coordinator(t, limits)
	scope := Scope{Resource: 10, Domain: 7}
	old := snapshot(t, c, ownerA, scope)
	replace(t, c, ownerB, Scope{Resource: 20, Domain: 7}, Acquisition{ID: 1, Start: 0, End: 0})
	replace(t, c, ownerB, Scope{Resource: 30, Domain: 7}, Acquisition{ID: 2, Start: 0, End: 0})
	if _, err := c.ReplaceOwned(ownerA, scope, old.Revision, nil, nil); err != ErrRevision {
		t.Fatalf("unchanged decision accepted obsolete capacity guard: %v", err)
	}
	view := snapshot(t, c, ownerA, scope)
	if view.Available != 1 {
		t.Fatalf("global available capacity=%d, want1", view.Available)
	}
	proposed := []Acquisition{{ID: 1, Start: 0, End: 0}, {ID: 2, Start: 2, End: 2}}
	if _, err := c.ReplaceOwned(ownerA, scope, view.Revision, proposed, nil); err != ErrCapacity {
		t.Fatalf("cross-resource capacity overcommit: %v", err)
	}
	if c.ranges != 2 || len(snapshot(t, c, ownerA, scope).Own) != 0 {
		t.Fatal("capacity refusal changed ranges")
	}
	if err := c.RetireSession(ownerB.Session); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReplaceOwned(ownerA, scope, view.Revision, proposed, nil); err != ErrRevision {
		t.Fatalf("retirement did not invalidate guard: %v", err)
	}
	replace(t, c, ownerA, scope, proposed...)
	view = snapshot(t, c, ownerA, scope)
	if err := c.Invalidate(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReplaceOwned(ownerA, scope, view.Revision, view.Own, nil); err != ErrRevision {
		t.Fatalf("external expiry/lease fact accepted old no-op: %v", err)
	}
	view = snapshot(t, c, ownerA, scope)
	revision, err := c.ReplaceOwned(ownerA, scope, view.Revision, view.Own, nil)
	if err != nil || revision != view.Revision {
		t.Fatalf("verified no-op changed revision: %d %v", revision, err)
	}
}

func TestRangesRejectMalformedSetsWithoutChangingOwnership(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	scope := Scope{Resource: 10, Enforced: true}
	held := Acquisition{ID: 1, Start: 10, End: 20, Exclusive: true}
	replace(t, c, ownerA, scope, held)
	for _, set := range [][]Acquisition{
		{{Start: 0, End: 0}}, {{ID: 2, Start: 2, End: 1}}, {{ID: 2, Start: 1, End: 2, Boundary: true}},
		{{ID: 2, Start: 0, End: 0}, {ID: 2, Start: 5, End: 5}},
	} {
		revision, _ := c.Revision()
		if _, err := c.ReplaceOwned(ownerB, scope, revision, set, nil); err != ErrInvalid {
			t.Fatalf("malformed set %+v: %v", set, err)
		}
	}
	view := snapshot(t, c, ownerB, scope)
	if _, err := c.ReplaceOwned(ownerB, scope, view.Revision, []Acquisition{{ID: 2, Start: 20, End: 21, Exclusive: true}}, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("inclusive endpoint did not conflict: %v", err)
	}
	if len(snapshot(t, c, ownerB, scope).Own) != 0 || !reflect.DeepEqual(snapshot(t, c, ownerA, scope).Own, []Acquisition{held}) {
		t.Fatal("failed CAS changed another owner")
	}
	if _, err := c.ReplaceOwned(ownerB, scope, 0, nil, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	for _, invalid := range []Scope{{}, {Resource: 10, Domain: 1, Enforced: true}} {
		if _, err := c.Snapshot(ownerA, invalid); err != ErrInvalid {
			t.Fatal(err)
		}
	}
	if result, err := c.Snapshot(Owner{Session: 9}, scope); err != nil || len(result.Own) != 0 || len(result.Other) != 1 {
		t.Fatalf("unseen owner snapshot failed: %+v %v", result, err)
	}
	if _, err := c.Snapshot(Owner{}, scope); err != ErrInvalid {
		t.Fatal(err)
	}
	for _, span := range []Span{{Start: 2, End: 1}, {Start: 1, End: 2, Boundary: true}} {
		if err := c.CheckIO(10, nil, span, true); err != ErrInvalid {
			t.Fatal(err)
		}
	}
	if err := c.CheckIO(0, nil, Span{}, false); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.CheckIO(10, &Owner{}, Span{}, false); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.CheckIO(10, &Owner{Session: 9}, Span{Boundary: true}, false); err != nil {
		t.Fatal(err)
	}
}

func TestRangeSnapshotOwnerAndWorkBudgetsAreIndependent(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRanges = 4
	limits.MaxOwnerRanges = 2
	limits.MaxSetRanges = 2
	limits.MaxSnapshotRanges = 1
	limits.MaxWork = 1
	c := coordinator(t, limits)
	scope := Scope{Resource: 10, Enforced: true}
	replace(t, c, ownerA, scope, Acquisition{ID: 1, Start: 0, End: 0}, Acquisition{ID: 2, Start: 2, End: 2})
	if result, err := c.Snapshot(ownerB, scope); err != ErrCapacity || result.Own != nil || result.Other != nil {
		t.Fatalf("snapshot exposed an over-budget prefix: %+v %v", result, err)
	}
	revision, _ := c.Revision()
	if _, err := c.ReplaceOwned(ownerA, Scope{Resource: 20}, revision, []Acquisition{{ID: 3, Start: 0, End: 0}}, nil); err != ErrCapacity {
		t.Fatalf("owner aggregate capacity ignored: %v", err)
	}
	if _, err := c.ReplaceOwned(ownerB, scope, revision, []Acquisition{{ID: 1, Start: 4, End: 4}}, nil); err != ErrCapacity {
		t.Fatalf("unbounded conflict scan: %v", err)
	}
	if err := c.CheckIO(10, nil, Span{Start: 4, End: 4}, true); err != ErrCapacity {
		t.Fatalf("unbounded I/O conflict scan: %v", err)
	}
	if _, err := c.ReplaceOwned(ownerB, scope, revision, []Acquisition{{ID: 1}, {ID: 2}, {ID: 3}}, nil); err != ErrCapacity {
		t.Fatalf("set budget ignored: %v", err)
	}
	if c.ranges != 2 {
		t.Fatal("budget failure changed held range count")
	}
}

func TestObservationalSnapshotsDoNotConsumeOwnersOrInvalidateFirstCAS(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxOwners = 1
	c := coordinator(t, limits)
	scope := Scope{Resource: 10, Enforced: true}
	initial, _ := c.Revision()
	for id := uint64(0); id < 100; id++ {
		actor := Owner{Session: 1, ID: id}
		view := snapshot(t, c, actor, scope)
		if view.Revision != initial || len(view.Own) != 0 || len(c.owners) != 0 {
			t.Fatal("observational snapshot allocated owner state")
		}
		if err := c.CheckIO(10, &actor, Span{}, false); err != nil {
			t.Fatalf("unseen actor cannot perform ordinary I/O: %v", err)
		}
	}
	view := snapshot(t, c, ownerA, scope)
	if _, err := c.ReplaceOwned(ownerA, scope, view.Revision, []Acquisition{{ID: 1, Start: 0, End: 9, Exclusive: true}}, nil); err != nil {
		t.Fatalf("first CAS invalidated its own snapshot: %v", err)
	}
	other := snapshot(t, c, ownerB, scope)
	if len(other.Other) != 1 || other.OwnerAvailable != 0 {
		t.Fatalf("full owner budget hid observation or promised a slot: %+v", other)
	}
	if revision, err := c.ReplaceOwned(ownerB, scope, other.Revision, nil, nil); err != nil || revision != other.Revision || len(c.owners) != 1 {
		t.Fatalf("empty no-op allocated an owner: %d %v", revision, err)
	}
	wait := registerWait(t, c, 1, ownerB, scope, []Acquisition{{ID: 2, Start: 0, End: 9}}, true)
	if len(c.owners) != 1 || len(c.waits) != 1 {
		t.Fatal("wait-only actor consumed held-owner capacity")
	}
	wait.Cancel()
}

func TestFinalScopeCleanupReclaimsIdleOwnersWithoutLosingOtherScopes(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	one, two := Scope{Resource: 10}, Scope{Resource: 20, Domain: 1}
	rangeA := Acquisition{ID: 1, Start: 0, End: 9}
	replace(t, c, ownerA, one, rangeA)
	replace(t, c, ownerA, two, rangeA)
	replace(t, c, ownerA, one)
	if count, err := c.OwnerRangeCount(ownerA); err != nil || count != 1 {
		t.Fatalf("file cleanup removed another scope: %d %v", count, err)
	}
	replace(t, c, ownerA, two)
	if count, err := c.OwnerRangeCount(ownerA); err != nil || count != 0 || len(c.owners) != 0 {
		t.Fatalf("final cleanup retained idle owner: %d %v", count, err)
	}
	wait := registerWait(t, c, 1, ownerB, one, nil, false)
	if err := c.RetireOwner(ownerB); err != nil {
		t.Fatal(err)
	}
	if _, err := wait.Await(context.Background()); err != ErrRetired {
		t.Fatal(err)
	}
	before, _ := c.Revision()
	if err := c.RetireOwner(ownerB); err != nil {
		t.Fatal(err)
	}
	if after, _ := c.Revision(); after != before {
		t.Fatal("already-idle owner cleanup churned the guard")
	}
	if _, err := c.OwnerRangeCount(Owner{}); err != ErrInvalid {
		t.Fatal(err)
	}
}

func TestBoundaryIntersectionMatchesInteriorCuts(t *testing.T) {
	for anchor := uint64(0); anchor <= 8; anchor++ {
		boundary := Acquisition{ID: 1, Start: anchor, End: anchor, Boundary: true}
		for start := uint64(0); start <= 8; start++ {
			for end := start; end <= 8; end++ {
				ordinary := Acquisition{ID: 2, Start: start, End: end}
				crosses := false
				for cut := start + 1; cut <= end; cut++ {
					if cut == anchor {
						crosses = true
					}
				}
				if overlap(boundary, ordinary) != crosses || overlap(ordinary, boundary) != crosses {
					t.Fatalf("boundary%d vs[%d,%d] = wrong intersection", anchor, start, end)
				}
				if overlap(boundary, Acquisition{Start: start, End: start, Boundary: true}) {
					t.Fatal("two boundaries intersected")
				}
			}
		}
	}
	maximum := Acquisition{ID: 1, Start: math.MaxUint64, End: math.MaxUint64, Boundary: true, Exclusive: true}
	if !overlap(maximum, Acquisition{Start: math.MaxUint64 - 1, End: math.MaxUint64}) || overlap(maximum, Acquisition{Start: math.MaxUint64, End: math.MaxUint64}) {
		t.Fatal("maximum boundary overflowed")
	}
	if overlap(Acquisition{Boundary: true}, Acquisition{Start: 0, End: math.MaxUint64}) {
		t.Fatal("zero boundary intersected unsigned content")
	}
	c := coordinator(t, DefaultLimits())
	scope := Scope{Resource: 10, Enforced: true}
	replace(t, c, ownerA, scope, maximum)
	view := snapshot(t, c, ownerB, scope)
	if _, err := c.ReplaceOwned(ownerB, scope, view.Revision, []Acquisition{{ID: 2, Start: math.MaxUint64 - 1, End: math.MaxUint64}}, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("CAS crossed protected boundary: %v", err)
	}
}
