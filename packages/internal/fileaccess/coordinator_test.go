package fileaccess

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestPublicationGuardFailurePreservesClaimsRangesAndWaits(t *testing.T) {
	scope := Scope{Resource: 10}
	for _, test := range []struct {
		name string
		call func(*Coordinator, uint64, Guard) error
	}{
		{"register claim", func(c *Coordinator, _ uint64, g Guard) error { return c.RegisterClaim(2, 2, 10, Claim{Uses: 2}, g) }},
		{"replace claim", func(c *Coordinator, _ uint64, g Guard) error { return c.ReplaceClaim(1, Claim{Uses: 3}, g) }},
		{"unchanged claim", func(c *Coordinator, _ uint64, g Guard) error { return c.ReplaceClaim(1, Claim{Uses: 1}, g) }},
		{"replace ranges", func(c *Coordinator, r uint64, g Guard) error {
			_, err := c.ReplaceOwned(ownerA, scope, r, []Acquisition{{ID: 2, End: 19}}, g)
			return err
		}},
		{"new range owner", func(c *Coordinator, r uint64, g Guard) error {
			_, err := c.ReplaceOwned(ownerB, Scope{Resource: 30}, r, []Acquisition{{ID: 1}}, g)
			return err
		}},
		{"unchanged ranges", func(c *Coordinator, r uint64, g Guard) error {
			_, err := c.ReplaceOwned(ownerA, scope, r, []Acquisition{{ID: 1, End: 9}}, g)
			return err
		}},
		{"register wait", func(c *Coordinator, r uint64, g Guard) error {
			_, err := c.RegisterWait(2, ownerB, scope, r, []Acquisition{{ID: 2, End: 9, Exclusive: true}}, true, g)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := coordinator(t, DefaultLimits())
			if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 1}, nil); err != nil {
				t.Fatal(err)
			}
			replace(t, c, ownerA, scope, Acquisition{ID: 1, End: 9})
			pending := registerWait(t, c, 1, ownerC, Scope{Resource: 20}, nil, false)
			before := snapshot(t, c, ownerA, scope)
			claim := c.claims[1]
			failure := errors.Join(context.Canceled, errors.New("native reference retired"))
			calls := 0
			guard := func() error {
				calls++
				if c.mu.TryLock() {
					c.mu.Unlock()
					t.Error("publication guard ran outside coordinator ordering")
				}
				if c.revision != before.Revision || !pending.pending || len(c.waits) != 1 || c.claims[1] != claim {
					t.Error("authority changed before final guard")
				}
				return failure
			}
			if err := test.call(c, before.Revision, guard); err != failure || calls != 1 {
				t.Fatalf("guard result=%v,calls=%d", err, calls)
			}
			if after := snapshot(t, c, ownerA, scope); !reflect.DeepEqual(after, before) || len(c.owners) != 1 || c.ranges != 1 || len(c.claims) != 1 || c.claims[1] != claim {
				t.Fatalf("rejected publication changed retained authority: %+v", after)
			}
			if len(c.waits) != 1 || c.waitRanges != 0 || c.dependencies != 0 || !pending.pending {
				t.Fatal("rejected publication changed wait budgets or dependencies")
			}
			select {
			case <-pending.done:
				t.Fatal("rejected publication notified a new revision")
			default:
			}
			if err := test.call(c, before.Revision, func() error { calls++; return nil }); err != nil || calls != 2 {
				t.Fatalf("allowed publication=%v,calls=%d", err, calls)
			}
		})
	}
}

func TestPublicationGuardsFollowConflictAndCapacityChecks(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxWaits = 0
	c := coordinator(t, limits)
	if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 1, Excludes: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(2, 2, 10, Claim{Uses: 1}, nil); err != nil {
		t.Fatal(err)
	}
	scope := Scope{Resource: 10}
	revision := replace(t, c, ownerA, scope, Acquisition{ID: 1, End: 9, Exclusive: true})
	calls := 0
	guard := func() error { calls++; return nil }
	for _, test := range []struct {
		call func() error
		want error
	}{
		{func() error { return c.RegisterClaim(3, 3, 10, Claim{Uses: 2}, guard) }, ErrConflict},
		{func() error { return c.ReplaceClaim(1, Claim{Uses: 1, Excludes: 1}, guard) }, ErrConflict},
		{func() error {
			_, err := c.ReplaceOwned(ownerB, scope, revision, []Acquisition{{ID: 2}}, guard)
			return err
		}, ErrConflict},
		{func() error { _, err := c.RegisterWait(1, ownerB, scope, revision, nil, true, guard); return err }, ErrCapacity},
	} {
		if err := test.call(); !errors.Is(err, test.want) {
			t.Fatalf("admission=%v,want %v", err, test.want)
		}
	}
	if calls != 0 {
		t.Fatalf("publication guards ran before rejected admission: %d", calls)
	}
}

func TestLimitsRejectUnboundedAndIncoherentState(t *testing.T) {
	for _, change := range []func(*Limits){
		func(l *Limits) { l.MaxClaims = 0 }, func(l *Limits) { l.MaxOwners = math.MaxInt },
		func(l *Limits) { l.MaxRanges = -1 }, func(l *Limits) { l.MaxOwnerRanges = l.MaxRanges + 1 },
		func(l *Limits) { l.MaxSetRanges = l.MaxOwnerRanges + 1 }, func(l *Limits) { l.MaxSnapshotRanges = 0 },
		func(l *Limits) { l.MaxWaitRanges = 0 }, func(l *Limits) { l.MaxWork = 0 },
		func(l *Limits) { l.MaxWaits = -1 }, func(l *Limits) { l.MaxWaits = math.MaxInt },
		func(l *Limits) { l.MaxDependencies = -1 }, func(l *Limits) { l.MaxDependencies = math.MaxInt },
	} {
		limits := DefaultLimits()
		change(&limits)
		if limits.Check() != ErrInvalid {
			t.Fatalf("accepted invalid limits: %+v", limits)
		}
		if value, err := New(limits); value != nil || err != ErrInvalid {
			t.Fatalf("constructed invalid coordinator: %v %v", value, err)
		}
	}
	limits := DefaultLimits()
	limits.MaxWaits = 0
	limits.MaxDependencies = 0
	c := coordinator(t, limits)
	revision, _ := c.Revision()
	if _, err := c.RegisterWait(1, ownerA, Scope{Resource: 1}, revision, nil, false, nil); err != ErrCapacity {
		t.Fatalf("disabled wait budget admitted work: %v", err)
	}
}

func TestOwnerRetirementReclaimsAllScopesButNotOtherSessions(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxOwners = 2
	c := coordinator(t, limits)
	for _, scope := range []Scope{{Resource: 1, Domain: 0}, {Resource: 2, Enforced: true}} {
		replace(t, c, ownerA, scope, Acquisition{ID: 1, Start: 0, End: 0})
	}
	scope := Scope{Resource: 3}
	replace(t, c, ownerB, scope, Acquisition{ID: 1, Start: 0, End: 0})
	before := snapshot(t, c, ownerC, scope)
	if len(before.Other) != 1 || before.OwnerAvailable != 0 {
		t.Fatalf("owner ceiling hid conflicts or offered capacity: %+v", before)
	}
	if _, err := c.ReplaceOwned(ownerC, Scope{Resource: 4}, before.Revision, []Acquisition{{ID: 1}}, nil); err != ErrCapacity {
		t.Fatal(err)
	}

	ownWait := registerWait(t, c, 1, ownerA, scope, nil, false)
	otherWait := registerWait(t, c, 2, ownerB, scope, nil, false)
	if err := c.RetireOwner(ownerA); err != nil {
		t.Fatal(err)
	}
	if _, err := ownWait.Await(context.Background()); err != ErrRetired {
		t.Fatal(err)
	}
	if _, err := otherWait.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.ranges != 1 || len(c.scopes) != 1 || len(snapshot(t, c, ownerB, scope).Own) != 1 {
		t.Fatal("retirement leaked own state or cleared another session")
	}
	replace(t, c, ownerC, Scope{Resource: 4}, Acquisition{ID: 1})
	if err := c.RetireOwner(ownerA); err != nil {
		t.Fatal(err)
	}
	if err := c.RetireOwner(Owner{}); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.RetireSession(0); err != ErrInvalid {
		t.Fatal(err)
	}
}

func TestCloseAndRevisionExhaustionCannotExposeSuccessfulAdmission(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		c := coordinator(t, DefaultLimits())
		scope := Scope{Resource: 10}
		if err := c.RegisterClaim(1, ownerA.Session, 10, Claim{Uses: 1}, nil); err != nil {
			t.Fatal(err)
		}
		replace(t, c, ownerA, scope, Acquisition{ID: 1, Start: 0, End: 9})
		wait := registerWait(t, c, 1, ownerB, scope, nil, false)
		want := ErrClosed
		if exhaust {
			c.revision = math.MaxUint64
			want = ErrExhausted
			if err := c.Invalidate(); err != want {
				t.Fatal(err)
			}
		} else {
			c.Close()
		}
		if _, err := wait.Await(context.Background()); err != want {
			t.Fatal(err)
		}
		if _, err := c.Revision(); err != want {
			t.Fatal(err)
		}
		for _, operation := range []func() error{
			func() error { _, err := c.OwnerRangeCount(ownerC); return err }, func() error { return c.RetireOwner(ownerA) },
			func() error { return c.RetireSession(1) }, func() error { return c.Invalidate() },
			func() error { return c.CheckClaim(2, 2, 10, Claim{}) }, func() error { return c.RegisterClaim(2, 2, 10, Claim{}, nil) },
			func() error { return c.CloseClaim(1) },
			func() error { return c.ReplaceClaim(1, Claim{}, nil) }, func() error { return c.CheckUse(10, 1, 1) },
			func() error { _, err := c.ClaimCount(10); return err }, func() error { _, err := c.Snapshot(ownerA, scope); return err },
			func() error { _, err := c.ReplaceOwned(ownerA, scope, 1, nil, nil); return err },
			func() error { return c.CheckIO(10, nil, Span{}, false) },
			func() error { _, err := c.RegisterWait(2, ownerA, scope, 1, nil, false, nil); return err },
			func() error { return c.CancelOwnerWaits(ownerA, scope) },
		} {
			if err := operation(); err != want {
				t.Fatalf("failed coordinator admitted operation: %v, want%v", err, want)
			}
		}
		c.Close()
		c.Close()
		if len(c.owners) != 0 || len(c.scopes) != 0 || len(c.claims) != 0 || len(c.claimResources) != 0 || c.ranges != 0 || len(c.waits) != 0 {
			t.Fatal("closed coordinator retained resources")
		}
	}
}

func TestWaitRejectsInvalidAndStaleRegistrationWithoutLeakingState(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	scope := Scope{Resource: 10}
	revision, _ := c.Revision()
	if _, err := c.RegisterWait(0, ownerA, scope, revision, nil, false, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	if _, err := c.RegisterWait(1, ownerA, scope, 0, nil, false, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	if _, err := c.RegisterWait(1, Owner{}, scope, revision, nil, false, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	if _, err := c.RegisterWait(1, ownerA, scope, revision, []Acquisition{{ID: 0}}, false, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.Invalidate(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterWait(1, ownerA, scope, revision, nil, false, nil); err != ErrRevision {
		t.Fatal(err)
	}
	wait := registerWait(t, c, 1, ownerA, scope, nil, false)
	revision, _ = c.Revision()
	if _, err := c.RegisterWait(1, ownerA, scope, revision, nil, false, nil); err != ErrInvalid {
		t.Fatal(err)
	}
	wait.Cancel()
	next := registerWait(t, c, 1, ownerA, scope, nil, false)
	wait.Cancel()
	if !next.pending {
		t.Fatal("old cancellation removed a reused registration ID")
	}
	if err := c.CancelOwnerWaits(Owner{}, scope); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.Invalidate(); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.waitRanges != 0 || c.dependencies != 0 || len(c.waits) != 0 {
		t.Fatal("terminal waits retained capacity")
	}
}
