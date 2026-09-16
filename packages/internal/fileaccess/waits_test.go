package fileaccess

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

func registerWait(t *testing.T, c *Coordinator, id uint64, owner Owner, scope Scope, proposed []Acquisition, detect bool) *Wait {
	t.Helper()
	revision, err := c.Revision()
	if err != nil {
		t.Fatal(err)
	}
	wait, err := c.RegisterWait(id, owner, scope, revision, proposed, detect, nil)
	if err != nil {
		t.Fatal(err)
	}
	return wait
}

func TestRevisionWaitBlocksWithoutGrantingAndReleasesDependencies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinator(t, DefaultLimits())
		scope := Scope{Resource: 10, Domain: 7}
		rangeA := Acquisition{ID: 1, Start: 0, End: 9, Exclusive: true}
		revision := replace(t, c, ownerA, scope, rangeA)
		wait := registerWait(t, c, 1, ownerB, scope, []Acquisition{{ID: 2, Start: 0, End: 9}}, true)
		if c.dependencies != 1 || c.waitRanges != 1 {
			t.Fatalf("wait did not retain bounded dependency: %d/%d", c.dependencies, c.waitRanges)
		}
		done := make(chan error, 1)
		go func() {
			got, err := wait.Await(context.Background())
			if err == nil && got <= revision {
				err = ErrRevision
			}
			done <- err
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("unchanged revision wait returned: %v", err)
		default:
		}
		replace(t, c, ownerA, scope)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if c.dependencies != 0 || c.waitRanges != 0 || len(c.waits) != 0 || wait.proposed != nil || wait.dependencies != nil {
			t.Fatal("notified wait retained its graph or range memory")
		}
		if len(snapshot(t, c, ownerB, scope).Own) != 0 {
			t.Fatal("revision notification granted a range")
		}
	})
}

func TestDeadlockDetectionSpansSessionsAndResources(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	scopes := []Scope{{Resource: 10, Domain: 7}, {Resource: 20, Domain: 7}, {Resource: 30, Domain: 7}}
	owners := []Owner{ownerA, ownerB, ownerC}
	intent := []Acquisition{{ID: 1, Start: 0, End: 9, Exclusive: true}}
	for i, owner := range owners {
		replace(t, c, owner, scopes[i], intent...)
	}
	first := registerWait(t, c, 1, ownerA, scopes[1], intent, true)
	registerWait(t, c, 2, ownerB, scopes[2], intent, true)
	revision, _ := c.Revision()
	if _, err := c.RegisterWait(3, ownerC, scopes[0], revision, intent, true, nil); err != ErrDeadlock {
		t.Fatalf("cross-session resource cycle = %v", err)
	}
	if len(c.waits) != 2 || c.dependencies != 2 {
		t.Fatal("rejected cycle consumed wait capacity")
	}
	first.Cancel()
	if _, err := first.Await(context.Background()); err != ErrCanceled {
		t.Fatal(err)
	}
	registerWait(t, c, 3, ownerC, scopes[0], intent, true)
}

func TestChangedStateCannotLeaveStaleDeadlockEdges(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	one, two := Scope{Resource: 10, Domain: 7}, Scope{Resource: 20, Domain: 7}
	intent := []Acquisition{{ID: 1, Start: 0, End: 9, Exclusive: true}}
	replace(t, c, ownerA, one, intent...)
	replace(t, c, ownerB, two, intent...)
	old := registerWait(t, c, 1, ownerA, two, intent, true)
	replace(t, c, ownerB, two)
	if _, err := old.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.dependencies != 0 {
		t.Fatal("old dependency survived its range revision")
	}
	current := registerWait(t, c, 2, ownerB, one, intent, true)
	if err := c.Invalidate(); err != nil {
		t.Fatal(err)
	}
	if _, err := current.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWaitCancellationAndScopedCleanupPreserveOtherDomains(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	one, two := Scope{Resource: 10, Domain: 0}, Scope{Resource: 10, Domain: 1}
	first := registerWait(t, c, 1, ownerA, one, nil, false)
	second := registerWait(t, c, 2, ownerA, two, nil, false)
	third := registerWait(t, c, 3, ownerB, one, nil, false)
	if err := c.CancelOwnerWaits(ownerA, one); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Await(context.Background()); err != ErrCanceled {
		t.Fatal(err)
	}
	first.Cancel()
	if !second.pending || !third.pending || len(c.waits) != 2 {
		t.Fatal("file-domain cleanup canceled another domain or session")
	}
	cause := errors.New("owner withdrew the wait")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if _, err := second.Await(ctx); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("wait cancellation lost cause: %v", err)
	}
	if len(c.waits) != 1 {
		t.Fatal("canceled wait retained registration")
	}
	if err := c.RetireSession(ownerB.Session); err != nil {
		t.Fatal(err)
	}
	if _, err := third.Await(context.Background()); err != ErrRetired {
		t.Fatal(err)
	}
	if len(c.waits) != 0 {
		t.Fatal("retired session retained waits")
	}
}

func TestRevisionOnlyWaitDoesNotParticipateInCyclePolicy(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	one, two := Scope{Resource: 10, Domain: 7}, Scope{Resource: 20, Domain: 7}
	intent := []Acquisition{{ID: 1, Start: 0, End: 9, Exclusive: true}}
	replace(t, c, ownerA, one, intent...)
	replace(t, c, ownerB, two, intent...)
	registerWait(t, c, 1, ownerA, two, intent, true)
	wait := registerWait(t, c, 2, ownerB, one, intent, false)
	if len(wait.dependencies) != 0 || c.dependencies != 1 {
		t.Fatal("revision-only wait added graph edges")
	}
	wait.Cancel()
}

func TestWaitBudgetsFailWithoutClaimingNoCycle(t *testing.T) {
	intent := []Acquisition{{ID: 1, Start: 0, End: 9, Exclusive: true}}
	scope := Scope{Resource: 10, Domain: 7}
	t.Run("wait slots", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxWaits = 1
		c := coordinator(t, limits)
		wait := registerWait(t, c, 1, ownerA, scope, nil, false)
		revision, _ := c.Revision()
		if _, err := c.RegisterWait(2, ownerA, scope, revision, nil, false, nil); err != ErrCapacity {
			t.Fatal(err)
		}
		wait.Cancel()
		registerWait(t, c, 2, ownerA, scope, nil, false)
	})
	t.Run("pending ranges", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxWaitRanges = 1
		c := coordinator(t, limits)
		revision, _ := c.Revision()
		if _, err := c.RegisterWait(1, ownerA, scope, revision, []Acquisition{{ID: 1}, {ID: 2}}, false, nil); err != ErrCapacity {
			t.Fatal(err)
		}
		registerWait(t, c, 2, ownerA, scope, intent, false)
		if c.waitRanges != 1 {
			t.Fatal("pending range accounting is wrong")
		}
	})
	t.Run("dependency edges", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxDependencies = 1
		c := coordinator(t, limits)
		shared := Acquisition{ID: 1, Start: 0, End: 9}
		replace(t, c, ownerA, scope, shared)
		replace(t, c, ownerC, scope, shared)
		revision, _ := c.Revision()
		if _, err := c.RegisterWait(1, ownerB, scope, revision, intent, true, nil); err != ErrCapacity {
			t.Fatalf("truncated dependency graph: %v", err)
		}
		if c.dependencies != 0 || len(c.waits) != 0 {
			t.Fatal("failed dependency scan retained state")
		}
	})
	t.Run("bounded cycle search", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxWork = 1
		c := coordinator(t, limits)
		one, two := Scope{Resource: 10, Domain: 7}, Scope{Resource: 20, Domain: 7}
		replace(t, c, ownerA, one, intent...)
		replace(t, c, ownerB, two, intent...)
		registerWait(t, c, 1, ownerA, two, intent, true)
		revision, _ := c.Revision()
		if _, err := c.RegisterWait(2, ownerB, one, revision, intent, true, nil); err != ErrCapacity {
			t.Fatalf("insufficient cycle search claimed a conclusion: %v", err)
		}
	})
}
