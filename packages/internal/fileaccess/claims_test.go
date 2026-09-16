package fileaccess

import (
	"errors"
	"sync"
	"testing"
)

func coordinator(t *testing.T, limits Limits) *Coordinator {
	t.Helper()
	c, err := New(limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	return c
}

func TestClaimsUseBothDirectionsWithoutZeroUseExceptions(t *testing.T) {
	for _, test := range []struct {
		name      string
		old, next Claim
		conflict  bool
	}{
		{"compatible", Claim{Uses: 1, Excludes: 4}, Claim{Uses: 2}, false},
		{"existing exclusion", Claim{Uses: 1, Excludes: 2}, Claim{Uses: 2}, true},
		{"new exclusion", Claim{Uses: 1}, Claim{Uses: 2, Excludes: 1}, true},
		{"zero use still excludes", Claim{Uses: 1}, Claim{Excludes: 1}, true},
		{"both metadata only", Claim{}, Claim{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := coordinator(t, DefaultLimits())
			if err := c.RegisterClaim(1, 1, 10, test.old, nil); err != nil {
				t.Fatal(err)
			}
			err := c.RegisterClaim(2, 2, 10, test.next, nil)
			if errors.Is(err, ErrConflict) != test.conflict || !test.conflict && err != nil {
				t.Fatalf("claim admission = %v, conflict=%t", err, test.conflict)
			}
			if test.conflict {
				var conflict *ClaimConflict
				if !errors.As(err, &conflict) || conflict.Handle != 1 || conflict.Claim != test.old || conflict.Error() != ErrConflict.Error() {
					t.Fatalf("incorrect conflict witness: %v", err)
				}
			}
			if err := c.RegisterClaim(3, 2, 11, test.next, nil); err != nil {
				t.Fatalf("another resource conflicted: %v", err)
			}
		})
	}
}

func TestClaimChecksValidateActualRightsAndFreshState(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	if err := c.CheckClaim(1, 1, 10, Claim{Uses: 2}); err != nil {
		t.Fatal(err)
	}
	if count, err := c.ClaimCount(10); err != nil || count != 0 {
		t.Fatalf("check registered a claim: %d %v", count, err)
	}
	if err := c.RegisterClaim(2, 2, 10, Claim{Uses: 1, Excludes: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 2}, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale precheck bypassed registration: %v", err)
	}
	if err := c.CheckUse(10, 0, 2); !errors.Is(err, ErrConflict) {
		t.Fatalf("anonymous write bypassed exclusion: %v", err)
	}
	if err := c.CheckUse(10, 2, 2); err != ErrAccess {
		t.Fatalf("retained read claim wrote: %v", err)
	}
	if err := c.CheckUse(11, 2, 0); err != ErrUnknownClaim {
		t.Fatalf("mismatched resource accepted zero use: %v", err)
	}
	if err := c.CheckUse(10, 99, 0); err != ErrUnknownClaim {
		t.Fatalf("unknown claim accepted zero use: %v", err)
	}
	if err := c.CheckUse(10, 2, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(10, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseClaim(2); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(10, 0, 2); err != nil {
		t.Fatalf("closed claim still excludes: %v", err)
	}
	if err := c.CloseClaim(2); err != ErrUnknownClaim {
		t.Fatalf("unknown close = %v", err)
	}
	if len(c.claimResources) != 0 {
		t.Fatal("closed resource retained claim map")
	}
}

func TestClaimCapacityAndSessionRetirementAreExact(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxClaims = 2
	c := coordinator(t, limits)
	if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(2, 2, 10, Claim{Uses: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(3, 1, 11, Claim{}, nil); err != ErrCapacity {
		t.Fatalf("claim ceiling = %v", err)
	}
	if err := c.RetireSession(1); err != nil {
		t.Fatal(err)
	}
	if count, err := c.ClaimCount(10); err != nil || count != 1 {
		t.Fatalf("session retirement removed wrong claims: %d %v", count, err)
	}
	if err := c.CheckUse(10, 2, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(3, 3, 11, Claim{}, nil); err != nil {
		t.Fatalf("claim capacity was not reclaimed: %v", err)
	}
	for _, args := range [][3]uint64{{0, 1, 10}, {4, 0, 10}, {4, 1, 0}, {2, 2, 10}} {
		if err := c.RegisterClaim(args[0], args[1], args[2], Claim{}, nil); err != ErrInvalid {
			t.Fatalf("invalid claim %v: %v", args, err)
		}
	}
	if _, err := c.ClaimCount(0); err != ErrInvalid {
		t.Fatal(err)
	}
	if err := c.CheckUse(0, 0, 0); err != ErrInvalid {
		t.Fatal(err)
	}
}

func TestConcurrentExclusiveClaimsCannotBothBeAdmitted(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	var joined sync.WaitGroup
	results := make([]error, 16)
	for i := range results {
		joined.Add(1)
		go func() {
			defer joined.Done()
			results[i] = c.RegisterClaim(uint64(i+1), uint64(i+1), 10, Claim{Uses: 2, Excludes: 2}, nil)
		}()
	}
	joined.Wait()
	admitted := 0
	for _, err := range results {
		if err == nil {
			admitted++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if admitted != 1 {
		t.Fatalf("exclusive claims admitted = %d, want1", admitted)
	}
}

func TestClaimReplacementHasNoAdmissionGapAndPreservesRejectedState(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 1, Excludes: 4}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterClaim(2, 2, 10, Claim{Uses: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.ReplaceClaim(1, Claim{Uses: 1, Excludes: 6}, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("claim replacement ignored existing use: %v", err)
	}
	if err := c.CheckUse(10, 0, 4); !errors.Is(err, ErrConflict) {
		t.Fatalf("rejected replacement dropped old exclusion: %v", err)
	}
	if err := c.CheckUse(10, 0, 2); err != nil {
		t.Fatalf("rejected replacement installed new exclusion: %v", err)
	}
	if err := c.CloseClaim(2); err != nil {
		t.Fatal(err)
	}
	before, _ := c.Revision()
	if err := c.ReplaceClaim(1, Claim{Uses: 3, Excludes: 6}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(10, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(10, 0, 2); !errors.Is(err, ErrConflict) {
		t.Fatal("replacement did not exclude anonymous write")
	}
	after, _ := c.Revision()
	if after <= before {
		t.Fatal("claim change did not invalidate guards")
	}
	if err := c.ReplaceClaim(1, Claim{Uses: 3, Excludes: 6}, nil); err != nil {
		t.Fatal(err)
	}
	if unchanged, _ := c.Revision(); unchanged != after {
		t.Fatal("unchanged claim replacement churned revision")
	}
	if err := c.ReplaceClaim(99, Claim{}, nil); err != ErrUnknownClaim {
		t.Fatal(err)
	}
}

func TestEstablishedEffectsCheckCurrentExclusionsAndBoundException(t *testing.T) {
	c := coordinator(t, DefaultLimits())
	if err := c.RegisterClaim(1, 1, 10, Claim{Uses: 1, Excludes: 4}, nil); err != nil {
		t.Fatal(err)
	}
	before, _ := c.Revision()
	if err := c.CheckExclusions(10, 1, 4); err != nil {
		t.Fatalf("established effect required new rights: %v", err)
	}
	if err := c.CheckUse(10, 1, 4); err != ErrAccess {
		t.Fatalf("ordinary use bypassed advertised rights: %v", err)
	}
	if err := c.CheckExclusions(10, 0, 4); !errors.Is(err, ErrConflict) {
		t.Fatalf("anonymous effect borrowed exception: %v", err)
	}
	for _, args := range [][3]uint64{{11, 1, 4}, {10, 99, 0}} {
		if err := c.CheckExclusions(args[0], args[1], args[2]); err != ErrUnknownClaim {
			t.Fatalf("unbound exception %v: %v", args, err)
		}
	}
	if err := c.CheckExclusions(0, 0, 0); err != ErrInvalid {
		t.Fatal(err)
	}
	if after, _ := c.Revision(); after != before {
		t.Fatal("exclusion observation changed guard revision")
	}
	if err := c.RegisterClaim(2, 2, 10, Claim{Excludes: 4}, nil); err != nil {
		t.Fatal(err)
	}
	var conflict *ClaimConflict
	if err := c.CheckExclusions(10, 1, 4); !errors.As(err, &conflict) || conflict.Handle != 2 {
		t.Fatalf("established effect ignored competing exclusion: %v", err)
	}
	if err := c.CloseClaim(2); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckExclusions(10, 1, 4); err != nil {
		t.Fatalf("released exclusion remained effective: %v", err)
	}
	if err := c.CloseClaim(1); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckExclusions(10, 1, 0); err != ErrUnknownClaim {
		t.Fatalf("closed exception remained valid: %v", err)
	}
	c.Close()
	if err := c.CheckExclusions(10, 0, 4); err != ErrClosed {
		t.Fatalf("fenced exclusion check succeeded: %v", err)
	}
}
