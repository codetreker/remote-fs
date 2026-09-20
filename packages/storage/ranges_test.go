package storage

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestNeutralRangeExtentsPreserveUnsignedBoundary(t *testing.T) {
	for _, r := range []Range{{Kind: Bytes, Start: ^uint64(0), Length: 1}, {Kind: Bytes, Length: ^uint64(0)}, {Kind: Boundary}, {Kind: Boundary, CutAt: ^uint64(0)}} {
		if err := r.Check(); err != nil {
			t.Fatalf("valid extent%+v=%v", r, err)
		}
	}
	for _, r := range []Range{{}, {Kind: Bytes}, {Kind: Bytes, Start: ^uint64(0), Length: 2}, {Kind: Bytes, Length: 1, CutAt: 1}, {Kind: Boundary, Start: 1}, {Kind: Boundary, Length: 1}, {Kind: 3}} {
		if !errors.Is(r.Check(), syscall.EINVAL) {
			t.Fatalf("invalid extent accepted:%+v", r)
		}
	}
}
func TestRangeCommandsPreserveDomainsClaimsAndReceiptOwnership(t *testing.T) {
	id, err := NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := NewClaimID(id, 3)
	if err != nil || claim.Check() != nil {
		t.Fatalf("claim=%q %v", claim, err)
	}
	for _, bad := range []ClaimID{"", ClaimID(string(id) + ":-1"), ClaimID(string(id) + ":64"), ClaimID(string(id) + ":03"), "x:3", ClaimID(strings.Repeat("x", 1024))} {
		if bad.Check() == nil {
			t.Fatalf("invalid claim accepted%q", bad)
		}
	}
	if _, err := NewClaimID("bad", 0); err == nil {
		t.Fatal("bad request claim accepted")
	}
	base := RangeCommand{Domain: DomainRecord, Mode: RangeShared, Range: Range{Kind: Bytes, Length: 1}, Edit: Replace}
	exact := RangeCommand{Domain: DomainEnforced, Mode: RangeExclusive, Range: Range{Kind: Boundary, CutAt: 1}, Edit: AddExact, Policy: RangePolicy{DenyOthers: ReadData | WriteData}}
	for _, c := range []RangeCommand{base, exact, {Domain: DomainWholeFile, Mode: RangeExclusive, Range: base.Range, Edit: Replace, Conversion: DropBeforeAcquire}, {Domain: DomainRecord, Mode: RangeShared, Range: base.Range, Edit: Subtract}, {Domain: DomainEnforced, Mode: RangeExclusive, Range: exact.Range, Edit: RemoveExact, Claim: claim, Policy: exact.Policy}} {
		if err := c.Check(); err != nil {
			t.Fatalf("valid command%+v=%v", c, err)
		}
	}
	for _, mutate := range []func(*RangeCommand){func(c *RangeCommand) { c.Range.Kind = 0 }, func(c *RangeCommand) { c.Mode = 0 }, func(c *RangeCommand) { c.Domain = 0 }, func(c *RangeCommand) { c.Edit = 0 }, func(c *RangeCommand) { c.Conversion = 4 }, func(c *RangeCommand) { c.Range = Range{Kind: Boundary} }, func(c *RangeCommand) { c.Claim = claim }, func(c *RangeCommand) { c.Edit = Subtract; c.Wait = true }, func(c *RangeCommand) { c.Policy.DenyOthers = ReadEntries }, func(c *RangeCommand) { c.Policy.DenySelf = ReadData }, func(c *RangeCommand) { c.Edit = AddExact }, func(c *RangeCommand) { c.Conversion = DropBeforeAcquire }} {
		c := base
		mutate(&c)
		if c.Check() == nil {
			t.Fatalf("invalid command accepted:%+v", c)
		}
	}
	for _, mutate := range []func(*RangeCommand){func(c *RangeCommand) { c.Edit = Replace }, func(c *RangeCommand) { c.Policy = RangePolicy{} }, func(c *RangeCommand) { c.Edit = RemoveExact }, func(c *RangeCommand) { c.Edit = RemoveExact; c.Claim = claim; c.Wait = true }} {
		c := exact
		mutate(&c)
		if c.Check() == nil {
			t.Fatalf("invalid enforced command accepted:%+v", c)
		}
	}
	failedAt := 1
	original := RangeAttempt{Commands: []RangeCommand{base}, Claims: []ClaimID{claim}, Effects: []RangeEffect{{Claim: claim, Command: exact}}, FailedAt: &failedAt}
	copy := original.Clone()
	copy.Commands[0].Wait = true
	copy.Claims[0] = "other"
	copy.Effects[0].Released = true
	*copy.FailedAt = 0
	if original.Commands[0].Wait || original.Claims[0] != claim || original.Effects[0].Released || *original.FailedAt != 1 {
		t.Fatal("receipt clone aliases history")
	}
}
