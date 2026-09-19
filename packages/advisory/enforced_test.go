package advisory

import (
	"errors"
	"reflect"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func enforced(mode storage.RangeMode, extent storage.Range) storage.RangeCommand {
	policy := storage.RangePolicy{DenySelf: storage.WriteData, DenyOthers: storage.WriteData}
	if mode == storage.RangeExclusive {
		policy = storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}
	}
	return storage.RangeCommand{Domain: storage.DomainEnforced, Mode: mode, Range: extent, Edit: storage.AddExact, Policy: policy}
}

func bytesRange(start, length uint64) storage.Range {
	return storage.Range{Kind: storage.Bytes, Start: start, Length: length}
}

func attachUse(t *testing.T, s *Session, owner storage.UseOwner) storage.UseScope {
	t.Helper()
	binding := s.bindings[owner]
	if err := s.coordinator.AddUse(background, binding.node, binding.scope, storage.UseClaim{Uses: storage.ReadData | storage.WriteData}); err != nil {
		t.Fatal(err)
	}
	return binding.scope
}

func removal(command storage.RangeCommand, claim storage.ClaimID) storage.RangeCommand {
	command.Edit, command.Claim, command.Wait = storage.RemoveExact, claim, false
	return command
}

func TestExactMultiplicityAndHolderIOPolicy(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	scope := attachUse(t, s, o)
	extent := bytesRange(10, 10)
	exclusive := enforced(storage.RangeExclusive, extent)
	shared := enforced(storage.RangeShared, extent)
	x := apply(t, s, 1, o, exclusive)
	ss := apply(t, s, 1, o, shared, shared)
	wantState(t, ss, storage.Granted, "")
	if len(ss.Claims) != 2 || ss.Claims[0] == ss.Claims[1] || c.ranges != 3 {
		t.Fatalf("exact claims were merged: %+v ranges=%d", ss, c.ranges)
	}
	for _, test := range []struct {
		scope storage.UseScope
		uses  storage.Uses
		deny  bool
	}{{scope, storage.ReadData, false}, {scope, storage.WriteData, true}, {storage.UseScope{}, storage.ReadData, true}, {storage.UseScope{}, storage.WriteData, true}} {
		err := c.CheckIO(background, 1, test.scope, extent, test.uses)
		if errors.Is(err, storage.ErrRangeConflict) != test.deny || err != nil && !test.deny {
			t.Fatalf("I/O %+v = %v", test, err)
		}
	}
	wantState(t, apply(t, s, 1, o, removal(exclusive, x.Claims[0])), storage.Released, "")
	if err := c.CheckIO(background, 1, storage.UseScope{}, extent, storage.ReadData); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckIO(background, 1, scope, extent, storage.WriteData); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("shared holder gained write access: %v", err)
	}
	wantState(t, apply(t, s, 1, o, removal(shared, ss.Claims[0])), storage.Released, "")
	if c.ranges != 1 {
		t.Fatal("one exact removal released multiple acquisitions")
	}
	wantState(t, apply(t, s, 1, o, removal(shared, ss.Claims[0])), storage.Rejected, storage.RangeNotHeld)
	wantState(t, apply(t, s, 1, o, removal(shared, ss.Claims[1])), storage.Released, "")
	if err := c.CheckIO(background, 1, scope, extent, storage.WriteData); err != nil {
		t.Fatal(err)
	}
}

func TestExactBatchFailureHasNoPartialEffect(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := session(t, c), session(t, c)
	ao, bo := owner(t, a, 1, 0), owner(t, b, 1, 0)
	wantState(t, apply(t, a, 1, ao, enforced(storage.RangeExclusive, bytesRange(20, 10))), storage.Granted, "")
	before := c.ranges
	first := enforced(storage.RangeShared, bytesRange(0, 10))
	second := enforced(storage.RangeShared, bytesRange(20, 10))
	rejected := apply(t, b, 1, bo, first, second)
	wantState(t, rejected, storage.Rejected, storage.RangeBlocked)
	if c.ranges != before || len(rejected.Claims) != 0 || len(rejected.Effects) != 0 {
		t.Fatalf("failed batch retained a prefix: %+v", rejected)
	}
	one := apply(t, b, 1, bo, first)
	wrong := removal(first, one.Claims[0])
	wrong.Range = bytesRange(1, 9)
	wantState(t, apply(t, b, 1, bo, wrong), storage.Rejected, storage.RangeInvalid)
	missing, err := storage.NewClaimID(requestID(t, b), 0)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, apply(t, b, 1, bo, removal(first, one.Claims[0]), removal(first, missing)), storage.Rejected, storage.RangeNotHeld)
	if c.ranges != before+1 {
		t.Fatal("failed removal batch changed live claims")
	}
}

func TestExactReplayAndReceiptCopies(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	command := enforced(storage.RangeShared, bytesRange(0, 100))
	commands := []storage.RangeCommand{command, command}
	id := requestID(t, s)
	first, err := s.Apply(background, 1, o, commands, id, ordered)
	if err != nil {
		t.Fatal(err)
	}
	expected := first.Clone()
	commands[0].Range.Start = 999
	first.Commands[0].Range.Start = 999
	first.Claims[0] = "changed"
	first.Effects[0].Command.Range.Start = 999
	replayed, err := s.Apply(background, 1, o, []storage.RangeCommand{command, command}, id, ordered)
	if err != nil {
		t.Fatal(err)
	}
	replayed.HistoryRemaining, expected.HistoryRemaining = 0, 0
	if !reflect.DeepEqual(replayed, expected) || c.ranges != 2 {
		t.Fatalf("replay changed claims/receipt: %+v want %+v", replayed, expected)
	}
}

func TestBoundaryClaimsProtectOnlyCrossingSpans(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	zero := enforced(storage.RangeExclusive, storage.Range{Kind: storage.Boundary})
	cut := enforced(storage.RangeExclusive, storage.Range{Kind: storage.Boundary, CutAt: 10})
	granted := apply(t, s, 1, o, zero, cut)
	if len(granted.Claims) != 2 || c.ranges != 2 {
		t.Fatal("boundary claims were discarded")
	}
	for _, test := range []struct {
		span storage.Range
		deny bool
	}{{bytesRange(0, 1), false}, {bytesRange(9, 1), false}, {bytesRange(10, 1), false}, {bytesRange(9, 2), true}} {
		err := c.CheckIO(background, 1, storage.UseScope{}, test.span, storage.WriteData)
		if errors.Is(err, storage.ErrRangeConflict) != test.deny || err != nil && !test.deny {
			t.Fatalf("boundary I/O %+v = %v", test, err)
		}
	}
	wantState(t, apply(t, s, 1, o, removal(cut, granted.Claims[1])), storage.Released, "")
	if err := c.CheckIO(background, 1, storage.UseScope{}, bytesRange(9, 2), storage.WriteData); err != nil {
		t.Fatal(err)
	}
}
