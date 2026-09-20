package advisory

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func oversizedBacking(value string) string {
	backing := strings.Repeat("x", 1<<20) + value
	return backing[len(backing)-len(value):]
}

func assertOwnedString(t *testing.T, retained, original string) {
	t.Helper()
	if retained != original {
		t.Fatalf("retained value changed: %q != %q", retained, original)
	}
	if unsafe.StringData(retained) == unsafe.StringData(original) {
		t.Fatal("retained short string pins the caller's larger allocation")
	}
}

func TestRegisteredOwnersOwnScopeTokenBytes(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	scope := storage.UseScope{Token: oversizedBacking("registered-scope")}
	o, err := s.NewOwner(background, 1, scope, storage.OwnerOptions{Lifetime: storage.OwnerExplicit})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnedString(t, s.bindings[o].scope.Token, scope.Token)
	returned, err := s.OwnerScope(background, o)
	if err != nil {
		t.Fatal(err)
	}
	assertOwnedString(t, returned.Token, scope.Token)
}

func TestNativeUseClaimsOwnScopeTokenBytes(t *testing.T) {
	c := fixture(t, DefaultConfig())
	scope := storage.UseScope{Token: oversizedBacking("native-scope")}
	claim := storage.UseClaim{Uses: storage.ReadData}
	if err := c.AddUse(background, 1, scope, claim); err != nil {
		t.Fatal(err)
	}
	if err := c.AddUse(background, 1, scope, claim); err != nil {
		t.Fatal(err)
	}
	if len(c.uses) != 1 {
		t.Fatal("repeated use admission duplicated its claim")
	}
	for retained := range c.uses {
		assertOwnedString(t, retained.Token, scope.Token)
	}
}

func TestRangeActionsOwnRequestIDBytes(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	id := storage.LockRequestID(oversizedBacking(string(requestID(t, s))))
	result, err := s.Apply(background, 1, o, []storage.RangeCommand{record(storage.RangeShared, 0, 1)}, id, ordered)
	if err != nil {
		t.Fatal(err)
	}
	assertOwnedString(t, string(result.Request), string(id))
	if len(s.actions) != 1 {
		t.Fatal("action admission did not retain exactly one request")
	}
	for key, action := range s.actions {
		assertOwnedString(t, string(key), string(id))
		assertOwnedString(t, string(action.id), string(id))
		assertOwnedString(t, string(action.result.Request), string(id))
	}
}

func TestRangeActionsOwnCommandClaimBytes(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	command := enforced(storage.RangeShared, bytesRange(0, 10))
	granted := apply(t, s, 1, o, command)
	claim := storage.ClaimID(oversizedBacking(string(granted.Claims[0])))
	for attempt := range 2 {
		id := requestID(t, s)
		result, err := s.Apply(background, 1, o, []storage.RangeCommand{removal(command, claim)}, id, ordered)
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			wantState(t, result, storage.Released, "")
			assertOwnedString(t, string(result.Effects[0].Claim), string(claim))
			assertOwnedString(t, string(result.Effects[0].Command.Claim), string(claim))
		} else {
			wantState(t, result, storage.Rejected, storage.RangeNotHeld)
		}
		assertOwnedString(t, string(result.Commands[0].Claim), string(claim))
		action := s.actions[id]
		assertOwnedString(t, string(action.commands[0].Claim), string(claim))
		assertOwnedString(t, string(action.result.Commands[0].Claim), string(claim))
	}
}
