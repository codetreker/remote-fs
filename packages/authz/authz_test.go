package authz_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAuthorizerFuncPreservesHostContextIntentAndError(t *testing.T) {
	type identityKey struct{}
	ctx := context.WithValue(t.Context(), identityKey{}, "host-owned-identity")
	request := authz.AccessRequest{
		Volume: "configured-volume", Operation: storage.OpFileReplaceAndRetainAt,
		Effects:   storage.EffectRetained | storage.EffectCreated | storage.EffectEntryDetached | storage.EffectPreparedChanged,
		Claim:     storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent, Excludes: storage.RemoveEntry},
		Reference: 11, Node: 42, Parent: 12, Destination: 13,
	}
	cause := errors.New("policy lookup failed")
	calls := 0
	policy := authz.AuthorizerFunc(func(got context.Context, copied authz.AccessRequest) error {
		calls++
		if got != ctx || got.Value(identityKey{}) != "host-owned-identity" || copied != request {
			t.Fatalf("adapter changed host context or intent: %+v", copied)
		}
		copied.Volume = "different"
		copied.Effects = 0
		copied.Claim.Uses = 0
		copied.Reference, copied.Node, copied.Parent, copied.Destination = 0, 0, 0, 0
		return cause
	})
	var authorizer authz.Authorizer = policy
	if err := authorizer.Authorize(ctx, request); err != cause || calls != 1 {
		t.Fatalf("adapter calls=%d error=%v", calls, err)
	}
	if request.Volume != "configured-volume" || request.Effects == 0 || request.Claim.Uses != storage.ReadContent|storage.WriteContent || request.Reference != 11 || request.Node != 42 || request.Parent != 12 || request.Destination != 13 {
		t.Fatal("policy changed the caller's request value")
	}
	if err := authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }).Authorize(ctx, request); err != nil {
		t.Fatalf("allow decision changed: %v", err)
	}
}

func TestDeniedMarkerSurvivesHostErrorWrappingAndJoining(t *testing.T) {
	for _, failure := range []error{authz.ErrDenied, fmt.Errorf("host policy: %w", authz.ErrDenied), errors.Join(errors.New("host detail"), authz.ErrDenied)} {
		if !errors.Is(failure, authz.ErrDenied) {
			t.Fatalf("explicit refusal marker was lost: %v", failure)
		}
	}
	if errors.Is(errors.New("access denied"), authz.ErrDenied) {
		t.Fatal("matching text invented an explicit policy decision")
	}
}

func TestMetadataOnlyRetainPreservesItsClaimAndDenial(t *testing.T) {
	claim := storage.AccessClaim{Excludes: storage.RemoveEntry}
	request := authz.AccessRequest{Volume: "configured-volume", Operation: storage.OpFileRetain, Effects: storage.EffectRetained, Claim: claim, Node: 17}
	calls := 0
	policy := authz.AuthorizerFunc(func(_ context.Context, got authz.AccessRequest) error {
		calls++
		if got != request {
			t.Fatalf("changed generic authorization intent: %+v", got)
		}
		if got.Claim.Uses != 0 || got.Effects != storage.EffectRetained {
			t.Fatal("metadata-only retain acquired content access or mutation effects")
		}
		got.Claim.Uses = storage.WriteContent
		return authz.ErrDenied
	})
	if err := policy.Authorize(t.Context(), request); !errors.Is(err, authz.ErrDenied) || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	if request.Claim != claim {
		t.Fatal("policy changed caller's intent")
	}
}
