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
	options := storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true},
		ExpectedID: 42, Mode: 0o600,
	}
	request := authz.AccessRequest{Volume: "configured-volume", Operation: storage.OpFileOpen, Open: options.OpenAccess}
	cause := errors.New("policy lookup failed")
	calls := 0
	policy := authz.AuthorizerFunc(func(got context.Context, copied authz.AccessRequest) error {
		calls++
		if got != ctx || got.Value(identityKey{}) != "host-owned-identity" || copied != request {
			t.Fatalf("adapter changed host context or intent: %+v", copied)
		}
		copied.Volume = "different"
		copied.Open.Read = false
		return cause
	})
	var authorizer authz.Authorizer = policy
	if err := authorizer.Authorize(ctx, request); err != cause || calls != 1 {
		t.Fatalf("adapter calls=%d error=%v", calls, err)
	}
	if request.Volume != "configured-volume" || !request.Open.Read {
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
