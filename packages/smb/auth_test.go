package smb

import (
	"context"
	"testing"
	"time"
)

func TestPrincipalContextKeepsRequestLifetimeAndIsolatesIdentity(t *testing.T) {
	type requestKey struct{}
	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), requestKey{}, "request"), deadline)
	defer cancel()
	first := Principal{SID: "S-1-5-21-1", Name: "first"}
	child := WithPrincipal(parent, first)
	first.SID = "mutated"
	principal, ok := PrincipalFromContext(child)
	if !ok || principal.SID != "S-1-5-21-1" || principal.Name != "first" {
		t.Fatalf("principal snapshot = %+v, %v", principal, ok)
	}
	if got, ok := child.Deadline(); !ok || got != deadline || child.Value(requestKey{}) != "request" {
		t.Fatal("principal context lost request lifetime or values")
	}
	overridden := WithPrincipal(child, Principal{SID: "S-1-5-21-2", Name: "second"})
	if principal, ok := PrincipalFromContext(overridden); !ok || principal.SID != "S-1-5-21-2" {
		t.Fatalf("override = %+v, %v", principal, ok)
	}
	if principal, _ := PrincipalFromContext(child); principal.SID != "S-1-5-21-1" {
		t.Fatal("child override changed another request's principal")
	}
	cancel()
	if child.Err() != context.Canceled || overridden.Err() != context.Canceled {
		t.Fatal("request cancellation did not reach authenticated contexts")
	}
}

func TestPrincipalContextDoesNotReadUnrelatedValues(t *testing.T) {
	type foreignKey struct{}
	for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), foreignKey{}, Principal{SID: "S-1-5-21-1"})} {
		if principal, ok := PrincipalFromContext(ctx); ok || principal != (Principal{}) {
			t.Fatalf("untrusted context value became a principal: %+v, %v", principal, ok)
		}
	}
}
