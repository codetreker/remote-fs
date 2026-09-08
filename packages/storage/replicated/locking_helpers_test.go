package replicated_test

import (
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func replicaLockOwner(t *testing.T, s locking.Service) locking.OwnerRef {
	t.Helper()
	ticket, err := s.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.CreateOwner(t.Context(), session.ID, "replica-owner")
	if err != nil {
		t.Fatal(err)
	}
	return o.Ref
}

func replicaLockGrant(t *testing.T, s locking.Service, owner locking.OwnerRef) locking.GrantRef {
	t.Helper()
	resource, err := s.Resolve(t.Context(), owner, "file")
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: "replica-acquire", Resource: resource, Mode: locking.Exclusive, TTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Outcome != locking.Granted || result.Grant == nil {
		t.Fatal("replica acquisition did not return its authoritative grant")
	}
	return result.Grant.Ref
}
