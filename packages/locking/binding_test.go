package locking_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func accessContext(t *testing.T, subject, access string) context.Context {
	t.Helper()
	ctx, err := storage.WithAccessBinding(context.Background(), storage.AccessBinding{SubjectID: subject, AccessID: access})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func boundOwner(t *testing.T, a *locking.Authority, ctx context.Context) (locking.EnrollmentTicket, locking.Session, locking.OwnerRef) {
	t.Helper()
	ticket, err := a.BeginEnrollment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := a.OpenSession(ctx, ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := a.CreateOwner(ctx, session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	return ticket, session, owner.Ref
}

func denied(t *testing.T, err error) {
	t.Helper()
	if locking.CodeOf(err) != locking.Denied || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected denied capability, got %v", err)
	}
}

func TestAuthorityCapabilitiesRequireOriginalAccessContext(t *testing.T) {
	h := newContractHarness(t, nil)
	ownerCtx := accessContext(t, "subject", "access-a")
	ticket, session, owner := boundOwner(t, h.a, ownerCtx)
	resource, err := h.a.Resolve(ownerCtx, owner, "a")
	if err != nil {
		t.Fatal(err)
	}
	request := locking.AcquireRequest{Owner: owner, Request: "acquire", Resource: resource, Mode: locking.Exclusive, TTL: time.Second}
	action, err := h.a.Acquire(ownerCtx, request)
	if err != nil || action.Grant == nil {
		t.Fatalf("acquire: %+v, %v", action, err)
	}
	grant := action.Grant.Ref
	for name, ctx := range map[string]context.Context{
		"same subject new access": accessContext(t, "subject", "access-b"),
		"other subject":           accessContext(t, "other", "access-a"),
		"absent binding":          context.Background(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.a.OpenSession(ctx, ticket)
			denied(t, err)
			_, err = h.a.CreateOwner(ctx, session.ID, "owner")
			denied(t, err)
			_, err = h.a.CreateOwner(ctx, session.ID, "new-owner")
			denied(t, err)
			_, err = h.a.Resolve(ctx, owner, "a")
			denied(t, err)
			_, err = h.a.Acquire(ctx, request)
			denied(t, err)
			_, err = h.a.Renew(ctx, locking.RenewRequest{Owner: owner, Request: "renew", Grant: grant, TTL: time.Second})
			denied(t, err)
			_, err = h.a.QueryAction(ctx, owner, request.Request)
			denied(t, err)
			_, err = h.a.QueryGrant(ctx, owner, grant)
			denied(t, err)
			_, err = h.a.Cancel(ctx, owner, request.Request)
			denied(t, err)
			_, err = h.a.Release(ctx, owner, grant)
			denied(t, err)
			denied(t, h.a.RetireOwner(ctx, owner))
			denied(t, h.a.CloseSession(ctx, session.ID))
			called := false
			err = h.a.Publish(ctx, locking.Publication{Kind: locking.WriteMutation, Targets: []locking.BackendKey{"a"}, Scope: locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}}, func() locking.PublicationOutcome {
				called = true
				return locking.PublicationOutcome{Known: true}
			})
			denied(t, err)
			if called {
				t.Fatal("unauthorized publication ran")
			}
		})
	}
	if _, err := h.a.QueryGrant(ownerCtx, owner, grant); err != nil {
		t.Fatalf("denied requests damaged original grant: %v", err)
	}
	if err := h.a.RetireOwner(ownerCtx, owner); err != nil {
		t.Fatal(err)
	}
	other := accessContext(t, "subject", "access-b")
	denied(t, h.a.RetireOwner(other, owner))
	if err := h.a.CloseSession(ownerCtx, session.ID); err != nil {
		t.Fatal(err)
	}
	denied(t, h.a.CloseSession(other, session.ID))
	_, err = h.a.OpenSession(other, ticket)
	denied(t, err)
	retired, err := h.a.OpenSession(ownerCtx, ticket)
	if err != nil || !retired.Retired {
		t.Fatalf("original terminal replay: %+v, %v", retired, err)
	}
}

func TestAuthorityUnconsumedTicketIsBoundAndBounded(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxTickets = 1 })
	ctx := accessContext(t, "subject", "access-a")
	ticket, err := h.a.BeginEnrollment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.a.OpenSession(accessContext(t, "subject", "access-b"), ticket)
	denied(t, err)
	_, err = h.a.BeginEnrollment(ctx)
	contractCode(t, err, locking.Capacity)
	q, err := h.a.Quiescence(context.Background())
	if err != nil || q.Tickets != 1 || !q.Busy() {
		t.Fatalf("ticket obligations: %+v, %v", q, err)
	}
	contractCode(t, h.a.ReserveRetirement(context.Background()), locking.Conflict)
	if _, err := h.a.OpenSession(ctx, ticket); err != nil {
		t.Fatalf("denied theft or retirement affected ticket: %v", err)
	}
}

func TestAuthorityUnboundCapabilitiesRejectBoundAccess(t *testing.T) {
	h := newContractHarness(t, nil)
	ticket, session := h.session()
	owner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ctx := accessContext(t, "subject", "access")
	_, err = h.a.OpenSession(ctx, ticket)
	denied(t, err)
	_, err = h.a.CreateOwner(ctx, session.ID, "owner")
	denied(t, err)
	denied(t, h.a.RetireOwner(ctx, owner.Ref))
	denied(t, h.a.CloseSession(ctx, session.ID))
	if err := h.a.CloseSession(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityRetiredIncarnationCleanupStillChecksAccessBinding(t *testing.T) {
	first := newContractHarness(t, nil)
	ctx := accessContext(t, "subject", "access-a")
	_, session, owner := boundOwner(t, first.a, ctx)
	second := newContractHarness(t, nil)
	other := accessContext(t, "subject", "access-b")
	denied(t, second.a.CloseSession(other, session.ID))
	denied(t, second.a.RetireOwner(other, owner))
	if err := second.a.CloseSession(ctx, session.ID); err != nil {
		t.Fatalf("original access cannot acknowledge retired session: %v", err)
	}
	if err := second.a.RetireOwner(ctx, owner); err != nil {
		t.Fatalf("original access cannot acknowledge retired owner: %v", err)
	}
}
