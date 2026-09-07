package locking_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityIdleLifetimeRetiresCapabilitiesWithSystemClock(t *testing.T) {
	opts := locking.DefaultOptions()
	opts.MaxLease = time.Millisecond
	opts.MaxWait = time.Millisecond
	opts.SessionIdle = 100 * time.Millisecond
	opts.ResourceTTL = time.Millisecond
	opts.TicketTTL = time.Second
	p := &contractPersistence{start: time.Now()}
	a, err := locking.New(context.Background(), opts, newContractNative(), p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	})
	ticket, err := a.BeginEnrollment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session, err := a.OpenSession(context.Background(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	limit := time.NewTimer(5 * time.Second)
	defer limit.Stop()
	for {
		status, err := a.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if status.Sessions == 0 {
			break
		}
		select {
		case <-limit.C:
			t.Fatal("expired session was retained indefinitely")
		case <-time.After(time.Millisecond):
		}
	}
	_, err = a.QueryAction(context.Background(), owner.Ref, "unseen")
	contractCode(t, err, locking.Retired)
	_, err = a.CreateOwner(context.Background(), session.ID, "late")
	contractCode(t, err, locking.Retired)
	if err := a.RetireOwner(context.Background(), owner.Ref); err != nil {
		t.Fatalf("retired owner cleanup: %v", err)
	}
	if err := a.CloseSession(context.Background(), session.ID); err != nil {
		t.Fatalf("retired session cleanup: %v", err)
	}
}

func TestMutationScopeCopiesAndPreservesExplicitAnonymousContext(t *testing.T) {
	base := context.Background()
	if locking.HasScope(base) {
		t.Fatal("empty context has a scope")
	}
	scope := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}, Grants: []locking.GrantRef{{ID: "grant", Resource: "resource", Generation: 1}}}
	ctx := locking.WithScope(base, scope)
	scope.Grants[0].Generation = 2
	if !locking.HasScope(ctx) || locking.ScopeFromContext(ctx).Grants[0].Generation != 1 {
		t.Fatal("context retained mutable caller slice")
	}
	extracted := locking.ScopeFromContext(ctx)
	extracted.Grants[0].Generation = 3
	if locking.ScopeFromContext(ctx).Grants[0].Generation != 1 {
		t.Fatal("scope extraction leaked retained slice")
	}
	cloned := locking.CloneScope(extracted)
	cloned.Grants[0].Generation = 4
	if extracted.Grants[0].Generation != 3 {
		t.Fatal("scope clone leaked source slice")
	}
	ctx = locking.WithScope(ctx, locking.MutationScope{})
	if !locking.HasScope(ctx) || locking.ScopeFromContext(ctx).Owner.Owner != "" {
		t.Fatal("explicit anonymous scope did not replace prior scope")
	}
	for _, invalid := range []locking.MutationScope{
		{Owner: locking.OwnerRef{Session: "session"}},
		{Grants: []locking.GrantRef{{ID: "g", Resource: "r", Generation: 1}}},
		{Owner: locking.OwnerRef{Session: locking.SessionID(strings.Repeat("s", 513)), Owner: "o"}},
		{Owner: scope.Owner, Grants: []locking.GrantRef{{ID: locking.GrantID(strings.Repeat("g", 513)), Resource: "r", Generation: 1}}},
		{Owner: scope.Owner, Grants: []locking.GrantRef{{ID: "g", Resource: "r", Generation: 1}, {ID: "g", Resource: "r", Generation: 1}}},
	} {
		contractCode(t, locking.ValidateScope(invalid, 16), locking.Invalid)
	}
}

func TestAuthorityBackendFencePreservesCauseAndPreventsGrantProgress(t *testing.T) {
	h := newContractHarness(t, nil)
	owner, waiter := h.owner(), h.owner()
	h.grant(h.request(owner, "holder", "a", locking.Exclusive))
	request := h.request(waiter, "waiting", "a", locking.Exclusive)
	request.Wait = time.Second
	result, err := h.a.Acquire(context.Background(), request)
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("queue: %+v, %v", result, err)
	}
	h.a.Fence(nil)
	if err := h.a.Check(context.Background()); err != nil {
		t.Fatalf("nil fence changed health: %v", err)
	}
	cause := errors.New("backend accepted state is unknown")
	h.closeErr = cause
	h.a.Fence(cause)
	h.a.Fence(errors.New("later failure"))
	if err := h.a.Check(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("fence lost original cause: %v", err)
	}
	_, err = h.a.QueryAction(context.Background(), waiter, request.Request)
	contractCode(t, err, locking.Unavailable)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.a.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("health query lost cancellation: %v", err)
	}
}

func TestAuthorityPanickedPublicationFencesAndDrains(t *testing.T) {
	h := newContractHarness(t, nil)
	func() {
		defer func() {
			if recover() != "native publication panic" {
				t.Error("publication panic was swallowed")
			}
		}()
		_ = h.a.Publish(context.Background(), locking.Publication{Kind: locking.CreateMutation}, func() locking.PublicationOutcome { panic("native publication panic") })
	}()
	contractCode(t, h.a.Check(context.Background()), locking.Unavailable)
	h.closeErr = h.a.Close()
	if h.closeErr == nil {
		t.Fatal("closing unknown publication discarded failure")
	}
}

func TestAuthorityKnownFailurePreservesBindingAndUndeclaredRetirementFences(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	grant := h.grant(h.request(owner, "holder", "a", locking.Exclusive))
	scope := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant.Grant.Ref}}
	cause := errors.New("known metadata failure")
	called, err := h.publish("a", scope, locking.PublicationOutcome{Known: true, Changed: true, Err: cause})
	if !called || !errors.Is(err, cause) {
		t.Fatalf("partial failure: called %t, error %v", called, err)
	}
	if err := h.a.Check(context.Background()); err != nil {
		t.Fatalf("known outcome poisoned authority: %v", err)
	}
	status, err := h.a.QueryGrant(context.Background(), owner, grant.Grant.Ref)
	if err != nil || status.State != locking.Active {
		t.Fatalf("known partial failure retired identity: %+v, %v", status, err)
	}
	called, err = h.publish("a", scope, locking.PublicationOutcome{Known: true, Retired: []locking.BackendKey{"b"}})
	if !called {
		t.Fatal("retirement validation happened before native outcome")
	}
	contractCode(t, err, locking.Unavailable)
	contractCode(t, h.a.Check(context.Background()), locking.Unavailable)
	h.closeErr = h.a.Close()
}

func TestAuthorityUnseenCancellationReceiptCannotAcquireAnIntentLater(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	if _, err := h.a.Cancel(context.Background(), owner, "cancelled"); err != nil {
		t.Fatal(err)
	}
	original, err := h.a.QueryAction(context.Background(), owner, "cancelled")
	if err != nil || original.Receipt.Acquire != nil {
		t.Fatalf("unseen cancellation receipt: %+v, %v", original, err)
	}
	request := h.request(owner, "cancelled", "a", locking.Exclusive)
	for _, mode := range []locking.Mode{locking.Exclusive, locking.Shared} {
		request.Mode = mode
		result, err := h.a.Acquire(context.Background(), request)
		if err != nil || !reflect.DeepEqual(result.Receipt, original.Receipt) || result.Grant != nil {
			t.Fatalf("delayed acquisition rewrote tombstone: %+v, %v", result, err)
		}
	}
}
