package locking_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthoritySharedConflictAndRequestReplay(t *testing.T) {
	h := newContractHarness(t, nil)
	a, b, c := h.owner(), h.owner(), h.owner()
	reqA := h.request(a, "read-a", "a", locking.Shared)
	grantA := h.grant(reqA)
	grantB := h.grant(h.request(b, "read-b", "a", locking.Shared))
	reqC := h.request(c, "write-c", "a", locking.Exclusive)
	rejected, err := h.a.Acquire(context.Background(), reqC)
	contractRejected(t, rejected, err, locking.Conflict)

	replayed, err := h.a.Acquire(context.Background(), reqA)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, grantA.Receipt) {
		t.Fatalf("duplicate acquisition = %+v, %v; want original receipt", replayed, err)
	}
	mismatch := reqA
	mismatch.TTL += time.Millisecond
	_, err = h.a.Acquire(context.Background(), mismatch)
	contractCode(t, err, locking.RequestMismatch)

	h.release(a, grantA.Grant.Ref)
	h.release(b, grantB.Grant.Ref)
	replayed, err = h.a.Acquire(context.Background(), reqC)
	contractRejected(t, replayed, err, locking.Conflict)
	if !reflect.DeepEqual(replayed.Receipt, rejected.Receipt) {
		t.Fatalf("conflict replay changed its receipt: %+v", replayed.Receipt)
	}
	reqC.Request = "write-c-after-release"
	h.grant(reqC)
}

func TestAuthorityAlreadyHeldIsRetained(t *testing.T) {
	for _, held := range []locking.Mode{locking.Shared, locking.Exclusive} {
		t.Run(string(held), func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			grant := h.grant(h.request(owner, "first", "a", held))
			second := h.request(owner, "second", "a", locking.Exclusive)
			result, err := h.a.Acquire(context.Background(), second)
			contractRejected(t, result, err, locking.AlreadyHeld)
			h.release(owner, grant.Grant.Ref)
			result, err = h.a.Acquire(context.Background(), second)
			contractRejected(t, result, err, locking.AlreadyHeld)
		})
	}
}

func TestAuthorityReturnedResultsCannotChangeRetainedIntentOrGrant(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	request := h.request(owner, "intent", "a", locking.Shared)
	result := h.grant(request)
	ref := result.Grant.Ref
	if result.Receipt.Acquire == nil || result.Receipt.Grant == nil {
		t.Fatalf("successful receipt lacks original intent or grant: %+v", result.Receipt)
	}
	result.Receipt.Acquire.Mode = locking.Exclusive
	result.Receipt.Grant.Generation++
	result.Grant.Mode = locking.Exclusive
	result.Grant.Ref.Generation++
	replayed, err := h.a.Acquire(context.Background(), request)
	if err != nil || replayed.Receipt.Acquire == nil || *replayed.Receipt.Acquire != request || replayed.Receipt.Grant == nil || *replayed.Receipt.Grant != ref || replayed.Grant == nil || replayed.Grant.Ref != ref || replayed.Grant.Mode != locking.Shared {
		t.Fatalf("returned result mutation changed authority state: %+v, %v", replayed, err)
	}
	called, err := h.publish("a", locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{ref}}, locking.PublicationOutcome{Known: true, Changed: true})
	contractCode(t, err, locking.Conflict)
	if called {
		t.Fatal("returned grant mutation changed shared protection to exclusive authorization")
	}
}

func TestAuthorityPublicationRequiresExactLiveExclusiveProof(t *testing.T) {
	tests := []struct {
		name   string
		mode   locking.Mode
		owner  bool
		proof  bool
		retire string
		target locking.BackendKey
		want   locking.Code
	}{
		{name: "anonymous shared conflict", mode: locking.Shared, target: "a", want: locking.Conflict},
		{name: "anonymous exclusive conflict", mode: locking.Exclusive, target: "a", want: locking.Conflict},
		{name: "exclusive owner must present proof", mode: locking.Exclusive, owner: true, target: "a", want: locking.Conflict},
		{name: "shared holder cannot mutate", mode: locking.Shared, proof: true, target: "a", want: locking.Conflict},
		{name: "exclusive proof permits mutation", mode: locking.Exclusive, proof: true, target: "a"},
		{name: "unrelated proof on unlocked target", mode: locking.Exclusive, proof: true, target: "b", want: locking.UnrelatedProof},
		{name: "released proof has no anonymous fallback", mode: locking.Exclusive, proof: true, retire: "release", target: "a", want: locking.StaleGrant},
		{name: "expired proof without successor", mode: locking.Exclusive, proof: true, retire: "expire", target: "a", want: locking.StaleGrant},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			grant := h.grant(h.request(owner, "acquire", "a", test.mode))
			scope := locking.MutationScope{}
			if test.owner {
				scope.Owner = owner
			}
			if test.proof {
				scope.Owner = owner
				scope.Grants = []locking.GrantRef{grant.Grant.Ref}
			}
			switch test.retire {
			case "release":
				h.release(owner, grant.Grant.Ref)
			case "expire":
				h.clock.advance(10 * time.Second)
			}
			called, err := h.publish(test.target, scope, locking.PublicationOutcome{Known: true, Changed: true})
			if test.want == "" {
				if err != nil || !called {
					t.Fatalf("publication = called %t, error %v; want success", called, err)
				}
			} else {
				contractCode(t, err, test.want)
				if called {
					t.Fatal("rejected publication executed its transition")
				}
			}
		})
	}
}

func TestAuthorityTerminalReceiptsRemainImmutable(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	request := h.request(owner, "acquire", "a", locking.Exclusive)
	original := h.grant(request)
	h.clock.advance(time.Second)
	renewRequest := locking.RenewRequest{Owner: owner, Request: "renew", Grant: original.Grant.Ref, TTL: 20 * time.Second}
	renewed, err := h.a.Renew(context.Background(), renewRequest)
	if err != nil || renewed.Receipt.Outcome != locking.Renewed || renewed.Grant == nil {
		t.Fatalf("Renew = %+v, %v", renewed, err)
	}
	if renewed.Grant.DeadlineMillis <= original.Grant.DeadlineMillis || renewed.Grant.Revision <= original.Grant.Revision {
		t.Fatalf("renewal did not advance deadline and revision: %+v", renewed.Grant)
	}
	h.release(owner, original.Grant.Ref)
	for _, test := range []struct {
		request locking.RequestID
		want    locking.ActionReceipt
	}{{request.Request, original.Receipt}, {renewRequest.Request, renewed.Receipt}} {
		result, err := h.a.QueryAction(context.Background(), owner, test.request)
		if err != nil || !reflect.DeepEqual(result.Receipt, test.want) || result.Grant == nil || result.Grant.State != locking.Released {
			t.Fatalf("QueryAction = %+v, %v; want immutable receipt with released grant", result, err)
		}
	}
	replayed, err := h.a.Renew(context.Background(), renewRequest)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, renewed.Receipt) || replayed.Grant.State != locking.Released {
		t.Fatalf("replayed renewal = %+v, %v", replayed, err)
	}
}

func TestAuthorityCancelPreventsLateOrPendingAcquisition(t *testing.T) {
	for _, state := range []string{"unseen", "pending", "granted"} {
		t.Run(state, func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			request := h.request(owner, "intent", "a", locking.Exclusive)
			var blocker locking.ActionResult
			var blockerOwner locking.OwnerRef
			if state == "pending" {
				blockerOwner = h.owner()
				blocker = h.grant(h.request(blockerOwner, "blocker", "a", locking.Exclusive))
				request.Wait = time.Minute
				result, err := h.a.Acquire(context.Background(), request)
				if err != nil || result.Receipt.Outcome != locking.Pending {
					t.Fatalf("queued Acquire = %+v, %v", result, err)
				}
			} else if state == "granted" {
				h.grant(request)
			}
			cancelled, err := h.a.Cancel(context.Background(), owner, request.Request)
			if err != nil || cancelled.Released != (state == "granted") {
				t.Fatalf("Cancel = %+v, %v", cancelled, err)
			}
			if state == "pending" {
				h.release(blockerOwner, blocker.Grant.Ref)
			}
			replayed, err := h.a.Acquire(context.Background(), request)
			if err != nil {
				t.Fatalf("Acquire after Cancel: %v", err)
			}
			if state == "granted" {
				if replayed.Receipt.Outcome != locking.Granted || replayed.Grant == nil || replayed.Grant.State != locking.Released {
					t.Fatalf("cancelled won acquisition = %+v", replayed)
				}
			} else if replayed.Receipt.Outcome != locking.Cancelled || replayed.Grant != nil {
				t.Fatalf("cancelled acquisition = %+v, want no grant", replayed)
			}
			repeated, err := h.a.Cancel(context.Background(), owner, request.Request)
			if err != nil || repeated.Outcome != cancelled.Outcome || repeated.Released != cancelled.Released {
				t.Fatalf("repeated Cancel = %+v, %v; original %+v", repeated, err, cancelled)
			}
		})
	}
}

func TestAuthorityHistoryCapacityPreservesCleanupAndReplay(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.ActionsPerOwner = 2 })
	owner := h.owner()
	firstRequest := h.request(owner, "first", "a", locking.Exclusive)
	first := h.grant(firstRequest)
	second := h.grant(h.request(owner, "second", "b", locking.Exclusive))
	third := h.request(owner, "third", "c", locking.Exclusive)
	result, err := h.a.Acquire(context.Background(), third)
	contractCode(t, err, locking.Capacity)
	if result.Recorded {
		t.Fatal("over-capacity action was recorded")
	}
	_, err = h.a.Cancel(context.Background(), owner, "unknown")
	contractCode(t, err, locking.OutcomeUnknown)

	h.release(owner, first.Grant.Ref)
	replayed, err := h.a.Acquire(context.Background(), firstRequest)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, first.Receipt) || replayed.Grant.State != locking.Released {
		t.Fatalf("full-history replay = %+v, %v", replayed, err)
	}
	result, err = h.a.Acquire(context.Background(), third)
	contractCode(t, err, locking.Capacity)
	if result.Recorded {
		t.Fatal("release evicted an action history record")
	}
	if err := h.a.RetireOwner(context.Background(), owner); err != nil {
		t.Fatalf("RetireOwner at capacity: %v", err)
	}
	called, err := h.publish("b", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	if err != nil || !called {
		t.Fatalf("retirement retained grant %v: publication called %t, %v", second.Grant.Ref, called, err)
	}
	_, err = h.a.Acquire(context.Background(), third)
	contractCode(t, err, locking.Retired)
}

func TestAuthorityRetirementIsTerminalAndEnrollmentReplays(t *testing.T) {
	h := newContractHarness(t, nil)
	ticket, session := h.session()
	owner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	request := h.request(owner.Ref, "intent", "a", locking.Exclusive)
	h.grant(request)
	if err := h.a.RetireOwner(context.Background(), owner.Ref); err != nil {
		t.Fatal(err)
	}
	replayedOwner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil || replayedOwner.Ref != owner.Ref || !replayedOwner.Retired {
		t.Fatalf("CreateOwner replay = %+v, %v; want original retired owner", replayedOwner, err)
	}
	_, err = h.a.Acquire(context.Background(), request)
	contractCode(t, err, locking.Retired)
	if err := h.a.CloseSession(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	replayedSession, err := h.a.OpenSession(context.Background(), ticket)
	if err != nil || replayedSession.ID != session.ID || !replayedSession.Retired {
		t.Fatalf("OpenSession replay = %+v, %v; want original retired session", replayedSession, err)
	}
	_, err = h.a.CreateOwner(context.Background(), session.ID, "late-owner")
	contractCode(t, err, locking.Retired)
	if err := h.a.CloseSession(context.Background(), session.ID); err != nil {
		t.Fatalf("repeated CloseSession: %v", err)
	}
}

func TestAuthorityQueuedWriterPrecedesLaterReader(t *testing.T) {
	h := newContractHarness(t, nil)
	a, b, c := h.owner(), h.owner(), h.owner()
	first := h.grant(h.request(a, "first-reader", "a", locking.Shared))
	writer := h.request(b, "writer", "a", locking.Exclusive)
	writer.Wait = time.Minute
	result, err := h.a.Acquire(context.Background(), writer)
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("writer = %+v, %v; want pending", result, err)
	}
	reader := h.request(c, "later-reader", "a", locking.Shared)
	reader.Wait = time.Minute
	result, err = h.a.Acquire(context.Background(), reader)
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("later reader = %+v, %v; want pending", result, err)
	}
	h.release(a, first.Grant.Ref)
	won := h.awaitAction(b, writer.Request, locking.Granted)
	result, err = h.a.QueryAction(context.Background(), c, reader.Request)
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("reader overtook queued writer: %+v, %v", result, err)
	}
	h.release(b, won.Grant.Ref)
	h.awaitAction(c, reader.Request, locking.Granted)
}

func TestAuthorityQueuedAcquisitionExpiresWithoutNewGrant(t *testing.T) {
	h := newContractHarness(t, nil)
	a, b := h.owner(), h.owner()
	first := h.grant(h.request(a, "first", "a", locking.Exclusive))
	request := h.request(b, "waiting", "a", locking.Exclusive)
	request.Wait = time.Second
	result, err := h.a.Acquire(context.Background(), request)
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("Acquire = %+v, %v; want pending", result, err)
	}
	h.clock.advance(time.Second)
	timedOut := h.awaitAction(b, request.Request, locking.TimedOut)
	h.release(a, first.Grant.Ref)
	replayed, err := h.a.Acquire(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, timedOut.Receipt) || replayed.Grant != nil {
		t.Fatalf("timed-out replay = %+v, %v", replayed, err)
	}
}

func TestAuthorityRenewalNeverShortensProtection(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	original := h.grant(h.request(owner, "acquire", "a", locking.Exclusive))
	h.clock.advance(time.Second)
	request := locking.RenewRequest{Owner: owner, Request: "short-renewal", Grant: original.Grant.Ref, TTL: time.Second}
	renewed, err := h.a.Renew(context.Background(), request)
	if err != nil || renewed.Grant == nil || renewed.Grant.DeadlineMillis != original.Grant.DeadlineMillis {
		t.Fatalf("short renewal = %+v, %v; want original deadline %d", renewed, err, original.Grant.DeadlineMillis)
	}
	h.clock.advance(2 * time.Second)
	called, err := h.publish("a", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	contractCode(t, err, locking.Conflict)
	if called {
		t.Fatal("short renewal shortened existing protection")
	}
	replayed, err := h.a.Renew(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, renewed.Receipt) || replayed.Grant.DeadlineMillis != original.Grant.DeadlineMillis {
		t.Fatalf("renewal replay extended deadline: %+v, %v", replayed, err)
	}
}

func TestAuthorityRenewalCannotReviveGrantExpiredDuringDurabilityWait(t *testing.T) {
	h := newContractHarness(t, func(_ *locking.Options, p *contractPersistence) { p.max = time.Second })
	owner := h.owner()
	request := h.request(owner, "acquire", "a", locking.Exclusive)
	request.TTL = time.Second
	grant := h.grant(request)
	entered, resume := h.store.blockRaises()
	t.Cleanup(resume)
	type response struct {
		result locking.ActionResult
		err    error
	}
	done := make(chan response, 1)
	renew := locking.RenewRequest{Owner: owner, Request: "renew", Grant: grant.Grant.Ref, TTL: 10 * time.Second}
	go func() {
		result, err := h.a.Renew(context.Background(), renew)
		done <- response{result: result, err: err}
	}()
	if d := contractAwait(t, entered); d < renew.TTL {
		t.Fatalf("durable watermark raise = %v, want at least %v", d, renew.TTL)
	}
	h.clock.advance(time.Second)
	resume()
	result := contractAwait(t, done)
	contractRejected(t, result.result, result.err, locking.StaleGrant)
	status, err := h.a.QueryGrant(context.Background(), owner, grant.Grant.Ref)
	if err != nil || status.State != locking.Expired {
		t.Fatalf("QueryGrant = %+v, %v; want expired", status, err)
	}
	called, err := h.publish("a", locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant.Grant.Ref}}, locking.PublicationOutcome{Known: true})
	contractCode(t, err, locking.StaleGrant)
	if called {
		t.Fatal("expired authorization published after durable preparation")
	}
}

func TestAuthorityUnknownPublicationFencesFutureOperations(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	request := h.request(owner, "future", "b", locking.Exclusive)
	cause := errors.New("commit outcome unavailable")
	h.closeErr = cause
	called, err := h.publish("a", locking.MutationScope{}, locking.PublicationOutcome{Known: false, Err: cause})
	if !called || !errors.Is(err, cause) {
		t.Fatalf("unknown publication called %t, error %v; want preserved cause", called, err)
	}
	contractCode(t, err, locking.Unavailable)
	called, err = h.publish("b", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	contractCode(t, err, locking.Unavailable)
	if called {
		t.Fatal("fenced authority admitted a new transition")
	}
	_, err = h.a.Acquire(context.Background(), request)
	contractCode(t, err, locking.Unavailable)
	status, err := h.a.Status(context.Background())
	if err != nil || !status.Unavailable {
		t.Fatalf("Status = %+v, %v; want unavailable", status, err)
	}
}

func TestAuthorityRecoveryPreservesPersistedDurationAcrossConfigurationDecrease(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, p *contractPersistence) {
		o.MaxLease = time.Second
		p.max = 20 * time.Second
		p.start = o.Clock.Now()
	})
	status, err := h.a.Status(context.Background())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis != 20_000 {
		t.Fatalf("recovery Status = %+v, %v", status, err)
	}
	called, err := h.publish("a", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	contractCode(t, err, locking.Recovering)
	if called {
		t.Fatal("recovery admitted mutation")
	}
	h.clock.advance(19 * time.Second)
	called, err = h.publish("a", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	contractCode(t, err, locking.Recovering)
	if called {
		t.Fatal("decreased maximum shortened recovery")
	}
	h.clock.advance(time.Second)
	called, err = h.publish("a", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	if err != nil || !called {
		t.Fatalf("publication after recovery called %t, error %v", called, err)
	}
	owner := h.owner()
	request := h.request(owner, "after-recovery", "a", locking.Exclusive)
	request.TTL = time.Second
	h.grant(request)
}
