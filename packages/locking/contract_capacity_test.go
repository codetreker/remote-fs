package locking_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthoritySessionAndConsumedTicketLimitsAreIndependent(t *testing.T) {
	t.Run("sessions survive ticket expiry", func(t *testing.T) {
		h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxSessions = 1 })
		_, first := h.session()
		h.clock.advance(time.Minute)
		ticket, err := h.a.BeginEnrollment(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.a.OpenSession(context.Background(), ticket)
		contractCode(t, err, locking.Capacity)
		if err := h.a.CloseSession(context.Background(), first.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.a.OpenSession(context.Background(), ticket); err != nil {
			t.Fatalf("session capacity after close: %v", err)
		}
	})
	t.Run("consumed ticket survives session close", func(t *testing.T) {
		h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxTickets = 1 })
		firstTicket, first := h.session()
		secondTicket, err := h.a.BeginEnrollment(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.a.OpenSession(context.Background(), secondTicket)
		contractCode(t, err, locking.Capacity)
		if err := h.a.CloseSession(context.Background(), first.ID); err != nil {
			t.Fatal(err)
		}
		_, err = h.a.OpenSession(context.Background(), secondTicket)
		contractCode(t, err, locking.Capacity)
		h.clock.advance(time.Minute)
		h.session()
		if reopened, err := h.a.OpenSession(context.Background(), firstTicket); err == nil {
			t.Fatalf("expired ticket reopened a session: %+v", reopened)
		}
	})
}

func TestAuthorityOwnerLimitsAndCreationHistory(t *testing.T) {
	for _, global := range []bool{false, true} {
		name := "per session"
		if global {
			name = "global"
		}
		t.Run(name, func(t *testing.T) {
			h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
				if global {
					o.MaxOwners = 1
				} else {
					o.OwnersPerSession = 1
				}
			})
			_, firstSession := h.session()
			secondSession := firstSession
			if global {
				_, secondSession = h.session()
			}
			first, err := h.a.CreateOwner(context.Background(), firstSession.ID, "first")
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.a.CreateOwner(context.Background(), secondSession.ID, "second")
			contractCode(t, err, locking.Capacity)
			if err := h.a.RetireOwner(context.Background(), first.Ref); err != nil {
				t.Fatal(err)
			}
			if _, err := h.a.CreateOwner(context.Background(), secondSession.ID, "second"); err != nil {
				t.Fatalf("owner capacity after retirement: %v", err)
			}
		})
	}
	t.Run("creation history remains after retirement", func(t *testing.T) {
		h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.OwnerActionsPerSession = 1 })
		_, session := h.session()
		first, err := h.a.CreateOwner(context.Background(), session.ID, "first")
		if err != nil {
			t.Fatal(err)
		}
		if err := h.a.RetireOwner(context.Background(), first.Ref); err != nil {
			t.Fatal(err)
		}
		_, err = h.a.CreateOwner(context.Background(), session.ID, "second")
		contractCode(t, err, locking.Capacity)
		replayed, err := h.a.CreateOwner(context.Background(), session.ID, "first")
		if err != nil || replayed.Ref != first.Ref || !replayed.Retired {
			t.Fatalf("full creator history replay = %+v, %v", replayed, err)
		}
	})
}

func TestAuthorityGlobalActionHistoryRejectsBeforeChangingState(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxActions = 1 })
	firstOwner, secondOwner := h.owner(), h.owner()
	first := h.grant(h.request(firstOwner, "first", "a", locking.Exclusive))
	request := h.request(secondOwner, "second", "b", locking.Exclusive)
	result, err := h.a.Acquire(context.Background(), request)
	contractCode(t, err, locking.Capacity)
	if result.Recorded {
		t.Fatal("global history exhaustion recorded new action")
	}
	_, err = h.a.QueryAction(context.Background(), secondOwner, request.Request)
	contractCode(t, err, locking.OutcomeUnknown)
	called, err := h.publish("b", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	if err != nil || !called {
		t.Fatalf("unadmitted acquisition protected target: called %t, %v", called, err)
	}
	h.release(firstOwner, first.Grant.Ref)
	result, err = h.a.Acquire(context.Background(), request)
	contractCode(t, err, locking.Capacity)
	if result.Recorded {
		t.Fatal("grant cleanup evicted global action history")
	}
	if err := h.a.RetireOwner(context.Background(), firstOwner); err != nil {
		t.Fatal(err)
	}
	h.grant(request)
}

func TestAuthorityResourceCapacityRecoversAfterReferenceExpiry(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxResources = 1 })
	owner := h.owner()
	old := h.resource(owner, "a")
	_, err := h.a.Resolve(context.Background(), owner, "b")
	contractCode(t, err, locking.Capacity)
	h.clock.advance(time.Minute)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = h.a.Resolve(context.Background(), owner, "b")
		if err == nil {
			break
		}
		if locking.CodeOf(err) != locking.Capacity || time.Now().After(deadline) {
			t.Fatalf("resource capacity after reference expiry: %v", err)
		}
		runtime.Gosched()
	}
	result, err := h.a.Acquire(context.Background(), locking.AcquireRequest{Owner: owner, Request: "old-reference", Resource: old, Mode: locking.Exclusive, TTL: time.Second})
	contractRejected(t, result, err, locking.StaleResource)
}

func TestAuthorityGrantCapacityRejectionsAreRetained(t *testing.T) {
	for _, global := range []bool{false, true} {
		name := "per owner"
		if global {
			name = "global"
		}
		t.Run(name, func(t *testing.T) {
			h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
				if global {
					o.MaxGrants = 1
				} else {
					o.GrantsPerOwner = 1
				}
			})
			firstOwner := h.owner()
			secondOwner := firstOwner
			if global {
				secondOwner = h.owner()
			}
			first := h.grant(h.request(firstOwner, "first", "a", locking.Exclusive))
			request := h.request(secondOwner, "second", "b", locking.Exclusive)
			result, err := h.a.Acquire(context.Background(), request)
			contractRejected(t, result, err, locking.Capacity)
			h.release(firstOwner, first.Grant.Ref)
			result, err = h.a.Acquire(context.Background(), request)
			contractRejected(t, result, err, locking.Capacity)
			request.Request = "after-capacity-release"
			h.grant(request)
		})
	}
}

func TestAuthorityQueueLimitsRejectBeforeAddingWaiters(t *testing.T) {
	for _, limit := range []string{"global", "per owner", "per resource"} {
		t.Run(limit, func(t *testing.T) {
			h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
				switch limit {
				case "global":
					o.MaxQueued = 1
				case "per owner":
					o.QueuedPerOwner = 1
				case "per resource":
					o.QueuedPerResource = 1
				}
			})
			blocker := h.owner()
			h.grant(h.request(blocker, "block-a", "a", locking.Exclusive))
			h.grant(h.request(blocker, "block-b", "b", locking.Exclusive))
			firstOwner := h.owner()
			secondOwner := firstOwner
			if limit != "per owner" {
				secondOwner = h.owner()
			}
			firstRequest := h.request(firstOwner, "first", "a", locking.Exclusive)
			firstRequest.Wait = time.Minute
			first, err := h.a.Acquire(context.Background(), firstRequest)
			if err != nil || first.Receipt.Outcome != locking.Pending {
				t.Fatalf("first waiter = %+v, %v", first, err)
			}
			secondPath := "b"
			if limit == "per resource" {
				secondPath = "a"
			}
			secondRequest := h.request(secondOwner, "second", secondPath, locking.Exclusive)
			secondRequest.Wait = time.Minute
			result, err := h.a.Acquire(context.Background(), secondRequest)
			contractRejected(t, result, err, locking.Capacity)
			status, err := h.a.Status(context.Background())
			if err != nil || status.Queued != 1 {
				t.Fatalf("queue status = %+v, %v; want exactly one waiter", status, err)
			}
			if _, err := h.a.Cancel(context.Background(), firstOwner, firstRequest.Request); err != nil {
				t.Fatal(err)
			}
			result, err = h.a.Acquire(context.Background(), secondRequest)
			contractRejected(t, result, err, locking.Capacity)
			secondRequest.Request = "after-cancel"
			result, err = h.a.Acquire(context.Background(), secondRequest)
			if err != nil || result.Receipt.Outcome != locking.Pending {
				t.Fatalf("waiter after queue release = %+v, %v", result, err)
			}
		})
	}
}

func TestAuthorityMutationProofAndRequestByteLimits(t *testing.T) {
	t.Run("proof count", func(t *testing.T) {
		h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxProofs = 1 })
		owner := h.owner()
		first := h.grant(h.request(owner, "first", "a", locking.Exclusive))
		second := h.grant(h.request(owner, "second", "b", locking.Exclusive))
		called := false
		err := h.native.Guard(context.Background(), "a", func() error {
			return h.native.Guard(context.Background(), "b", func() error {
				return h.a.Publish(context.Background(), locking.Publication{
					Kind: locking.RenameMutation, Targets: []locking.BackendKey{"a", "b"},
					Scope: locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{first.Grant.Ref, second.Grant.Ref}},
				}, func() locking.PublicationOutcome {
					called = true
					return locking.PublicationOutcome{Known: true, Changed: true}
				})
			})
		})
		contractCode(t, err, locking.Invalid)
		if called {
			t.Fatal("over-limit proofs admitted publication")
		}
		called, err = h.publish("a", locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{first.Grant.Ref}}, locking.PublicationOutcome{Known: true, Changed: true})
		if err != nil || !called {
			t.Fatalf("proof boundary called %t, %v", called, err)
		}
	})
	t.Run("request bytes", func(t *testing.T) {
		h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.MaxRequestBytes = 8 })
		owner := h.owner()
		request := h.request(owner, "123456789", "a", locking.Exclusive)
		result, err := h.a.Acquire(context.Background(), request)
		contractCode(t, err, locking.Invalid)
		if result.Recorded {
			t.Fatal("invalid request consumed action history")
		}
		request.Request = "12345678"
		h.grant(request)
	})
}

func TestAuthorityRenewalAtHistoryCapacityPreservesAcknowledgedInterval(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.ActionsPerOwner = 1 })
	owner := h.owner()
	original := h.grant(h.request(owner, "acquire", "a", locking.Exclusive))
	renew := locking.RenewRequest{Owner: owner, Request: "renew", Grant: original.Grant.Ref, TTL: time.Minute}
	result, err := h.a.Renew(context.Background(), renew)
	contractCode(t, err, locking.Capacity)
	if result.Recorded {
		t.Fatal("renewal at history capacity was admitted")
	}
	status, err := h.a.QueryGrant(context.Background(), owner, original.Grant.Ref)
	if err != nil || status.State != locking.Active || status.DeadlineMillis != original.Grant.DeadlineMillis {
		t.Fatalf("failed renewal changed acknowledged interval: %+v, %v", status, err)
	}
	h.release(owner, original.Grant.Ref)
}

func TestAuthorityResolveExpiryDoesNotShortenExistingGrant(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
		o.ResourceTTL = time.Second
		o.MaxResources = 1
	})
	owner := h.owner()
	grant := h.grant(h.request(owner, "acquire", "a", locking.Exclusive))
	h.clock.advance(time.Second)
	status, err := h.a.QueryGrant(context.Background(), owner, grant.Grant.Ref)
	if err != nil || status.State != locking.Active {
		t.Fatalf("reference expiry changed grant: %+v, %v", status, err)
	}
	_, err = h.a.Resolve(context.Background(), owner, "b")
	contractCode(t, err, locking.Capacity)
	called, err := h.publish("a", locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant.Grant.Ref}}, locking.PublicationOutcome{Known: true, Changed: true})
	if err != nil || !called {
		t.Fatalf("reference expiry invalidated live proof: called %t, %v", called, err)
	}
}

func TestAuthorityDurationLimitsRejectUnboundedIntent(t *testing.T) {
	for _, field := range []string{"lease", "wait"} {
		t.Run(field, func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			request := h.request(owner, "invalid", "a", locking.Exclusive)
			if field == "lease" {
				request.TTL = time.Minute + time.Millisecond
			} else {
				request.Wait = time.Minute + time.Millisecond
			}
			result, err := h.a.Acquire(context.Background(), request)
			contractCode(t, err, locking.Invalid)
			if result.Recorded {
				t.Fatal("invalid duration consumed action history")
			}
			request.TTL, request.Wait = time.Minute, time.Minute
			h.grant(request)
		})
	}
}
