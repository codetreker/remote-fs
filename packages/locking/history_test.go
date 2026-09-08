package locking_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityResolveAdvertisesHistoryExtensionIndependentlyOfResourceExpiry(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
		o.MaxLease = time.Second
		o.MaxWait = time.Second
		o.SessionIdle = 10 * time.Second
		o.ResourceTTL = time.Second
	})
	_, session := h.session()
	owner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.a.Cancel(context.Background(), owner.Ref, "retained"); err != nil {
		t.Fatal(err)
	}
	h.clock.advance(5 * time.Second)
	resource, err := h.a.Resolve(context.Background(), owner.Ref, "a")
	if err != nil {
		t.Fatal(err)
	}
	if resource.NowMillis != 5000 || resource.ExpiresMillis != 6000 || resource.HistoryExpiresMillis != 15000 || resource.HistoryExpiresMillis <= owner.HistoryExpiresMillis {
		t.Fatalf("Resolve did not distinguish extended history from resource validity: %+v, previous history %d", resource, owner.HistoryExpiresMillis)
	}
	h.clock.advance(6 * time.Second)
	result, err := h.a.QueryAction(context.Background(), owner.Ref, "retained")
	if err != nil || result.Receipt.Outcome != locking.Cancelled || result.HistoryExpiresMillis != 21000 {
		t.Fatalf("Resolve's advertised extension did not retain history past prior expiry: %+v, %v", result, err)
	}
}

func TestAuthoritySuccessfulControlRepliesAdvertiseCurrentHistory(t *testing.T) {
	h := newContractHarness(t, nil)
	ticket, session := h.session()
	var now int64
	advance := func() { h.clock.advance(time.Second); now += 1000 }
	check := func(label string, actual int64) {
		t.Helper()
		if actual != now+600000 {
			t.Fatalf("%s history = %d, want %d", label, actual, now+600000)
		}
	}
	advance()
	session, err := h.a.OpenSession(context.Background(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	check("OpenSession replay", session.HistoryExpiresMillis)
	advance()
	owner, err := h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	check("CreateOwner", owner.HistoryExpiresMillis)
	advance()
	owner, err = h.a.CreateOwner(context.Background(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	check("CreateOwner replay", owner.HistoryExpiresMillis)
	advance()
	resource, err := h.a.Resolve(context.Background(), owner.Ref, "a")
	if err != nil {
		t.Fatal(err)
	}
	check("Resolve", resource.HistoryExpiresMillis)
	request := locking.AcquireRequest{Owner: owner.Ref, Request: "acquire", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute}
	advance()
	acquired, err := h.a.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	check("Acquire", acquired.HistoryExpiresMillis)
	check("Acquire grant", acquired.Grant.HistoryExpiresMillis)
	advance()
	action, err := h.a.QueryAction(context.Background(), owner.Ref, request.Request)
	if err != nil {
		t.Fatal(err)
	}
	check("QueryAction", action.HistoryExpiresMillis)
	advance()
	grant, err := h.a.QueryGrant(context.Background(), owner.Ref, acquired.Grant.Ref)
	if err != nil {
		t.Fatal(err)
	}
	check("QueryGrant", grant.HistoryExpiresMillis)
	renew := locking.RenewRequest{Owner: owner.Ref, Request: "renew", Grant: grant.Ref, TTL: time.Minute}
	advance()
	action, err = h.a.Renew(context.Background(), renew)
	if err != nil {
		t.Fatal(err)
	}
	check("Renew", action.HistoryExpiresMillis)
	advance()
	action, err = h.a.Renew(context.Background(), renew)
	if err != nil {
		t.Fatal(err)
	}
	check("Renew replay", action.HistoryExpiresMillis)
	advance()
	action, err = h.a.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	check("Acquire replay", action.HistoryExpiresMillis)
	advance()
	released, err := h.a.Release(context.Background(), owner.Ref, grant.Ref)
	if err != nil {
		t.Fatal(err)
	}
	check("Release", released.HistoryExpiresMillis)
	check("Release grant", released.Grant.HistoryExpiresMillis)
	advance()
	released, err = h.a.Release(context.Background(), owner.Ref, grant.Ref)
	if err != nil {
		t.Fatal(err)
	}
	check("Release replay", released.HistoryExpiresMillis)
	advance()
	cancelled, err := h.a.Cancel(context.Background(), owner.Ref, request.Request)
	if err != nil {
		t.Fatal(err)
	}
	check("Cancel known", cancelled.HistoryExpiresMillis)
	advance()
	cancelled, err = h.a.Cancel(context.Background(), owner.Ref, request.Request)
	if err != nil {
		t.Fatal(err)
	}
	check("Cancel replay", cancelled.HistoryExpiresMillis)
	advance()
	cancelled, err = h.a.Cancel(context.Background(), owner.Ref, "unseen")
	if err != nil {
		t.Fatal(err)
	}
	check("Cancel unseen", cancelled.HistoryExpiresMillis)
	advance()
	late := request
	late.Request = "unseen"
	action, err = h.a.Acquire(context.Background(), late)
	if err != nil {
		t.Fatal(err)
	}
	check("Cancelled Acquire replay", action.HistoryExpiresMillis)
}

func TestAuthorityRecordedRejectionsAdvertiseHistoryWithTheirError(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	request := h.request(owner, "first", "a", locking.Exclusive)
	initial := h.grant(request)
	h.clock.advance(time.Second)
	duplicate := request
	duplicate.Request = "already-held"
	result, err := h.a.Acquire(context.Background(), duplicate)
	contractRejected(t, result, err, locking.AlreadyHeld)
	if result.HistoryExpiresMillis != 601000 {
		t.Fatalf("admitted failure hid extended history: %+v", result)
	}
	h.clock.advance(time.Second)
	result, err = h.a.QueryAction(context.Background(), owner, duplicate.Request)
	contractRejected(t, result, err, locking.AlreadyHeld)
	if result.HistoryExpiresMillis != 602000 {
		t.Fatalf("rejected query hid extended history: %+v", result)
	}
	h.release(owner, initial.Grant.Ref)
	h.clock.advance(time.Second)
	result, err = h.a.Renew(context.Background(), locking.RenewRequest{Owner: owner, Request: "released-renewal", Grant: initial.Grant.Ref, TTL: time.Second})
	contractRejected(t, result, err, locking.StaleGrant)
	if result.HistoryExpiresMillis != 603000 {
		t.Fatalf("rejected renewal hid extended history: %+v", result)
	}
}

func TestAuthorityCancelledReplayCannotExtendHistoryWithoutAReply(t *testing.T) {
	for _, operation := range []string{"acquire", "renew"} {
		t.Run(operation, func(t *testing.T) {
			h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) {
				o.MaxLease = 10 * time.Second
				o.MaxWait = 10 * time.Second
				o.SessionIdle = 10 * time.Second
			})
			owner := h.owner()
			request := h.request(owner, "acquire", "a", locking.Exclusive)
			initial := h.grant(request)
			renew := locking.RenewRequest{Owner: owner, Request: "renew", Grant: initial.Grant.Ref, TTL: 10 * time.Second}
			if _, err := h.a.Renew(context.Background(), renew); err != nil {
				t.Fatal(err)
			}
			h.clock.advance(5 * time.Second)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			t.Cleanup(unblock)
			published := make(chan error, 1)
			go func() {
				published <- h.native.Guard(context.Background(), "a", func() error {
					return h.a.Publish(context.Background(), locking.Publication{Kind: locking.WriteMutation, Targets: []locking.BackendKey{"a"}, Scope: locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{initial.Grant.Ref}}}, func() locking.PublicationOutcome {
						close(entered)
						<-resume
						return locking.PublicationOutcome{Known: true, Changed: true}
					})
				})
			}()
			contractAwait(t, entered)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, done := make(chan struct{}), make(chan error, 1)
			go func() {
				close(started)
				var err error
				if operation == "acquire" {
					_, err = h.a.Acquire(ctx, request)
				} else {
					_, err = h.a.Renew(ctx, renew)
				}
				done <- err
			}()
			contractAwait(t, started)
			select {
			case err := <-done:
				t.Fatalf("replay crossed unfinished publication: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			cancel()
			if err := contractAwait(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("waiting replay lost cancellation: %v", err)
			}
			unblock()
			if err := contractAwait(t, published); err != nil {
				t.Fatal(err)
			}
			h.clock.advance(5 * time.Second)
			_, err := h.a.QueryAction(context.Background(), owner, request.Request)
			contractCode(t, err, locking.Retired)
		})
	}
}
