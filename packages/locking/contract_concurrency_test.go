package locking_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityPublicationOrdersSameResourceReleaseWithoutBlockingOtherFiles(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	grant := h.grant(h.request(owner, "acquire", "a", locking.Exclusive))
	entered := make(chan struct{})
	resume := make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	published := make(chan error, 1)
	go func() {
		published <- h.native.Guard(context.Background(), "a", func() error {
			return h.a.Publish(context.Background(), locking.Publication{
				Kind: locking.WriteMutation, Targets: []locking.BackendKey{"a"},
				Scope: locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant.Grant.Ref}},
			}, func() locking.PublicationOutcome {
				close(entered)
				<-resume
				return locking.PublicationOutcome{Known: true, Changed: true}
			})
		})
	}()
	contractAwait(t, entered)

	type releaseResponse struct {
		result locking.ReleaseResult
		err    error
	}
	released := make(chan releaseResponse, 1)
	releaseStarted := make(chan struct{})
	go func() {
		close(releaseStarted)
		result, err := h.a.Release(context.Background(), owner, grant.Grant.Ref)
		released <- releaseResponse{result: result, err: err}
	}()
	contractAwait(t, releaseStarted)

	otherPublished := make(chan error, 1)
	go func() {
		called, err := h.publish("b", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
		if err == nil && !called {
			err = locking.Wrap(locking.Unavailable, "independent publication was not executed", nil)
		}
		otherPublished <- err
	}()
	if err := contractAwait(t, otherPublished); err != nil {
		t.Fatalf("independent publication: %v", err)
	}
	select {
	case response := <-released:
		t.Fatalf("release overtook unfinished publication: %+v", response)
	default:
	}
	unblock()
	if err := contractAwait(t, published); err != nil {
		t.Fatalf("original publication: %v", err)
	}
	response := contractAwait(t, released)
	if response.err != nil || response.result.State != locking.Released {
		t.Fatalf("ordered release = %+v", response)
	}
}

func TestAuthorityRetiredTargetRejectsPendingIntentAndPreservesOriginalReceipt(t *testing.T) {
	h := newContractHarness(t, nil)
	owner, waiter := h.owner(), h.owner()
	originalRequest := h.request(owner, "holder", "a", locking.Exclusive)
	original := h.grant(originalRequest)
	request := h.request(waiter, "waiter", "a", locking.Exclusive)
	request.Wait = time.Minute
	queued, err := h.a.Acquire(context.Background(), request)
	if err != nil || queued.Receipt.Outcome != locking.Pending {
		t.Fatalf("queued acquisition = %+v, %v", queued, err)
	}
	err = h.native.Guard(context.Background(), "a", func() error {
		return h.a.Publish(context.Background(), locking.Publication{
			Kind: locking.RemoveMutation, Targets: []locking.BackendKey{"a"},
			Scope: locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{original.Grant.Ref}},
		}, func() locking.PublicationOutcome {
			h.native.nodes["a"].live = false
			return locking.PublicationOutcome{Known: true, Changed: true, Retired: []locking.BackendKey{"a"}}
		})
	})
	if err != nil {
		t.Fatalf("remove publication: %v", err)
	}
	rejected := h.awaitAction(waiter, request.Request, locking.Rejected)
	if rejected.Grant != nil {
		t.Fatalf("removed target produced a grant: %+v", rejected)
	}
	replayed, replayErr := h.a.Acquire(context.Background(), request)
	contractRejected(t, replayed, replayErr, rejected.Receipt.Code)
	if !reflect.DeepEqual(replayed.Receipt, rejected.Receipt) {
		t.Fatal("removed-target rejection changed on replay")
	}
	result, err := h.a.QueryAction(context.Background(), owner, originalRequest.Request)
	if err != nil || !reflect.DeepEqual(result.Receipt, original.Receipt) || result.Grant == nil || result.Grant.State != locking.TargetGone {
		t.Fatalf("retired target receipt = %+v, %v", result, err)
	}
}

func TestAuthorityQueuedIntentSurvivesControlRequestCancellation(t *testing.T) {
	h := newContractHarness(t, nil)
	holder, waiter := h.owner(), h.owner()
	first := h.grant(h.request(holder, "holder", "a", locking.Exclusive))
	request := h.request(waiter, "waiter", "a", locking.Exclusive)
	request.Wait = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	result, err := h.a.Acquire(ctx, request)
	cancel()
	if err != nil || result.Receipt.Outcome != locking.Pending {
		t.Fatalf("queued acquisition = %+v, %v", result, err)
	}
	h.release(holder, first.Grant.Ref)
	won := h.awaitAction(waiter, request.Request, locking.Granted)
	replayed, err := h.a.Acquire(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed.Receipt, won.Receipt) || replayed.Grant == nil || replayed.Grant.Ref != won.Grant.Ref {
		t.Fatalf("request cancellation changed retained intent: %+v, %v", replayed, err)
	}
}
