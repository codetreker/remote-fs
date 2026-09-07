package locking_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityFractionalMillisecondsRejectBeforeRecording(t *testing.T) {
	for _, field := range []string{"acquire TTL", "acquire Wait", "renew TTL"} {
		t.Run(field, func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			request := h.request(owner, "intent", "a", locking.Exclusive)
			if field == "renew TTL" {
				grant := h.grant(request)
				renew := locking.RenewRequest{Owner: owner, Request: "renew", Grant: grant.Grant.Ref, TTL: time.Second + time.Nanosecond}
				result, err := h.a.Renew(context.Background(), renew)
				contractCode(t, err, locking.Invalid)
				if result.Recorded {
					t.Fatal("fractional renewal consumed action history")
				}
				renew.TTL = time.Second
				result, err = h.a.Renew(context.Background(), renew)
				if err != nil || result.Receipt.Outcome != locking.Renewed {
					t.Fatalf("corrected renewal = %+v, %v", result, err)
				}
				return
			}
			if field == "acquire TTL" {
				request.TTL += time.Nanosecond
			} else {
				request.Wait = time.Millisecond + time.Nanosecond
			}
			result, err := h.a.Acquire(context.Background(), request)
			contractCode(t, err, locking.Invalid)
			if result.Recorded {
				t.Fatal("fractional acquisition consumed action history")
			}
			request.TTL, request.Wait = time.Second, time.Millisecond
			h.grant(request)
		})
	}
}

func TestAuthorityRemainingMillisFloorsActualRemainingInterval(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	request := h.request(owner, "intent", "a", locking.Exclusive)
	request.TTL = time.Millisecond
	h.clock.advance(100 * time.Microsecond)
	grant := h.grant(request)
	h.clock.advance(800 * time.Microsecond)
	status, err := h.a.QueryGrant(context.Background(), owner, grant.Grant.Ref)
	if err != nil || status.State != locking.Active || status.RemainingMillis != 0 {
		t.Fatalf("sub-millisecond remainder = %+v, %v; want active with zero whole milliseconds", status, err)
	}
	if status.DeadlineMillis-status.NowMillis != 1 {
		t.Fatal("fixture does not cross independently rounded response ticks")
	}
}

type contractWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *contractWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestAuthorityQueriesWaitForPublicationBeforeReportingExpiredGrant(t *testing.T) {
	for _, query := range []string{"action", "grant"} {
		t.Run(query, func(t *testing.T) {
			h := newContractHarness(t, nil)
			owner := h.owner()
			request := h.request(owner, "intent", "a", locking.Exclusive)
			grant := h.grant(request)
			entered := make(chan struct{})
			resume := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
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
			h.clock.advance(request.TTL)
			type response struct {
				state locking.GrantState
				err   error
			}
			done := make(chan response, 1)
			ctx := &contractWaitContext{Context: context.Background(), waiting: make(chan struct{})}
			go func() {
				if query == "action" {
					result, err := h.a.QueryAction(ctx, owner, request.Request)
					state := locking.GrantState("")
					if result.Grant != nil {
						state = result.Grant.State
					}
					done <- response{state: state, err: err}
				} else {
					result, err := h.a.QueryGrant(ctx, owner, grant.Grant.Ref)
					done <- response{state: result.State, err: err}
				}
			}()
			select {
			case result := <-done:
				t.Fatalf("query crossed unfinished publication: %+v", result)
			case <-ctx.waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("query did not reach its cancellable wait")
			}
			unblock()
			if err := contractAwait(t, published); err != nil {
				t.Fatal(err)
			}
			result := contractAwait(t, done)
			if result.err != nil || result.state != locking.Expired {
				t.Fatalf("query after publication = %+v; want expired", result)
			}
		})
	}
}

func contractAwaitUnavailable(t *testing.T, h *contractHarness) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := h.a.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if status.Unavailable {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("authority did not become unavailable")
		}
		runtime.Gosched()
	}
}

func contractClosePending(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Close returned while a native operation remained blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestAuthorityCloseWaitsForPublicationWithoutDiscoveredResources(t *testing.T) {
	h := newContractHarness(t, nil)
	entered := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	published := make(chan error, 1)
	go func() {
		published <- h.native.Guard(context.Background(), "a", func() error {
			return h.a.Publish(context.Background(), locking.Publication{
				Kind: locking.WriteMutation, Targets: []locking.BackendKey{"a"},
			}, func() locking.PublicationOutcome {
				close(entered)
				<-resume
				return locking.PublicationOutcome{Known: true, Changed: true}
			})
		})
	}()
	contractAwait(t, entered)
	status, err := h.a.Status(context.Background())
	if err != nil || status.Resources != 0 {
		t.Fatalf("publication fixture has discovered resources: %+v, %v", status, err)
	}
	closed := make(chan error, 1)
	go func() { closed <- h.a.Close() }()
	contractAwaitUnavailable(t, h)
	contractClosePending(t, closed)
	unblock()
	if err := contractAwait(t, published); err != nil {
		t.Fatalf("publication: %v", err)
	}
	if err := contractAwait(t, closed); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAuthorityForgetFailureFencesAndIsNotRetriedByClose(t *testing.T) {
	h := newContractHarness(t, func(o *locking.Options, _ *contractPersistence) { o.ResourceTTL = time.Millisecond })
	owner := h.owner()
	registered, releaseTimer := h.clock.holdTimer(h.clock.Now().Add(time.Millisecond))
	t.Cleanup(releaseTimer)
	h.resource(owner, "a")
	cause := errors.New("native pin cleanup outcome unavailable")
	h.closeErr = cause
	h.native.setForget("a", func() error { return cause })
	// Hold the registered timer until clock advancement so maintenance cannot
	// consume an earlier wake and replace it with a later relative timer.
	contractAwait(t, registered)
	h.clock.advance(time.Millisecond)
	releaseTimer()
	contractAwaitUnavailable(t, h)
	called, err := h.publish("b", locking.MutationScope{}, locking.PublicationOutcome{Known: true, Changed: true})
	if called || !errors.Is(err, cause) {
		t.Fatalf("publication after cleanup failure called %t, error %v", called, err)
	}
	contractCode(t, err, locking.Unavailable)
	for range 2 {
		if err := h.a.Close(); !errors.Is(err, cause) {
			t.Fatalf("Close lost cleanup error: %v", err)
		}
		if count := h.native.forgetCount("a"); count != 1 {
			t.Fatalf("Forget calls = %d, want one uncertain cleanup attempt", count)
		}
	}
}

func TestAuthorityCloseDrainsAdmittedAcquisitionDurabilityWork(t *testing.T) {
	h := newContractHarness(t, func(_ *locking.Options, p *contractPersistence) { p.max = time.Second })
	owner := h.owner()
	request := h.request(owner, "intent", "a", locking.Exclusive)
	request.TTL = 2 * time.Second
	entered := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	h.store.mu.Lock()
	h.store.raise = func(context.Context, time.Duration) error {
		close(entered)
		<-resume
		return nil
	}
	h.store.mu.Unlock()
	acquired := make(chan error, 1)
	go func() {
		_, err := h.a.Acquire(context.Background(), request)
		acquired <- err
	}()
	contractAwait(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- h.a.Close() }()
	contractAwaitUnavailable(t, h)
	contractClosePending(t, closed)
	if count := h.native.forgetCount("a"); count != 0 {
		t.Fatalf("Close released %d native pins while acquisition preparation remained active", count)
	}
	unblock()
	if err := contractAwait(t, acquired); err == nil {
		t.Fatal("acquisition acknowledged a grant after closing began")
	}
	if err := contractAwait(t, closed); err != nil {
		t.Fatalf("Close after acquisition drained: %v", err)
	}
	if count := h.native.forgetCount("a"); count != 1 {
		t.Fatalf("Forget calls after drain = %d, want one", count)
	}
}

func TestAuthorityConcurrentCloseSharesCleanupAndFailure(t *testing.T) {
	h := newContractHarness(t, nil)
	owner := h.owner()
	h.resource(owner, "a")
	entered := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	cause := errors.New("native cleanup failed")
	h.closeErr = cause
	h.native.setForget("a", func() error {
		close(entered)
		<-resume
		return cause
	})
	first := make(chan error, 1)
	go func() { first <- h.a.Close() }()
	contractAwait(t, entered)
	second := make(chan error, 1)
	go func() { second <- h.a.Close() }()
	contractClosePending(t, second)
	unblock()
	for _, done := range []<-chan error{first, second} {
		if err := contractAwait(t, done); !errors.Is(err, cause) {
			t.Fatalf("concurrent Close lost shared cleanup failure: %v", err)
		}
	}
	if count := h.native.forgetCount("a"); count != 1 {
		t.Fatalf("concurrent Close invoked Forget %d times, want one", count)
	}
}
