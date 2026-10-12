package locking_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityRetirementReservationPreservesBusyCapabilities(t *testing.T) {
	h := newContractHarness(t, nil)
	ctx := accessContext(t, "subject", "access")
	ticket, session, owner := boundOwner(t, h.a, ctx)
	resource, err := h.a.Resolve(ctx, owner, "a")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := h.a.Acquire(ctx, locking.AcquireRequest{Owner: owner, Request: "grant", Resource: resource, Mode: locking.Shared, TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	q, err := h.a.Quiescence(context.Background())
	if err != nil || q.Sessions != 1 || q.Tickets != 1 || q.Owners != 1 || q.Resources != 1 || q.Actions != 1 || q.Grants != 1 {
		t.Fatalf("live snapshot: %+v, %v", q, err)
	}
	contractCode(t, h.a.ReserveRetirement(context.Background()), locking.Conflict)
	if _, err := h.a.QueryGrant(ctx, owner, grant.Grant.Ref); err != nil {
		t.Fatalf("busy retirement invalidated live grant: %v", err)
	}
	if err := h.a.CloseSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	contractCode(t, h.a.ReserveRetirement(context.Background()), locking.Conflict)
	retired, err := h.a.OpenSession(ctx, ticket)
	if err != nil || !retired.Retired {
		t.Fatalf("busy retirement lost terminal ticket: %+v, %v", retired, err)
	}
	h.clock.advance(time.Minute)
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := h.a.ReserveRetirement(context.Background())
		if err == nil {
			break
		}
		if locking.CodeOf(err) != locking.Conflict || time.Now().After(deadline) {
			t.Fatalf("retirement after history expiry: %v", err)
		}
		runtime.Gosched()
	}
	contractCode(t, h.a.Check(ctx), locking.Retired)
	_, err = h.a.BeginEnrollment(ctx)
	contractCode(t, err, locking.Retired)
	q, err = h.a.Quiescence(context.Background())
	if err != nil || q.Busy() || !q.Reserved {
		t.Fatalf("reserved snapshot: %+v, %v", q, err)
	}
	h.a.CancelRetirement()
	if _, err := h.a.BeginEnrollment(ctx); err != nil {
		t.Fatalf("canceled retirement did not reopen admission: %v", err)
	}
}

func TestAuthorityRetirementReservationOrdersConcurrentEnrollment(t *testing.T) {
	for range 32 {
		h := newContractHarness(t, nil)
		ctx := accessContext(t, "subject", "access")
		start := make(chan struct{})
		reserve := make(chan error, 1)
		enroll := make(chan error, 1)
		go func() {
			<-start
			reserve <- h.a.ReserveRetirement(context.Background())
		}()
		go func() {
			<-start
			_, err := h.a.BeginEnrollment(ctx)
			enroll <- err
		}()
		close(start)
		reserved, enrolled := contractAwait(t, reserve), contractAwait(t, enroll)
		if reserved == nil {
			contractCode(t, enrolled, locking.Retired)
		} else {
			contractCode(t, reserved, locking.Conflict)
			if enrolled != nil {
				t.Fatalf("enrollment preceding retirement failed: %v", enrolled)
			}
		}
	}
}

func TestAuthorityRetirementCannotInterruptAdmittedPublication(t *testing.T) {
	h := newContractHarness(t, nil)
	entered := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- h.a.Publish(context.Background(), locking.Publication{Kind: locking.CreateMutation}, func() locking.PublicationOutcome {
			close(entered)
			<-resume
			return locking.PublicationOutcome{Known: true, Changed: true}
		})
	}()
	contractAwait(t, entered)
	q, err := h.a.Quiescence(context.Background())
	if err != nil || q.Publications != 1 {
		t.Fatalf("publication snapshot: %+v, %v", q, err)
	}
	contractCode(t, h.a.ReserveRetirement(context.Background()), locking.Conflict)
	close(resume)
	if err := contractAwait(t, done); err != nil {
		t.Fatalf("busy retirement interrupted publication: %v", err)
	}
	if err := h.a.ReserveRetirement(context.Background()); err != nil {
		t.Fatalf("finished publication still busy: %v", err)
	}
}

func TestAuthorityRetirementRetainsRecoveryPromise(t *testing.T) {
	h := newContractHarness(t, func(_ *locking.Options, p *contractPersistence) {
		p.max = time.Second
		p.start = p.start.Add(time.Hour)
	})
	h.clock.advance(time.Second - time.Nanosecond)
	q, err := h.a.Quiescence(context.Background())
	if err != nil || q.RecoveryRemainingMillis != 1 || !q.Busy() {
		t.Fatalf("sub-millisecond recovery snapshot: %+v, %v", q, err)
	}
	contractCode(t, h.a.ReserveRetirement(context.Background()), locking.Conflict)
	h.clock.advance(time.Nanosecond)
	if err := h.a.ReserveRetirement(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityRetirementHealthReadDoesNotReopenAdmission(t *testing.T) {
	h := newContractHarness(t, nil)
	ctx := context.Background()
	if err := h.a.CheckRetirement(ctx); err != nil {
		t.Fatalf("healthy retirement read=%v", err)
	}
	if err := h.a.ReserveRetirement(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := h.a.Quiescence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.a.CheckRetirement(ctx); err != nil {
		t.Fatalf("reserved retirement read=%v", err)
	}
	contractCode(t, h.a.Check(ctx), locking.Retired)
	_, err = h.a.BeginEnrollment(ctx)
	contractCode(t, err, locking.Retired)
	_, err = h.a.Acquire(ctx, locking.AcquireRequest{})
	contractCode(t, err, locking.Retired)
	_, err = h.a.Renew(ctx, locking.RenewRequest{})
	contractCode(t, err, locking.Retired)
	published := false
	err = h.a.Publish(ctx, locking.Publication{Kind: locking.CreateMutation}, func() locking.PublicationOutcome {
		published = true
		return locking.PublicationOutcome{Known: true, Changed: true}
	})
	contractCode(t, err, locking.Retired)
	if published {
		t.Fatal("retirement read authorized publication")
	}
	after, err := h.a.Quiescence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before || !after.Reserved {
		t.Fatalf("retirement read changed reservation or resource ownership: before=%+v after=%+v", before, after)
	}
}

func TestAuthorityRetirementHealthReadPreservesFailuresAndPrecedence(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		h := newContractHarness(t, nil)
		if err := h.a.ReserveRetirement(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := h.a.CheckRetirement(ctx); err != ctx.Err() {
			t.Fatalf("context error was replaced: %v", err)
		}
	})
	t.Run("fenced reserved authority", func(t *testing.T) {
		h := newContractHarness(t, nil)
		if err := h.a.ReserveRetirement(context.Background()); err != nil {
			t.Fatal(err)
		}
		cause := errors.New("publication outcome unavailable")
		h.closeErr = cause
		h.a.Fence(cause)
		err := h.a.CheckRetirement(context.Background())
		contractCode(t, err, locking.Unavailable)
		if !errors.Is(err, cause) {
			t.Fatalf("fence cause was lost: %v", err)
		}
		contractCode(t, h.a.Check(context.Background()), locking.Retired)
	})
	t.Run("closed authority", func(t *testing.T) {
		h := newContractHarness(t, nil)
		if err := h.a.ReserveRetirement(context.Background()); err != nil {
			t.Fatal(err)
		}
		cause := errors.New("fenced before close")
		h.closeErr = cause
		h.a.Fence(cause)
		if err := h.a.Close(); !errors.Is(err, cause) {
			t.Fatalf("close lost fence: %v", err)
		}
		contractCode(t, h.a.CheckRetirement(context.Background()), locking.Retired)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := h.a.CheckRetirement(ctx); err != ctx.Err() {
			t.Fatalf("closure took precedence over cancellation: %v", err)
		}
	})
}
