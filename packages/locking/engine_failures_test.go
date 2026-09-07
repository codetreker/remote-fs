package locking_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestAuthorityFailedDurableRaiseRetainsRejectionAndExistingInterval(t *testing.T) {
	cause := errors.New("lease witness sync failed")
	h := newContractHarness(t, func(_ *locking.Options, p *contractPersistence) { p.max = time.Second })
	owner := h.owner()
	request := h.request(owner, "initial", "a", locking.Exclusive)
	request.TTL = time.Second
	initial := h.grant(request)
	h.store.mu.Lock()
	h.store.raise = func(context.Context, time.Duration) error { return cause }
	h.store.mu.Unlock()
	renewal := locking.RenewRequest{Owner: owner, Request: "failed-renewal", Grant: initial.Grant.Ref, TTL: 2 * time.Second}
	failed, err := h.a.Renew(context.Background(), renewal)
	contractRejected(t, failed, err, locking.Unavailable)
	if !errors.Is(err, cause) {
		t.Fatalf("renewal lost durable failure: %v", err)
	}
	if failed.Grant.State != locking.Active || failed.Grant.DeadlineMillis != initial.Grant.DeadlineMillis {
		t.Fatalf("failed renewal changed acknowledged protection: %+v", failed)
	}
	h.store.mu.Lock()
	h.store.raise = nil
	h.store.mu.Unlock()
	replayed, err := h.a.Renew(context.Background(), renewal)
	contractRejected(t, replayed, err, locking.Unavailable)
	if !reflect.DeepEqual(replayed.Receipt, failed.Receipt) || !errors.Is(err, cause) {
		t.Fatalf("failed renewal replay changed: %+v, %v", replayed, err)
	}
	renewal.Request = "successful-renewal"
	result, err := h.a.Renew(context.Background(), renewal)
	if err != nil || result.Grant.DeadlineMillis <= initial.Grant.DeadlineMillis {
		t.Fatalf("new renewal after storage recovery: %+v, %v", result, err)
	}
}

func TestAuthoritySerializesNondecreasingDurableRaises(t *testing.T) {
	h := newContractHarness(t, func(_ *locking.Options, p *contractPersistence) { p.max = 0 })
	owner := h.owner()
	larger := h.request(owner, "larger", "a", locking.Exclusive)
	smaller := h.request(owner, "smaller", "b", locking.Exclusive)
	larger.TTL = 10 * time.Second
	smaller.TTL = 5 * time.Second
	entered, resume := h.store.blockRaises()
	t.Cleanup(resume)
	type reply struct {
		result locking.ActionResult
		err    error
	}
	first, second := make(chan reply, 1), make(chan reply, 1)
	go func() { r, e := h.a.Acquire(context.Background(), larger); first <- reply{r, e} }()
	if got := contractAwait(t, entered); got != larger.TTL {
		t.Fatalf("first raise = %v", got)
	}
	go func() { r, e := h.a.Acquire(context.Background(), smaller); second <- reply{r, e} }()
	resume()
	for _, ch := range []<-chan reply{first, second} {
		result := contractAwait(t, ch)
		if result.err != nil || result.result.Receipt.Outcome != locking.Granted {
			t.Fatalf("acquisition after raise: %+v", result)
		}
	}
	h.store.mu.Lock()
	observed := append([]time.Duration(nil), h.store.observed...)
	maximum := h.store.max
	h.store.mu.Unlock()
	if !reflect.DeepEqual(observed, []time.Duration{larger.TTL}) || maximum != larger.TTL {
		t.Fatalf("durable raises moved backward or repeated: %v, maximum %v", observed, maximum)
	}
}

type failedEvidence struct {
	*contractPersistence
	err error
}

func (p failedEvidence) MaxLease(context.Context) (time.Duration, error) { return 0, p.err }

func TestAuthorityRejectsInvalidConfigurationAndUnreadableEvidence(t *testing.T) {
	cause := errors.New("recovery evidence is missing")
	clock := newContractClock()
	options := locking.DefaultOptions()
	options.Clock = clock
	p := &contractPersistence{start: clock.Now()}
	_, err := locking.New(context.Background(), options, newContractNative(), failedEvidence{p, cause})
	if !errors.Is(err, cause) {
		t.Fatalf("New lost evidence failure: %v", err)
	}
	contractCode(t, err, locking.Unavailable)
	for name, change := range map[string]func(*locking.Options){
		"zero capacity": func(o *locking.Options) { o.MaxSessions = 0 },
		"zero duration": func(o *locking.Options) { o.ResourceTTL = 0 },
		"short idle":    func(o *locking.Options) { o.SessionIdle = o.MaxLease - time.Millisecond },
	} {
		t.Run(name, func(t *testing.T) {
			o := options
			change(&o)
			_, err := locking.New(context.Background(), o, newContractNative(), failedEvidence{p, cause})
			contractCode(t, err, locking.Invalid)
			if errors.Is(err, cause) {
				t.Fatal("invalid configuration reached persistence")
			}
		})
	}
	_, err = locking.New(context.Background(), options, nil, p)
	contractCode(t, err, locking.Invalid)
	_, err = locking.New(context.Background(), options, newContractNative(), &contractPersistence{})
	contractCode(t, err, locking.Invalid)
}

func TestLockErrorsPreserveClassificationAndCauses(t *testing.T) {
	cause := errors.New("native failure")
	cases := map[locking.Code]syscall.Errno{
		locking.Invalid: syscall.EINVAL, locking.RequestMismatch: syscall.EINVAL,
		locking.UnsupportedTarget: syscall.EOPNOTSUPP, locking.Conflict: syscall.EBUSY, locking.AlreadyHeld: syscall.EBUSY,
		locking.Capacity: syscall.EAGAIN, locking.Recovering: syscall.EAGAIN, locking.Retired: syscall.ESTALE,
		locking.StaleResource: syscall.ESTALE, locking.StaleGrant: syscall.ESTALE, locking.UnrelatedProof: syscall.ESTALE,
		locking.OutcomeUnknown: syscall.EIO, locking.Unavailable: syscall.EIO,
	}
	for code, errno := range cases {
		t.Run(string(code), func(t *testing.T) {
			err := locking.Wrap(code, "operation failed", cause)
			if !errors.Is(err, cause) || !errors.Is(err, errno) || !errors.Is(err, &locking.Error{Code: code}) || locking.CodeOf(err) != code {
				t.Fatalf("error lost code or cause: %v", err)
			}
			var typed *locking.Error
			if !errors.As(err, &typed) || typed.Classification() != errno || !strings.Contains(err.Error(), "operation failed") {
				t.Fatalf("error classification: %v", err)
			}
		})
	}
	if locking.CodeOf(cause) != locking.Unavailable {
		t.Fatal("untyped failure invented a known outcome")
	}
}
