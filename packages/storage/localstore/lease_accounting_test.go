package localstore_test

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func TestLocalstoreLeasePublicationAccountingControlsOuterQuota(t *testing.T) {
	config := testConfig(privateRoot(t))
	options := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &options, true
	native := open(t, config)
	defer closeStore(t, native)
	initial := bytes.Repeat([]byte("a"), 1024)
	if err := native.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := native.Write(t.Context(), "held", initial); err != nil {
		t.Fatal(err)
	}
	bounded, err := limited.New(t.Context(), native, 4096)
	if err != nil {
		t.Fatalf("admitting native accounting for the lock-enabled quota wrapper: %v", err)
	}
	assertUsed := func(want int64) {
		t.Helper()
		space, err := bounded.Space(t.Context())
		if err != nil || space.Total != 4096 || space.Used != want {
			t.Fatalf("outer quota = %+v, %v; want %d charged bytes", space, err, want)
		}
	}
	assertUsed(1024)
	service := native.LockService()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(t.Context(), owner.Ref, "held")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner.Ref, Request: "lease", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute,
	})
	if err != nil || grant.Grant == nil || grant.Receipt.Outcome != locking.Granted {
		t.Fatalf("grant outcome = %v, %v", grant.Receipt.Outcome, err)
	}
	scope := locking.MutationScope{Owner: owner.Ref, Grants: []locking.GrantRef{grant.Grant.Ref}}
	scoped := locking.WithScope(t.Context(), scope)
	grown := bytes.Repeat([]byte("b"), 2048)
	rejected := errors.New("publication accounting rejected growth")
	var attempted [][2]int64
	rejecting := storage.WithPublicationAccounting(scoped, func(previous, next int64) (storage.PublicationSettlement, error) {
		attempted = append(attempted, [2]int64{previous, next})
		return nil, rejected
	})
	if err := bounded.Write(rejecting, "held", grown); !errors.Is(err, rejected) {
		t.Fatalf("rejected accounting write = %v", err)
	}
	if len(attempted) != 1 || attempted[0] != [2]int64{1024, 2048} {
		t.Fatalf("native preparation = %v", attempted)
	}
	assertUsed(1024)
	if body, err := native.Read(t.Context(), "held"); err != nil || !bytes.Equal(body, initial) {
		t.Fatalf("rejected publication changed the file: length=%d, err=%v", len(body), err)
	}
	invalid := locking.CloneScope(scope)
	invalid.Grants[0].Generation++
	if err := bounded.Write(locking.WithScope(t.Context(), invalid), "held", grown); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("invalid proof write = %v", err)
	}
	assertUsed(1024)
	type transition struct {
		previous, next int64
		outcomes       []storage.PublicationResult
	}
	var transitions []*transition
	accounted := storage.WithPublicationAccounting(scoped, func(previous, next int64) (storage.PublicationSettlement, error) {
		change := &transition{previous: previous, next: next}
		transitions = append(transitions, change)
		return func(result storage.PublicationResult) error {
			change.outcomes = append(change.outcomes, result)
			return nil
		}, nil
	})
	if err := bounded.Write(accounted, "held", grown); err != nil {
		t.Fatal(err)
	}
	assertUsed(2048)
	if err := bounded.Write(accounted, "held", grown[:512]); err != nil {
		t.Fatal(err)
	}
	assertUsed(512)
	if err := bounded.Remove(accounted, "held"); err != nil {
		t.Fatal(err)
	}
	assertUsed(0)
	if _, err := native.Stat(t.Context(), "held"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("removed file = %v", err)
	}
	want := [][2]int64{{1024, 2048}, {2048, 512}, {512, 0}}
	if len(transitions) != len(want) {
		t.Fatalf("publication count = %d, want %d", len(transitions), len(want))
	}
	for i, change := range transitions {
		if [2]int64{change.previous, change.next} != want[i] || len(change.outcomes) != 1 || change.outcomes[0] != storage.PublicationApplied {
			t.Fatalf("publication %d = %+v; want sizes %v and one Applied", i, change, want[i])
		}
	}
}
