package replicated

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type allocationUnitSessionProbe struct {
	httprest.FileSessionWithBarrier
	unit                           uint64
	identity                       storage.FileSessionIdentityResult
	checkErr, callErr, identityErr error
	calls, identityCalls           int
	changeIdentity                 bool
}

func (p *allocationUnitSessionProbe) CheckFileSessionIdentity() error { return p.checkErr }
func (p *allocationUnitSessionProbe) CheckAllocationReporting() error { return p.checkErr }
func (p *allocationUnitSessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.identityCalls++
	result := p.identity
	if p.changeIdentity && p.identityCalls > 1 {
		result.Backend.Authority = "changed"
	}
	return result, p.identityErr
}
func (p *allocationUnitSessionProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	p.calls++
	return p.unit, p.callErr
}

func TestAllocationUnitUsesExactAuthoritativeSession(t *testing.T) {
	probe := &allocationUnitSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
	wrapper := retainedTestSession(t, probe)
	if err := wrapper.CheckAllocationReporting(); err != nil {
		t.Fatal(err)
	}
	got, err := wrapper.AllocationUnitBytes(t.Context())
	if err != nil || got != probe.unit || probe.calls != 1 || probe.identityCalls != 2 {
		t.Fatalf("allocation unit=%d %v, calls=%d identityCalls=%d", got, err, probe.calls, probe.identityCalls)
	}
}

func TestAllocationUnitRejectsUnknownAndSubstitutedFacts(t *testing.T) {
	failure := errors.New("authority unavailable")
	for _, scenario := range []string{"check", "capture", "identity", "malformed", "replacement", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &allocationUnitSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
			wrapper := retainedTestSession(t, probe)
			ctx := t.Context()
			want := failure
			switch scenario {
			case "check":
				probe.checkErr = failure
			case "capture":
				probe.callErr = failure
			case "identity":
				probe.identityErr = failure
			case "malformed":
				probe.unit = 0
				want = syscall.EIO
			case "replacement":
				probe.changeIdentity = true
				want = syscall.ESTALE
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			got, err := wrapper.AllocationUnitBytes(ctx)
			if got != 0 || !errors.Is(err, want) {
				t.Fatalf("allocation unit=%d %v want %v", got, err, want)
			}
		})
	}
}
