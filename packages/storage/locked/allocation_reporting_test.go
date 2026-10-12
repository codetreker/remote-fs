package locked

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"testing"
)

type allocationSessionProbe struct {
	storage.FileSession
	unit                           uint64
	identity                       storage.FileSessionIdentityResult
	checkErr, callErr, identityErr error
	calls, identityCalls           int
}

func (p *allocationSessionProbe) CheckFileSessionIdentity() error { return p.checkErr }
func (p *allocationSessionProbe) CheckAllocationReporting() error { return p.checkErr }
func (p *allocationSessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.identityCalls++
	return p.identity, p.identityErr
}
func (p *allocationSessionProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	p.calls++
	return p.unit, p.callErr
}

func TestAllocationUnitUsesExactAuthoritativeSession(t *testing.T) {
	probe := &allocationSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
	if err := wrapper.CheckAllocationReporting(); err != nil {
		t.Fatal(err)
	}
	got, err := wrapper.AllocationUnitBytes(t.Context())
	if err != nil || got != probe.unit || probe.calls != 1 || probe.identityCalls != 1 {
		t.Fatalf("allocation unit=%d %v, calls=%d identityCalls=%d", got, err, probe.calls, probe.identityCalls)
	}
}

func TestAllocationUnitRejectsUnknownFacts(t *testing.T) {
	failure := errors.New("authority unavailable")
	for _, scenario := range []string{"check", "capture", "identity", "malformed", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &allocationSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
			wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
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
