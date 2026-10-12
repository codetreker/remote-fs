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
	onUnit                         func()
}

func (p *allocationUnitSessionProbe) CheckFileSessionIdentity() error { return p.checkErr }
func (p *allocationUnitSessionProbe) CheckAllocationReporting() error { return p.checkErr }
func (p *allocationUnitSessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.identityCalls++
	return p.identity, p.identityErr
}
func (p *allocationUnitSessionProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	p.calls++
	if p.onUnit != nil {
		p.onUnit()
	}
	return p.unit, p.callErr
}

func TestAllocationUnitUsesExactAuthoritativeSession(t *testing.T) {
	probe := &allocationUnitSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
	wrapper := retainedTestSession(t, probe)
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

func TestAllocationUnitUnsupportedKeepsAllocationReporting(t *testing.T) {
	probe := &allocationUnitSessionProbe{callErr: syscall.EOPNOTSUPP, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
	wrapper := retainedTestSession(t, probe)
	if err := wrapper.CheckAllocationReporting(); err != nil {
		t.Fatal(err)
	}
	if unit, err := wrapper.AllocationUnitBytes(t.Context()); unit != 0 || !errors.Is(err, syscall.EOPNOTSUPP) || probe.calls != 1 {
		t.Fatalf("optional unit=%d %v, calls=%d", unit, err, probe.calls)
	}
}

func TestAllocationUnitRejectsRetiredReplicaAndCanceledCapture(t *testing.T) {
	for _, scenario := range []string{"unhealthy before", "retired session", "closed replica during capture", "unhealthy during capture", "canceled during capture"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &allocationUnitSessionProbe{unit: 4096, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
			wrapper := retainedTestSession(t, probe)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := error(syscall.EIO)
			switch scenario {
			case "unhealthy before":
				wrapper.base.failure = errors.New("replica failed")
			case "retired session":
				wrapper.closing = true
				want = syscall.ESTALE
			case "closed replica during capture":
				probe.onUnit = func() { wrapper.base.mu.Lock(); wrapper.base.closing = true; wrapper.base.mu.Unlock() }
			case "unhealthy during capture":
				probe.onUnit = func() {
					wrapper.base.mu.Lock()
					wrapper.base.failure = errors.New("replica failed")
					wrapper.base.mu.Unlock()
				}
			case "canceled during capture":
				probe.onUnit = cancel
				want = context.Canceled
			}
			unit, err := wrapper.AllocationUnitBytes(ctx)
			if unit != 0 || !errors.Is(err, want) {
				t.Fatalf("unit=%d %v, want %v", unit, err, want)
			}
			expectedCalls := 1
			if scenario == "unhealthy before" || scenario == "retired session" {
				expectedCalls = 0
			}
			if probe.calls != expectedCalls {
				t.Fatalf("unit calls=%d, want %d", probe.calls, expectedCalls)
			}
		})
	}
}
