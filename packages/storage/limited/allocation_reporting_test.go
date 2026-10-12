package limited

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"testing"
)

type allocationSessionProbe struct {
	storage.FileSession
	calls int
}

func (*allocationSessionProbe) CheckAllocationReporting() error { return nil }
func (p *allocationSessionProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	p.calls++
	return 4096, nil
}

func TestAllocationUnitRefusesMaskedAllocationReporting(t *testing.T) {
	probe := &allocationSessionProbe{}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{limit: MinLimit}}
	if err := wrapper.CheckAllocationReporting(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	if got, err := wrapper.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("allocation unit=%d %v", got, err)
	}
	if probe.calls != 0 {
		t.Fatal("allowance bypassed allocation masking")
	}
}

func TestAllocationUnitPreservesAllowanceFailureAndCancellation(t *testing.T) {
	failure := errors.New("accounting uncertain")
	for _, faulted := range []bool{false, true} {
		probe := &allocationSessionProbe{}
		allowance := &Storage{limit: MinLimit}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		want := error(context.Canceled)
		if faulted {
			allowance.fault = failure
			want = failure
		}
		wrapper := &fileSession{FileSession: probe, storage: allowance}
		if got, err := wrapper.AllocationUnitBytes(ctx); got != 0 || !errors.Is(err, want) {
			t.Fatalf("allocation unit=%d %v want %v", got, err, want)
		}
		if probe.calls != 0 {
			t.Fatal("failed allowance dispatched unit query")
		}
	}
}
