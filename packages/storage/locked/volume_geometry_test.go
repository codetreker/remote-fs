package locked

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"testing"
)

type geometrySessionProbe struct {
	storage.FileSession
	geometry                       storage.VolumeGeometry
	identity                       storage.FileSessionIdentityResult
	checkErr, callErr, identityErr error
	calls, identityCalls           int
	changeIdentity                 bool
}

func (p *geometrySessionProbe) CheckFileSessionIdentity() error { return p.checkErr }
func (p *geometrySessionProbe) CheckVolumeGeometry() error      { return p.checkErr }
func (p *geometrySessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.identityCalls++
	result := p.identity
	if p.changeIdentity && p.identityCalls > 1 {
		result.Backend.Authority = "changed"
	}
	return result, p.identityErr
}
func (p *geometrySessionProbe) VolumeGeometry(context.Context) (storage.VolumeGeometry, error) {
	p.calls++
	return p.geometry, p.callErr
}

func TestVolumeGeometryUsesExactAuthoritativeSession(t *testing.T) {
	probe := &geometrySessionProbe{geometry: storage.VolumeGeometry{Volume: "volume", BytesPerSector: 512, SectorsPerAllocationUnit: 8}, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
	if err := wrapper.CheckVolumeGeometry(); err != nil {
		t.Fatal(err)
	}
	got, err := wrapper.VolumeGeometry(t.Context())
	if err != nil || got != probe.geometry || probe.calls != 1 || probe.identityCalls != 2 {
		t.Fatalf("geometry=%+v %v, calls=%d identityCalls=%d", got, err, probe.calls, probe.identityCalls)
	}
}

func TestVolumeGeometryRejectsUnknownAndSubstitutedFacts(t *testing.T) {
	failure := errors.New("authority unavailable")
	for _, scenario := range []string{"check", "capture", "identity", "volume", "malformed", "replacement", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &geometrySessionProbe{geometry: storage.VolumeGeometry{Volume: "volume", BytesPerSector: 512, SectorsPerAllocationUnit: 8}, identity: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}}
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
			case "volume":
				probe.geometry.Volume = "other"
				want = syscall.EIO
			case "malformed":
				probe.geometry.BytesPerSector = 0
				want = syscall.EINVAL
			case "replacement":
				probe.changeIdentity = true
				want = syscall.ESTALE
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			got, err := wrapper.VolumeGeometry(ctx)
			if got != (storage.VolumeGeometry{}) || !errors.Is(err, want) {
				t.Fatalf("geometry=%+v %v want %v", got, err, want)
			}
		})
	}
}
