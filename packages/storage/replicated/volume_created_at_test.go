package replicated

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type volumeCreatedSessionProbe struct {
	*identitySessionProbe
	created   time.Time
	err       error
	calls     int
	onCreated func(context.Context)
}

func (p *volumeCreatedSessionProbe) VolumeCreatedAt(ctx context.Context) (time.Time, error) {
	p.calls++
	if p.onCreated != nil {
		p.onCreated(ctx)
	}
	return p.created, p.err
}

func TestVolumeCreatedAtKeepsExistingIdentityPreflightOptional(t *testing.T) {
	identity := storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 7}, SessionEpoch: "session-epoch"}
	probe := &identitySessionProbe{result: identity}
	wrapper := retainedTestSession(t, probe)
	if err := wrapper.CheckFileSessionIdentity(); err != nil {
		t.Fatal(err)
	}
	if got, err := wrapper.FileSessionIdentity(t.Context()); got != identity || err != nil {
		t.Fatalf("identity=%+v,%v", got, err)
	}
	if got, err := wrapper.VolumeCreatedAt(t.Context()); !got.IsZero() || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("optional creation time=%v,%v", got, err)
	}
	if probe.calls != 1 {
		t.Fatalf("creation lookup repeated identity: calls=%d", probe.calls)
	}
}

func TestVolumeCreatedAtUsesExactSessionAndCanonicalTime(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("source", 3600))
	probe := &volumeCreatedSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: created}
	wrapper := retainedTestSession(t, probe)
	got, err := wrapper.VolumeCreatedAt(t.Context())
	if err != nil || got != created.UTC() || got.Location() != time.UTC || probe.calls != 1 || probe.identitySessionProbe.calls != 0 {
		t.Fatalf("creation time=%v,%v, creation calls=%d identity calls=%d", got, err, probe.calls, probe.identitySessionProbe.calls)
	}
}

func TestVolumeCreatedAtPreservesKnownYearOne(t *testing.T) {
	probe := &volumeCreatedSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: time.Time{}}
	wrapper := retainedTestSession(t, probe)
	got, err := wrapper.VolumeCreatedAt(t.Context())
	if err != nil || got != (time.Time{}) || got.Location() != time.UTC || probe.calls != 1 {
		t.Fatalf("known year one=%v,%v, calls=%d", got, err, probe.calls)
	}
}

func TestVolumeCreatedAtPreservesOriginalErrors(t *testing.T) {
	failure := errors.New("creation time unavailable")
	for _, scenario := range []string{"error", "unsupported"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &volumeCreatedSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: time.Unix(1, 0)}
			want := error(failure)
			switch scenario {
			case "error":
				probe.err = failure
			case "unsupported":
				probe.err = syscall.EOPNOTSUPP
				want = syscall.EOPNOTSUPP
			}
			wrapper := retainedTestSession(t, probe)
			if got, err := wrapper.VolumeCreatedAt(t.Context()); !got.IsZero() || err != want || probe.calls != 1 {
				t.Fatalf("creation time=%v,%v, want original %v, calls=%d", got, err, want, probe.calls)
			}
		})
	}
}

func TestVolumeCreatedAtRejectsCanceledOrRetiredSession(t *testing.T) {
	for _, scenario := range []string{"canceled before", "closing before", "failed before", "canceled capture", "closing capture", "failed capture", "closed replica capture"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &volumeCreatedSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: time.Unix(1, 0)}
			wrapper := retainedTestSession(t, probe)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := error(context.Canceled)
			expectedCalls := 1
			switch scenario {
			case "canceled before":
				cancel()
				expectedCalls = 0
			case "closing before":
				wrapper.closing = true
				want = syscall.ESTALE
				expectedCalls = 0
			case "failed before":
				wrapper.base.failure = errors.New("replica failed")
				want = syscall.EIO
				expectedCalls = 0
			case "canceled capture":
				probe.onCreated = func(context.Context) { cancel() }
			case "closing capture":
				probe.onCreated = func(ctx context.Context) {
					wrapper.mu.Lock()
					wrapper.closing = true
					wrapper.mu.Unlock()
					wrapper.stop()
					<-ctx.Done()
				}
			case "failed capture":
				probe.onCreated = func(context.Context) {
					wrapper.base.mu.Lock()
					wrapper.base.failure = errors.New("replica failed")
					wrapper.base.mu.Unlock()
				}
				want = syscall.EIO
			case "closed replica capture":
				probe.onCreated = func(context.Context) {
					wrapper.base.mu.Lock()
					wrapper.base.closing = true
					wrapper.base.mu.Unlock()
				}
				want = syscall.EIO
			}
			got, err := wrapper.VolumeCreatedAt(ctx)
			if !got.IsZero() || !errors.Is(err, want) {
				t.Fatalf("creation time=%v,%v, want %v", got, err, want)
			}
			if probe.calls != expectedCalls {
				t.Fatalf("calls=%d, want %d", probe.calls, expectedCalls)
			}
		})
	}
}
