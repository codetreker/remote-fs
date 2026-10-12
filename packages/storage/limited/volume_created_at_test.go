package limited

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

type volumeCreationSessionProbe struct {
	*identitySessionProbe
	created time.Time
	err     error
	calls   int
	after   func()
}

func (p *volumeCreationSessionProbe) VolumeCreatedAt(context.Context) (time.Time, error) {
	p.calls++
	if p.after != nil {
		p.after()
	}
	return p.created, p.err
}

func TestVolumeCreationIsOptionalForIdentityPreflight(t *testing.T) {
	probe := &identitySessionProbe{}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
	if err := wrapper.CheckFileSessionIdentity(); err != nil {
		t.Fatal(err)
	}
	if created, err := wrapper.VolumeCreatedAt(t.Context()); !created.IsZero() || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing creation=%v %v", created, err)
	}
}

func TestVolumeCreationFollowsSessionAndOwnsCanonicalFact(t *testing.T) {
	for _, created := range []time.Time{time.Date(2026, 10, 12, 3, 4, 5, 6, time.FixedZone("offset", 3600)), {}} {
		probe := &volumeCreationSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: created}
		inner := &fileSession{FileSession: probe, storage: &Storage{}}
		wrapper := &fileSession{FileSession: inner, storage: &Storage{}}
		got, err := wrapper.VolumeCreatedAt(t.Context())
		if err != nil || got != created.UTC() || got.Location() != time.UTC || probe.calls != 1 || probe.identitySessionProbe.calls != 0 {
			t.Fatalf("creation=%v %v sourcecalls=%d identitycalls=%d", got, err, probe.calls, probe.identitySessionProbe.calls)
		}
	}
}

func TestVolumeCreationPreservesSourceFailureAndCancellation(t *testing.T) {
	cause := errors.New("volume fact unavailable")
	probe := &volumeCreationSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: time.Now(), err: cause}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
	if got, err := wrapper.VolumeCreatedAt(t.Context()); !got.IsZero() || err != cause {
		t.Fatalf("failed creation=%v %v", got, err)
	}
	probe.err = nil
	ctx, cancel := context.WithCancel(t.Context())
	probe.after = cancel
	if got, err := wrapper.VolumeCreatedAt(ctx); !got.IsZero() || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled creation=%v %v", got, err)
	}
	if probe.calls != 2 {
		t.Fatalf("sourcecalls=%d", probe.calls)
	}
	if got, err := wrapper.VolumeCreatedAt(ctx); !got.IsZero() || !errors.Is(err, context.Canceled) || probe.calls != 2 {
		t.Fatalf("pre-canceled creation=%v %v sourcecalls=%d", got, err, probe.calls)
	}
}

func TestVolumeCreationPreservesAllowanceHealthBeforeAndAfterCapture(t *testing.T) {
	cause := errors.New("allowance accounting uncertain")
	allowance := &Storage{fault: cause}
	probe := &volumeCreationSessionProbe{identitySessionProbe: &identitySessionProbe{}, created: time.Now()}
	wrapper := &fileSession{FileSession: probe, storage: allowance}
	if got, err := wrapper.VolumeCreatedAt(t.Context()); !got.IsZero() || err != cause || probe.calls != 0 {
		t.Fatalf("unhealthy creation=%v %v sourcecalls=%d", got, err, probe.calls)
	}
	allowance.fault = nil
	probe.after = func() { allowance.countMu.Lock(); defer allowance.countMu.Unlock(); allowance.fault = cause }
	if got, err := wrapper.VolumeCreatedAt(t.Context()); !got.IsZero() || err != cause || probe.calls != 1 {
		t.Fatalf("post-capture unhealthy creation=%v %v sourcecalls=%d", got, err, probe.calls)
	}
}
