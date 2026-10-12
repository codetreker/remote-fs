package objectstore

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type creationNativeProbe struct {
	*identityCapabilityProbe
	created time.Time
	err     error
	calls   int
	hook    func()
}

func (p *creationNativeProbe) VolumeCreatedAt(context.Context) (time.Time, error) {
	p.calls++
	if p.hook != nil {
		p.hook()
	}
	return p.created, p.err
}

func TestObjectVolumeCreatedAtUsesItsNativeSessionGuards(t *testing.T) {
	for _, scenario := range []string{"valid", "absent", "zero", "source", "backend", "retired", "publication", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			session := actionTestSession(t, 10)
			session.cleanup = context.Background()
			session.files = make(map[retainedReference]struct{})
			session.options.MaxOperations = 1
			backend := storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
			identity := &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}, result: backend}
			probe := &creationNativeProbe{identityCapabilityProbe: identity, created: time.Date(2026, 10, 12, 0, 0, 0, 1, time.UTC)}
			session.native, session.storage.meta = probe, probe
			session.storage.objects = struct{ BoundedObjects }{}
			session.backendIdentity, session.hasBackendIdentity, session.epoch = backend, true, "session"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var want error
			switch scenario {
			case "absent":
				session.native, session.storage.meta = identity, identity
				want = syscall.EOPNOTSUPP
			case "zero":
				probe.created = time.Time{}
			case "source":
				probe.err = errors.New("creation source refused")
				want = probe.err
			case "backend":
				identity.result.Volume = "different"
				want = syscall.ESTALE
			case "retired":
				session.active = false
				want = syscall.ESTALE
			case "publication":
				probe.hook = func() { session.mu.Lock(); session.active = false; session.mu.Unlock() }
				want = syscall.ESTALE
			case "cancel":
				probe.hook = cancel
				want = context.Canceled
			}
			got, err := session.VolumeCreatedAt(ctx)
			if want != nil {
				if !got.IsZero() || !errors.Is(err, want) {
					t.Fatalf("creation=%v, %v; want %v", got, err, want)
				}
			} else if err != nil || !got.Equal(probe.created) {
				t.Fatalf("creation=%v, %v", got, err)
			}
		})
	}
}

type identityAuthorityProbe struct {
	metastore.Store
	metastore.FileStore
	metastore.BoundedLister
	metastore.AtomicFileOpener
	metastore.NodeReferences
	fileErr error
}

func (p *identityAuthorityProbe) CheckFileStore() error      { return p.fileErr }
func (p *identityAuthorityProbe) CheckAtomicFileOpen() error { return nil }
func (p *identityAuthorityProbe) CheckNodeReferences() error { return nil }
func (p *identityAuthorityProbe) Usage(context.Context) (int64, error) {
	return 0, p.fileErr
}

type identityCapabilityProbe struct {
	*identityAuthorityProbe
	err    error
	getErr error
	domain *advisory.Coordinator
	result storage.BackendIdentityResult
}

func (p *identityCapabilityProbe) CheckBackendIdentity() error         { return p.err }
func (p *identityCapabilityProbe) CheckStableReferenceIdentity() error { return p.err }
func (p *identityCapabilityProbe) CheckOpenMetadataAccess() error      { return p.err }
func (p *identityCapabilityProbe) BackendIdentity(context.Context) (storage.BackendIdentityResult, error) {
	return p.result, p.getErr
}
func (p *identityCapabilityProbe) Advisory(context.Context) (*advisory.Coordinator, error) {
	return p.domain, nil
}

func TestFileIdentityPreflightRequiresCompleteNativeCapabilities(t *testing.T) {
	missing := &identityAuthorityProbe{}
	volume := &Storage{meta: missing, objects: struct{ BoundedObjects }{}}
	session := &fileSession{storage: volume, native: missing}
	for name, check := range map[string]func() error{
		"backend": volume.CheckBackendIdentity, "session": session.CheckFileSessionIdentity,
		"stable reference": session.CheckStableReferenceIdentity, "open metadata": session.CheckOpenMetadataAccess,
	} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing %s capability accepted: %v", name, err)
		}
	}
	refusal := errors.New("native identity refused")
	for _, authority := range []*identityCapabilityProbe{
		{identityAuthorityProbe: &identityAuthorityProbe{fileErr: refusal}},
		{identityAuthorityProbe: &identityAuthorityProbe{}, err: refusal},
	} {
		volume.meta, session.native = authority, authority
		for name, check := range map[string]func() error{
			"backend": volume.CheckBackendIdentity, "session": session.CheckFileSessionIdentity,
			"stable reference": session.CheckStableReferenceIdentity, "open metadata": session.CheckOpenMetadataAccess,
		} {
			if err := check(); !errors.Is(err, refusal) {
				t.Fatalf("refused %s capability accepted: %v", name, err)
			}
		}
	}
}

func TestBackendIdentityValidatesNativeGetterBeforeReturningFacts(t *testing.T) {
	native := &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}}
	volume := &Storage{meta: native, objects: struct{ BoundedObjects }{}}
	if got, err := volume.BackendIdentity(t.Context()); !errors.Is(err, syscall.EINVAL) || got != (storage.BackendIdentityResult{}) {
		t.Fatalf("malformed native identity returned: %+v, %v", got, err)
	}
	native.result = storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
	if got, err := volume.BackendIdentity(t.Context()); err != nil || got != native.result {
		t.Fatalf("valid native identity changed: %+v, %v", got, err)
	}
	native.getErr = syscall.EIO
	if got, err := volume.BackendIdentity(t.Context()); !errors.Is(err, syscall.EIO) || got != (storage.BackendIdentityResult{}) {
		t.Fatalf("failed native getter returned facts: %+v, %v", got, err)
	}
	volume.closeDone = make(chan struct{})
	native.getErr = nil
	if _, err := volume.BackendIdentity(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed storage returned identity: %v", err)
	}
}

func TestNewFileSessionRejectsUnverifiedIdentityBeforeSessionEffects(t *testing.T) {
	domain, err := advisory.New(advisory.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	native := &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}, domain: domain}
	volume := &Storage{meta: native, objects: struct{ BoundedObjects }{}}
	for _, refusal := range []error{syscall.EINVAL, syscall.EIO, syscall.EOPNOTSUPP} {
		native.err, native.getErr = nil, nil
		switch refusal {
		case syscall.EINVAL:
			native.result = storage.BackendIdentityResult{}
		case syscall.EIO:
			native.getErr = refusal
		case syscall.EOPNOTSUPP:
			native.err = refusal
		}
		if session, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, refusal) || session != nil {
			t.Fatalf("unverified identity admitted session: %v, %v", session, err)
		}
		if len(volume.fileSessions) != 0 {
			t.Fatal("rejected identity retained a session")
		}
	}
}

func TestOldIdentityOnlyAuthorityStillEnrollsFileSession(t *testing.T) {
	domain, err := advisory.New(advisory.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	native := &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}, domain: domain,
		result: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}}
	volume := &Storage{meta: native, objects: struct{ BoundedObjects }{}}
	session, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := storage.VolumeCreatedAt(t.Context(), session); !got.IsZero() || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("old authority creation fact=%v, %v", got, err)
	}
	if _, err := session.Status(t.Context()); err != nil {
		t.Fatalf("missing optional fact invalidated session: %v", err)
	}
}
