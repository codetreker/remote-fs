package objectstore

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

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
