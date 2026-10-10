package limited

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type identityBackendProbe struct {
	publicationBackend
	result            storage.BackendIdentityResult
	checkErr, callErr error
	calls             int
}

func (p *identityBackendProbe) CheckBackendIdentity() error { return p.checkErr }
func (p *identityBackendProbe) BackendIdentity(context.Context) (storage.BackendIdentityResult, error) {
	p.calls++
	return p.result, p.callErr
}

type identitySessionProbe struct {
	storage.FileSession
	result            storage.FileSessionIdentityResult
	checkErr, callErr error
	calls, openCalls  int
	options           storage.OpenAtOptions
	opened            storage.OpenResult
}

func (p *identitySessionProbe) CheckFileSessionIdentity() error     { return p.checkErr }
func (p *identitySessionProbe) CheckStableReferenceIdentity() error { return p.checkErr }
func (p *identitySessionProbe) CheckOpenMetadataAccess() error      { return p.checkErr }
func (p *identitySessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.calls++
	return p.result, p.callErr
}
func (p *identitySessionProbe) CheckAtomicFileOpen() error { return nil }
func (p *identitySessionProbe) OpenAt(_ context.Context, _ storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.openCalls++
	p.options = options
	return p.opened, p.callErr
}

func TestBackendIdentityFollowsCompleteWrapperChain(t *testing.T) {
	expected := storage.BackendIdentityResult{Volume: "durable-volume", Authority: "live-authority", RootNodeID: 7}
	probe := &identityBackendProbe{result: expected}
	inner := &Storage{backing: probe}
	wrapper := &Storage{backing: inner}
	if err := wrapper.CheckBackendIdentity(); err != nil {
		t.Fatal(err)
	}
	if result, err := wrapper.BackendIdentity(t.Context()); result != expected || err != nil {
		t.Fatalf("identity = %+v, %v", result, err)
	}
	cause := errors.New("authority identity unavailable")
	probe.checkErr = cause
	if err := wrapper.CheckBackendIdentity(); !errors.Is(err, cause) {
		t.Fatalf("check = %v", err)
	}
	if result, err := wrapper.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || !errors.Is(err, cause) || probe.calls != 1 {
		t.Fatalf("preflight = %+v, %v, calls=%d", result, err, probe.calls)
	}
	probe.checkErr = nil
	probe.callErr = cause
	if result, err := wrapper.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || !errors.Is(err, cause) {
		t.Fatalf("failed getter = %+v, %v", result, err)
	}
	probe.callErr = nil
	for _, malformed := range []storage.BackendIdentityResult{{}, {Volume: "volume", Authority: "bad authority", RootNodeID: 7}, {Volume: "volume", Authority: "authority"}} {
		probe.result = malformed
		if result, err := wrapper.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("malformed identity = %+v, %v", result, err)
		}
	}
	inner.backing = &struct{ publicationBackend }{}
	if err := wrapper.CheckBackendIdentity(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported check = %v", err)
	}
	if _, err := wrapper.BackendIdentity(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported getter = %v", err)
	}
}

func TestSessionIdentityAndOpenPreflightFollowCompleteWrapperChain(t *testing.T) {
	expected := storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "durable-volume", Authority: "live-authority", RootNodeID: 7}, SessionEpoch: "session-epoch"}
	probe := &identitySessionProbe{result: expected}
	inner := &fileSession{FileSession: probe, storage: &Storage{}}
	wrapper := &fileSession{FileSession: inner, storage: &Storage{}}
	checks := []func() error{wrapper.CheckFileSessionIdentity, wrapper.CheckStableReferenceIdentity, wrapper.CheckOpenMetadataAccess}
	for _, check := range checks {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := wrapper.FileSessionIdentity(t.Context()); result != expected || err != nil {
		t.Fatalf("session identity = %+v, %v", result, err)
	}
	cause := errors.New("session identity unavailable")
	probe.checkErr = cause
	for _, check := range checks {
		if err := check(); !errors.Is(err, cause) {
			t.Fatalf("failed check = %v", err)
		}
	}
	if result, err := wrapper.FileSessionIdentity(t.Context()); result != (storage.FileSessionIdentityResult{}) || !errors.Is(err, cause) || probe.calls != 1 {
		t.Fatalf("preflight = %+v, %v, calls=%d", result, err, probe.calls)
	}
	probe.checkErr = nil
	probe.callErr = cause
	if result, err := wrapper.FileSessionIdentity(t.Context()); result != (storage.FileSessionIdentityResult{}) || !errors.Is(err, cause) {
		t.Fatalf("failed getter = %+v, %v", result, err)
	}
	probe.callErr = nil
	for _, malformed := range []storage.FileSessionIdentityResult{{}, {Backend: expected.Backend}, {Backend: expected.Backend, SessionEpoch: "invalid epoch"}} {
		probe.result = malformed
		if result, err := wrapper.FileSessionIdentity(t.Context()); result != (storage.FileSessionIdentityResult{}) || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("malformed identity = %+v, %v", result, err)
		}
	}
	inner.FileSession = &struct{ storage.FileSession }{}
	for _, check := range checks {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported check = %v", err)
		}
	}
	if _, err := wrapper.FileSessionIdentity(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported getter = %v", err)
	}
}

type openIdentityFileProbe struct {
	storage.File
	id uint64
}

func (p *openIdentityFileProbe) ReferenceNodeID() (uint64, error) { return p.id, nil }

func TestOpenAtPreservesIndependentMetadataOptionsAndReferenceIdentity(t *testing.T) {
	probe := &identitySessionProbe{opened: storage.OpenResult{File: &openIdentityFileProbe{id: 19}, Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}, Outcome: storage.Opened}}
	wrapper := &fileSession{FileSession: probe, storage: &Storage{}}
	for _, options := range []storage.OpenAtOptions{
		{Read: true, Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.WriteMetadata, Existing: storage.Keep},
		{Write: true, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, MetadataAccess: storage.ReadMetadata, Existing: storage.Keep},
		{Read: true, Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep},
	} {
		result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options)
		if err != nil || !reflect.DeepEqual(probe.options, options) {
			t.Fatalf("OpenAt options = %+v, %v", probe.options, err)
		}
		if id, err := storage.ReferenceNodeID(result.File); id != result.Attr.ID || err != nil {
			t.Fatalf("reference identity = %d, %v", id, err)
		}
	}
	cause := errors.New("open returned retained cleanup ownership")
	probe.callErr = cause
	result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Read: true, MetadataAccess: storage.WriteMetadata})
	if !errors.Is(err, cause) {
		t.Fatalf("open failure = %v", err)
	}
	if id, err := storage.ReferenceNodeID(result.File); id != 19 || err != nil {
		t.Fatalf("failed open reference identity = %d, %v", id, err)
	}
}

func TestOpenAtRejectsUnverifiedMetadataPermissionsBeforeOpen(t *testing.T) {
	probe := &identitySessionProbe{}
	wrapper := &fileSession{FileSession: &struct {
		storage.FileSession
		storage.AtomicFileOpener
	}{AtomicFileOpener: probe}, storage: &Storage{}}
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Read: true, MetadataAccess: storage.WriteMetadata}); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) || probe.openCalls != 0 {
		t.Fatalf("unsupported metadata open = %+v, %v, calls=%d", result, err, probe.openCalls)
	}
	probe.checkErr = errors.New("metadata access cannot be enforced")
	wrapper.FileSession = probe
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Write: true, MetadataAccess: storage.ReadMetadata}); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, probe.checkErr) || probe.openCalls != 0 {
		t.Fatalf("failed metadata preflight = %+v, %v, calls=%d", result, err, probe.openCalls)
	}
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Read: true}); err != nil || probe.openCalls != 1 {
		t.Fatalf("zero metadata open = %v, calls=%d", err, probe.openCalls)
	}
}

func TestBackendIdentityRejectsQuotaHealthFailure(t *testing.T) {
	probe := &identityBackendProbe{result: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 7}}
	cause := errors.New("quota publication outcome is unknown")
	wrapper := &Storage{backing: probe, fault: cause}
	if err := wrapper.CheckBackendIdentity(); !errors.Is(err, cause) {
		t.Fatalf("faulted backend preflight = %v", err)
	}
	if result, err := wrapper.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || !errors.Is(err, cause) || probe.calls != 0 {
		t.Fatalf("faulted backend identity = %+v, %v, calls=%d", result, err, probe.calls)
	}
}

type inlineCloseSessionProbe struct {
	storage.FileSession
	err error
}

func (p *inlineCloseSessionProbe) CheckInlineCloseSettlement() error { return p.err }

func TestInlineCloseSettlementFollowsCompleteWrapperChain(t *testing.T) {
	probe := &inlineCloseSessionProbe{}
	inner := &fileSession{FileSession: probe}
	wrapper := &fileSession{FileSession: inner}
	if err := wrapper.CheckInlineCloseSettlement(); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("released close still owes settlement")
	probe.err = cause
	if err := wrapper.CheckInlineCloseSettlement(); !errors.Is(err, cause) {
		t.Fatalf("native settlement failure = %v", err)
	}
	inner.FileSession = &struct{ storage.FileSession }{}
	if err := wrapper.CheckInlineCloseSettlement(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing inline settlement = %v", err)
	}
}
