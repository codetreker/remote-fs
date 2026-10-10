package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type identitySessionProbe struct {
	httprest.FileSessionWithBarrier
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
func (p *identitySessionProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	result, _, err := p.OpenAtWithBarrier(ctx, selection, options)
	return result, err
}
func (p *identitySessionProbe) OpenAtWithBarrier(_ context.Context, _ storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, *httprest.MutationBarrier, error) {
	p.openCalls++
	p.options = options
	return p.opened, &httprest.MutationBarrier{Incarnation: "log"}, p.callErr
}

func TestSessionIdentityAndOpenPreflightFollowRemote(t *testing.T) {
	expected := storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "durable-volume", Authority: "live-authority", RootNodeID: 7}, SessionEpoch: "session-epoch"}
	probe := &identitySessionProbe{result: expected}
	wrapper := retainedTestSession(t, probe)
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
	wrapper.remote = &struct {
		httprest.FileSessionWithBarrier
	}{}
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
	httprest.FileWithBarrier
	id uint64
}

func (p *openIdentityFileProbe) ReferenceNodeID() (uint64, error) { return p.id, nil }

func TestOpenAtPreservesIndependentMetadataOptionsAndReferenceIdentity(t *testing.T) {
	probe := &identitySessionProbe{opened: storage.OpenResult{File: &openIdentityFileProbe{id: 19}, Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}, Outcome: storage.Opened}}
	wrapper := retainedTestSession(t, probe)
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
}

func TestSessionIdentityRetirementPreservesLocalAdmission(t *testing.T) {
	probe := &identitySessionProbe{result: storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 7}, SessionEpoch: "session-epoch"}}
	wrapper := retainedTestSession(t, probe)
	wrapper.mu.Lock()
	wrapper.closing = true
	wrapper.mu.Unlock()
	if result, err := wrapper.FileSessionIdentity(t.Context()); result != (storage.FileSessionIdentityResult{}) || !errors.Is(err, syscall.ESTALE) || probe.calls != 0 {
		t.Fatalf("retired identity = %+v, %v, calls=%d", result, err, probe.calls)
	}
}

func TestOpenAtRejectsUnverifiedMetadataPermissionsBeforeOpen(t *testing.T) {
	probe := &identitySessionProbe{opened: storage.OpenResult{File: &openIdentityFileProbe{id: 19}, Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}, Outcome: storage.Opened}}
	wrapper := retainedTestSession(t, &struct {
		httprest.FileSessionWithBarrier
		httprest.AtomicFileOpenerWithBarrier
	}{AtomicFileOpenerWithBarrier: probe})
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Read: true, MetadataAccess: storage.WriteMetadata}); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) || probe.openCalls != 0 {
		t.Fatalf("unsupported metadata open = %+v, %v, calls=%d", result, err, probe.openCalls)
	}
	probe.checkErr = errors.New("metadata access cannot be enforced")
	wrapper.remote = probe
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Write: true, MetadataAccess: storage.ReadMetadata}); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, probe.checkErr) || probe.openCalls != 0 {
		t.Fatalf("failed metadata preflight = %+v, %v, calls=%d", result, err, probe.openCalls)
	}
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, storage.OpenAtOptions{Read: true}); err != nil || probe.openCalls != 1 {
		t.Fatalf("zero metadata open = %v, calls=%d", err, probe.openCalls)
	}
}

type inlineCloseSessionProbe struct {
	httprest.FileSessionWithBarrier
}

func (*inlineCloseSessionProbe) CheckInlineCloseSettlement() error { return nil }

func TestReplicaRefusesInlineCloseSettlement(t *testing.T) {
	for _, remote := range []httprest.FileSessionWithBarrier{nil, &inlineCloseSessionProbe{}} {
		if err := (&fileSession{remote: remote}).CheckInlineCloseSettlement(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("replica inline settlement = %v", err)
		}
	}
}
