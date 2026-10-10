package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type treeIdentityBackend struct {
	endpointStorage
	identity      storage.BackendIdentityResult
	identityCalls atomic.Int32
	raw           storage.FileSession
}

func (b *treeIdentityBackend) BackendIdentity(context.Context) (storage.BackendIdentityResult, error) {
	b.identityCalls.Add(1)
	return b.identity, nil
}

func (b *treeIdentityBackend) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	b.sessionOpens.Add(1)
	return b.raw, nil
}

type treeIdentitySession struct {
	*endpointFileSession
	identity storage.FileSessionIdentityResult
}

func (s *treeIdentitySession) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	return s.identity, nil
}

type treeIdentityMissingCapabilities struct{ storage.FileSession }

func newTreeIdentityBackend() (*treeIdentityBackend, *treeIdentitySession) {
	base := newEndpointFileSession()
	raw := &treeIdentitySession{
		endpointFileSession: base,
		identity:            storage.FileSessionIdentityResult{Backend: endpointBackendIdentity(), SessionEpoch: base.status.Epoch},
	}
	return &treeIdentityBackend{identity: endpointBackendIdentity(), raw: raw}, raw
}

func treeIdentityFixture(t *testing.T, config Config, backend *treeIdentityBackend, share Share) (*Server, *connection, *session, *Export) {
	t.Helper()
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	share.Backend = backend
	export, err := server.Publish(share)
	if err != nil {
		t.Fatal(err)
	}
	c := newConnection(server, nil)
	s := &session{id: 21, principal: testPrincipal("0123456789abcdef"), trees: make(map[uint32]*tree)}
	c.sessions[s.id] = s
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := c.closeOwnedExport(WithPrincipal(ctx, s.principal), export); err != nil {
			t.Error(err)
		}
		if err := export.Unpublish(ctx); err != nil {
			t.Error(err)
		}
		c.cancel()
	})
	return server, c, s, export
}

func treeIdentityShare() Share {
	return Share{Name: "data", Volume: "authorization-volume", BackendVolume: "test-volume", RootNodeID: 1}
}

func treeIdentityCloseCount(s *endpointFileSession) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func TestTreeBackendIdentityDeniedBeforeGetterAndEnrollment(t *testing.T) {
	backend, raw := newTreeIdentityBackend()
	config := endpointConfig()
	config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpFileBackendIdentity {
			if request.Volume != "authorization-volume" {
				t.Errorf("identity authorization volume = %q", request.Volume)
			}
			return authz.ErrDenied
		}
		return nil
	})
	_, c, s, export := treeIdentityFixture(t, config, backend, treeIdentityShare())
	header := wire.Header{}
	if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &header); status != statusDenied {
		t.Fatalf("denied identity status = %#x", status)
	}
	if backend.identityCalls.Load() != 0 || backend.sessionOpens.Load() != 0 || treeIdentityCloseCount(raw.endpointFileSession) != 0 {
		t.Fatalf("denied identity touched backend: getters=%d opens=%d closes=%d", backend.identityCalls.Load(), backend.sessionOpens.Load(), treeIdentityCloseCount(raw.endpointFileSession))
	}
	if len(s.trees) != 0 || len(s.authorities) != 0 || export.refs != 0 {
		t.Fatalf("denied identity retained ownership: trees=%d authorities=%d refs=%d", len(s.trees), len(s.authorities), export.refs)
	}
}

func TestTreeBackendIdentityMustMatchHostPinsBeforeEnrollment(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*storage.BackendIdentityResult)
	}{
		{"volume", func(identity *storage.BackendIdentityResult) { identity.Volume = "other-volume" }},
		{"root", func(identity *storage.BackendIdentityResult) { identity.RootNodeID = 2 }},
		{"invalid descriptor", func(identity *storage.BackendIdentityResult) { identity.Authority = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, raw := newTreeIdentityBackend()
			test.change(&backend.identity)
			_, c, s, export := treeIdentityFixture(t, endpointConfig(), backend, treeIdentityShare())
			if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &wire.Header{}); status != statusIO {
				t.Fatalf("pin mismatch status = %#x", status)
			}
			if backend.identityCalls.Load() != 1 || backend.sessionOpens.Load() != 0 || treeIdentityCloseCount(raw.endpointFileSession) != 0 {
				t.Fatalf("pin mismatch enrollment: getters=%d opens=%d closes=%d", backend.identityCalls.Load(), backend.sessionOpens.Load(), treeIdentityCloseCount(raw.endpointFileSession))
			}
			if len(s.trees) != 0 || len(s.authorities) != 0 || export.refs != 0 {
				t.Fatalf("pin mismatch retained ownership: trees=%d authorities=%d refs=%d", len(s.trees), len(s.authorities), export.refs)
			}
		})
	}
}

func TestTreeSessionIdentityMustMatchBackendAndStatus(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*storage.FileSessionIdentityResult)
	}{
		{"volume", func(identity *storage.FileSessionIdentityResult) { identity.Backend.Volume = "other-volume" }},
		{"authority", func(identity *storage.FileSessionIdentityResult) { identity.Backend.Authority = "other-authority" }},
		{"root", func(identity *storage.FileSessionIdentityResult) { identity.Backend.RootNodeID = 2 }},
		{"session epoch", func(identity *storage.FileSessionIdentityResult) { identity.SessionEpoch = "other-epoch" }},
		{"invalid descriptor", func(identity *storage.FileSessionIdentityResult) { identity.Backend.Authority = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, raw := newTreeIdentityBackend()
			test.change(&raw.identity)
			_, c, s, export := treeIdentityFixture(t, endpointConfig(), backend, treeIdentityShare())
			if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &wire.Header{}); status != statusIO {
				t.Fatalf("session identity mismatch status = %#x", status)
			}
			if backend.identityCalls.Load() != 1 || backend.sessionOpens.Load() != 1 || treeIdentityCloseCount(raw.endpointFileSession) != 1 {
				t.Fatalf("session mismatch cleanup: getters=%d opens=%d closes=%d", backend.identityCalls.Load(), backend.sessionOpens.Load(), treeIdentityCloseCount(raw.endpointFileSession))
			}
			if len(s.trees) != 0 || len(s.authorities) != 0 || export.refs != 0 {
				t.Fatalf("session mismatch retained ownership: trees=%d authorities=%d refs=%d", len(s.trees), len(s.authorities), export.refs)
			}
		})
	}
}

func TestTreeMissingCapabilitiesRefusesPublicationAndClosesSession(t *testing.T) {
	backend, raw := newTreeIdentityBackend()
	backend.raw = treeIdentityMissingCapabilities{FileSession: raw}
	_, c, s, export := treeIdentityFixture(t, endpointConfig(), backend, treeIdentityShare())
	if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &wire.Header{}); status != statusUnsupported {
		t.Fatalf("missing capability status = %#x", status)
	}
	if backend.sessionOpens.Load() != 1 || treeIdentityCloseCount(raw.endpointFileSession) != 1 {
		t.Fatalf("missing capability cleanup: opens=%d closes=%d", backend.sessionOpens.Load(), treeIdentityCloseCount(raw.endpointFileSession))
	}
	if len(s.trees) != 0 || len(s.authorities) != 0 || export.refs != 0 {
		t.Fatalf("missing capability retained ownership: trees=%d authorities=%d refs=%d", len(s.trees), len(s.authorities), export.refs)
	}
}

func TestTreePublishedIdentityRemainsExact(t *testing.T) {
	backend, raw := newTreeIdentityBackend()
	_, c, s, export := treeIdentityFixture(t, endpointConfig(), backend, treeIdentityShare())
	header := wire.Header{}
	if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &header); status != statusOK {
		t.Fatalf("tree connect status = %#x", status)
	}
	got := s.trees[header.TreeID]
	if got == nil || got.authority.identity != raw.identity {
		t.Fatalf("tree authority descriptor = %+v", got)
	}
}

func TestFileHandleCapacityIsSharedAcrossTreesAndReleasedByCleanup(t *testing.T) {
	for _, test := range []struct {
		name                             string
		handles, unresolved, diagnostics int
	}{
		{"handles", 1, 2, 2 * diagnosticOwnerBytes},
		{"unresolved owners", 2, 1, 2 * diagnosticOwnerBytes},
		{"diagnostics", 2, 2, diagnosticOwnerBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := endpointConfig()
			config.Limits.MaxHandles = test.handles
			config.Limits.MaxUnresolvedOwners = test.unresolved
			config.Limits.MaxDiagnosticBytes = test.diagnostics
			server, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			export := &Export{server: server}
			s := &session{id: 11}
			left, right := &tree{id: 1, export: export}, &tree{id: 2, export: export}
			if !left.beginFileWork(s) || !right.beginFileWork(s) {
				t.Fatal("tree admission failed")
			}
			defer left.endFileWork()
			defer right.endFileWork()
			first, err := left.reserveFileHandle(s, config.Limits.MaxHandles)
			if err != nil {
				t.Fatal(err)
			}
			file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}
			first.file = file
			if second, err := right.reserveFileHandle(s, config.Limits.MaxHandles); second != nil || !errors.Is(err, syscall.EMFILE) {
				t.Fatalf("cross-tree capacity admission: handle=%v err=%v", second, err)
			}
			if len(right.handles) != 0 || len(server.HandleOwners()) != 1 || file.closes.Load() != 0 {
				t.Fatalf("failed reservation changed ownership: right=%d global=%d closes=%d", len(right.handles), len(server.HandleOwners()), file.closes.Load())
			}
			if err := left.closeFileHandle(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if len(left.handles) != 0 || len(server.HandleOwners()) != 0 || file.closes.Load() != 1 {
				t.Fatalf("cleanup retained capacity: left=%d global=%d closes=%d", len(left.handles), len(server.HandleOwners()), file.closes.Load())
			}
			second, err := right.reserveFileHandle(s, config.Limits.MaxHandles)
			if err != nil {
				t.Fatalf("capacity did not recover: %v", err)
			}
			if second.id == first.id {
				t.Fatal("released FileId reused across trees")
			}
			right.releaseFileHandle(second)
			if len(server.HandleOwners()) != 0 {
				t.Fatal("final release retained diagnostic owner")
			}
		})
	}
}

func TestFileIdAllocatorAvoidsRelatedPlaceholdersAndExhaustion(t *testing.T) {
	s := &session{id: 17}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("tree admission failed")
	}
	defer tree.endFileWork()
	first, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	incarnation := binary.LittleEndian.Uint64(first.id[:8])
	sequence := binary.LittleEndian.Uint64(first.id[8:])
	if incarnation == 0 || incarnation == math.MaxUint64 || sequence == 0 || sequence == math.MaxUint64 || first.id.IsRelatedPlaceholder() || first.id.HasPartialRelatedPlaceholder() {
		t.Fatalf("allocated reserved FileId: %x", first.id)
	}
	tree.releaseFileHandle(first)
	s.nextFileID = math.MaxUint64 - 2
	last, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(last.id[8:]) != math.MaxUint64-1 || binary.LittleEndian.Uint64(last.id[:8]) != incarnation {
		t.Fatalf("last valid FileId = %x", last.id)
	}
	tree.releaseFileHandle(last)
	if handle, err := tree.reserveFileHandle(s, 1); handle != nil || !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("exhausted allocator: handle=%v err=%v", handle, err)
	}
	if s.nextFileID != math.MaxUint64-1 || len(tree.handles) != 0 {
		t.Fatalf("exhaustion changed ownership: sequence=%d handles=%d", s.nextFileID, len(tree.handles))
	}
}
