package locked_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestNewRequiresAnEnforcingBackend(t *testing.T) {
	var typedNil *missingAuthority
	var typedNilService *locking.Authority
	for name, backend := range map[string]locked.Backend{
		"nil backend":         nil,
		"typed nil backend":   typedNil,
		"missing authority":   &missingAuthority{},
		"typed nil authority": &missingAuthority{service: typedNilService},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := locked.New(backend); err == nil {
				t.Fatal("constructed a facade without a usable bound authority")
			}
		})
	}
	cause := errors.New("bounded validation failed")
	if _, err := locked.New(&missingAuthority{boundsError: cause}); !errors.Is(err, cause) {
		t.Fatalf("bounded validation returned %v, want its original cause", err)
	}
}

type missingAuthority struct {
	storage.BoundedStorage
	service     locking.Service
	boundsError error
}

func (s *missingAuthority) CheckBounded() error          { return s.boundsError }
func (s *missingAuthority) LockService() locking.Service { return s.service }

func TestScopedViewsFreezeProofsAndPreserveAnonymousEnforcement(t *testing.T) {
	backend := pairedBackend(t)
	ctx := t.Context()
	if err := backend.Write(ctx, "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	if facade.LockService() != backend.LockService() {
		t.Fatal("facade replaced its backend authority")
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
	proof := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
	view, err := facade.WithScope(proof)
	if err != nil {
		t.Fatal(err)
	}
	proof.Grants[0].Generation++
	proof.Owner = locking.OwnerRef{}
	if err := view.Write(ctx, "file", []byte("after")); err != nil {
		t.Fatalf("changing the caller's scope changed the scoped view: %v", err)
	}
	if err := facade.Write(ctx, "file", []byte("anonymous")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("anonymous write bypassed the scoped X grant: %v", err)
	}
	if _, err := facade.LockService().Release(ctx, owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := view.Write(ctx, "file", []byte("stale")); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released scoped write returned %v, want ESTALE", err)
	}
	content, err := view.Read(ctx, "file")
	if err != nil || string(content) != "after" {
		t.Fatalf("snapshot read through a released scope = %q, %v", content, err)
	}
	if err := facade.Write(ctx, "file", []byte("unlocked")); err != nil {
		t.Fatalf("released view altered the anonymous facade: %v", err)
	}
}

func TestScopedSharedGrantCannotAuthorizeMutation(t *testing.T) {
	backend := pairedBackend(t)
	ctx := t.Context()
	if err := backend.Write(ctx, "file", []byte("stable")); err != nil {
		t.Fatal(err)
	}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Shared)
	view, err := facade.Scope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Remove(ctx, "file"); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("shared grant authorized removal: %v", err)
	}
	content, err := view.ReadBounded(ctx, "file", 100)
	if err != nil || string(content) != "stable" {
		t.Fatalf("shared snapshot read = %q, %v", content, err)
	}
}

func TestScopeValidationRejectsMalformedProofSets(t *testing.T) {
	facade, err := locked.New(pairedBackend(t))
	if err != nil {
		t.Fatal(err)
	}
	owner := locking.OwnerRef{Session: "session", Owner: "owner"}
	grant := locking.GrantRef{ID: "grant", Resource: "resource", Generation: 1}
	for name, scope := range map[string]locking.MutationScope{
		"incomplete owner":    {Owner: locking.OwnerRef{Session: "session"}},
		"proof without owner": {Grants: []locking.GrantRef{grant}},
		"duplicate proof":     {Owner: owner, Grants: []locking.GrantRef{grant, grant}},
		"empty proof":         {Owner: owner, Grants: []locking.GrantRef{{}}},
		"proof capacity":      {Owner: owner, Grants: make([]locking.GrantRef, locking.DefaultOptions().MaxProofs+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := facade.WithScope(scope); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("malformed scope returned %v, want EINVAL", err)
			}
		})
	}
}

func TestFacadeScopesOnlyMutationsAndPreservesErrors(t *testing.T) {
	cause := errors.New("injected backend failure")
	probe := &scopeProbe{Backend: pairedBackend(t), err: cause}
	facade, err := locked.New(probe)
	if err != nil {
		t.Fatal(err)
	}
	scope := locking.MutationScope{
		Owner:  locking.OwnerRef{Session: "session", Owner: "owner"},
		Grants: []locking.GrantRef{{ID: "grant", Resource: "resource", Generation: 1}},
	}
	view, err := facade.WithScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	ctx := locking.WithScope(t.Context(), scope)
	mode := fs.FileMode(0o600)
	for _, operation := range []struct {
		name     string
		mutation bool
		run      func() error
	}{
		{"write", true, func() error { return view.Write(ctx, "file", nil) }},
		{"create", true, func() error { return view.Create(ctx, "file") }},
		{"mkdir", true, func() error { return view.Mkdir(ctx, "dir") }},
		{"remove", true, func() error { return view.Remove(ctx, "file") }},
		{"remove directory", true, func() error { return view.RemoveDir(ctx, "dir") }},
		{"rename", true, func() error { return view.Rename(ctx, "from", "to") }},
		{"attributes", true, func() error { return view.SetAttr(ctx, "file", storage.AttrChange{Mode: &mode}) }},
		{"stat", false, func() error { _, err := view.Stat(ctx, "file"); return err }},
		{"read", false, func() error { _, err := view.Read(ctx, "file"); return err }},
		{"bounded read", false, func() error { _, err := view.ReadBounded(ctx, "file", 1); return err }},
		{"list", false, func() error { _, err := view.List(ctx, "dir"); return err }},
		{"bounded list", false, func() error { return view.ListBounded(ctx, "dir", nil) }},
		{"space", false, func() error { _, err := view.Space(ctx); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, cause) {
				t.Fatalf("backend failure returned %v, want original cause", err)
			}
			want := locking.MutationScope{}
			if operation.mutation {
				want = scope
			}
			if !reflect.DeepEqual(probe.observed, want) {
				t.Fatalf("backend received scope %+v, want %+v", probe.observed, want)
			}
		})
	}
}

// The probe injects one failure before the native call so each facade entrypoint can
// be checked without coupling its assertions to unrelated filesystem outcomes.
type scopeProbe struct {
	locked.Backend
	err      error
	observed locking.MutationScope
}

func (s *scopeProbe) record(ctx context.Context) error {
	s.observed = locking.ScopeFromContext(ctx)
	return s.err
}
func (s *scopeProbe) Write(ctx context.Context, _ string, _ []byte) error { return s.record(ctx) }
func (s *scopeProbe) SetAttr(ctx context.Context, _ string, _ storage.AttrChange) error {
	return s.record(ctx)
}
func (s *scopeProbe) Create(ctx context.Context, _ string) error    { return s.record(ctx) }
func (s *scopeProbe) Mkdir(ctx context.Context, _ string) error     { return s.record(ctx) }
func (s *scopeProbe) Remove(ctx context.Context, _ string) error    { return s.record(ctx) }
func (s *scopeProbe) RemoveDir(ctx context.Context, _ string) error { return s.record(ctx) }
func (s *scopeProbe) Rename(ctx context.Context, _, _ string) error { return s.record(ctx) }
func (s *scopeProbe) Stat(ctx context.Context, _ string) (storage.Attr, error) {
	return storage.Attr{}, s.record(ctx)
}
func (s *scopeProbe) Read(ctx context.Context, _ string) ([]byte, error) { return nil, s.record(ctx) }
func (s *scopeProbe) ReadBounded(ctx context.Context, _ string, _ int64) ([]byte, error) {
	return nil, s.record(ctx)
}
func (s *scopeProbe) List(ctx context.Context, _ string) ([]storage.Entry, error) {
	return nil, s.record(ctx)
}
func (s *scopeProbe) ListBounded(ctx context.Context, _ string, _ *storage.ListResult) error {
	return s.record(ctx)
}
func (s *scopeProbe) Space(ctx context.Context) (storage.Space, error) {
	return storage.Space{}, s.record(ctx)
}

func acquire(t *testing.T, service locking.Service, path string, mode locking.Mode) (locking.OwnerRef, locking.GrantRef) {
	t.Helper()
	ctx := t.Context()
	ticket, err := service.BeginEnrollment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(ctx, ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(ctx, session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(ctx, owner.Ref, path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Acquire(ctx, locking.AcquireRequest{
		Owner: owner.Ref, Request: "acquire", Resource: resource, Mode: mode, TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Outcome != locking.Granted || result.Grant == nil {
		t.Fatalf("acquisition returned %+v", result)
	}
	return owner.Ref, result.Grant.Ref
}

func pairedBackend(t *testing.T) *localdir.Storage {
	t.Helper()
	base := t.TempDir()
	config := localdir.Config{
		Root:      filepath.Join(base, "root"),
		StateRoot: filepath.Join(base, "state"),
		Locks:     locking.DefaultOptions(),
		Limits:    localdir.DefaultLimits(),
	}
	for _, dir := range []string{config.Root, config.StateRoot} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := localdir.Init(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	backend, err := localdir.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close directory: %v", err)
		}
	})
	return backend
}

func TestExplicitAnonymousContextClearsAnInheritedScope(t *testing.T) {
	backend := pairedBackend(t)
	ctx := t.Context()
	if err := backend.Write(ctx, "file", []byte("protected")); err != nil {
		t.Fatal(err)
	}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
	inner, err := facade.WithScope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := locked.New(inner)
	if err != nil {
		t.Fatal(err)
	}
	anonymous := locking.WithScope(ctx, locking.MutationScope{})
	if err := outer.Write(anonymous, "file", []byte("inherited")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("explicit anonymous mutation inherited backend proofs: %v", err)
	}
	content, err := outer.Read(anonymous, "file")
	if err != nil || string(content) != "protected" {
		t.Fatalf("anonymous rejection changed content: %q, %v", content, err)
	}
}
