// Package locked exposes a backend's bound file-lock authority and immutable mutation scopes.
package locked

import (
	"context"
	"errors"
	"reflect"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Backend binds its service to every mutation, including anonymous calls. Implementations
// must authorize final publication through that service; the facade cannot add this guarantee
// to an opaque Storage by checking a path before a mutation.
type Backend interface {
	storage.BoundedStorage
	LockService() locking.Service
}

// Storage owns no backend lifetime. Scoped copies share the backend and authority.
type Storage struct {
	backend Backend
	service locking.Service
	scope   *locking.MutationScope
}

var _ Backend = (*Storage)(nil)

func New(backend Backend) (*Storage, error) {
	if isNil(backend) {
		return nil, errors.New("locked: an enforcing backend is required")
	}
	if err := backend.CheckBounded(); err != nil {
		return nil, err
	}
	service := backend.LockService()
	if isNil(service) {
		return nil, errors.New("locked: the backend has no bound file-lock authority")
	}
	return &Storage{backend: backend, service: service}, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (s *Storage) LockService() locking.Service { return s.service }
func (s *Storage) CheckBounded() error          { return s.backend.CheckBounded() }

// WithScope copies the proof set without asserting current grant validity. Final publication
// validates every proof again. The returned view never closes the shared backend.
func (s *Storage) WithScope(scope locking.MutationScope) (*Storage, error) {
	if err := locking.ValidateScope(scope, locking.DefaultOptions().MaxProofs); err != nil {
		return nil, err
	}
	copied := *s
	frozen := locking.CloneScope(scope)
	copied.scope = &frozen
	return &copied, nil
}

func (s *Storage) Scope(scope locking.MutationScope) (storage.BoundedStorage, error) {
	return s.WithScope(scope)
}

func (s *Storage) mutationContext(ctx context.Context) context.Context {
	scope := locking.ScopeFromContext(ctx)
	if locking.HasScope(ctx) {
		return locking.WithScope(ctx, scope)
	}
	if s.scope != nil {
		return locking.WithScope(ctx, *s.scope)
	}
	return locking.WithScope(ctx, locking.MutationScope{})
}

func readContext(ctx context.Context) context.Context {
	return locking.WithScope(ctx, locking.MutationScope{})
}

func (s *Storage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	return s.backend.Stat(readContext(ctx), path)
}
func (s *Storage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	return s.backend.List(readContext(ctx), path)
}
func (s *Storage) ListBounded(ctx context.Context, path string, result *storage.ListResult) error {
	return s.backend.ListBounded(readContext(ctx), path, result)
}
func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	return s.backend.Read(readContext(ctx), path)
}
func (s *Storage) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.backend.ReadBounded(readContext(ctx), path, maxBytes)
}
func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	return s.backend.Space(readContext(ctx))
}
func (s *Storage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	return s.backend.SetAttr(s.mutationContext(ctx), path, change)
}
func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	return s.backend.Write(s.mutationContext(ctx), path, content)
}
func (s *Storage) Create(ctx context.Context, path string) error {
	return s.backend.Create(s.mutationContext(ctx), path)
}
func (s *Storage) Mkdir(ctx context.Context, path string) error {
	return s.backend.Mkdir(s.mutationContext(ctx), path)
}
func (s *Storage) Remove(ctx context.Context, path string) error {
	return s.backend.Remove(s.mutationContext(ctx), path)
}
func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	return s.backend.RemoveDir(s.mutationContext(ctx), path)
}
func (s *Storage) Rename(ctx context.Context, from, to string) error {
	return s.backend.Rename(s.mutationContext(ctx), from, to)
}
