package replicated

import (
	"context"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ locking.Service = (*Storage)(nil)

// LockService returns the remote authority used by this replica's mutations.
func (s *Storage) LockService() locking.Service { return s }

// Scope returns a view with an immutable mutation proof set. Reads retain the
// replica's ordinary visibility guarantees; they do not validate live grants.
// An explicit context scope overrides the view, including an anonymous empty scope.
// The view shares this Storage's subscription and never owns its Close.
func (s *Storage) Scope(scope locking.MutationScope) (storage.BoundedStorage, error) {
	if _, err := s.remote.Scope(scope); err != nil {
		return nil, err
	}
	return &scopedStorage{
		BoundedStorage: s,
		Service:        s,
		base:           s,
		scope:          locking.CloneScope(scope),
	}, nil
}

type scopedStorage struct {
	storage.BoundedStorage
	locking.Service
	base  *Storage
	scope locking.MutationScope
}

func (s *scopedStorage) LockService() locking.Service { return s.Service }

func (s *scopedStorage) mutationContext(ctx context.Context) context.Context {
	if locking.HasScope(ctx) {
		return ctx
	}
	return locking.WithScope(ctx, s.scope)
}

func (s *scopedStorage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	return s.base.SetAttr(s.mutationContext(ctx), path, change)
}

func (s *scopedStorage) Write(ctx context.Context, path string, content []byte) error {
	return s.base.Write(s.mutationContext(ctx), path, content)
}

func (s *scopedStorage) Create(ctx context.Context, path string) error {
	return s.base.Create(s.mutationContext(ctx), path)
}

func (s *scopedStorage) Mkdir(ctx context.Context, path string) error {
	return s.base.Mkdir(s.mutationContext(ctx), path)
}

func (s *scopedStorage) Remove(ctx context.Context, path string) error {
	return s.base.Remove(s.mutationContext(ctx), path)
}

func (s *scopedStorage) RemoveDir(ctx context.Context, path string) error {
	return s.base.RemoveDir(s.mutationContext(ctx), path)
}

func (s *scopedStorage) Rename(ctx context.Context, from, to string) error {
	return s.base.Rename(s.mutationContext(ctx), from, to)
}

// Lock control operations always query the authority, including while the
// metadata stream is unavailable. Replica health cannot establish lease state.
func (s *Storage) BeginEnrollment(ctx context.Context) (locking.EnrollmentTicket, error) {
	return s.remote.BeginEnrollment(ctx)
}

func (s *Storage) OpenSession(ctx context.Context, ticket locking.EnrollmentTicket) (locking.Session, error) {
	return s.remote.OpenSession(ctx, ticket)
}

func (s *Storage) CreateOwner(ctx context.Context, session locking.SessionID, request locking.RequestID) (locking.Owner, error) {
	return s.remote.CreateOwner(ctx, session, request)
}

func (s *Storage) RetireOwner(ctx context.Context, owner locking.OwnerRef) error {
	return s.remote.RetireOwner(ctx, owner)
}

func (s *Storage) CloseSession(ctx context.Context, session locking.SessionID) error {
	return s.remote.CloseSession(ctx, session)
}

func (s *Storage) Resolve(ctx context.Context, owner locking.OwnerRef, path string) (locking.ResourceRef, error) {
	return s.remote.Resolve(ctx, owner, path)
}

func (s *Storage) Acquire(ctx context.Context, request locking.AcquireRequest) (locking.ActionResult, error) {
	return s.remote.Acquire(ctx, request)
}

func (s *Storage) Renew(ctx context.Context, request locking.RenewRequest) (locking.ActionResult, error) {
	return s.remote.Renew(ctx, request)
}

func (s *Storage) Release(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.ReleaseResult, error) {
	return s.remote.Release(ctx, owner, grant)
}

func (s *Storage) Cancel(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.CancelResult, error) {
	return s.remote.Cancel(ctx, owner, request)
}

func (s *Storage) QueryAction(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.ActionResult, error) {
	return s.remote.QueryAction(ctx, owner, request)
}

func (s *Storage) QueryGrant(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.GrantStatus, error) {
	return s.remote.QueryGrant(ctx, owner, grant)
}
