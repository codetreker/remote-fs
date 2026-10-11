package httprest_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

var _ locked.Backend = failing{}
var _ locking.Service = failing{}

func (f failing) LockService() locking.Service { return f }

func (f failing) BeginEnrollment(context.Context) (locking.EnrollmentTicket, error) {
	return "", f.err
}

func (f failing) OpenSession(context.Context, locking.EnrollmentTicket) (locking.Session, error) {
	return locking.Session{}, f.err
}

func (f failing) CreateOwner(context.Context, locking.SessionID, locking.RequestID) (locking.Owner, error) {
	return locking.Owner{}, f.err
}

func (f failing) RetireOwner(context.Context, locking.OwnerRef) error   { return f.err }
func (f failing) CloseSession(context.Context, locking.SessionID) error { return f.err }

func (f failing) Resolve(context.Context, locking.OwnerRef, string) (locking.ResourceRef, error) {
	return locking.ResourceRef{}, f.err
}

func (f failing) Acquire(context.Context, locking.AcquireRequest) (locking.ActionResult, error) {
	return locking.ActionResult{}, f.err
}

func (f failing) Renew(context.Context, locking.RenewRequest) (locking.ActionResult, error) {
	return locking.ActionResult{}, f.err
}

func (f failing) Release(context.Context, locking.OwnerRef, locking.GrantRef) (locking.ReleaseResult, error) {
	return locking.ReleaseResult{}, f.err
}

func (f failing) Cancel(context.Context, locking.OwnerRef, locking.RequestID) (locking.CancelResult, error) {
	return locking.CancelResult{}, f.err
}

func (f failing) QueryAction(context.Context, locking.OwnerRef, locking.RequestID) (locking.ActionResult, error) {
	return locking.ActionResult{}, f.err
}

func (f failing) QueryGrant(context.Context, locking.OwnerRef, locking.GrantRef) (locking.GrantStatus, error) {
	return locking.GrantStatus{}, f.err
}

func TestFailingStorageLockServiceReturnsOnlyTheInjectedFailure(t *testing.T) {
	injected := errors.New("the backing authority is unavailable")
	service := failingStorage(t, injected).LockService()
	if service == nil {
		t.Fatal("the failure fixture has no bound service")
	}
	ctx := t.Context()
	owner := locking.OwnerRef{Session: "session", Owner: "owner"}
	grant := locking.GrantRef{ID: "grant", Resource: "resource", Generation: 1}
	for _, test := range []struct {
		name string
		call func() (any, error)
	}{
		{"begin enrollment", func() (any, error) { return service.BeginEnrollment(ctx) }},
		{"open session", func() (any, error) { return service.OpenSession(ctx, "ticket") }},
		{"create owner", func() (any, error) { return service.CreateOwner(ctx, owner.Session, "create") }},
		{"retire owner", func() (any, error) { return nil, service.RetireOwner(ctx, owner) }},
		{"close session", func() (any, error) { return nil, service.CloseSession(ctx, owner.Session) }},
		{"resolve", func() (any, error) { return service.Resolve(ctx, owner, "file") }},
		{"acquire", func() (any, error) {
			return service.Acquire(ctx, locking.AcquireRequest{Owner: owner, Request: "acquire"})
		}},
		{"renew", func() (any, error) {
			return service.Renew(ctx, locking.RenewRequest{Owner: owner, Request: "renew", Grant: grant})
		}},
		{"release", func() (any, error) { return service.Release(ctx, owner, grant) }},
		{"cancel", func() (any, error) { return service.Cancel(ctx, owner, "acquire") }},
		{"query action", func() (any, error) { return service.QueryAction(ctx, owner, "acquire") }},
		{"query grant", func() (any, error) { return service.QueryGrant(ctx, owner, grant) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.call()
			if err != injected {
				t.Fatalf("error = %v, want the original injected error %v", err, injected)
			}
			if result != nil && !reflect.ValueOf(result).IsZero() {
				t.Fatalf("a failed authority operation returned a nonzero result: %+v", result)
			}
		})
	}
}
