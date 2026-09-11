// Package authz describes volume operations for a host-provided authorization policy.
// The host authenticates callers, stores identity in context, and supplies current
// decisions. This package defines neither identities nor path-level permissions.
package authz

import (
	"context"
	"errors"

	"github.com/codetreker/remote-fs/packages/storage"
)

// AccessRequest identifies the host-configured volume and an operation on it.
// Volume is trusted configuration, not a name selected by the remote request.
type AccessRequest struct {
	Volume    string
	Operation storage.Operation
	// Open preserves the validated storage.OpFileOpen or storage.OpFileOpenNode
	// intent. Other operations carry its zero value.
	Open storage.OpenAccess
}

// Authorizer reads the host's current policy using identity from ctx. It must
// support concurrent calls, honor cancellation, and not reenter the same handler.
// Nil allows the action; ErrDenied marks a decided refusal; other errors mean
// authorization could not be completed. Decisions are not cached by the adapter.
type Authorizer interface {
	Authorize(context.Context, AccessRequest) error
}

type AuthorizerFunc func(context.Context, AccessRequest) error

func (f AuthorizerFunc) Authorize(ctx context.Context, request AccessRequest) error {
	return f(ctx, request)
}

// ErrDenied is an explicit policy decision, including when wrapped or joined with
// another error. A policy that cannot decide must return an error without this marker.
var ErrDenied = errors.New("access denied")
