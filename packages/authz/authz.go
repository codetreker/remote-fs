// Package authz describes volume operations for a host-provided authorization policy.
// The host authenticates callers, stores identity in context, and supplies current
// decisions. This package defines neither identities nor path-level permissions.
package authz

import (
	"context"
	"errors"
)

// Operation identifies one semantic action independently of its transport method.
// Policies should deny operations they do not recognize.
type Operation string

// OpenAccess preserves the complete validated intent of FileOpen or FileOpenNode.
// Other operations carry its zero value. FileOpenNode never carries Create or Exclusive.
type OpenAccess struct {
	Read      bool
	Write     bool
	Create    bool
	Truncate  bool
	Exclusive bool
}

// AccessRequest identifies the host-configured volume and an operation on it.
// Volume is trusted configuration, not a name selected by the remote request.
type AccessRequest struct {
	Volume    string
	Operation Operation
	Open      OpenAccess
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
