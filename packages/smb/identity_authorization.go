package smb

import (
	"context"
	"errors"
)

// ErrIdentityDenied marks a completed local identity-policy refusal. Other
// errors mean admission could not be decided and must fail as I/O uncertainty.
var ErrIdentityDenied = errors.New("local SMB identity denied")

// IdentityAuthorizer decides whether one verified local operating-system
// identity may establish or refresh an SMB session. It must support concurrent
// calls and honor cancellation. Identity admission is independent of the
// volume-operation policy in Config.Authorize.
type IdentityAuthorizer interface {
	AuthorizeIdentity(context.Context, Principal) error
}

type IdentityAuthorizerFunc func(context.Context, Principal) error

func (f IdentityAuthorizerFunc) AuthorizeIdentity(ctx context.Context, principal Principal) error {
	return f(ctx, principal)
}

func identityAuthorizationStatus(err error) uint32 {
	if errors.Is(err, ErrIdentityDenied) {
		return statusDenied
	}
	return statusIO
}
