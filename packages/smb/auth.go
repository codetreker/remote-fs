// Package smb exposes an authenticated SMB 3.1.1 endpoint backed by a remote volume.
package smb

import "context"

// Principal is the identity verified by the authentication provider. SID is the
// stable identity; Name is diagnostic and is never used to grant access.
type Principal struct{ SID, Name string }

// Authenticator starts an independent SPNEGO exchange. Implementations must be
// safe for concurrent calls and use their current credentials for new exchanges.
type Authenticator interface {
	Begin(context.Context) (Authentication, error)
}

// Authentication owns its native security context until Close. Step returns
// owned token and key buffers; the server clears keys after deriving signing keys.
type Authentication interface {
	Step(context.Context, []byte) (AuthenticationResult, error)
	Close() error
}

type AuthenticationResult struct {
	Token      []byte
	Continue   bool
	Principal  Principal
	SessionKey []byte
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
