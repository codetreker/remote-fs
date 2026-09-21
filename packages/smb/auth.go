// Package smb defines authentication contracts for embedded SMB endpoints.
package smb

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// LogonSessionID identifies one operating-system logon session. Windows
// providers encode TOKEN_STATISTICS.AuthenticationId as 16 lowercase hex
// digits: the unsigned high uint32 followed by the low uint32.
type LogonSessionID string

func (id LogonSessionID) Valid() bool {
	if len(id) != 16 {
		return false
	}
	for _, character := range id {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

// Principal is the identity verified by an authentication provider. SID and
// LogonSession form the authorization identity; Name is diagnostic only.
type Principal struct {
	SID          string
	LogonSession LogonSessionID
	Name         string
}

func (p Principal) Valid() bool {
	return canonicalSID(p.SID) && p.LogonSession.Valid()
}

func (p Principal) SameIdentity(other Principal) bool {
	return p.Valid() && other.Valid() && p.SID == other.SID && p.LogonSession == other.LogonSession
}

func canonicalSID(sid string) bool {
	parts := strings.Split(sid, "-")
	if len(parts) < 4 || len(parts) > 18 || parts[0] != "S" || parts[1] != "1" {
		return false
	}
	for index, part := range parts[2:] {
		bits := 32
		if index == 0 {
			bits = 48
		}
		value, err := strconv.ParseUint(part, 10, bits)
		if err != nil || strconv.FormatUint(value, 10) != part {
			return false
		}
	}
	return true
}

// Authenticator starts an independent SPNEGO exchange. Implementations must
// be safe for concurrent calls and use current credentials for each exchange.
type Authenticator interface {
	Begin(context.Context) (Authentication, error)
}

// Authentication owns its native credential and security context until Close.
// Step returns owned token and key buffers. Callers clear the session key after
// deriving protocol keys. Close must serialize with Step and be retryable when
// native cleanup fails.
type Authentication interface {
	Step(context.Context, []byte) (AuthenticationResult, error)
	Close() error
}

type AuthenticationResult struct {
	Token      []byte
	Continue   bool
	Principal  Principal
	SessionKey []byte
	// ExpiresAt is the final native security-context expiry. It is zero only
	// when the provider explicitly has no expiry; incomplete results leave it zero.
	ExpiresAt time.Time
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok
}
