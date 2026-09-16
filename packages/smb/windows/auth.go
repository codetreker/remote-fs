// Package windows supplies SSPI authentication and Windows identity policies
// for local SMB endpoints. Importing this package and constructing an
// Authenticator do not acquire credentials or change system settings.
package windows

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
)

var (
	ErrUnsupported          = errors.New("Windows 11 version 24H2 or later is required")
	ErrAuthenticationClosed = errors.New("the Windows authentication exchange is closed")
	ErrInvalidSID           = errors.New("a canonical Windows user SID is required")
)

// Authenticator uses inbound Negotiate credentials from the Windows identity of
// the hosting process. Each Begin acquires independent credentials, so new
// exchanges use the host's current security configuration. No password is retained.
// Native SSPI calls are synchronous. Cancellation rejects completed native work
// before returning an identity; Close waits for an active native call to finish.
type Authenticator struct{}

// NewAuthenticator constructs a provider without acquiring a security context.
func NewAuthenticator() *Authenticator { return &Authenticator{} }

func (*Authenticator) Begin(ctx context.Context) (smb.Authentication, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return beginAuthentication(ctx)
}

// CurrentUserSID returns the current process user's SID. It does not impersonate
// another identity or include the linked elevated/non-elevated login session.
func CurrentUserSID() (string, error) { return currentUserSID() }

// AllowSID accepts only requests carrying this exact SSPI-verified identity.
// SMB places that identity in context; request names and tokens are not consulted.
func AllowSID(sid string) (authz.Authorizer, error) {
	if !canonicalSID(sid) {
		return nil, ErrInvalidSID
	}
	return authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		principal, ok := smb.PrincipalFromContext(ctx)
		if !ok || principal.SID != sid {
			return authz.ErrDenied
		}
		return nil
	}), nil
}

func canonicalSID(sid string) bool {
	parts := strings.Split(sid, "-")
	if len(parts) < 4 || len(parts) > 18 || parts[0] != "S" || parts[1] != "1" {
		return false
	}
	for i, part := range parts[2:] {
		bits := 32
		if i == 0 {
			bits = 48
		}
		n, err := strconv.ParseUint(part, 10, bits)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return false
		}
	}
	return true
}

// SecurityError preserves a native SECURITY_STATUS without formatting credentials
// or authentication tokens into diagnostics.
type SecurityError struct {
	Operation string
	Status    uint32
}

func (e *SecurityError) Error() string {
	return fmt.Sprintf("Windows %s failed with security status 0x%08x", e.Operation, e.Status)
}
