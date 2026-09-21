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
	ErrInvalidLogonSession  = errors.New("a canonical Windows logon session is required")
	ErrIdentityNotAllowed   = errors.New("the Windows identity is not eligible for local SMB access")
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

// CurrentIdentity returns the process token's user and logon-session identity.
// It does not impersonate another identity or follow a linked token.
func CurrentIdentity() (smb.Principal, error) { return currentIdentity() }

// AllowIdentity accepts only requests carrying this exact SSPI-verified user and
// logon-session identity. The diagnostic display name is not an authority fact.
func AllowIdentity(identity smb.Principal) (authz.Authorizer, error) {
	if !canonicalSID(identity.SID) {
		return nil, ErrInvalidSID
	}
	if forbiddenAccountSID(identity.SID) {
		return nil, ErrIdentityNotAllowed
	}
	if !canonicalLogonSession(identity.LogonSession) {
		return nil, ErrInvalidLogonSession
	}
	return authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		principal, ok := smb.PrincipalFromContext(ctx)
		if !ok || principal.SID != identity.SID || principal.LogonSession != identity.LogonSession {
			return authz.ErrDenied
		}
		return nil
	}), nil
}

func forbiddenAccountSID(sid string) bool {
	if sid == "S-1-5-7" || sid == "S-1-5-18" || sid == "S-1-5-19" || sid == "S-1-5-20" {
		return true
	}
	parts := strings.Split(sid, "-")
	return len(parts) >= 6 && parts[0] == "S" && parts[1] == "1" && parts[2] == "5" && parts[3] == "21" && parts[len(parts)-1] == "501"
}

func canonicalLogonSession(id smb.LogonSessionID) bool {
	text := string(id)
	if len(text) != 16 {
		return false
	}
	n, err := strconv.ParseUint(text, 16, 64)
	return err == nil && fmt.Sprintf("%016x", n) == text
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
