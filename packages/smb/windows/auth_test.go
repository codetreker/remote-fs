package windows

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
)

func TestSIDPolicyUsesVerifiedIdentityAndPreservesCancellation(t *testing.T) {
	identity := smb.Principal{SID: "S-1-5-21-10-20-30-1001", LogonSession: "0123456789abcdef", Name: "original"}
	policy, err := AllowIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"verified with another display name", smb.WithPrincipal(t.Context(), smb.Principal{SID: identity.SID, LogonSession: identity.LogonSession, Name: "renamed"}), nil},
		{"missing identity", t.Context(), authz.ErrDenied},
		{"same user from another logon", smb.WithPrincipal(t.Context(), smb.Principal{SID: identity.SID, LogonSession: "fedcba9876543210", Name: identity.Name}), authz.ErrDenied},
		{"different user", smb.WithPrincipal(t.Context(), smb.Principal{SID: "S-1-5-21-10-20-30-1002", LogonSession: identity.LogonSession, Name: identity.Name}), authz.ErrDenied},
		{"name is not identity", smb.WithPrincipal(t.Context(), smb.Principal{Name: identity.SID}), authz.ErrDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := policy.Authorize(test.ctx, authz.AccessRequest{}); !errors.Is(err, test.want) {
				t.Fatalf("Authorize = %v, want %v", err, test.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(smb.WithPrincipal(t.Context(), identity))
	cancel()
	if err := policy.Authorize(ctx, authz.AccessRequest{}); err != context.Canceled {
		t.Fatalf("canceled authorization = %v", err)
	}
}

func TestSIDPolicyRejectsNoncanonicalIdentities(t *testing.T) {
	for _, sid := range []string{"", "user", "s-1-5-21", "S-2-5-21", "S-1-05-21", "S-1-5-021", "S-1-5-21 ", "S-1-5-4294967296", "S-1-281474976710656-1", "S-1-5-1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16"} {
		if policy, err := AllowIdentity(smb.Principal{SID: sid, LogonSession: "0123456789abcdef"}); policy != nil || !errors.Is(err, ErrInvalidSID) {
			t.Errorf("AllowIdentity(%q) = %v, %v", sid, policy, err)
		}
	}
}

func TestIdentityPolicyRejectsNoncanonicalLogonSessions(t *testing.T) {
	for _, session := range []smb.LogonSessionID{"", "0", "0123456789abcde", "0123456789abcdef0", "0123456789abcdeF", "0123456789abcdeg", " 123456789abcdef"} {
		identity := smb.Principal{SID: "S-1-5-21-10-20-30-1001", LogonSession: session}
		if policy, err := AllowIdentity(identity); policy != nil || !errors.Is(err, ErrInvalidLogonSession) {
			t.Errorf("AllowIdentity(%q) = %v, %v", session, policy, err)
		}
	}
}

func TestIdentityPolicyRejectsAnonymousGuestAndServiceAccounts(t *testing.T) {
	for _, sid := range []string{"S-1-5-7", "S-1-5-18", "S-1-5-19", "S-1-5-20", "S-1-5-21-10-20-30-501"} {
		identity := smb.Principal{SID: sid, LogonSession: "0123456789abcdef"}
		if policy, err := AllowIdentity(identity); policy != nil || !errors.Is(err, ErrIdentityNotAllowed) {
			t.Errorf("AllowIdentity(%q) = %v, %v", sid, policy, err)
		}
	}
}

func TestCanceledAuthenticationDoesNotAcquirePlatformResources(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if exchange, err := NewAuthenticator().Begin(ctx); exchange != nil || err != context.Canceled {
		t.Fatalf("Begin = %v, %v", exchange, err)
	}
}

func TestSecurityStatusKeepsItsNativeCode(t *testing.T) {
	err := &SecurityError{Operation: "AcceptSecurityContext", Status: 0x8009030c}
	if message := err.Error(); !strings.Contains(message, "AcceptSecurityContext") || !strings.Contains(message, "0x8009030c") {
		t.Fatalf("native security diagnostic = %q", message)
	}
}
