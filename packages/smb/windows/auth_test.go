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
	const sid = "S-1-5-21-10-20-30-1001"
	policy, err := AllowSID(sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"verified", smb.WithPrincipal(t.Context(), smb.Principal{SID: sid, Name: "user"}), nil},
		{"missing identity", t.Context(), authz.ErrDenied},
		{"same display name", smb.WithPrincipal(t.Context(), smb.Principal{SID: "S-1-5-21-10-20-30-1002", Name: "user"}), authz.ErrDenied},
		{"name is not SID", smb.WithPrincipal(t.Context(), smb.Principal{Name: sid}), authz.ErrDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := policy.Authorize(test.ctx, authz.AccessRequest{}); !errors.Is(err, test.want) {
				t.Fatalf("Authorize = %v, want %v", err, test.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(smb.WithPrincipal(t.Context(), smb.Principal{SID: sid}))
	cancel()
	if err := policy.Authorize(ctx, authz.AccessRequest{}); err != context.Canceled {
		t.Fatalf("canceled authorization = %v", err)
	}
}

func TestSIDPolicyRejectsNoncanonicalIdentities(t *testing.T) {
	for _, sid := range []string{"", "user", "s-1-5-21", "S-2-5-21", "S-1-05-21", "S-1-5-021", "S-1-5-21 ", "S-1-5-4294967296", "S-1-281474976710656-1", "S-1-5-1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16"} {
		if policy, err := AllowSID(sid); policy != nil || !errors.Is(err, ErrInvalidSID) {
			t.Errorf("AllowSID(%q) = %v, %v", sid, policy, err)
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
