//go:build !windows

package windows

import (
	"errors"
	"testing"
)

func TestAuthenticationRefusesNonWindowsHosts(t *testing.T) {
	if exchange, err := NewAuthenticator().Begin(t.Context()); exchange != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Begin = %v, %v", exchange, err)
	}
	if sid, err := CurrentUserSID(); sid != "" || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CurrentUserSID = %q, %v", sid, err)
	}
}
