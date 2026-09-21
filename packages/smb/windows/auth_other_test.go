//go:build !windows

package windows

import (
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb"
)

func TestAuthenticationRefusesNonWindowsHosts(t *testing.T) {
	if exchange, err := NewAuthenticator().Begin(t.Context()); exchange != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Begin = %v, %v", exchange, err)
	}
	if identity, err := CurrentIdentity(); identity != (smb.Principal{}) || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CurrentIdentity = %+v, %v", identity, err)
	}
}
