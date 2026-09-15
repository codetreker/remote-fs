//go:build !windows

package windows

import (
	"errors"
	"testing"
)

func TestPlatformOperationsRefuseNonWindowsHosts(t *testing.T) {
	if exchange, err := NewAuthenticator().Begin(t.Context()); exchange != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Begin = %v, %v", exchange, err)
	}
	if sid, err := CurrentUserSID(); sid != "" || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CurrentUserSID = %q, %v", sid, err)
	}
	if mapping, err := Map(t.Context(), MappingOptions{LocalPath: "R:", Share: "work", TCPPort: 1445}); mapping != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Map = %v, %v", mapping, err)
	}
}
