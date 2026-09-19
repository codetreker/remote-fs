//go:build !windows

package smb

import (
	"errors"
	"syscall"
	"testing"
)

func TestSMBNativeNameComparisonIsUnsupportedElsewhere(t *testing.T) {
	if _, err := nativeNameCompare([]uint16{'a'}, []uint16{'A'}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("pretended Windows comparison: %v", err)
	}
	backend, session := namespaceFixture()
	if _, err := resolveName(t.Context(), backend, session, "Folder", DefaultLimits(), allowNamespace); !errors.Is(err, syscall.EOPNOTSUPP) || backend.calls != 0 {
		t.Fatalf("unsupported resolver reached backend: %v calls=%d", err, backend.calls)
	}
	if _, err := resolveName(t.Context(), backend, session, "bad.", DefaultLimits(), allowNamespace); !errors.Is(err, errNameInvalid) {
		t.Fatalf("invalid name lost classification: %v", err)
	}
}
