//go:build !windows

package smb

import (
	"errors"
	"syscall"
	"testing"
)

func TestSMBNativeNameComparisonIsUnsupportedElsewhere(t *testing.T) {
	if err := nameComparisonAvailable(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("name comparison availability = %v", err)
	}
	if _, err := nativeNameCompare([]uint16{'a'}, []uint16{'A'}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("pretended Windows comparison: %v", err)
	}
}
