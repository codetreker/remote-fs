package storage

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestUseScopeAndClaimsAreBoundedNeutralValues(t *testing.T) {
	for _, scope := range []UseScope{{Token: "reference"}, {Token: strings.Repeat("x", MaxScopeBytes)}} {
		if err := scope.Check(); err != nil {
			t.Fatalf("valid scope %+v: %v", scope, err)
		}
	}
	for _, scope := range []UseScope{{}, {Token: "bad\x00scope"}, {Token: strings.Repeat("x", MaxScopeBytes+1)}} {
		if !errors.Is(scope.Check(), syscall.EINVAL) {
			t.Fatalf("invalid scope accepted: %+v", scope)
		}
	}
	for _, claim := range []UseClaim{{}, {Uses: AllUses}, {Uses: ReadData | WriteData, Deny: ReadData | DeleteName}} {
		if err := claim.Check(); err != nil {
			t.Fatalf("valid claim %+v: %v", claim, err)
		}
	}
	if !errors.Is((UseClaim{Uses: 1 << 7}).Check(), syscall.EINVAL) {
		t.Fatal("unknown use bit accepted")
	}
}

func TestOwnerOptionsAndMetadataUpdatesRejectUnboundedInputs(t *testing.T) {
	for _, options := range []OwnerOptions{{Lifetime: OwnerReference}, {Lifetime: OwnerExplicit, Group: 42}} {
		if err := options.Check(); err != nil {
			t.Fatalf("valid owner options %+v: %v", options, err)
		}
	}
	if !errors.Is((OwnerOptions{}).Check(), syscall.EINVAL) {
		t.Fatal("owner without lifetime accepted")
	}
	if err := CheckMetadataUpdate("client", nil, nil); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(CheckMetadataUpdate("bad name", nil, nil), syscall.EINVAL) {
		t.Fatal("invalid namespace accepted")
	}
	if !errors.Is(CheckMetadataUpdate("client", make([]byte, MaxObservationTokenBytes+1), nil), syscall.EINVAL) {
		t.Fatal("unbounded expected version accepted")
	}
	if !errors.Is(CheckMetadataUpdate("client", nil, make([]byte, MaxMetadataValueBytes+1)), syscall.EFBIG) {
		t.Fatal("unbounded metadata payload accepted")
	}
}

func TestCapabilityErrorsPreserveActionableClassifications(t *testing.T) {
	for _, test := range []struct {
		err  error
		want syscall.Errno
	}{{ErrUseConflict, syscall.EAGAIN}, {ErrRangeConflict, syscall.EAGAIN}, {ErrConditionConflict, syscall.EAGAIN}, {ErrInvalidScope, syscall.ESTALE}} {
		if !errors.Is(test.err, test.want) {
			t.Fatalf("%v does not unwrap to %v", test.err, test.want)
		}
	}
}
