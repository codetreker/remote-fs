package advisory

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNativeUseClaimsAreSymmetricAndScopeSpecific(t *testing.T) {
	c := fixture(t, DefaultConfig())
	a, b := storage.UseScope{Token: "native-a"}, storage.UseScope{Token: "native-b"}
	claim := storage.UseClaim{Uses: storage.ReadData, Deny: storage.WriteData}
	if err := c.AddUse(background, 1, a, claim); err != nil {
		t.Fatal(err)
	}
	if err := c.AddUse(background, 1, b, claim); err != nil {
		t.Fatal(err)
	}
	for _, denied := range []storage.UseClaim{{Uses: storage.WriteData}, {Uses: storage.ReadData, Deny: storage.ReadData}} {
		if err := c.AddUse(background, 1, storage.UseScope{Token: "denied"}, denied); !errors.Is(err, storage.ErrUseConflict) {
			t.Fatalf("asymmetric sharing admission %+v: %v", denied, err)
		}
	}
	if err := c.CheckUse(background, 1, storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous write escaped sharing: %v", err)
	}
	if err := c.CheckUse(background, 1, a, storage.ReadData); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(background, 1, a, storage.WriteData); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("reference gained undeclared use: %v", err)
	}
	self := storage.UseScope{Token: "self-deny"}
	if err := c.AddUse(background, 2, self, storage.UseClaim{Uses: storage.WriteData, Deny: storage.WriteData}); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(background, 2, self, storage.WriteData); err != nil {
		t.Fatalf("own deny blocked own admitted use: %v", err)
	}
	if err := c.CheckUse(background, 2, storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous use borrowed holder exemption: %v", err)
	}
	if err := c.CheckUse(background, 1, self, storage.ReadData); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("wrong target scope: %v", err)
	}
	if err := c.AddUse(background, 1, a, claim); err != nil {
		t.Fatal(err)
	}
	if err := c.AddUse(background, 2, a, claim); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("reference token rebound: %v", err)
	}
	if err := c.DropUse(background, 2, a); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("wrong-node release = %v", err)
	}
	if err := c.DropUse(background, 1, a); err != nil {
		t.Fatal(err)
	}
	if err := c.DropUse(background, 1, a); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckUse(background, 1, a, storage.ReadData); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("retired reference scope = %v", err)
	}
}

func TestNativeUseAndRangeOwnersShareCapacity(t *testing.T) {
	config := DefaultConfig()
	config.MaxOwners = 1
	c := fixture(t, config)
	s := session(t, c)
	scope := storage.UseScope{Token: "native"}
	if err := c.AddUse(background, 1, scope, storage.UseClaim{Uses: storage.ReadData}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NewOwner(background, 1, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference}); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("registered owner bypassed aggregate capacity: %v", err)
	}
	if err := c.DropUse(background, 1, scope); err != nil {
		t.Fatal(err)
	}
	o := owner(t, s, 1, 0)
	if err := c.AddUse(background, 1, scope, storage.UseClaim{Uses: storage.ReadData}); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("native claim bypassed aggregate capacity: %v", err)
	}
	if err := s.RetireOwner(background, o); err != nil {
		t.Fatal(err)
	}
	if err := c.AddUse(background, 1, scope, storage.UseClaim{Uses: storage.ReadData}); err != nil {
		t.Fatalf("owner retirement failed to recover capacity: %v", err)
	}
}

func TestUseAndIOValidationRejectWithoutStateChange(t *testing.T) {
	c := fixture(t, DefaultConfig())
	ctx, cancel := context.WithCancel(background)
	cancel()
	scope := storage.UseScope{Token: "valid"}
	for _, call := range []func() error{
		func() error { return c.AddUse(background, 0, scope, storage.UseClaim{}) },
		func() error { return c.AddUse(background, 1, storage.UseScope{}, storage.UseClaim{}) },
		func() error { return c.AddUse(background, 1, scope, storage.UseClaim{Uses: 128}) },
		func() error { return c.DropUse(background, 0, scope) },
		func() error { return c.DropUse(background, 1, storage.UseScope{}) },
		func() error { return c.CheckUse(background, 0, storage.UseScope{}, 0) },
		func() error { return c.CheckUse(background, 1, storage.UseScope{}, 128) },
		func() error { return c.CheckIO(background, 0, storage.UseScope{}, bytesRange(0, 1), storage.ReadData) },
		func() error { return c.CheckIO(background, 1, storage.UseScope{}, storage.Range{}, storage.ReadData) },
		func() error {
			return c.CheckIO(background, 1, storage.UseScope{}, bytesRange(0, 1), storage.DeleteName)
		},
		func() error {
			return c.CheckIO(background, 1, storage.UseScope{}, storage.Range{Kind: storage.Boundary}, storage.ReadData)
		},
	} {
		if err := call(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid use call = %v", err)
		}
	}
	for _, call := range []func() error{
		func() error { return c.AddUse(ctx, 1, scope, storage.UseClaim{}) },
		func() error { return c.DropUse(ctx, 1, scope) },
		func() error { return c.CheckUse(ctx, 1, scope, 0) },
		func() error { return c.CheckIO(ctx, 1, scope, bytesRange(0, 1), storage.ReadData) },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled use call = %v", err)
		}
	}
	if len(c.uses) != 0 {
		t.Fatal("invalid call registered a use claim")
	}
}
