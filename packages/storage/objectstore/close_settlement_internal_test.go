package objectstore

import (
	"errors"
	"syscall"
	"testing"
)

type inlineSettlementProbe struct {
	*identityAuthorityProbe
	err   error
	calls int
}

func (p *inlineSettlementProbe) CheckInlineCloseSettlement() error {
	p.calls++
	return p.err
}

func TestInlineCloseSettlementRequiresCompleteNativePromise(t *testing.T) {
	missing := &identityAuthorityProbe{}
	volume := &Storage{meta: missing, objects: struct{ BoundedObjects }{}}
	session := &fileSession{storage: volume, native: missing}
	if err := session.CheckInlineCloseSettlement(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unproven native settlement accepted: %v", err)
	}
	refusal := errors.New("native settlement refused")
	native := &inlineSettlementProbe{identityAuthorityProbe: missing, err: refusal}
	volume.meta, session.native = native, native
	if err := session.CheckInlineCloseSettlement(); !errors.Is(err, refusal) || native.calls != 1 {
		t.Fatalf("native settlement refusal lost: %v, calls=%d", err, native.calls)
	}
	native.err = nil
	if err := session.CheckInlineCloseSettlement(); err != nil || native.calls != 2 {
		t.Fatalf("valid native settlement rejected: %v, calls=%d", err, native.calls)
	}
	native.fileErr = refusal
	if err := session.CheckInlineCloseSettlement(); !errors.Is(err, refusal) || native.calls != 2 {
		t.Fatalf("unsupported native file storage bypassed: %v, calls=%d", err, native.calls)
	}
}
