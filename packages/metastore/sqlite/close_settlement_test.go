package sqlite

import (
	"errors"
	"syscall"
	"testing"
)

func TestInlineCloseSettlementRequiresNativeRetentionOwnership(t *testing.T) {
	store := &Store{}
	if err := store.CheckInlineCloseSettlement(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported retention promised inline settlement: %v", err)
	}
}
