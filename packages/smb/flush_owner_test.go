package smb

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

func TestFlushOwnerRetriesOnlySameReferenceSync(t *testing.T) {
	server, tree, h, file, _, _, _ := ioOwnerFixture(t)
	calls := 0
	file.syncCall = func() error {
		calls++
		if calls == 1 {
			return syscall.EIO
		}
		return nil
	}
	file.mutate = nil
	owner, err := server.reserveFlushOwner(tree, h, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.confirm(t.Context(), func(context.Context) error { return nil }); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first confirmation: %v", err)
	}
	owner.recordResponse(ResponseSent)
	if err := owner.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || owner.pending() {
		t.Fatalf("confirmation calls=%d pending=%v", calls, owner.pending())
	}
	status := server.Status()
	if len(status.WriteFailures) != 1 || status.WriteFailures[0].Durability != WriteDurabilityConfirmed || status.PendingFlushes != 0 || status.RetainedWriteBytes != 0 {
		t.Fatalf("confirmation facts: %+v", status)
	}
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err != nil {
		t.Fatal(err)
	}
}

func TestFlushOwnerFailureRetirementKeepsDurabilityUnknown(t *testing.T) {
	server, tree, h, file, _, _, _ := ioOwnerFixture(t)
	file.syncCall = func() error { return syscall.EIO }
	owner, err := server.reserveFlushOwner(tree, h, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = owner.confirm(t.Context(), func(context.Context) error { return nil })
	owner.recordResponse(ResponseSent)
	_ = owner.retire(true, true)
	facts := server.Status().WriteFailures
	if len(facts) != 1 || facts[0].Durability != WriteDurabilityUnknown || !facts[0].ReferenceReleased || !facts[0].ChainSettled {
		t.Fatalf("retired confirmation facts: %+v", facts)
	}
}
