package smb

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWriteFailureAcknowledgmentWaitsForFinalResponseAndIsAtomic(t *testing.T) {
	server, tree, h, file, session, command, effects := ioOwnerFixture(t)
	file.mutate = func(storage.FileMutation) (storage.Attr, error) { return storage.Attr{}, syscall.EIO }
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
	}
	owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = owner.execute(t.Context(), func(context.Context) error { return nil }, nil, nil)
	_ = owner.retire(true, true)
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err == nil {
		t.Fatal("ack raced response")
	}
	owner.recordResponse(ResponseSendFailed)
	before := server.Status().WriteFailures[0]
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id, owner.id}); err == nil {
		t.Fatal("duplicate ack accepted")
	}
	stale := owner.id
	stale.Generation++
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id, stale}); err == nil {
		t.Fatal("partially invalid ack accepted")
	}
	if got := server.Status().WriteFailures; len(got) != 1 || got[0] != before {
		t.Fatalf("failed batch changed facts: %+v", got)
	}
	owner.recordResponse(ResponseSent)
	if got := server.Status().WriteFailures[0]; got != before {
		t.Fatal("terminal facts changed after final response")
	}
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteOwnerReservationBoundsPayloadAndDiagnosticsBeforeDispatch(t *testing.T) {
	server, tree, h, file, _, command, effects := ioOwnerFixture(t)
	file.mutate = func(storage.FileMutation) (storage.Attr, error) {
		t.Fatal("dispatch before reservation")
		return storage.Attr{}, nil
	}
	server.config.Limits.MaxRetainedWriteBytes = 1
	if _, err := server.reserveWriteOwner(tree, h, 1, command, effects); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("byte bound: %v", err)
	}
	if server.Status().WriteOwners != 0 {
		t.Fatal("failed reservation retained slot")
	}
	server.config.Limits.MaxRetainedWriteBytes = 1 << 20
	server.config.Limits.MaxWriteOwners = 1
	if _, err := server.reserveWriteOwner(tree, h, 1, command, effects); err != nil {
		t.Fatal(err)
	}
	if _, err := server.reserveFlushOwner(tree, h, 2); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("slot bound: %v", err)
	}
}
