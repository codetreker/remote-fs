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
	owner, err := server.reserveMutationOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil)
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
	if _, err := server.reserveMutationOwner(tree, h, 1, command, effects); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("byte bound: %v", err)
	}
	if server.Status().WriteOwners != 0 {
		t.Fatal("failed reservation retained slot")
	}
	server.config.Limits.MaxRetainedWriteBytes = 1 << 20
	server.config.Limits.MaxWriteOwners = 1
	if _, err := server.reserveMutationOwner(tree, h, 1, command, effects); err != nil {
		t.Fatal(err)
	}
	if _, err := server.reserveFlushOwner(tree, h, 2); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("slot bound: %v", err)
	}
}

func TestWriteLedgerConcurrentRetryReservationsRespectServerPeak(t *testing.T) {
	server, baseTree, h, _, _, command := typedOwnerFixture(t, "rename")
	first, err := server.reserveMutationCommand(baseTree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	otherTree := &tree{export: &Export{server: server, share: Share{Volume: "other"}}, authority: baseTree.authority}
	second, err := server.reserveMutationCommand(otherTree, h, 2, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldBytes := command.payloadBytes(nil)
	freshBytes := largerOwnerNameCommand(command).payloadBytes(nil)
	limit := 2*oldBytes + freshBytes
	start := make(chan struct{})
	type result struct {
		owner *mutationOwner
		old   int
		err   error
	}
	results := make(chan result, 2)
	for _, owner := range []*mutationOwner{first, second} {
		go func() {
			<-start
			old, err := server.writes.reserveReplacementPayload(owner.id, freshBytes, limit)
			results <- result{owner, old, err}
		}()
	}
	close(start)
	accepted := 0
	var winner *mutationOwner
	for range 2 {
		result := <-results
		if result.err == nil {
			accepted++
			winner = result.owner
			if result.old != oldBytes {
				t.Fatalf("wrong original charge: %d", result.old)
			}
		} else if !errors.Is(result.err, syscall.ENOMEM) {
			t.Fatal(result.err)
		}
	}
	if accepted != 1 || server.Status().RetainedWriteBytes != limit {
		t.Fatalf("concurrent replacement exceeded server peak: accepted=%d bytes=%d", accepted, server.Status().RetainedWriteBytes)
	}
	server.writes.setPayloadCharge(winner.id, oldBytes)
	if server.Status().RetainedWriteBytes != 2*oldBytes {
		t.Fatal("rollback did not restore both original charges")
	}
	server.writes.releasePayload(first.id)
	server.writes.releasePayload(second.id)
	if server.Status().RetainedWriteBytes != 0 {
		t.Fatal("original charges survived release")
	}
}
