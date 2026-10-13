//go:build linux

package smb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNameReplicaUnknownRenameRetainsCommandUntilCloseBarrierSettles(t *testing.T) {
	f, local, gate, capture := newReplicaCloseProofSMBFixture(t)
	id := openInfoFileForTest(t, f, "unknown", accessDelete|accessReadData)
	h := f.tree.findFileHandle(id)
	raw := f.tree.authority.raw
	intercept := nameMutationInterceptForTest(raw)
	var commands []storage.NameCommand
	intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
		commands = append(commands, *mutationCommand{name: &command}.clone().name)
		return intercept.NamespaceAccess.MutateName(ctx, command)
	}
	f.tree.authority.raw = intercept
	t.Cleanup(func() { f.tree.authority.raw = raw })
	gate.armed.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	call, finish := ioResponseContextForTest(ctx)
	_, status := f.connection.setFileInfo(call, f.session, f.tree, setInfoRequestForTest(id, wire.FileRenameInformation, renameInformationForTest("renamed", false)))
	finish()
	if status != statusIO {
		t.Fatalf("withheld rename confirmation=%#x", status)
	}
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("rename event did not reach replica delivery gate")
	}
	attr, err := f.volume.Stat(ctx, "renamed")
	if err != nil || attr.ID != h.nodeID {
		t.Fatalf("authority rename=%+v, %v", attr, err)
	}
	if h.pendingWrite == nil || len(commands) < 2 {
		t.Fatalf("unconfirmed rename owner=%p calls=%d", h.pendingWrite, len(commands))
	}
	original := commands[0]
	if original.Guards == nil || original.Action == "" || original.Target.NodeID != h.nodeID {
		t.Fatalf("rename did not retain guarded original action: %+v", original)
	}
	before := f.server.Status()
	if before.RetainedWriteBytes == 0 || len(before.WriteFailures) != 1 || before.WriteFailures[0].PayloadReleased || before.WriteFailures[0].Operation != storage.OpFileMutateName {
		t.Fatalf("unconfirmed rename diagnostics=%+v", before)
	}
	closeErr := f.tree.closeFileHandle(ctx, h)
	var settlement *storage.CloseSettlementError
	if !errors.As(closeErr, &settlement) || settlement.State != storage.CloseSettlementPending {
		t.Fatalf("withheld replica CLOSE=%v", closeErr)
	}
	capture.mu.Lock()
	replies := append([]ioNativeCloseReply(nil), capture.replies...)
	capture.mu.Unlock()
	if len(replies) != 1 || replies[0].Barrier == nil || replies[0].CloseResult == nil || !replies[0].CloseResult.Released {
		t.Fatalf("native close did not retain mutation barrier: %+v", replies)
	}
	barrier := replies[0].Barrier
	if int64(local.Position()) >= barrier.Position || f.tree.findFileHandle(id) != h || !h.released || handleState(h.state.Load()) != handleBarrierOnly {
		t.Fatalf("native release prematurely retired rename: position=%d barrier=%d handle=%p state=%d", local.Position(), barrier.Position, f.tree.findFileHandle(id), h.state.Load())
	}
	h.pendingWrite.mu.Lock()
	retained := *h.pendingWrite.command.clone().name
	retired := h.pendingWrite.retired
	h.pendingWrite.mu.Unlock()
	if retired || !reflect.DeepEqual(original, retained) {
		t.Fatalf("native release discarded frozen rename: %+v -> %+v retired=%t", original, retained, retired)
	}
	for _, command := range commands {
		if !reflect.DeepEqual(original, command) {
			t.Fatal("rename reconciliation minted a new action or changed guards")
		}
	}
	pending := f.server.Status()
	if pending.RetainedWriteBytes != before.RetainedWriteBytes || pending.WriteFailures[0].PayloadReleased || pending.WriteFailures[0].ChainSettled || pending.WriteFailures[0].Terminal {
		t.Fatalf("unconfirmed close discarded rename charge: %+v", pending)
	}
	attempt := *h.attempt
	gate.release()
	if err := f.tree.closeFileHandle(ctx, h); err == nil {
		t.Fatal("confirmed CLOSE hid original rename confirmation failure")
	}
	if int64(local.Position()) < barrier.Position || f.tree.findFileHandle(id) != nil {
		t.Fatalf("confirmed barrier retained reference: position=%d barrier=%d", local.Position(), barrier.Position)
	}
	capture.mu.Lock()
	closeCalls := len(capture.replies)
	capture.mu.Unlock()
	if closeCalls != 1 || *h.attempt != attempt {
		t.Fatalf("confirmation redispatched native CLOSE: calls=%d attempt=%+v", closeCalls, h.attempt)
	}
	terminal := f.server.Status()
	if terminal.RetainedWriteBytes != 0 || terminal.RetainedDirectoryBytes != 0 || len(terminal.WriteFailures) != 1 || !terminal.WriteFailures[0].PayloadReleased || !terminal.WriteFailures[0].ReferenceReleased || !terminal.WriteFailures[0].ChainSettled || !terminal.WriteFailures[0].Terminal {
		t.Fatalf("settled rename retained resources: %+v", terminal)
	}
	if data, err := f.volume.Read(ctx, "renamed"); err != nil || string(data) != "before" {
		t.Fatalf("settled rename changed contents: %q, %v", data, err)
	}
}
