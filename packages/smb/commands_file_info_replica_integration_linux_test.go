//go:build linux

package smb

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileInfoReplicaUnknownMutationRetainsIntentUntilCloseBarrierSettles(t *testing.T) {
	for _, class := range []byte{wire.FileBasicInformation, wire.FileEndOfFileInformation} {
		t.Run(map[byte]string{wire.FileBasicInformation: "basic", wire.FileEndOfFileInformation: "eof"}[class], func(t *testing.T) {
			f, local, gate, capture := newReplicaCloseProofSMBFixture(t)
			id := openInfoFileForTest(t, f, "unknown", accessWriteAttr|accessWriteData)
			h := f.tree.findFileHandle(id)
			gate.armed.Store(true)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			data := eofInfoForTest(2)
			if class == wire.FileBasicInformation {
				data = wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden})
			}
			call, finish := ioResponseContextForTest(ctx)
			_, status := f.connection.setFileInfo(call, f.session, f.tree, setInfoRequestForTest(id, class, data))
			finish()
			if status != statusIO {
				t.Fatalf("withheld SET response=%#x", status)
			}
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("SET mutation event did not reach delivery gate")
			}
			attr, err := f.volume.Stat(ctx, "unknown")
			if err != nil {
				t.Fatal(err)
			}
			meta, err := decodeWindowsMetadata(attr.Metadata)
			if err != nil {
				t.Fatal(err)
			}
			if class == wire.FileBasicInformation && (meta.Attributes != dosHidden || attr.Size != 6) || class == wire.FileEndOfFileInformation && (meta.Attributes != dosArchive || attr.Size != 2) {
				t.Fatalf("native committed wrong mutation: %+v %+v", attr, meta)
			}
			if h.pendingWrite == nil {
				t.Fatal("unknown SET lost its intent owner")
			}
			h.pendingWrite.mu.Lock()
			original := cloneMutationCommand(*h.pendingWrite.command.file)
			h.pendingWrite.mu.Unlock()
			before := f.server.Status()
			if before.RetainedWriteBytes == 0 || len(before.WriteFailures) != 1 || before.WriteFailures[0].PayloadReleased {
				t.Fatalf("unknown SET diagnostics=%+v", before)
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
				t.Fatalf("native release=%+v", replies)
			}
			barrier := replies[0].Barrier
			if int64(local.Position()) >= barrier.Position || f.tree.findFileHandle(id) != h || !h.released || handleState(h.state.Load()) != handleBarrierOnly {
				t.Fatalf("release prematurely settled reference: position=%d barrier=%d handle=%p", local.Position(), barrier.Position, f.tree.findFileHandle(id))
			}
			h.pendingWrite.mu.Lock()
			retained := cloneMutationCommand(*h.pendingWrite.command.file)
			retired := h.pendingWrite.retired
			h.pendingWrite.mu.Unlock()
			if retired || original.Action != retained.Action || original.Kind != retained.Kind || original.Size != retained.Size || !bytes.Equal(original.ExpectedMetadata[windowsMetadataKey], retained.ExpectedMetadata[windowsMetadataKey]) || !bytes.Equal(original.Metadata[windowsMetadataKey].Data, retained.Metadata[windowsMetadataKey].Data) {
				t.Fatalf("native release lost original SET intent: %+v -> %+v retired=%t", original, retained, retired)
			}
			pending := f.server.Status()
			if pending.RetainedWriteBytes != before.RetainedWriteBytes || pending.WriteFailures[0].PayloadReleased || pending.WriteFailures[0].ChainSettled || pending.WriteFailures[0].Terminal {
				t.Fatalf("native release discarded unsettled SET: %+v", pending)
			}
			attempt := *h.attempt
			gate.release()
			if err := f.tree.closeFileHandle(ctx, h); err == nil {
				t.Fatal("settled CLOSE hid original mutation error")
			}
			if int64(local.Position()) < barrier.Position || f.tree.findFileHandle(id) != nil {
				t.Fatalf("confirmed barrier retained reference: position=%d barrier=%d", local.Position(), barrier.Position)
			}
			capture.mu.Lock()
			closeCalls := len(capture.replies)
			capture.mu.Unlock()
			if closeCalls != 1 || *h.attempt != attempt {
				t.Fatalf("barrier confirmation created a new close: calls=%d attempt=%+v", closeCalls, h.attempt)
			}
			terminal := f.server.Status()
			if terminal.RetainedWriteBytes != 0 || len(terminal.WriteFailures) != 1 || !terminal.WriteFailures[0].PayloadReleased || !terminal.WriteFailures[0].ReferenceReleased || !terminal.WriteFailures[0].ChainSettled || !terminal.WriteFailures[0].Terminal {
				t.Fatalf("terminal SET fact=%+v", terminal)
			}
		})
	}
}
