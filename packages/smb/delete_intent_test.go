package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDeleteOnCloseRequiresDeleteAccessWithoutOwnShareDelete(t *testing.T) {
	for _, disposition := range []uint32{1, 2, 3, 4, 5} {
		for _, share := range []uint32{0, 1, 2, 3, 4, 5, 6, 7} {
			req := wire.CreateRequest{Disposition: disposition, Options: createDeleteOnClose, DesiredAccess: accessReadData | accessWriteData | accessDelete, ShareAccess: share}
			intent, err := classifyCreate(req)
			if err != nil || !intent.deleteOnClose || intent.use.Uses&storage.DeleteName == 0 || (intent.use.Deny&storage.DeleteName != 0) != (share&4 == 0) {
				t.Fatalf("disposition=%d share=%d intent=%+v err=%v", disposition, share, intent, err)
			}
		}
	}
	for _, access := range []uint32{0, accessGenericRead, accessGenericWrite, accessReadAttr} {
		_, err := classifyCreate(wire.CreateRequest{Disposition: 1, Options: createDeleteOnClose, DesiredAccess: access})
		if !errors.Is(err, syscall.EACCES) {
			t.Fatalf("access=%x err=%v", access, err)
		}
	}
}

func TestDeleteOwnerConfigurationAndSharedReservation(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxUnresolvedOwners = 2
	config.Limits.MaxDiagnosticBytes = 2 * diagnosticOwnerBytes
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	share := treeIdentityShare()
	share.Backend = &endpointStorage{}
	share.DeleteIntentOwner = "owner-one"
	invalid := share
	invalid.DeleteIntentOwner = "bad\x00owner"
	if _, err := server.Publish(invalid); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	failed := share
	failed.Backend = &endpointStorage{checkErr: syscall.EIO}
	if _, err := server.Publish(failed); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if server.deleteRecoverySlots != 0 {
		t.Fatal("failed Publish retained recovery charge")
	}
	export, err := server.Publish(share)
	if err != nil {
		t.Fatal(err)
	}
	if server.deleteRecoverySlots != 1 || share.Backend.(*endpointStorage).sessionOpens.Load() != 0 {
		t.Fatal("Publish did not reserve one slot without enrollment")
	}
	alias := share
	alias.Name = "alias"
	if _, err := server.Publish(alias); !errors.Is(err, ErrConfig) {
		t.Fatalf("duplicate owner accepted: %v", err)
	}
	alias.DeleteIntentOwner = "different-owner"
	if _, err := server.Publish(alias); !errors.Is(err, ErrConfig) {
		t.Fatalf("duplicate binding accepted: %v", err)
	}
	tree := &tree{export: export}
	session := &session{id: 15}
	if !tree.beginFileWork(session) {
		t.Fatal("admission refused")
	}
	defer tree.endFileWork()
	handle, err := tree.reserveFileHandle(session, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.reserveFileHandle(session, 3); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("ordinary handle consumed recovery reservation: %v", err)
	}
	tree.releaseFileHandle(handle)
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if server.deleteRecoverySlots != 0 {
		t.Fatal("Unpublish retained unused reservation")
	}
}

func TestDeleteOnCloseAdmissionGatesOnlyRequestedEffect(t *testing.T) {
	server, export, _, ctx := deleteRecoveryFixture(t, endpointConfig())
	if err := export.admitDeleteOpen(); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := export.admitDeleteOpen(); err != nil {
		t.Fatal(err)
	}
	export.deleteRecovery.busy.Store(true)
	if err := export.admitDeleteOpen(); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := export.beginDeleteRecovery(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	export.endDeleteOpen()
	if err := export.beginDeleteRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	export.deleteRecovery.busy.Store(false)
	if err := export.Unpublish(ctx); err != nil {
		t.Fatal(err)
	}
	if server.Status().DeleteRecoveryPendingExports != 0 {
		t.Fatal("removed export retained discovery status")
	}
}

func TestDeleteReferenceSettlementRetainsOriginalDiagnosticUntilRecovery(t *testing.T) {
	rejectQuery := true
	config := endpointConfig()
	config.Authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
		if rejectQuery && r.Operation == storage.OpFileQueryDeleteIntent {
			return authz.ErrDenied
		}
		return nil
	})
	server, export, backend, ctx := deleteRecoveryFixture(t, config)
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	backend.intents = append(backend.intents, storage.DeleteIntentStatus{ID: "00000000000000000000000000000001", NodeID: 2, Outcome: storage.DeleteIntentCompleted})
	raw, err := backend.NewFileSession(ctx, config.Limits.FileSession)
	if err != nil {
		t.Fatal(err)
	}
	tree := &tree{export: export, authority: &authoritySession{raw: raw}}
	session := &session{id: 19}
	if !tree.beginFileWork(session) {
		t.Fatal("admission refused")
	}
	defer tree.endFileWork()
	handle, err := tree.reserveFileHandle(session, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle.deleteAttempt = &deleteIntentAttempt{id: backend.intents[0].ID}
	handle.recordOpenDiagnostic()
	handle.file = &handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}
	if err := tree.closeFileHandle(ctx, handle); !errors.Is(err, authz.ErrDenied) {
		t.Fatal(err)
	}
	if tree.findFileHandle(handle.id) != nil || !handle.deleteRefSettled.Load() {
		t.Fatal("settled reference retained protocol identifier")
	}
	owners := server.HandleOwners()
	if len(owners) != 1 || owners[0].DeleteIntent != backend.intents[0].ID || server.Status().LocalDeleteOwners != 1 {
		t.Fatalf("diagnostic charge lost: %+v", owners)
	}
	rejectQuery = false
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(server.HandleOwners()) != 0 || server.deleteRecoverySlots != 1 {
		t.Fatal("intent did not release exactly its original handle charge")
	}
	if err := export.Unpublish(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRecoveryReservationAlsoBoundsWrites(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxDiagnosticBytes = 2 * diagnosticOwnerBytes
	server, export, _, _ := deleteRecoveryFixture(t, config)
	tree := &tree{export: export}
	session := &session{id: 32}
	if !tree.beginFileWork(session) {
		t.Fatal("admission refused")
	}
	defer tree.endFileWork()
	h, err := tree.reserveFileHandle(session, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.file = &handleFileStub{}
	if _, err := server.reserveIORecord(tree, h, 0, false); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("write took reserved slot: %v", err)
	}
	tree.releaseFileHandle(h)
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDeletePendingInformationHasNoLinkedName(t *testing.T) {
	file := validInfoFileProbe()
	file.info.PendingUnlink = true
	file.info.PendingGeneration = []byte("pending-generation")
	c, s, tree, h := newInfoCommandFixture(t, file, accessReadAttr)
	body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, wire.FileStandardInformation, 24))
	if status != statusOK || len(body) != 32 || binary.LittleEndian.Uint32(body[24:]) != 0 || body[28] != 1 {
		t.Fatalf("pending standard information status=%x body=%x", status, body)
	}
	if closeStatusError(syscall.ENOTEMPTY) != statusDirectoryNotEmpty {
		t.Fatal("directory close error lost specific status")
	}
}
