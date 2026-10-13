//go:build linux

package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSignedDeleteOnCloseWithoutDeleteAccessHasNoEffect(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	existing := f.server.exports["DATA"]
	if err := existing.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	share := existing.share
	share.DeleteIntentOwner = "signed-delete-owner"
	export, err := f.server.Publish(share)
	if err != nil {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(WithPrincipal(t.Context(), testPrincipal("0123456789abcdef"))); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), "existing", []byte("original bytes")); err != nil {
		t.Fatal(err)
	}
	original, err := f.volume.Stat(t.Context(), "existing")
	if err != nil {
		t.Fatal(err)
	}
	session, key := authenticateProtocol(t, client)
	tree := connectProtocolTree(t, client, key, session, 3, "data")
	for index, request := range []wire.Request{createRequestForTest("absent", 2, accessWriteData|accessReadAttr), createRequestForTest("existing", 4, accessWriteData|accessReadAttr)} {
		binary.LittleEndian.PutUint32(request.Body[40:], createDeleteOnClose)
		message := uint64(4 + index)
		sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: message, SessionID: session, TreeID: tree, Credits: 1}, request.Body))
		response := readFrame(t, client)
		header, err := wire.ParseHeader(response)
		if err != nil || header.Command != wire.Create || header.MessageID != message || header.SessionID != session || header.TreeID != tree || header.Status != statusDenied || header.Flags&wire.FlagSigned == 0 || header.NextCommand != 0 || key.Verify(response) != nil || !bytes.Equal(response[wire.HeaderSize:], wire.ErrorResponseBody()) {
			t.Fatalf("missing DELETE signed response: header=%+v err=%v packet=%x", header, err, response)
		}
	}
	if _, err := f.volume.Stat(t.Context(), "absent"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("rejected CREATE made a name: %v", err)
	}
	after, err := f.volume.Stat(t.Context(), "existing")
	if err != nil || !reflect.DeepEqual(after, original) {
		t.Fatalf("rejected reset changed identity/content: %+v %v", after, err)
	}
	data, err := f.volume.Read(t.Context(), "existing")
	if err != nil || string(data) != "original bytes" {
		t.Fatalf("rejected reset changed bytes: %q %v", data, err)
	}
	page, err := f.raw.(storage.FileActions).ListDeleteIntents(t.Context(), share.DeleteIntentOwner, 0, 256)
	if err != nil || len(page.Intents) != 0 {
		t.Fatalf("rejected CREATE accepted an intent: %+v %v", page, err)
	}
	status := f.server.Status()
	if status.Handles != 0 || status.OpeningHandles != 0 || status.LocalDeleteOwners != 0 || len(f.server.HandleOwners()) != 0 {
		t.Fatalf("rejected CREATE retained owner: %+v", status)
	}
}
