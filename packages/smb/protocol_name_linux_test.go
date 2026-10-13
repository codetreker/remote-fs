//go:build linux

package smb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestSignedRelatedNameMutationWarningPreservesClose(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	create := createRequestForTest("original", 2, accessDelete)
	rename := setInfoRequestForTest(wire.InvalidFileID, wire.FileRenameInformation, renameInformationForTest("renamed-long", false))
	query := queryInfoRequestForTest(wire.InvalidFileID, wire.FileNormalizedNameInformation, 8)
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	copy(closeBody[8:], wire.InvalidFileID[:])
	headers := []wire.Header{
		{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1},
		{Command: wire.SetInfo, MessageID: 5, Credits: 1},
		{Command: wire.QueryInfo, MessageID: 6, Credits: 1},
		{Command: wire.Close, MessageID: 7, Credits: 1},
	}
	sendFrame(t, client, compoundRequest(t, key, headers, [][]byte{create.Body, rename.Body, query.Body, closeBody}, true))
	response := readFrame(t, client)
	statuses := []uint32{statusOK, statusOK, statusBufferOverflow, statusOK}
	offset := 0
	for index, expected := range headers {
		header, err := wire.ParseHeader(response[offset:])
		if err != nil || header.Command != expected.Command || header.MessageID != expected.MessageID || header.Status != statuses[index] || (header.Flags&wire.FlagRelated != 0) != (index > 0) {
			t.Fatalf("name compound %d header=%+v err=%v", index, header, err)
		}
		end := len(response)
		if header.NextCommand != 0 {
			end = offset + int(header.NextCommand)
		}
		if end < offset+64 || end > len(response) || key.Verify(response[offset:end]) != nil {
			t.Fatalf("name compound %d extent/signature", index)
		}
		if index == 2 {
			body := response[offset+64 : end]
			if len(body) != 16 || binary.LittleEndian.Uint32(body[8:]) != 26 || !bytes.Equal(body[12:], wire.EncodeUTF16(`\r`)) {
				t.Fatalf("signed name warning=%x", body)
			}
		}
		offset = end
	}
	if _, err := f.volume.Stat(t.Context(), "renamed-long"); err != nil {
		t.Fatal(err)
	}
	if status := f.server.Status(); status.Handles != 0 || status.RetainedDirectoryBytes != 0 {
		t.Fatalf("related name CLOSE retained=%+v", status)
	}
}
