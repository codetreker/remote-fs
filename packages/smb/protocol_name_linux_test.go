//go:build linux

package smb

import (
	"bytes"
	"encoding/binary"
	"reflect"
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

func TestSignedNameReplacementRejectsReadOnlyTargetWithoutEffects(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	if err := f.volume.Write(t.Context(), "source", []byte("source bytes")); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), "target", []byte("target bytes")); err != nil {
		t.Fatal(err)
	}
	targetBefore := seedIOWindowsMetadata(t, f, "target", dosReadOnly|dosHidden)
	sourceBefore, err := f.volume.Stat(t.Context(), "source")
	if err != nil || sourceBefore.ID == targetBefore.ID {
		t.Fatalf("source/target identities before replacement: source=%+v target=%+v err=%v", sourceBefore, targetBefore, err)
	}
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	create := createRequestForTest("source", 1, accessDelete)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
	response := readFrame(t, client)
	header, err := wire.ParseHeader(response)
	if err != nil || header.Command != wire.Create || header.MessageID != 4 || header.Status != statusOK || len(response) != 152 || key.Verify(response) != nil {
		t.Fatalf("signed source CREATE header=%+v err=%v packet=%x", header, err, response)
	}
	var id wire.FileID
	copy(id[:], response[128:144])
	rename := setInfoRequestForTest(id, wire.FileRenameInformation, renameInformationForTest("target", true))
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.SetInfo, MessageID: 5, SessionID: sessionID, TreeID: treeID, Credits: 1}, rename.Body))
	response = readFrame(t, client)
	header, err = wire.ParseHeader(response)
	if err != nil || header.Command != wire.SetInfo || header.MessageID != 5 || header.SessionID != sessionID || header.TreeID != treeID || header.Status != statusDenied || header.Flags&wire.FlagSigned == 0 || header.NextCommand != 0 || key.Verify(response) != nil || !bytes.Equal(response[wire.HeaderSize:], wire.ErrorResponseBody()) {
		t.Fatalf("signed READONLY target replacement header=%+v err=%v packet=%x", header, err, response)
	}
	for _, entry := range []struct {
		name    string
		content string
	}{{"source", "source bytes"}, {"target", "target bytes"}} {
		data, err := f.volume.Read(t.Context(), entry.name)
		if err != nil || string(data) != entry.content {
			t.Fatalf("rejected replacement changed %s content=%q err=%v", entry.name, data, err)
		}
	}
	sourceAfter, err := f.volume.Stat(t.Context(), "source")
	if err != nil || !reflect.DeepEqual(sourceAfter, sourceBefore) {
		t.Fatalf("rejected replacement changed source identity/binding/attributes: before=%+v after=%+v err=%v", sourceBefore, sourceAfter, err)
	}
	targetAfter, err := f.volume.Stat(t.Context(), "target")
	if err != nil || !reflect.DeepEqual(targetAfter, targetBefore) {
		t.Fatalf("rejected replacement changed target identity/binding/attributes: before=%+v after=%+v err=%v", targetBefore, targetAfter, err)
	}
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	copy(closeBody[8:], id[:])
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: 6, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
	response = readFrame(t, client)
	header, err = wire.ParseHeader(response)
	if err != nil || header.Command != wire.Close || header.MessageID != 6 || header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("signed source CLOSE header=%+v err=%v packet=%x", header, err, response)
	}
	if status := f.server.Status(); status.Handles != 0 || status.OpeningHandles != 0 || status.RetainedDirectoryBytes != 0 || status.WriteOwners != 0 || status.UnknownWrites != 0 || status.RetainedWriteBytes != 0 || len(status.WriteFailures) != 0 {
		t.Fatalf("rejected signed replacement retained ownership=%+v", status)
	}
}
