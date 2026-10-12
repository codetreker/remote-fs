//go:build linux

package smb

import (
	"encoding/binary"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestSignedRelatedFileInformationCommandsRetainOriginalFileID(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	create := createRequestForTest("information", 2, accessReadData|accessWriteData|accessReadAttr|accessWriteAttr)
	basic := setInfoRequestForTest(wire.InvalidFileID, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden}))
	eof := setInfoRequestForTest(wire.InvalidFileID, wire.FileEndOfFileInformation, eofInfoForTest(3))
	query := queryInfoRequestForTest(wire.InvalidFileID, wire.FileStandardInformation, 24)
	relatedSet := setInfoRequestForTest(wire.InvalidFileID, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosSystem}))
	relatedClose := make([]byte, 24)
	binary.LittleEndian.PutUint16(relatedClose, 24)
	copy(relatedClose[8:], wire.InvalidFileID[:])
	headers := []wire.Header{
		{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1},
		{Command: wire.SetInfo, MessageID: 5, Credits: 1},
		{Command: wire.SetInfo, MessageID: 6, Credits: 1},
		{Command: wire.QueryInfo, MessageID: 7, Credits: 1},
		{Command: wire.SetInfo, MessageID: 8, Credits: 1},
		{Command: wire.Close, MessageID: 9, Credits: 1},
	}
	sendFrame(t, client, compoundRequest(t, key, headers, [][]byte{create.Body, basic.Body, eof.Body, query.Body, relatedSet.Body, relatedClose}, true))
	response := readFrame(t, client)
	offset := 0
	var id wire.FileID
	for index, command := range []uint16{wire.Create, wire.SetInfo, wire.SetInfo, wire.QueryInfo, wire.SetInfo, wire.Close} {
		header, err := wire.ParseHeader(response[offset:])
		if err != nil || header.Command != command || header.Status != statusOK || header.MessageID != uint64(4+index) || (header.Flags&wire.FlagRelated != 0) != (index > 0) {
			t.Fatalf("compound command %d header=%+v err=%v", command, header, err)
		}
		end := len(response)
		if header.NextCommand != 0 {
			end = offset + int(header.NextCommand)
		}
		if end < offset+64 || end > len(response) || key.Verify(response[offset:end]) != nil {
			t.Fatalf("compound command %d extent/signature", command)
		}
		body := response[offset+64 : end]
		switch index {
		case 0:
			copy(id[:], body[64:80])
		case 1, 2, 4:
			if binary.LittleEndian.Uint16(body) != 2 {
				t.Fatalf("SET response=%x", body)
			}
		case 3:
			if len(body) != 32 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint32(body[4:]) != 24 || binary.LittleEndian.Uint64(body[8:]) != 4096 || binary.LittleEndian.Uint64(body[16:]) != 3 {
				t.Fatalf("Standard response=%x", body)
			}
		}
		offset = end
	}
	if data, err := f.volume.Read(t.Context(), "information"); err != nil || string(data) != "\x00\x00\x00" {
		t.Fatalf("compound EOF=%q err=%v", data, err)
	}
	create = createRequestForTest("information", 1, accessReadAttr)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: 11, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
	reopened := readFrame(t, client)
	reopenHeader, reopenErr := wire.ParseHeader(reopened)
	if reopenErr != nil || reopenHeader.Status != statusOK || key.Verify(reopened) != nil {
		t.Fatalf("reopen header=%+v err=%v", reopenHeader, reopenErr)
	}
	copy(id[:], reopened[128:144])
	for index, class := range []byte{wire.FileBasicInformation, wire.FileStandardInformation, wire.FileInternalInformation, wire.FileNetworkOpenInformation, wire.FileIdInformation} {
		request := queryInfoRequestForTest(id, class, 56)
		sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: uint64(12 + index), SessionID: sessionID, TreeID: treeID, Credits: 1}, request.Body))
		result := readFrame(t, client)
		header, err := wire.ParseHeader(result)
		if err != nil || header.Status != statusOK || header.Command != wire.QueryInfo || key.Verify(result) != nil || binary.LittleEndian.Uint32(result[68:72]) != uint32(fileInfoSize(class)) {
			t.Fatalf("signed class %d header=%+v body=%x err=%v", class, header, result[64:], err)
		}
		if class == wire.FileBasicInformation && binary.LittleEndian.Uint32(result[104:]) != dosSystem {
			t.Fatalf("Basic lost atomic attrs=%x", result[64:])
		}
	}
	short := queryInfoRequestForTest(id, wire.FileBasicInformation, 39)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: 17, SessionID: sessionID, TreeID: treeID, Credits: 1}, short.Body))
	result := readFrame(t, client)
	header, err := wire.ParseHeader(result)
	if err != nil || header.Status != statusInfoLengthMismatch || len(result) != 80 || key.Verify(result) != nil || result[66] != 1 || binary.LittleEndian.Uint32(result[68:72]) != 8 {
		t.Fatalf("signed short QUERY header=%+v body=%x err=%v", header, result[64:], err)
	}
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	copy(closeBody[8:], id[:])
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: 18, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil {
		t.Fatalf("signed CLOSE header=%+v err=%v", header, err)
	}
}
