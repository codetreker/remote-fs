//go:build linux

package smb

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSignedVolumeWarningsPreserveRelatedCloseAndExactVariablePrefixes(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	label := strings.Repeat("L", 29) + "💾Z"
	f.server.exports["DATA"].share.VolumeLabel = &label
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	message := uint64(4)
	for _, test := range []struct {
		name   string
		class  byte
		length uint32
	}{{"volume", wire.FileFsVolumeInformation, 79}, {"attribute", wire.FileFsAttributeInformation, 13}} {
		create := createRequestForTest(test.name, 2, accessReadData)
		query := volumeQueryForTest(wire.InvalidFileID, test.class, test.length)
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], wire.InvalidFileID[:])
		headers := []wire.Header{
			{Command: wire.Create, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1},
			{Command: wire.QueryInfo, MessageID: message + 1, Credits: 1},
			{Command: wire.Close, MessageID: message + 2, Credits: 1},
		}
		sendFrame(t, client, compoundRequest(t, key, headers, [][]byte{create.Body, query.Body, closeBody}, true))
		response := readFrame(t, client)
		offset := 0
		for index, command := range []uint16{wire.Create, wire.QueryInfo, wire.Close} {
			header, err := wire.ParseHeader(response[offset:])
			wantStatus := statusOK
			if index == 1 {
				wantStatus = statusBufferOverflow
			}
			if err != nil || header.Command != command || header.MessageID != message+uint64(index) || header.Status != wantStatus || (header.Flags&wire.FlagRelated != 0) != (index > 0) {
				t.Fatalf("signed compound %s command=%d header=%+v error=%v", test.name, command, header, err)
			}
			end := len(response)
			if header.NextCommand != 0 {
				end = offset + int(header.NextCommand)
			}
			if end < offset+wire.HeaderSize || end > len(response) || key.Verify(response[offset:end]) != nil {
				t.Fatalf("signed compound %s invalid extent/signature", test.name)
			}
			if index == 1 {
				body := response[offset+wire.HeaderSize : end]
				if len(body) < 8+int(test.length) || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint16(body[2:]) != 72 || binary.LittleEndian.Uint32(body[4:]) != test.length {
					t.Fatalf("warning lost QUERY_INFO success envelope=%x", body)
				}
				data := body[8 : 8+test.length]
				if test.class == wire.FileFsVolumeInformation {
					if binary.LittleEndian.Uint32(data[12:]) != uint32(len(wire.EncodeUTF16(label))) || !bytes.Equal(data[18:], wire.EncodeUTF16(label)[:test.length-18]) {
						t.Fatalf("Volume warning rounded odd bytes or shortened declared label=%x", data)
					}
				} else if binary.LittleEndian.Uint32(data) != 6 || binary.LittleEndian.Uint32(data[8:]) <= 1 || data[12] == 0 {
					t.Fatalf("Attribute warning fabricated capabilities or shortened declared name=%x", data)
				}
			}
			offset = end
		}
		if handles := f.server.Status().Handles; handles != 0 {
			t.Fatalf("warning suppressed related CLOSE and retained %d handles", handles)
		}
		message += 3
	}
	create := createRequestForTest("volume", 1, 0)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
	result := readFrame(t, client)
	header, err := wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || len(result) != 152 {
		t.Fatalf("signed zero-access reopen header=%+v error=%v", header, err)
	}
	var id wire.FileID
	copy(id[:], result[128:144])
	message++
	full := volumeQueryForTest(id, wire.FileFsVolumeInformation, 82)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, full.Body))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || len(result) != 154 || responseBudget(full) < len(result) || binary.LittleEndian.Uint32(result[68:]) != 82 {
		t.Fatalf("maximum Volume response exceeded truthful bound: header=%+v size=%d budget=%d error=%v", header, len(result), responseBudget(full), err)
	}
	created, err := storage.VolumeCreatedAt(t.Context(), f.raw)
	if err != nil {
		t.Fatal(err)
	}
	ticks, _ := toFiletime(created)
	if binary.LittleEndian.Uint64(result[72:]) != ticks || !bytes.Equal(result[90:], wire.EncodeUTF16(label)) {
		t.Fatalf("signed Volume lost native creation or exact full label=%x", result[72:])
	}
	serial32 := binary.LittleEndian.Uint32(result[80:])
	volumeFacts := bytes.Clone(result[72:])
	message++
	fileID := queryInfoRequestForTest(id, wire.FileIdInformation, 24)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, fileID.Body))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || len(result) != 96 || uint32(binary.LittleEndian.Uint64(result[72:])) != serial32 {
		t.Fatalf("32-bit volume serial diverged from existing 64-bit file serial=%x", result)
	}
	fileFacts := bytes.Clone(result[72:])
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	copy(closeBody[8:], id[:])
	message++
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || f.server.Status().Handles != 0 {
		t.Fatalf("signed final CLOSE=%+v error=%v", header, err)
	}
	message++
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.TreeDisconnect, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, wire.EmptyResponseBody()))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil {
		t.Fatalf("signed disconnect=%+v error=%v", header, err)
	}
	message++
	treeID = connectProtocolTree(t, client, key, sessionID, message, "data")
	message++
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || len(result) != 152 {
		t.Fatalf("signed reconnect CREATE=%+v error=%v", header, err)
	}
	copy(id[:], result[128:144])
	message++
	query := volumeQueryForTest(id, wire.FileFsVolumeInformation, 82)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, query.Body))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || !bytes.Equal(result[72:], volumeFacts) {
		t.Fatalf("reconnected authority changed immutable volume facts: header=%+v data=%x error=%v", header, result[72:], err)
	}
	message++
	fileID = queryInfoRequestForTest(id, wire.FileIdInformation, 24)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, fileID.Body))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || !bytes.Equal(result[72:], fileFacts) {
		t.Fatalf("reconnected authority changed 64-bit serial or object identity: header=%+v data=%x error=%v", header, result[72:], err)
	}
	message++
	copy(closeBody[8:], id[:])
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
	result = readFrame(t, client)
	header, err = wire.ParseHeader(result)
	if err != nil || header.Status != statusOK || key.Verify(result) != nil || f.server.Status().Handles != 0 {
		t.Fatalf("signed reconnect CLOSE=%+v error=%v", header, err)
	}
}
