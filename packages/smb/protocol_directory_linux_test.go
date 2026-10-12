//go:build linux

package smb

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestSignedDirectoryWarningsCloseAndExhaustionRetainsSuppressedClose(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Create(t.Context(), "d/💾long-name.txt"); err != nil {
		t.Fatal(err)
	}
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	message := uint64(4)
	for _, test := range []struct {
		name     string
		requests []wire.Request
		statuses []uint32
	}{
		{"partial", []wire.Request{directoryRequestForTest(wire.InvalidFileID, 12, 0, "*", 13)}, []uint32{statusBufferOverflow}},
		{"no-match", []wire.Request{directoryRequestForTest(wire.InvalidFileID, 12, 0, "absent", 512)}, []uint32{0xc000000f}},
		{"exhausted", []wire.Request{directoryRequestForTest(wire.InvalidFileID, 12, 0, "*", 512), directoryRequestForTest(wire.InvalidFileID, 12, 0, "", 512)}, []uint32{statusOK, 0x80000006}},
	} {
		create := createRequestForTest("d", 1, accessReadData)
		binary.LittleEndian.PutUint32(create.Body[40:], createDirectory)
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], wire.InvalidFileID[:])
		bodies := [][]byte{create.Body}
		headers := []wire.Header{{Command: wire.Create, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}}
		statuses := []uint32{statusOK}
		for index, request := range test.requests {
			headers = append(headers, wire.Header{Command: wire.QueryDirectory, MessageID: message + uint64(index+1), Credits: 1})
			bodies = append(bodies, request.Body)
			statuses = append(statuses, test.statuses[index])
		}
		headers = append(headers, wire.Header{Command: wire.Close, MessageID: message + uint64(len(headers)), Credits: 1})
		bodies = append(bodies, closeBody)
		closeStatus := statuses[len(statuses)-1]
		if closeStatus == statusBufferOverflow {
			closeStatus = statusOK
		}
		statuses = append(statuses, closeStatus)
		sendFrame(t, client, compoundRequest(t, key, headers, bodies, true))
		response := readFrame(t, client)
		offset := 0
		var id wire.FileID
		for index, expected := range headers {
			header, err := wire.ParseHeader(response[offset:])
			if err != nil || header.Command != expected.Command || header.MessageID != expected.MessageID || header.Status != statuses[index] || (header.Flags&wire.FlagRelated != 0) != (index > 0) {
				t.Fatalf("%s compound command=%d header=%+v error=%v", test.name, expected.Command, header, err)
			}
			end := len(response)
			if header.NextCommand != 0 {
				end = offset + int(header.NextCommand)
			}
			if end < offset+64 || end > len(response) || key.Verify(response[offset:end]) != nil {
				t.Fatalf("%s compound response extent/signature", test.name)
			}
			body := response[offset+64 : end]
			if index == 0 {
				copy(id[:], body[64:80])
			}
			if header.Status == statusBufferOverflow {
				if len(body) < 21 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint32(body[4:]) != 13 || binary.LittleEndian.Uint32(body[16:]) != uint32(len(wire.EncodeUTF16("💾long-name.txt"))) || body[20] != wire.EncodeUTF16("💾long-name.txt")[0] {
					t.Fatalf("signed partial warning lost full name length/raw odd prefix=%x", body)
				}
			}
			offset = end
		}
		message += uint64(len(headers))
		wantHandles := 1
		if closeStatus == statusOK {
			wantHandles = 0
		}
		if status := f.server.Status(); status.Handles != wantHandles || (status.RetainedDirectoryBytes != 0) != (wantHandles == 1) {
			t.Fatalf("%s related CLOSE ownership/charge=%+v, want handles=%d", test.name, status, wantHandles)
		}
		if wantHandles == 1 {
			copy(closeBody[8:], id[:])
			sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
			result := readFrame(t, client)
			header, err := wire.ParseHeader(result)
			if err != nil || header.Status != statusOK || key.Verify(result) != nil || f.server.Status().Handles != 0 || f.server.Status().RetainedDirectoryBytes != 0 {
				t.Fatalf("%s independent CLOSE header=%+v error=%v status=%+v", test.name, header, err, f.server.Status())
			}
			message++
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := f.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if status := f.server.Status(); status.RetainedDirectoryBytes != 0 || status.Handles != 0 || status.Trees != 0 || status.Sessions != 0 || !status.Stopped {
		t.Fatalf("Shutdown retained directory protocol resources=%+v", status)
	}
}

func TestSignedDirectorySnapshotsReleaseThroughProtocolRetirement(t *testing.T) {
	for _, retirement := range []string{"tree", "logoff", "disconnect", "shutdown"} {
		t.Run(retirement, func(t *testing.T) {
			f, client := startRealFileProtocolServer(t)
			if err := f.volume.Create(t.Context(), "child"); err != nil {
				t.Fatal(err)
			}
			sessionID, key := authenticateProtocol(t, client)
			treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
			create := createRequestForTest("", 1, accessReadData)
			binary.LittleEndian.PutUint32(create.Body[40:], createDirectory)
			sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
			result := readFrame(t, client)
			header, err := wire.ParseHeader(result)
			if err != nil || header.Status != statusOK || key.Verify(result) != nil || len(result) != 152 {
				t.Fatalf("retirement setup CREATE=%+v error=%v", header, err)
			}
			var id wire.FileID
			copy(id[:], result[128:144])
			query := directoryRequestForTest(id, 12, 2, "*", 512)
			sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryDirectory, MessageID: 5, SessionID: sessionID, TreeID: treeID, Credits: 1}, query.Body))
			result = readFrame(t, client)
			header, err = wire.ParseHeader(result)
			if err != nil || header.Status != statusOK || key.Verify(result) != nil || f.server.Status().RetainedDirectoryBytes <= 1024 {
				t.Fatalf("retirement setup did not retain real directory snapshot: header=%+v error=%v status=%+v", header, err, f.server.Status())
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			switch retirement {
			case "tree", "logoff":
				command, tree := wire.TreeDisconnect, treeID
				if retirement == "logoff" {
					command, tree = wire.Logoff, 0
				}
				sendFrame(t, client, signedRequest(t, key, wire.Header{Command: command, MessageID: 6, SessionID: sessionID, TreeID: tree, Credits: 1}, wire.EmptyResponseBody()))
				result = readFrame(t, client)
				header, err = wire.ParseHeader(result)
				if err != nil || header.Status != statusOK || key.Verify(result) != nil {
					t.Fatalf("retirement command=%d header=%+v error=%v", command, header, err)
				}
			case "disconnect":
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					status := f.server.Status()
					if status.RetainedDirectoryBytes == 0 && status.Handles == 0 {
						break
					}
					select {
					case <-ticker.C:
					case <-ctx.Done():
						t.Fatal("disconnected directory owner did not retire")
					}
				}
			case "shutdown":
				if err := f.server.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if status := f.server.Status(); status.RetainedDirectoryBytes != 0 || status.Handles != 0 {
				t.Fatalf("%s retained snapshot after protocol cleanup=%+v", retirement, status)
			}
		})
	}
}
