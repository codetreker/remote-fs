//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type signedNameBackend struct {
	*nativeDeleteBackend
	mutations atomic.Int32
}

func (b *signedNameBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	raw, err := b.nativeDeleteBackend.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &signedNameSession{nativeDeleteSession: raw.(*nativeDeleteSession), mutations: &b.mutations}, nil
}

type signedNameSession struct {
	*nativeDeleteSession
	mutations *atomic.Int32
}

func (s *signedNameSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	s.mutations.Add(1)
	return s.nativeDeleteSession.MutateName(ctx, command)
}

func TestSignedRelatedNameMutationWarningPreservesClose(t *testing.T) {
	backend := &signedNameBackend{nativeDeleteBackend: NewNativeNameProbeBackend()}
	server, client := startFileProtocolServerForBackend(t, backend)
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
	backend.mu.Lock()
	var renamed storage.NameObservation
	for _, entry := range backend.views[1] {
		if entry.Name == "renamed-long" {
			renamed = backend.binding(entry.Attr.ID)
		}
	}
	backend.mu.Unlock()
	if renamed.State != storage.NameLinked || string(renamed.RawLeaf) != "renamed-long" || backend.mutations.Load() != 1 {
		t.Fatalf("signed compound projected name=%+v mutation calls=%d", renamed, backend.mutations.Load())
	}
	if status := server.Status(); status.Handles != 0 || status.RetainedDirectoryBytes != 0 {
		t.Fatalf("related name CLOSE retained=%+v", status)
	}
}

func TestSignedNameReplacementRejectsReadOnlyTargetWithoutEffects(t *testing.T) {
	backend := &signedNameBackend{nativeDeleteBackend: NewNativeNameProbeBackend()}
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosReadOnly | dosHidden})
	if err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	targetBefore := backend.attr(4).Clone()
	targetBefore.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("readonly"), Data: data}}
	backend.updateAttr(targetBefore)
	sourceBefore := backend.attr(3).Clone()
	sourceName, targetName := backend.binding(3), backend.binding(4)
	backend.mu.Unlock()
	server, client := startFileProtocolServerForBackend(t, backend)
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	create := createRequestForTest("alpha.txt", 1, accessDelete)
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1}, create.Body))
	response := readFrame(t, client)
	header, err := wire.ParseHeader(response)
	if err != nil || header.Command != wire.Create || header.MessageID != 4 || header.Status != statusOK || len(response) != 152 || key.Verify(response) != nil {
		t.Fatalf("signed source CREATE header=%+v err=%v packet=%x", header, err, response)
	}
	var id wire.FileID
	copy(id[:], response[128:144])
	rename := setInfoRequestForTest(id, wire.FileRenameInformation, renameInformationForTest("beta.bin", true))
	sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.SetInfo, MessageID: 5, SessionID: sessionID, TreeID: treeID, Credits: 1}, rename.Body))
	response = readFrame(t, client)
	header, err = wire.ParseHeader(response)
	if err != nil || header.Command != wire.SetInfo || header.MessageID != 5 || header.SessionID != sessionID || header.TreeID != treeID || header.Status != statusDenied || header.Flags&wire.FlagSigned == 0 || header.NextCommand != 0 || key.Verify(response) != nil || !bytes.Equal(response[wire.HeaderSize:], wire.ErrorResponseBody()) {
		t.Fatalf("signed READONLY target replacement header=%+v err=%v packet=%x", header, err, response)
	}
	backend.mu.Lock()
	sourceAfter, targetAfter := backend.attr(3).Clone(), backend.attr(4).Clone()
	sourceBinding, targetBinding := backend.binding(3), backend.binding(4)
	backend.mu.Unlock()
	if !reflect.DeepEqual(sourceAfter, sourceBefore) || !reflect.DeepEqual(targetAfter, targetBefore) || !reflect.DeepEqual(sourceBinding, sourceName) || !reflect.DeepEqual(targetBinding, targetName) || backend.mutations.Load() != 0 {
		t.Fatalf("signed denied replacement changed projection: source=%+v target=%+v bindings=%+v/%+v mutations=%d", sourceAfter, targetAfter, sourceBinding, targetBinding, backend.mutations.Load())
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
	if status := server.Status(); status.Handles != 0 || status.OpeningHandles != 0 || status.RetainedDirectoryBytes != 0 || status.WriteOwners != 0 || status.UnknownWrites != 0 || status.RetainedWriteBytes != 0 || len(status.WriteFailures) != 0 {
		t.Fatalf("rejected signed replacement retained ownership=%+v", status)
	}
}
