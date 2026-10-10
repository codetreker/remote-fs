//go:build linux

package smb

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func startRealFileProtocolServer(t *testing.T) (realSMBFixture, net.Conn) {
	t.Helper()
	fixture := newRealSMBFixture(t)
	server, err := New(Config{
		Authenticator:     &protocolAuthenticator{},
		AuthorizeIdentity: IdentityAuthorizerFunc(func(context.Context, Principal) error { return nil }),
		Authorize:         authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }),
		Limits:            DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server.nameComparer = testNameCompare
	fixture.server = server
	identity, err := fixture.volume.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Publish(Share{Name: "data", Volume: "files", BackendVolume: identity.Volume, RootNodeID: identity.RootNodeID, Backend: fixture.volume}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background(), listener) }()
	client, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Error("Serve did not drain")
		}
	})
	return fixture, client
}

func TestSignedRelatedCreateCloseReportsAuthoritativeAllocation(t *testing.T) {
	fixture, client := startRealFileProtocolServer(t)
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	for index, test := range []struct {
		size       int
		allocation uint64
	}{
		{0, 0}, {1, 4096}, {4096, 4096}, {4097, 8192},
	} {
		t.Run(fmt.Sprintf("size-%d", test.size), func(t *testing.T) {
			name := fmt.Sprintf("size-%d", test.size)
			if test.size == 0 {
				if err := fixture.volume.Create(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			} else if err := fixture.volume.Write(t.Context(), name, make([]byte, test.size)); err != nil {
				t.Fatal(err)
			}
			attr, err := fixture.volume.Stat(t.Context(), name)
			if err != nil || !attr.AllocationKnown || uint64(attr.AllocationSize) != test.allocation {
				t.Fatalf("authority allocation for size %d = %+v, %v", test.size, attr, err)
			}

			create := createRequestForTest(name, 1, accessReadData|accessReadAttr)
			closeBody := make([]byte, 24)
			binary.LittleEndian.PutUint16(closeBody, 24)
			binary.LittleEndian.PutUint16(closeBody[2:], 1)
			copy(closeBody[8:], wire.InvalidFileID[:])
			headers := []wire.Header{
				{Command: wire.Create, MessageID: uint64(4 + index*2), SessionID: sessionID, TreeID: treeID, Credits: 1},
				{Command: wire.Close, MessageID: uint64(5 + index*2), Credits: 1},
			}
			packet := compoundRequest(t, key, headers, [][]byte{create.Body, closeBody}, true)
			sendFrame(t, client, packet)
			response := readFrame(t, client)
			first, err := wire.ParseHeader(response)
			if err != nil || first.Status != statusOK || first.NextCommand != 152 || first.Flags&wire.FlagRelated != 0 {
				t.Fatalf("CREATE response header = %+v, %v", first, err)
			}
			offset := int(first.NextCommand)
			second, err := wire.ParseHeader(response[offset:])
			if err != nil || second.Status != statusOK || second.Flags&wire.FlagRelated == 0 || second.NextCommand != 0 {
				t.Fatalf("CLOSE response header = %+v, %v", second, err)
			}
			if key.Verify(response[:offset]) != nil || key.Verify(response[offset:]) != nil {
				t.Fatal("compound response signature failed")
			}
			if len(response) != 152+124 || len(response) > responseBudget(wire.Request{Header: headers[0]})+responseBudget(wire.Request{Header: headers[1]}) {
				t.Fatalf("compound response size = %d", len(response))
			}
			created := response[wire.HeaderSize:offset]
			closed := response[offset+wire.HeaderSize:]
			var id wire.FileID
			copy(id[:], created[64:80])
			if id == (wire.FileID{}) || id == wire.InvalidFileID ||
				binary.LittleEndian.Uint16(created) != 89 ||
				binary.LittleEndian.Uint64(created[40:48]) != test.allocation ||
				binary.LittleEndian.Uint64(created[48:56]) != uint64(test.size) ||
				binary.LittleEndian.Uint16(closed) != 60 || binary.LittleEndian.Uint16(closed[2:4]) != 1 ||
				binary.LittleEndian.Uint64(closed[40:48]) != test.allocation ||
				binary.LittleEndian.Uint64(closed[48:56]) != uint64(test.size) {
				t.Fatalf("allocation response for size %d: CREATE=%x CLOSE=%x", test.size, created, closed)
			}
			if handles := fixture.server.Status().Handles; handles != 0 {
				t.Fatalf("related CLOSE retained %d handles", handles)
			}
		})
	}
}
