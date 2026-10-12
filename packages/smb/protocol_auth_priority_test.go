package smb

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type priorityRenewalAuthenticator struct{ protocolAuthenticator }

func (a *priorityRenewalAuthenticator) Begin(ctx context.Context) (Authentication, error) {
	attempt, err := a.protocolAuthenticator.Begin(ctx)
	return &priorityRenewalAuthentication{Authentication: attempt, renewed: a.begins.Load() > 1}, err
}

type priorityRenewalAuthentication struct {
	Authentication
	renewed bool
}

func (a *priorityRenewalAuthentication) Step(ctx context.Context, token []byte) (AuthenticationResult, error) {
	result, err := a.Authentication.Step(ctx, token)
	if err == nil && !result.Continue && a.renewed {
		result.Principal.Name = "renewed"
		result.SessionKey = []byte("fedcba9876543210")
	}
	return result, err
}

func prioritySetupPacket(message, session uint64, token string, flags uint32) []byte {
	packet := setupPacket(message, session, token)
	binary.LittleEndian.PutUint32(packet[16:20], flags)
	return packet
}

func TestSessionSetupPriorityPreservesTranscriptAndRenewsIdentity(t *testing.T) {
	authenticator := &priorityRenewalAuthenticator{}
	identities := make(chan Principal, 2)
	server, _, client := startConfiguredProtocolServer(t, Config{
		Authenticator: authenticator,
		AuthorizeIdentity: IdentityAuthorizerFunc(func(_ context.Context, principal Principal) error {
			identities <- principal
			return nil
		}),
		Authorize: authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
			principal, ok := PrincipalFromContext(ctx)
			if !ok || principal.Name != "renewed" || !principal.SameIdentity(testPrincipal("0123456789abcdef")) {
				return authz.ErrDenied
			}
			return nil
		}),
		Limits: DefaultLimits(),
	})
	packet := negotiatePacket()
	sendFrame(t, client, packet)
	response := readFrame(t, client)
	hash := signing.Preauth(signing.Preauth([64]byte{}, packet), response)
	packet = prioritySetupPacket(1, 0, "initial", 0x10)
	hash = signing.Preauth(hash, packet)
	sendFrame(t, client, packet)
	response = readFrame(t, client)
	header, err := wire.ParseHeader(response)
	if err != nil || header.Status != statusMoreProcessing || header.SessionID == 0 {
		t.Fatalf("priority challenge = %+v, %v", header, err)
	}
	sessionID := header.SessionID
	hash = signing.Preauth(hash, response)
	packet = prioritySetupPacket(2, sessionID, "proof", 0x70)
	hash = signing.Preauth(hash, packet)
	key, err := signing.NewSession(hash, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	defer key.Destroy()
	sendFrame(t, client, packet)
	response = readFrame(t, client)
	header, err = wire.ParseHeader(response)
	if err != nil || header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("priority authentication lost raw transcript = %+v, %v", header, err)
	}
	if principal := <-identities; principal != testPrincipal("0123456789abcdef") {
		t.Fatalf("initial identity = %+v", principal)
	}
	for index, token := range []string{"initial", "proof"} {
		flags := uint32(0x10)
		want := statusMoreProcessing
		if index == 1 {
			flags, want = 0x70, statusOK
		}
		packet = prioritySetupPacket(uint64(3+index), sessionID, token, flags)
		if err := key.Sign(packet); err != nil {
			t.Fatal(err)
		}
		sendFrame(t, client, packet)
		response = readFrame(t, client)
		header, err = wire.ParseHeader(response)
		if err != nil || header.Status != want || header.SessionID != sessionID || key.Verify(response) != nil {
			t.Fatalf("priority reauthentication %s = %+v, %v", token, header, err)
		}
	}
	principal := <-identities
	if principal.Name != "renewed" || !principal.SameIdentity(testPrincipal("0123456789abcdef")) {
		t.Fatalf("renewed identity = %+v", principal)
	}
	replacementKey, err := signing.NewSession(hash, []byte("fedcba9876543210"))
	if err != nil {
		t.Fatal(err)
	}
	defer replacementKey.Destroy()
	if replacementKey.Verify(response) == nil {
		t.Fatal("reauthentication replaced the established signing key")
	}
	packet = treeConnectPacket(5, sessionID, `\\localhost\data`)
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, client, packet)
	response = readFrame(t, client)
	header, err = wire.ParseHeader(response)
	if err != nil || header.Status != statusOK || header.TreeID == 0 || key.Verify(response) != nil {
		t.Fatalf("renewed caller authorization = %+v, %v", header, err)
	}
	if authenticator.begins.Load() != 2 || authenticator.closed.Load() != 2 || server.Status().Sessions != 1 {
		t.Fatalf("priority authentication ownership: begins=%d closed=%d status=%+v", authenticator.begins.Load(), authenticator.closed.Load(), server.Status())
	}
}

func TestSessionSetupPriorityDoesNotRelaxAuthenticationChecks(t *testing.T) {
	t.Run("unknown initial flag", func(t *testing.T) {
		server, _, client := startProtocolServer(t, DefaultLimits())
		sendFrame(t, client, negotiatePacket())
		_ = readFrame(t, client)
		sendFrame(t, client, prioritySetupPacket(1, 0, "initial", 0x70|0x80))
		header, err := wire.ParseHeader(readFrame(t, client))
		authenticator := server.config.Authenticator.(*protocolAuthenticator)
		if err != nil || header.Status != statusInvalid || header.SessionID != 0 || authenticator.begins.Load() != 0 {
			t.Fatalf("unknown initial flag reached authentication: header=%+v begins=%d error=%v", header, authenticator.begins.Load(), err)
		}
	})
	t.Run("unknown continuation flag", func(t *testing.T) {
		server, _, client := startProtocolServer(t, DefaultLimits())
		sendFrame(t, client, negotiatePacket())
		_ = readFrame(t, client)
		sendFrame(t, client, prioritySetupPacket(1, 0, "initial", 0x10))
		header, err := wire.ParseHeader(readFrame(t, client))
		if err != nil || header.Status != statusMoreProcessing {
			t.Fatalf("initial challenge = %+v, %v", header, err)
		}
		sendFrame(t, client, prioritySetupPacket(2, header.SessionID, "proof", 0x70|0x80))
		header, err = wire.ParseHeader(readFrame(t, client))
		authenticator := server.config.Authenticator.(*protocolAuthenticator)
		if err != nil || header.Status != statusInvalid || authenticator.begins.Load() != 1 || authenticator.closed.Load() != 0 {
			t.Fatalf("unknown continuation completed authentication: header=%+v begins=%d closed=%d error=%v", header, authenticator.begins.Load(), authenticator.closed.Load(), err)
		}
	})
	for _, test := range []struct {
		name   string
		flags  uint32
		signed bool
		mutate bool
		status uint32
	}{
		{name: "unsigned reauthentication", flags: 0x70, status: statusDenied},
		{name: "signed unknown flag", flags: 0x70 | 0x80, signed: true, status: statusInvalid},
		{name: "tampered priority", flags: 0x10, signed: true, mutate: true, status: statusDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, _, client := startProtocolServer(t, DefaultLimits())
			sessionID, key := authenticateProtocol(t, client)
			defer key.Destroy()
			packet := prioritySetupPacket(3, sessionID, "initial", test.flags)
			if test.signed {
				if err := key.Sign(packet); err != nil {
					t.Fatal(err)
				}
			}
			if test.mutate {
				binary.LittleEndian.PutUint32(packet[16:20], wire.FlagSigned|0x70)
			}
			sendFrame(t, client, packet)
			response := readFrame(t, client)
			header, err := wire.ParseHeader(response)
			if err != nil || header.Status != test.status || test.signed && key.Verify(response) != nil {
				t.Fatalf("rejected priority setup = %+v, %v", header, err)
			}
			authenticator := server.config.Authenticator.(*protocolAuthenticator)
			if authenticator.begins.Load() != 1 || authenticator.closed.Load() != 1 {
				t.Fatalf("rejected setup restarted authentication: begins=%d closed=%d", authenticator.begins.Load(), authenticator.closed.Load())
			}
			sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Echo, MessageID: 4, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody()))
			response = readFrame(t, client)
			header, err = wire.ParseHeader(response)
			if err != nil || header.Status != statusOK || key.Verify(response) != nil {
				t.Fatalf("rejected setup damaged established session = %+v, %v", header, err)
			}
		})
	}
}
