package wire

import (
	"bytes"
	"testing"
)

func TestUTF16(t *testing.T) {
	for _, value := range []string{"", "abc", "目录\\😀"} {
		got, err := DecodeUTF16(EncodeUTF16(value))
		if err != nil || got != value {
			t.Fatalf("%q: %q %v", value, got, err)
		}
	}
	for _, data := range [][]byte{{1}, {0, 0}, {0, 0xd8}, {0, 0xdc}, {0, 0xd8, 65, 0}} {
		if _, err := DecodeUTF16(data); err == nil {
			t.Fatalf("accepted %x", data)
		}
	}
}

func negotiatePacket(contexts ...Context) []byte {
	bodyLength := 38
	if len(contexts) != 0 {
		bodyLength = 40
		for index, context := range contexts {
			bodyLength += 8 + len(context.Data)
			if index+1 < len(contexts) {
				bodyLength = align8(HeaderSize+bodyLength) - HeaderSize
			}
		}
	}
	packet := requestPacket(Negotiate, 36, bodyLength)
	le.PutUint16(packet[66:68], 1)
	le.PutUint16(packet[100:102], Dialect311)
	if len(contexts) == 0 {
		return packet
	}
	le.PutUint32(packet[92:96], 104)
	le.PutUint16(packet[96:98], uint16(len(contexts)))
	offset := 104
	for index, context := range contexts {
		le.PutUint16(packet[offset:offset+2], context.Type)
		le.PutUint16(packet[offset+2:offset+4], uint16(len(context.Data)))
		copy(packet[offset+8:], context.Data)
		offset += 8 + len(context.Data)
		if index+1 < len(contexts) {
			offset = align8(offset)
		}
	}
	return packet
}

func TestNegotiate(t *testing.T) {
	preauth := Context{Type: ContextPreauthIntegrity, Data: []byte{1, 0, 0, 0, 1, 0}}
	packet := negotiatePacket(preauth)
	request := parseOne(t, packet)
	negotiation, err := request.Negotiate()
	if err != nil || len(negotiation.Dialects) != 1 || negotiation.Dialects[0] != Dialect311 || len(negotiation.Contexts) != 1 {
		t.Fatalf("negotiate: %+v %v", negotiation, err)
	}

	unknown := Context{Type: 0xffff, Data: []byte{1}}
	packet = negotiatePacket(preauth, unknown, unknown)
	if _, err := parseOne(t, packet).Negotiate(); err != nil {
		t.Fatalf("bounded duplicate unknown contexts were not ignored: %v", err)
	}
	requests, err := ParseFrame(packet, Limits{MaxBytes: len(packet), MaxCommands: 1, MaxContexts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requests[0].Negotiate(); err != nil {
		t.Fatalf("exact context limit failed: %v", err)
	}
	requests, err = ParseFrame(packet, Limits{MaxBytes: len(packet), MaxCommands: 1, MaxContexts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requests[0].Negotiate(); err == nil {
		t.Fatal("accepted context one above configured limit")
	}
	for _, contexts := range [][]Context{
		nil,
		{preauth, preauth},
		{preauth, {Type: ContextSigning, Data: []byte{1, 0, 1, 0}}, {Type: ContextSigning, Data: []byte{1, 0, 1, 0}}},
	} {
		if _, err := parseOne(t, negotiatePacket(contexts...)).Negotiate(); err == nil {
			t.Fatal("accepted missing or duplicate required context")
		}
	}

	packet = negotiatePacket(preauth)
	for _, mutate := range []func([]byte){
		func(value []byte) { le.PutUint16(value[66:68], 0) },
		func(value []byte) { le.PutUint16(value[66:68], 0xffff) },
		func(value []byte) { le.PutUint32(value[92:96], 105) },
		func(value []byte) { le.PutUint32(value[92:96], 0xfffffff8) },
		func(value []byte) { le.PutUint16(value[96:98], 17) },
		func(value []byte) { le.PutUint16(value[106:108], 0xffff) },
		func(value []byte) { le.PutUint32(value[16:20], FlagSigned) },
	} {
		malformed := bytes.Clone(packet)
		mutate(malformed)
		if _, err := parseOne(t, malformed).Negotiate(); err == nil {
			t.Fatal("invalid negotiate accepted")
		}
	}
}

func TestNegotiateCapabilityBodies(t *testing.T) {
	preauth, err := DecodePreauthCapabilities([]byte{2, 0, 3, 0, 1, 0, 9, 0, 4, 5, 6})
	if err != nil || len(preauth.HashAlgorithms) != 2 || preauth.HashAlgorithms[0] != HashSHA512 || !bytes.Equal(preauth.Salt, []byte{4, 5, 6}) {
		t.Fatalf("preauth: %+v %v", preauth, err)
	}
	signing, err := DecodeSigningCapabilities([]byte{2, 0, 1, 0, 2, 0})
	if err != nil || len(signing.Algorithms) != 2 || signing.Algorithms[0] != SigningAESCMAC {
		t.Fatalf("signing: %+v %v", signing, err)
	}
	for _, data := range [][]byte{nil, {0, 0, 0, 0}, {1, 0, 2, 0, 1, 0}, {2, 0, 0, 0, 1, 0}} {
		if _, err := DecodePreauthCapabilities(data); err == nil {
			t.Fatalf("accepted malformed preauth %x", data)
		}
	}
	for _, data := range [][]byte{nil, {0, 0}, {1, 0}, {1, 0, 1, 0, 0}} {
		if _, err := DecodeSigningCapabilities(data); err == nil {
			t.Fatalf("accepted malformed signing %x", data)
		}
	}
}

func TestSessionSetupAndTree(t *testing.T) {
	packet := requestPacket(SessionSetup, 25, 27)
	le.PutUint16(packet[76:78], 88)
	le.PutUint16(packet[78:80], 3)
	copy(packet[88:], "abc")
	setup, err := parseOne(t, packet).SessionSetup()
	if err != nil || string(setup.Token) != "abc" {
		t.Fatalf("token: %+v %v", setup, err)
	}
	le.PutUint16(packet[76:78], 80)
	if _, err := parseOne(t, packet).SessionSetup(); err == nil {
		t.Fatal("token in fixed request body accepted")
	}
	maximumToken := requestPacket(SessionSetup, 25, 24+65535)
	le.PutUint16(maximumToken[76:78], 88)
	le.PutUint16(maximumToken[78:80], 65535)
	if setup, err := parseOne(t, maximumToken).SessionSetup(); err != nil || len(setup.Token) != 65535 {
		t.Fatalf("maximum token: %d %v", len(setup.Token), err)
	}
	truncatedToken := maximumToken[:len(maximumToken)-1]
	if _, err := parseOne(t, truncatedToken).SessionSetup(); err == nil {
		t.Fatal("accepted declared token one byte beyond frame")
	}

	path := EncodeUTF16("\\\\localhost\\share")
	packet = requestPacket(TreeConnect, 9, 8+len(path))
	le.PutUint16(packet[68:70], 72)
	le.PutUint16(packet[70:72], uint16(len(path)))
	copy(packet[72:], path)
	got, err := parseOne(t, packet).TreePath()
	if err != nil || got != "\\\\localhost\\share" {
		t.Fatalf("path %q %v", got, err)
	}
	packet[66] = 1
	if _, err := parseOne(t, packet).TreePath(); err == nil {
		t.Fatal("tree-connect extension accepted")
	}
	for _, command := range []uint16{Logoff, TreeDisconnect, Cancel, Echo} {
		if err := parseOne(t, requestPacket(command, 4, 4)).Empty(); err != nil {
			t.Fatal(err)
		}
	}
	if err := parseOne(t, requestPacket(Create, 4, 4)).Empty(); err == nil {
		t.Fatal("nonempty command accepted as fixed empty body")
	}
}

func FuzzNegotiate(f *testing.F) {
	f.Add(negotiatePacket(Context{Type: ContextPreauthIntegrity, Data: []byte{1, 0, 0, 0, 1, 0}}))
	f.Fuzz(func(t *testing.T, packet []byte) {
		requests, err := ParseFrame(packet, testLimits)
		if err == nil && len(requests) != 0 {
			_, _ = requests[0].Negotiate()
		}
	})
}
