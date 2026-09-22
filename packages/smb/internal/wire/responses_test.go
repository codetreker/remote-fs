package wire

import (
	"bytes"
	"testing"
)

func TestNegotiationResponse(t *testing.T) {
	negotiation := Negotiation{
		SecurityMode: 3, Dialect: Dialect311, ServerGUID: [16]byte{9},
		MaxReadSize: 65536, MaxWriteSize: 65536, MaxTransactSize: 65536,
		Token: []byte{1, 2, 3},
		Contexts: []Context{
			{Type: ContextPreauthIntegrity, Data: []byte{1, 0, 0, 0, 1, 0}},
			{Type: ContextSigning, Data: []byte{1, 0, 1, 0}},
		},
	}
	body, err := NegotiateResponseBody(negotiation)
	if err != nil {
		t.Fatal(err)
	}
	offset := int(le.Uint32(body[60:64])) - HeaderSize
	if le.Uint16(body) != 65 || body[8] != 9 || le.Uint16(body[6:8]) != 2 || le.Uint16(body[56:58]) != 128 || offset&7 != 0 || le.Uint16(body[offset:]) != ContextPreauthIntegrity || le.Uint16(body[offset+16:]) != ContextSigning {
		t.Fatalf("negotiation %x", body)
	}
	negotiation.Dialect = Dialect300
	if _, err := NegotiateResponseBody(negotiation); err == nil {
		t.Fatal("legacy contexts accepted")
	}
	negotiation.Contexts = nil
	negotiation.Token = nil
	if _, err := NegotiateResponseBody(negotiation); err != nil {
		t.Fatal(err)
	}
	negotiation.Token = make([]byte, 65536)
	if _, err := NegotiateResponseBody(negotiation); err == nil {
		t.Fatal("oversized token accepted")
	}
	negotiation.Token = nil
	negotiation.Dialect = Dialect311
	negotiation.Contexts = []Context{{Type: ContextPreauthIntegrity, Data: make([]byte, 65536)}}
	if _, err := NegotiateResponseBody(negotiation); err == nil {
		t.Fatal("oversized context accepted")
	}
}

func TestControlResponses(t *testing.T) {
	body, err := SessionSetupResponseBody(2, []byte("token"))
	if err != nil || le.Uint16(body[4:6]) != 72 || string(body[8:]) != "token" {
		t.Fatalf("setup %x %v", body, err)
	}
	if _, err := SessionSetupResponseBody(0, make([]byte, 65536)); err == nil {
		t.Fatal("oversized token accepted")
	}
	if body, err := SessionSetupResponseBody(0, nil); err != nil || len(body) != 8 {
		t.Fatalf("empty setup %x %v", body, err)
	}
	body = TreeConnectResponseBody(1, 0x30, 8, 0x123)
	if le.Uint16(body) != 16 || body[2] != 1 || le.Uint32(body[12:]) != 0x123 {
		t.Fatal("tree response")
	}
	if !bytes.Equal(EmptyResponseBody(), []byte{4, 0, 0, 0}) || !bytes.Equal(ErrorResponseBody(), []byte{9, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("fixed response")
	}
}

func TestFileResponses(t *testing.T) {
	information := FileInformation{
		CreationTime: 1, AccessTime: 2, WriteTime: 3, ChangeTime: 4,
		AllocationSize: 5, EndOfFile: 6, Attributes: 7,
	}
	body := CreateResponseBody(CreateResult{Action: 2, FileInformation: information, FileID: FileID{99}})
	if len(body) != 88 || le.Uint16(body) != 89 || le.Uint64(body[48:56]) != 6 || body[64] != 99 {
		t.Fatalf("CREATE response = %x", body)
	}
	body = CloseResponseBody(1, information)
	if len(body) != 60 || le.Uint16(body) != 60 || le.Uint16(body[2:4]) != 1 || le.Uint64(body[48:56]) != 6 {
		t.Fatalf("CLOSE response = %x", body)
	}
	body = ReadResponseBody([]byte("abc"), 12)
	if le.Uint16(body) != 17 || body[2] != HeaderSize+16 || le.Uint32(body[4:8]) != 3 || le.Uint32(body[8:12]) != 12 || string(body[16:]) != "abc" {
		t.Fatalf("READ response = %x", body)
	}
	body = WriteResponseBody(12, 34)
	if le.Uint16(body) != 17 || le.Uint32(body[4:8]) != 12 || le.Uint32(body[8:12]) != 34 {
		t.Fatalf("WRITE response = %x", body)
	}
	body = BufferResponseBody([]byte("abc"))
	if le.Uint16(body) != 9 || le.Uint16(body[2:4]) != HeaderSize+8 || le.Uint32(body[4:8]) != 3 || string(body[8:]) != "abc" {
		t.Fatalf("buffer response = %x", body)
	}
	if body = BufferResponseBody(nil); len(body) != 8 || le.Uint16(body[2:4]) != 0 {
		t.Fatalf("empty buffer response = %x", body)
	}
}
