package wire

import (
	"bytes"
	"testing"
)

func TestNegotiationResponse(t *testing.T) {
	n := Negotiation{SecurityMode: 3, Dialect: Dialect311, ServerGUID: [16]byte{9}, MaxReadSize: 65536, MaxWriteSize: 65536, MaxTransactSize: 65536, Token: []byte{1, 2, 3}, Contexts: []Context{{Type: 1, Data: []byte{1, 0, 0, 0, 1, 0}}, {Type: 8, Data: []byte{1, 0, 1, 0}}}}
	b, err := NegotiateResponseBody(n)
	if err != nil {
		t.Fatal(err)
	}
	off := int(le.Uint32(b[60:64])) - 64
	if le.Uint16(b) != 65 || b[8] != 9 || le.Uint16(b[6:8]) != 2 || le.Uint16(b[56:58]) != 128 || off&7 != 0 || le.Uint16(b[off:]) != 1 || le.Uint16(b[off+16:]) != 8 {
		t.Fatalf("negotiation %x", b)
	}
	n.Dialect = Dialect300
	if _, err := NegotiateResponseBody(n); err == nil {
		t.Fatal("legacy contexts accepted")
	}
	n.Contexts = nil
	n.Token = nil
	if _, err := NegotiateResponseBody(n); err != nil {
		t.Fatal(err)
	}
	n.Token = make([]byte, 65536)
	if _, err := NegotiateResponseBody(n); err == nil {
		t.Fatal("oversized token accepted")
	}
	n.Token = nil
	n.Dialect = Dialect311
	n.Contexts = []Context{{Type: 1, Data: make([]byte, 65536)}}
	if _, err := NegotiateResponseBody(n); err == nil {
		t.Fatal("oversized context accepted")
	}
}

func TestResponses(t *testing.T) {
	b, err := SessionSetupResponseBody(2, []byte("token"))
	if err != nil || le.Uint16(b[4:6]) != 72 || string(b[8:]) != "token" {
		t.Fatalf("setup %x %v", b, err)
	}
	if _, err := SessionSetupResponseBody(0, make([]byte, 65536)); err == nil {
		t.Fatal("oversized token accepted")
	}
	if b, err := SessionSetupResponseBody(0, nil); err != nil || len(b) != 8 {
		t.Fatalf("empty setup %x %v", b, err)
	}
	b = TreeConnectResponseBody(1, 0x30, 8, 0x123)
	if le.Uint16(b) != 16 || b[2] != 1 || le.Uint32(b[12:]) != 0x123 {
		t.Fatal("tree")
	}
	f := FileInformation{CreationTime: 1, AccessTime: 2, WriteTime: 3, ChangeTime: 4, AllocationSize: 5, EndOfFile: 6, Attributes: 7}
	b = CreateResponseBody(CreateResult{Action: 2, FileInformation: f, FileID: FileID{99}})
	if len(b) != 88 || le.Uint16(b) != 89 || le.Uint64(b[48:56]) != 6 || b[64] != 99 {
		t.Fatalf("create %x", b)
	}
	b = CloseResponseBody(1, f)
	if len(b) != 60 || le.Uint16(b) != 60 || le.Uint16(b[2:4]) != 1 || le.Uint64(b[48:56]) != 6 {
		t.Fatalf("close %x", b)
	}
	b = ReadResponseBody([]byte("abc"), 12)
	if le.Uint16(b) != 17 || b[2] != 80 || le.Uint32(b[4:8]) != 3 || string(b[16:]) != "abc" {
		t.Fatal("read")
	}
	b = WriteResponseBody(12, 34)
	if le.Uint16(b) != 17 || le.Uint32(b[4:8]) != 12 || le.Uint32(b[8:12]) != 34 {
		t.Fatal("write")
	}
	b = BufferResponseBody([]byte("abc"))
	if le.Uint16(b) != 9 || le.Uint16(b[2:4]) != 72 || le.Uint32(b[4:8]) != 3 || string(b[8:]) != "abc" {
		t.Fatal("buffer")
	}
	if b = BufferResponseBody(nil); len(b) != 8 || le.Uint16(b[2:4]) != 0 {
		t.Fatal("empty buffer")
	}
	b = IOCTLResponseBody(7, FileID{8}, 9, []byte("abc"))
	if le.Uint16(b) != 49 || le.Uint32(b[4:8]) != 7 || b[8] != 8 || le.Uint32(b[32:36]) != 112 || string(b[48:]) != "abc" {
		t.Fatal("ioctl")
	}
	if b = IOCTLResponseBody(7, FileID{}, 0, nil); len(b) != 48 || le.Uint32(b[32:36]) != 0 {
		t.Fatal("empty ioctl")
	}
	if !bytes.Equal(EmptyResponseBody(), []byte{4, 0, 0, 0}) || !bytes.Equal(SetInfoResponseBody(), []byte{2, 0}) || !bytes.Equal(ErrorResponseBody(), []byte{9, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("fixed responses")
	}
}

func TestNotifyInformation(t *testing.T) {
	b := NotifyInformation([]Notification{{Action: 1, Name: "a"}, {Action: 2, Name: "b"}})
	next := int(le.Uint32(b))
	if next != 16 || le.Uint32(b[4:8]) != 1 || le.Uint32(b[8:12]) != 2 || le.Uint32(b[next:]) != 0 || le.Uint32(b[next+4:]) != 2 {
		t.Fatalf("notify %x", b)
	}
	if got := NotifyInformation(nil); len(got) != 0 {
		t.Fatal(got)
	}
}
