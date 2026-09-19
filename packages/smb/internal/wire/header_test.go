package wire

import (
	"bytes"
	"errors"
	"testing"
)

var testLimits = Limits{MaxBytes: 1 << 20, MaxCommands: 32, MaxContexts: 16}

func requestPacket(command uint16, size uint16, bodyLength int) []byte {
	b := make([]byte, 64+bodyLength)
	_ = (Header{Command: command, MessageID: 5, SessionID: 7, TreeID: 3, Credits: 1}).Encode(b)
	le.PutUint16(b[64:], size)
	return b
}

func parseOne(t *testing.T, p []byte) Request {
	t.Helper()
	rs, err := ParseFrame(p, testLimits)
	if err != nil || len(rs) != 1 {
		t.Fatalf("parse: %v (%d)", err, len(rs))
	}
	return rs[0]
}

func TestHeaderRoundTrip(t *testing.T) {
	for _, async := range []bool{false, true} {
		h := Header{CreditCharge: 3, Status: 0xdeadbeef, Command: Create, Credits: 5, Flags: FlagSigned, NextCommand: 80, MessageID: 9, ProcessID: 17, TreeID: 19, SessionID: 23, Signature: [16]byte{9}}
		if async {
			h.Flags |= FlagAsync
			h.AsyncID = 27
			h.ProcessID = 0
			h.TreeID = 0
		}
		p := make([]byte, 64)
		if err := h.Encode(p); err != nil {
			t.Fatal(err)
		}
		got, err := ParseHeader(p)
		if err != nil || got != h {
			t.Fatalf("header: %+v %v", got, err)
		}
	}
	if err := (Header{}).Encode(make([]byte, 63)); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	for _, p := range [][]byte{nil, make([]byte, 64), append([]byte("\xfeSMB"), make([]byte, 60)...)} {
		if _, err := ParseHeader(p); err == nil {
			t.Fatal("bad header accepted")
		}
	}
}

func TestCompoundFrame(t *testing.T) {
	a := requestPacket(Echo, 4, 8)
	le.PutUint32(a[20:24], uint32(len(a)))
	b := requestPacket(Echo, 4, 4)
	le.PutUint32(b[16:20], FlagRelated)
	packet := append(a, b...)
	rs, err := ParseFrame(packet, testLimits)
	if err != nil || len(rs) != 2 || len(rs[0].Packet) != 72 || rs[1].Header.Flags != FlagRelated {
		t.Fatalf("compound: %v, %+v", err, rs)
	}
	for _, limits := range []Limits{{0, 32, 16}, {1 << 20, 0, 16}, {1 << 20, 32, 0}, {80, 32, 16}, {1 << 20, 1, 16}} {
		if _, err := ParseFrame(packet, limits); err == nil {
			t.Fatal("limit ignored")
		}
	}
	for _, next := range []uint32{1, 64, 71, 80, 0xffffffff} {
		p := bytes.Clone(packet)
		le.PutUint32(p[20:24], next)
		if _, err := ParseFrame(p, testLimits); err == nil {
			t.Fatalf("next %d accepted", next)
		}
	}
	p := bytes.Clone(packet)
	le.PutUint32(p[16:20], FlagRelated)
	if _, err := ParseFrame(p, testLimits); err == nil {
		t.Fatal("first related accepted")
	}
	p = bytes.Clone(packet)
	le.PutUint32(p[16:20], FlagResponse)
	if _, err := ParseFrame(p, testLimits); err == nil {
		t.Fatal("response accepted")
	}
	if _, err := ParseFrame(packet[:65], testLimits); err == nil {
		t.Fatal("short body accepted")
	}
}

func TestResponseHeader(t *testing.T) {
	body := []byte{4, 0, 0, 0}
	packet := EncodeResponse(Header{Command: Echo, MessageID: 99}, body)
	h, err := ParseHeader(packet)
	if err != nil || h.Flags&FlagResponse == 0 || h.MessageID != 99 || !bytes.Equal(packet[64:], body) {
		t.Fatalf("response: %+v, %v", h, err)
	}
}
