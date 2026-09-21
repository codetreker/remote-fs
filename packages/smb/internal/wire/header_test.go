package wire

import (
	"bytes"
	"errors"
	"testing"
)

var testLimits = Limits{MaxBytes: 1 << 20, MaxCommands: 32, MaxContexts: 16}

func requestPacket(command, structureSize uint16, bodyLength int) []byte {
	packet := make([]byte, HeaderSize+bodyLength)
	_ = (Header{Command: command, MessageID: 5, SessionID: 7, TreeID: 3, Credits: 1}).Encode(packet)
	le.PutUint16(packet[HeaderSize:], structureSize)
	return packet
}

func parseOne(t *testing.T, packet []byte) Request {
	t.Helper()
	requests, err := ParseFrame(packet, testLimits)
	if err != nil || len(requests) != 1 {
		t.Fatalf("parse: %v (%d)", err, len(requests))
	}
	return requests[0]
}

func TestHeaderRoundTrip(t *testing.T) {
	for _, async := range []bool{false, true} {
		header := Header{CreditCharge: 3, Status: 0xdeadbeef, Command: Create, Credits: 5, Flags: FlagSigned, NextCommand: 80, MessageID: 9, ProcessID: 17, TreeID: 19, SessionID: 23, Signature: [16]byte{9}}
		if async {
			header.Flags |= FlagAsync
			header.AsyncID, header.ProcessID, header.TreeID = 27, 0, 0
		}
		packet := make([]byte, HeaderSize)
		if err := header.Encode(packet); err != nil {
			t.Fatal(err)
		}
		got, err := ParseHeader(packet)
		if err != nil || got != header {
			t.Fatalf("header: %+v %v", got, err)
		}
	}
	if err := (Header{}).Encode(make([]byte, HeaderSize-1)); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	for _, packet := range [][]byte{nil, make([]byte, HeaderSize), append([]byte("\xfeSMB"), make([]byte, HeaderSize-4)...)} {
		if _, err := ParseHeader(packet); err == nil {
			t.Fatal("bad header accepted")
		}
	}
}

func TestCompoundFrame(t *testing.T) {
	first := requestPacket(Echo, 4, 8)
	le.PutUint32(first[20:24], uint32(len(first)))
	second := requestPacket(Echo, 4, 4)
	le.PutUint32(second[16:20], FlagRelated)
	packet := append(first, second...)
	requests, err := ParseFrame(packet, testLimits)
	if err != nil || len(requests) != 2 || len(requests[0].Packet) != 72 || requests[1].Header.Flags != FlagRelated {
		t.Fatalf("compound: %v, %+v", err, requests)
	}
	if _, err := ParseFrame(packet, Limits{MaxBytes: len(packet), MaxCommands: 2, MaxContexts: 1}); err != nil {
		t.Fatalf("exact frame and command limits failed: %v", err)
	}
	if _, err := ParseFrame(packet, Limits{MaxBytes: len(packet) - 1, MaxCommands: 2, MaxContexts: 1}); err == nil {
		t.Fatal("accepted frame one byte above configured limit")
	}
	if _, err := ParseFrame(packet, Limits{MaxBytes: len(packet), MaxCommands: 1, MaxContexts: 1}); err == nil {
		t.Fatal("accepted command one above configured limit")
	}
	for _, limits := range []Limits{
		{MaxCommands: 32, MaxContexts: 16},
		{MaxBytes: 1 << 20, MaxContexts: 16},
		{MaxBytes: 1 << 20, MaxCommands: 32},
		{MaxBytes: 80, MaxCommands: 32, MaxContexts: 16},
		{MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 16},
		{MaxBytes: 0x1000000, MaxCommands: 32, MaxContexts: 16},
	} {
		if _, err := ParseFrame(packet, limits); err == nil {
			t.Fatal("limit ignored")
		}
	}
	for _, next := range []uint32{1, 64, 71, 80, 0xffffffff} {
		malformed := bytes.Clone(packet)
		le.PutUint32(malformed[20:24], next)
		if _, err := ParseFrame(malformed, testLimits); err == nil {
			t.Fatalf("next %d accepted", next)
		}
	}
	firstRelated := bytes.Clone(packet)
	le.PutUint32(firstRelated[16:20], FlagRelated)
	if _, err := ParseFrame(firstRelated, testLimits); err == nil {
		t.Fatal("first related command accepted")
	}
	response := bytes.Clone(packet)
	le.PutUint32(response[16:20], FlagResponse)
	if _, err := ParseFrame(response, testLimits); err == nil {
		t.Fatal("response accepted as request")
	}
	if _, err := ParseFrame(packet[:65], testLimits); err == nil {
		t.Fatal("short body accepted")
	}
}

func TestCompoundNextCommandDoesNotOverlapHeaders(t *testing.T) {
	first := requestPacket(Echo, 4, 8)
	second := requestPacket(Echo, 4, 4)
	packet := append(first, second...)
	for _, next := range []uint32{64, 65, 71, uint32(len(first) + 8), 0xfffffff8} {
		malformed := bytes.Clone(packet)
		le.PutUint32(malformed[20:24], next)
		if _, err := ParseFrame(malformed, testLimits); err == nil {
			t.Fatalf("accepted overlapping or unaligned next command %d", next)
		}
	}
}

func TestCompoundStyleCannotChangeWithinAFrame(t *testing.T) {
	compound := func(flags ...uint32) []byte {
		var packet []byte
		for index, flag := range flags {
			command := requestPacket(Echo, 4, 8)
			le.PutUint32(command[16:20], flag)
			if index+1 < len(flags) {
				le.PutUint32(command[20:24], uint32(len(command)))
			} else {
				command = command[:HeaderSize+4]
			}
			packet = append(packet, command...)
		}
		return packet
	}
	for _, flags := range [][]uint32{{0, 0, 0}, {0, FlagRelated, FlagRelated}} {
		if requests, err := ParseFrame(compound(flags...), testLimits); err != nil || len(requests) != 3 {
			t.Fatalf("valid compound style %x: %d %v", flags, len(requests), err)
		}
	}
	for _, flags := range [][]uint32{{0, FlagRelated, 0}, {0, 0, FlagRelated}} {
		if _, err := ParseFrame(compound(flags...), testLimits); err == nil {
			t.Fatalf("accepted mixed compound style %x", flags)
		}
	}
}

func TestResponseHeader(t *testing.T) {
	body := []byte{4, 0, 0, 0}
	packet := EncodeResponse(Header{Command: Echo, MessageID: 99}, body)
	header, err := ParseHeader(packet)
	if err != nil || header.Flags&FlagResponse == 0 || header.MessageID != 99 || !bytes.Equal(packet[HeaderSize:], body) {
		t.Fatalf("response: %+v, %v", header, err)
	}
}

func FuzzParseFrame(f *testing.F) {
	f.Add(requestPacket(Echo, 4, 4))
	f.Fuzz(func(_ *testing.T, packet []byte) { _, _ = ParseFrame(packet, testLimits) })
}
