package wire

import (
	"bytes"
	"testing"
)

func multiProtocolPacket(names ...string) []byte {
	p := make([]byte, 35)
	copy(p, "\xffSMB")
	p[4] = 0x72
	for _, name := range names {
		p = append(p, 2)
		p = append(p, name...)
		p = append(p, 0)
	}
	le.PutUint16(p[33:35], uint16(len(p)-35))
	return p
}

func TestMultiProtocolNegotiate(t *testing.T) {
	for _, p := range [][]byte{multiProtocolPacket("SMB 2.???"), multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???"), multiProtocolPacket("SMB 2.???", "other")} {
		if err := ParseMultiProtocolNegotiate(p); err != nil {
			t.Fatal(err)
		}
	}
	p := multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???")
	if len(p) != 69 {
		t.Fatal("native bootstrap fixture must be 69 SMB bytes plus TCP length")
	}
	for length := range len(p) {
		if err := ParseMultiProtocolNegotiate(p[:length]); err == nil {
			t.Fatalf("accepted truncation %d", length)
		}
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[0] = 0xfe }, func(b []byte) { b[4] = 0x75 }, func(b []byte) { b[9] |= 0x80 }, func(b []byte) { b[32] = 1 }, func(b []byte) { b[33] = 0 }, func(b []byte) { b[35] = 1 }, func(b []byte) { b[len(b)-1] = 1 }} {
		b := bytes.Clone(p)
		mutate(b)
		if err := ParseMultiProtocolNegotiate(b); err == nil {
			t.Fatal("accepted malformed bootstrap")
		}
	}
	for _, p := range [][]byte{multiProtocolPacket(), multiProtocolPacket(""), multiProtocolPacket("SMB 2.002"), multiProtocolPacket("NT LM 0.12"), multiProtocolPacket("smb 2.???"), multiProtocolPacket("SMB 2.???\x00junk")} {
		if err := ParseMultiProtocolNegotiate(p); err == nil {
			t.Fatal("accepted missing or invalid wildcard")
		}
	}
	p = multiProtocolPacket("SMB 2.???")
	p = append(p, 2)
	le.PutUint16(p[33:35], uint16(len(p)-35))
	if err := ParseMultiProtocolNegotiate(p); err == nil {
		t.Fatal("accepted incomplete dialect")
	}
	p = multiProtocolPacket("SMB 2.???")
	p = append(p, 0)
	if err := ParseMultiProtocolNegotiate(p); err == nil {
		t.Fatal("accepted trailing bytes outside byte count")
	}
}

func FuzzMultiProtocolNegotiate(f *testing.F) {
	f.Add(multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???"))
	f.Add(multiProtocolPacket("SMB 2.???"))
	f.Fuzz(func(t *testing.T, packet []byte) { _ = ParseMultiProtocolNegotiate(packet) })
}
