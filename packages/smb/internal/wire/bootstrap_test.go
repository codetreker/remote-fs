package wire

import (
	"bytes"
	"testing"
)

func multiProtocolPacket(names ...string) []byte {
	packet := make([]byte, 35)
	copy(packet, "\xffSMB")
	packet[4] = 0x72
	for _, name := range names {
		packet = append(packet, 2)
		packet = append(packet, name...)
		packet = append(packet, 0)
	}
	le.PutUint16(packet[33:35], uint16(len(packet)-35))
	return packet
}

func TestMultiProtocolNegotiate(t *testing.T) {
	for _, packet := range [][]byte{
		multiProtocolPacket("SMB 2.???"),
		multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???"),
		multiProtocolPacket("SMB 2.???", "other"),
	} {
		if err := ParseMultiProtocolNegotiate(packet); err != nil {
			t.Fatal(err)
		}
	}
	packet := multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???")
	if len(packet) != 69 {
		t.Fatal("native bootstrap fixture must be 69 SMB bytes")
	}
	for length := range len(packet) {
		if err := ParseMultiProtocolNegotiate(packet[:length]); err == nil {
			t.Fatalf("accepted truncation %d", length)
		}
	}
	for _, mutate := range []func([]byte){
		func(value []byte) { value[0] = 0xfe },
		func(value []byte) { value[4] = 0x75 },
		func(value []byte) { value[9] |= 0x80 },
		func(value []byte) { value[32] = 1 },
		func(value []byte) { value[33] = 0 },
		func(value []byte) { value[35] = 1 },
		func(value []byte) { value[len(value)-1] = 1 },
	} {
		malformed := bytes.Clone(packet)
		mutate(malformed)
		if err := ParseMultiProtocolNegotiate(malformed); err == nil {
			t.Fatal("accepted malformed bootstrap")
		}
	}
	for _, packet := range [][]byte{
		multiProtocolPacket(), multiProtocolPacket(""), multiProtocolPacket("SMB 2.002"),
		multiProtocolPacket("NT LM 0.12"), multiProtocolPacket("smb 2.???"),
		multiProtocolPacket("SMB 2.???\x00junk"),
	} {
		if err := ParseMultiProtocolNegotiate(packet); err == nil {
			t.Fatal("accepted missing or invalid wildcard")
		}
	}
}

func FuzzMultiProtocolNegotiate(f *testing.F) {
	f.Add(multiProtocolPacket("NT LM 0.12", "SMB 2.002", "SMB 2.???"))
	f.Add(multiProtocolPacket("SMB 2.???"))
	f.Fuzz(func(_ *testing.T, packet []byte) { _ = ParseMultiProtocolNegotiate(packet) })
}
