package wire

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func leaseData(version uint16, state uint32) []byte {
	size := 32
	if version == LeaseVersion2 {
		size = 52
	}
	b := make([]byte, size)
	for i := range 16 {
		b[i] = byte(i)
	}
	le.PutUint32(b[16:20], state)
	return b
}

func TestLeaseRequestFields(t *testing.T) {
	for _, version := range []uint16{LeaseVersion1, LeaseVersion2} {
		for state := uint32(0); state <= 7; state++ {
			b := leaseData(version, state)
			for i := 24; i < 32; i++ {
				b[i] = 0xff
			}
			if version == LeaseVersion1 {
				le.PutUint32(b[20:24], 0xffffffff)
			} else {
				le.PutUint32(b[20:24], LeaseParentKeySet)
				for i := 32; i < 48; i++ {
					b[i] = byte(i + 0xd0)
				}
				le.PutUint16(b[48:50], 0x1234)
				le.PutUint16(b[50:52], 0xffff)
			}
			lease, err := ParseLeaseRequest(b)
			if err != nil || lease.Version != version || lease.State != state || lease.Key[15] != 15 {
				t.Fatalf("version %d state %d: %+v %v", version, state, lease, err)
			}
			if version == LeaseVersion1 {
				if lease.HasParent || lease.Epoch != 0 || lease.ParentKey != ([16]byte{}) {
					t.Fatal("V1 consumed reserved metadata")
				}
			} else if !lease.HasParent || lease.ParentKey[0] != 0xf0 || lease.ParentKey[15] != 0xff || lease.Epoch != 0x1234 {
				t.Fatalf("V2 metadata %+v", lease)
			}
			clear(b)
			if lease.Key[15] != 15 || version == LeaseVersion2 && lease.ParentKey[0] != 0xf0 {
				t.Fatal("decoded identity aliases input")
			}
		}
	}
	b := make([]byte, 52)
	le.PutUint32(b[20:24], LeaseParentKeySet)
	zero, err := ParseLeaseRequest(b)
	if err != nil || !zero.HasParent || zero.Key != ([16]byte{}) || zero.ParentKey != ([16]byte{}) {
		t.Fatalf("opaque zero identities: %+v %v", zero, err)
	}
	le.PutUint32(b[20:24], 0)
	b[32] = 9
	le.PutUint16(b[48:50], 0xffff)
	ignored, err := ParseLeaseRequest(b)
	if err != nil || ignored.HasParent || ignored.ParentKey != ([16]byte{}) || ignored.Epoch != 0xffff {
		t.Fatalf("unflagged parent: %+v %v", ignored, err)
	}
}

func TestLeaseRequestMalformed(t *testing.T) {
	for n := range 70 {
		if n == 32 || n == 52 {
			continue
		}
		if _, err := ParseLeaseRequest(make([]byte, n)); err == nil {
			t.Fatalf("accepted length %d", n)
		}
	}
	for _, version := range []uint16{LeaseVersion1, LeaseVersion2} {
		for _, state := range []uint32{8, 0x80000000, 0xffffffff} {
			if _, err := ParseLeaseRequest(leaseData(version, state)); err == nil {
				t.Fatalf("accepted state %x", state)
			}
		}
	}
	for _, flags := range []uint32{1, 2, 8, 0xffffffff} {
		b := leaseData(LeaseVersion2, 0)
		le.PutUint32(b[20:24], flags)
		if _, err := ParseLeaseRequest(b); err == nil {
			t.Fatalf("accepted V2 flags %x", flags)
		}
	}
}

func TestLeaseSelection(t *testing.T) {
	c := CreateRequest{OplockLevel: OplockLease}
	if _, present, err := ParseLease(c); err != nil || present {
		t.Fatalf("missing context must fall back: %t %v", present, err)
	}
	c.Contexts = []CreateContext{{Name: []byte("MxAc")}, {Name: []byte("RqLs"), Data: leaseData(LeaseVersion2, 7)}}
	lease, present, err := ParseLease(c)
	if err != nil || !present || lease.Version != LeaseVersion2 {
		t.Fatalf("lease selection: %+v %t %v", lease, present, err)
	}
	c.Contexts = append(c.Contexts, CreateContext{Name: []byte("RqLs"), Data: leaseData(LeaseVersion1, 0)})
	if _, _, err := ParseLease(c); err == nil {
		t.Fatal("accepted duplicate RqLs")
	}
	c.Contexts = []CreateContext{{Name: []byte("RqLs"), Data: []byte{1}}}
	if _, _, err := ParseLease(c); err == nil {
		t.Fatal("accepted malformed selected lease")
	}
	c.OplockLevel = 0
	if _, present, err := ParseLease(c); err != nil || present {
		t.Fatalf("non-lease request must ignore inner lease data: %t %v", present, err)
	}
	for _, version := range []uint16{LeaseVersion1, LeaseVersion2} {
		c = CreateRequest{OplockLevel: OplockLease, Attributes: 0x10, Contexts: []CreateContext{{Name: []byte("RqLs"), Data: leaseData(version, 7)}}}
		if _, present, err := ParseLease(c); err != nil || !present {
			t.Fatalf("directory lease %d: %t %v", version, present, err)
		}
	}
}

func TestZeroRightsLeaseResponse(t *testing.T) {
	for _, version := range []uint16{LeaseVersion1, LeaseVersion2} {
		var first []byte
		for state := uint32(0); state <= 7; state++ {
			requestData := leaseData(version, state)
			if version == LeaseVersion2 {
				le.PutUint32(requestData[20:24], LeaseParentKeySet)
				for i := 32; i < 48; i++ {
					requestData[i] = byte(i + 0xd0)
				}
				le.PutUint16(requestData[48:50], 0x1234)
			}
			request, err := ParseLeaseRequest(requestData)
			if err != nil {
				t.Fatal(err)
			}
			response := LeaseResponse{Version: request.Version, Key: request.Key, HasParent: request.HasParent, ParentKey: request.ParentKey, Epoch: request.Epoch}
			data, err := LeaseResponseData(response)
			if err != nil {
				t.Fatal(err)
			}
			if state == 0 {
				first = data
			} else if !bytes.Equal(data, first) {
				t.Fatal("requested caching rights changed response")
			}
			if le.Uint32(data[16:20]) != 0 || le.Uint64(data[24:32]) != 0 {
				t.Fatal("cache rights or duration granted")
			}
		}
		want := "000102030405060708090a0b0c0d0e0f00000000000000000000000000000000"
		if version == LeaseVersion2 {
			want = "000102030405060708090a0b0c0d0e0f00000000040000000000000000000000f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff34120000"
		}
		if hex.EncodeToString(first) != want {
			t.Fatalf("response vector %d: %x", version, first)
		}
	}
	metadata := LeaseResponse{Version: LeaseVersion1, HasParent: true, ParentKey: [16]byte{99}, Epoch: 99}
	b, err := LeaseResponseData(metadata)
	if err != nil || len(b) != 32 || !bytes.Equal(b[16:], make([]byte, 16)) || metadata.Epoch != 99 {
		t.Fatalf("V1 view must omit retained V2 metadata: %x %v", b, err)
	}
	metadata.Version = LeaseVersion2
	metadata.HasParent = false
	b, err = LeaseResponseData(metadata)
	if err != nil || le.Uint32(b[20:24]) != 0 || !bytes.Equal(b[32:48], make([]byte, 16)) || le.Uint16(b[48:50]) != 99 {
		t.Fatalf("unflagged response parent: %x %v", b, err)
	}
	metadata.HasParent = true
	metadata.ParentKey = [16]byte{}
	b, err = LeaseResponseData(metadata)
	if err != nil || le.Uint32(b[20:24]) != LeaseParentKeySet {
		t.Fatal("zero parent key lost its presence flag")
	}
	if _, err := LeaseResponseData(LeaseResponse{Version: 3}); err == nil {
		t.Fatal("unknown lease version accepted")
	}
}

func TestLeaseAcknowledgment(t *testing.T) {
	for state := uint32(0); state <= 7; state++ {
		p := requestPacket(OplockBreak, 36, 40)
		for i := 66; i < 72; i++ {
			p[i] = 0xff
		}
		for i := 72; i < 88; i++ {
			p[i] = byte(i)
		}
		le.PutUint32(p[88:92], state)
		for i := 92; i < 100; i++ {
			p[i] = 0xff
		}
		ack, err := parseOne(t, p).LeaseAck()
		if err != nil || ack.State != state || ack.Key[0] != 72 || ack.Key[15] != 87 {
			t.Fatalf("ACK fields: %+v %v", ack, err)
		}
		clear(p)
		if ack.Key[0] != 72 {
			t.Fatal("ACK identity aliases input")
		}
	}
	p := requestPacket(OplockBreak, 36, 36)
	for _, state := range []uint32{8, 0xffffffff} {
		le.PutUint32(p[88:92], state)
		if _, err := parseOne(t, p).LeaseAck(); err == nil {
			t.Fatal("unknown ACK state bits accepted")
		}
	}
	for _, packet := range [][]byte{requestPacket(Echo, 36, 36), requestPacket(OplockBreak, 24, 36), requestPacket(OplockBreak, 36, 35)} {
		if _, err := parseOne(t, packet).LeaseAck(); err == nil {
			t.Fatal("wrong ACK command/structure accepted")
		}
	}
}

func leaseCreatePacket(version uint16) []byte {
	data := leaseData(version, 7)
	p := requestPacket(Create, 57, 56+24+len(data))
	p[67] = OplockLease
	le.PutUint32(p[112:116], 120)
	le.PutUint32(p[116:120], uint32(24+len(data)))
	context := p[120:]
	le.PutUint16(context[4:6], 16)
	le.PutUint16(context[6:8], 4)
	le.PutUint16(context[10:12], 24)
	le.PutUint32(context[12:16], uint32(len(data)))
	copy(context[16:20], "RqLs")
	copy(context[24:], data)
	return p
}

func TestCreateLeaseEnvelope(t *testing.T) {
	for _, version := range []uint16{LeaseVersion1, LeaseVersion2} {
		p := leaseCreatePacket(version)
		request := parseOne(t, p)
		create, err := request.Create()
		if err != nil {
			t.Fatal(err)
		}
		lease, present, err := ParseLease(create)
		if err != nil || !present || lease.Version != version || lease.State != 7 {
			t.Fatalf("nested CREATE: %+v %t %v", lease, present, err)
		}
		le.PutUint32(p[132:136], 0xffffffff)
		if _, err := parseOne(t, p).Create(); err == nil {
			t.Fatal("unbounded inner data admitted")
		}
	}
}

func FuzzLeaseRequest(f *testing.F) {
	f.Add(leaseData(LeaseVersion1, 7))
	f.Add(leaseData(LeaseVersion2, 7))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		lease, err := ParseLeaseRequest(data)
		if err != nil {
			return
		}
		out, err := LeaseResponseData(LeaseResponse{Version: lease.Version, Key: lease.Key, HasParent: lease.HasParent, ParentKey: lease.ParentKey, Epoch: lease.Epoch})
		if err != nil {
			t.Fatal(err)
		}
		if le.Uint32(out[16:20]) != 0 || le.Uint64(out[24:32]) != 0 {
			t.Fatal("cache grant escaped response policy")
		}
	})
}

func FuzzLeaseEnvelope(f *testing.F) {
	f.Add(leaseCreatePacket(LeaseVersion1))
	f.Add(leaseCreatePacket(LeaseVersion2))
	f.Add(requestPacket(OplockBreak, 36, 36))
	f.Fuzz(func(t *testing.T, data []byte) {
		requests, err := ParseFrame(data, Limits{MaxBytes: 1 << 16, MaxCommands: 8, MaxContexts: 8})
		if err != nil {
			return
		}
		for _, r := range requests {
			_ = decode(r)
		}
	})
}
