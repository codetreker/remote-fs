package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestCreateRequest(t *testing.T) {
	packet := requestPacket(Create, 57, 56)
	request, err := parseOne(t, packet).Create()
	if err != nil || request.Name != "" {
		t.Fatalf("root create = %+v, %v", request, err)
	}

	packet = append(packet, EncodeUTF16("file")...)
	le.PutUint16(packet[108:110], HeaderSize+56)
	le.PutUint16(packet[110:112], 8)
	le.PutUint32(packet[88:92], 0x123)
	le.PutUint32(packet[96:100], 7)
	context := make([]byte, 24)
	le.PutUint16(context[4:6], 16)
	le.PutUint16(context[6:8], 4)
	copy(context[16:20], "MxAc")
	le.PutUint16(context[10:12], 20)
	le.PutUint32(context[12:16], 4)
	copy(context[20:], []byte{1, 2, 3, 4})
	le.PutUint32(packet[112:116], 128)
	le.PutUint32(packet[116:120], uint32(len(context)))
	packet = append(packet, context...)
	request, err = parseOne(t, packet).Create()
	if err != nil || request.Name != "file" || request.DesiredAccess != 0x123 || request.ShareAccess != 7 ||
		len(request.Contexts) != 1 || string(request.Contexts[0].Name) != "MxAc" || !bytes.Equal(request.Contexts[0].Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("create = %+v, %v", request, err)
	}

	for _, corrupt := range []func([]byte){
		func(value []byte) { le.PutUint16(value[108:110], HeaderSize) },
		func(value []byte) { le.PutUint16(value[110:112], 0xffff) },
		func(value []byte) { le.PutUint32(value[112:116], 129) },
		func(value []byte) { le.PutUint32(value[116:120], 0xffffffff) },
		func(value []byte) { le.PutUint16(value[132:134], 0) },
		func(value []byte) { le.PutUint16(value[138:140], 0) },
		func(value []byte) { le.PutUint32(value[128:132], 16) },
	} {
		malformed := bytes.Clone(packet)
		corrupt(malformed)
		if _, err := parseOne(t, malformed).Create(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed CREATE accepted: %v", err)
		}
	}
}

func TestReadWriteCloseAndFlushRequests(t *testing.T) {
	packet := requestPacket(Read, 49, 48)
	packet[80] = 99
	le.PutUint32(packet[68:72], 4096)
	le.PutUint64(packet[72:80], 1234)
	read, err := parseOne(t, packet).Read()
	if err != nil || read.FileID[0] != 99 || read.Offset != 1234 || read.Length != 4096 {
		t.Fatalf("READ = %+v, %v", read, err)
	}
	le.PutUint16(packet[110:112], 2)
	if _, err := parseOne(t, packet).Read(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("missing READ channel data accepted: %v", err)
	}

	packet = requestPacket(Write, 49, 51)
	le.PutUint16(packet[66:68], HeaderSize+48)
	le.PutUint32(packet[68:72], 3)
	le.PutUint64(packet[72:80], 17)
	packet[80] = 42
	copy(packet[112:], "abc")
	write, err := parseOne(t, packet).Write()
	if err != nil || write.Offset != 17 || write.FileID[0] != 42 || string(write.Data) != "abc" {
		t.Fatalf("WRITE = %+v, %v", write, err)
	}
	le.PutUint32(packet[68:72], 0xffffffff)
	if _, err := parseOne(t, packet).Write(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized WRITE accepted: %v", err)
	}

	closePacket := requestPacket(Close, 24, 24)
	le.PutUint16(closePacket[66:68], 1)
	closePacket[72] = 55
	closeRequest, err := parseOne(t, closePacket).Close()
	if err != nil || closeRequest.Flags != 1 || closeRequest.FileID[0] != 55 {
		t.Fatalf("CLOSE = %+v, %v", closeRequest, err)
	}
	flushPacket := requestPacket(Flush, 24, 24)
	flushPacket[72] = 56
	flushRequest, err := parseOne(t, flushPacket).Flush()
	if err != nil || flushRequest.FileID[0] != 56 {
		t.Fatalf("FLUSH = %+v, %v", flushRequest, err)
	}
	if _, err := parseOne(t, requestPacket(Echo, 4, 4)).FileID(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unrelated FileID accepted: %v", err)
	}
}

func FuzzFileRequestDecoders(f *testing.F) {
	for _, fixture := range []struct {
		command uint16
		size    uint16
		length  int
	}{{Create, 57, 56}, {Close, 24, 24}, {Flush, 24, 24}, {Read, 49, 48}, {Write, 49, 48}} {
		f.Add(requestPacket(fixture.command, fixture.size, fixture.length))
	}
	f.Fuzz(func(t *testing.T, packet []byte) {
		requests, err := ParseFrame(packet, testLimits)
		if err != nil {
			return
		}
		for _, request := range requests {
			switch request.Header.Command {
			case Create:
				_, _ = request.Create()
			case Close:
				_, _ = request.Close()
			case Flush:
				_, _ = request.Flush()
			case Read:
				_, _ = request.Read()
			case Write:
				_, _ = request.Write()
			}
		}
	})
}
