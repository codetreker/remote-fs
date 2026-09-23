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
	copy(context[16:20], "DH2Q")
	le.PutUint16(context[10:12], 20)
	le.PutUint32(context[12:16], 4)
	copy(context[20:], []byte{1, 2, 3, 4})
	le.PutUint32(packet[112:116], 128)
	le.PutUint32(packet[116:120], uint32(len(context)))
	packet = append(packet, context...)
	request, err = parseOne(t, packet).Create()
	if err != nil || request.Name != "file" || request.DesiredAccess != 0x123 || request.ShareAccess != 7 ||
		len(request.Contexts) != 1 || string(request.Contexts[0].Name) != "DH2Q" || !bytes.Equal(request.Contexts[0].Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("create = %+v, %v", request, err)
	}

	for _, corrupt := range []func([]byte){
		func(value []byte) { le.PutUint16(value[64:66], 56) },
		func(value []byte) { value[72] = 1 },
		func(value []byte) { le.PutUint16(value[108:110], HeaderSize) },
		func(value []byte) { le.PutUint16(value[110:112], 0xffff) },
		func(value []byte) { le.PutUint32(value[112:116], 129) },
		func(value []byte) { le.PutUint32(value[116:120], 0xffffffff) },
		func(value []byte) { le.PutUint16(value[132:134], 0) },
		func(value []byte) { le.PutUint16(value[138:140], 0) },
		func(value []byte) { le.PutUint16(value[138:140], 16) },
		func(value []byte) { le.PutUint32(value[128:132], 16) },
	} {
		malformed := bytes.Clone(packet)
		corrupt(malformed)
		if _, err := parseOne(t, malformed).Create(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed CREATE accepted: %v", err)
		}
	}
}

func TestCloseRequest(t *testing.T) {
	packet := requestPacket(Close, 24, 24)
	le.PutUint16(packet[66:68], 1)
	packet[72] = 55
	request, err := parseOne(t, packet).Close()
	if err != nil || request.Flags != 1 || request.FileID[0] != 55 {
		t.Fatalf("close = %+v, %v", request, err)
	}
	for _, corrupt := range []func([]byte) []byte{
		func(value []byte) []byte { le.PutUint16(value[64:66], 23); return value },
		func(value []byte) []byte { value[68] = 1; return value },
		func(value []byte) []byte { return append(value, 0) },
	} {
		malformed := corrupt(bytes.Clone(packet))
		if _, err := parseOne(t, malformed).Close(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed CLOSE accepted: %v", err)
		}
	}
}

func TestFileResponseBodies(t *testing.T) {
	create := CreateResponseBody(CreateResponse{
		OplockLevel: 1, CreateAction: 2, CreationTime: 3, LastAccessTime: 4,
		LastWriteTime: 5, ChangeTime: 6, AllocationSize: 7, EndOfFile: 8,
		Attributes: 9, FileID: FileID{10},
	})
	if len(create) != 88 || le.Uint16(create) != 89 || create[2] != 1 ||
		le.Uint32(create[4:8]) != 2 || le.Uint64(create[8:16]) != 3 ||
		le.Uint64(create[16:24]) != 4 || le.Uint64(create[24:32]) != 5 ||
		le.Uint64(create[32:40]) != 6 || le.Uint64(create[40:48]) != 7 ||
		le.Uint64(create[48:56]) != 8 || le.Uint32(create[56:60]) != 9 ||
		create[64] != 10 || le.Uint32(create[80:84]) != 0 || le.Uint32(create[84:88]) != 0 {
		t.Fatalf("CREATE response = %x", create)
	}
	close := CloseResponseBody(CloseResponse{
		Flags: 1, CreationTime: 2, LastAccessTime: 3, LastWriteTime: 4,
		ChangeTime: 5, AllocationSize: 6, EndOfFile: 7, Attributes: 8,
	})
	if len(close) != 60 || le.Uint16(close) != 60 || le.Uint16(close[2:4]) != 1 ||
		le.Uint64(close[8:16]) != 2 || le.Uint64(close[16:24]) != 3 ||
		le.Uint64(close[24:32]) != 4 || le.Uint64(close[32:40]) != 5 ||
		le.Uint64(close[40:48]) != 6 || le.Uint64(close[48:56]) != 7 ||
		le.Uint32(close[56:60]) != 8 {
		t.Fatalf("CLOSE response = %x", close)
	}
}

func FuzzCreateCloseDecoders(f *testing.F) {
	f.Add(requestPacket(Create, 57, 56))
	f.Add(requestPacket(Close, 24, 24))
	f.Fuzz(func(_ *testing.T, packet []byte) {
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
			}
		}
	})
}
