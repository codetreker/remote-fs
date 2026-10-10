package wire

import (
	"bytes"
	"errors"
	"testing"
)

func createContextPacket(name, data []byte) []byte {
	dataOffset := align8(16 + len(name))
	context := make([]byte, align8(dataOffset+len(data)))
	le.PutUint16(context[4:6], 16)
	le.PutUint16(context[6:8], uint16(len(name)))
	copy(context[16:], name)
	if len(data) != 0 {
		le.PutUint16(context[10:12], uint16(dataOffset))
		le.PutUint32(context[12:16], uint32(len(data)))
		copy(context[dataOffset:], data)
	}
	return context
}

func createPacket(name string, contexts ...[]byte) []byte {
	packet := requestPacket(Create, 57, 56)
	if name != "" {
		encoded := EncodeUTF16(name)
		le.PutUint16(packet[108:110], uint16(len(packet)))
		le.PutUint16(packet[110:112], uint16(len(encoded)))
		packet = append(packet, encoded...)
	}
	if len(contexts) != 0 {
		packet = append(packet, make([]byte, align8(len(packet))-len(packet))...)
		start := len(packet)
		le.PutUint32(packet[112:116], uint32(start))
		for index, context := range contexts {
			context = bytes.Clone(context)
			if index+1 != len(contexts) {
				le.PutUint32(context, uint32(len(context)))
			}
			packet = append(packet, context...)
		}
		le.PutUint32(packet[116:120], uint32(len(packet)-start))
	}
	if len(packet) == HeaderSize+56 {
		packet = append(packet, 0)
	}
	return packet
}

func TestCreateRequest(t *testing.T) {
	root, err := parseOne(t, createPacket("")).Create()
	if err != nil || root.Name != "" || len(root.Contexts) != 0 {
		t.Fatalf("root CREATE = %+v, %v", root, err)
	}
	packet := createPacket("file", createContextPacket([]byte("DH2Q"), []byte{1, 2, 3, 4}))
	packet[66], packet[67] = 0xff, 0x09
	le.PutUint32(packet[68:72], 2)
	for index := 72; index < 88; index++ {
		packet[index] = 0xff
	}
	le.PutUint32(packet[88:92], 0x123)
	le.PutUint32(packet[92:96], 0x20)
	le.PutUint32(packet[96:100], 7)
	le.PutUint32(packet[100:104], 3)
	le.PutUint32(packet[104:108], 0x40)
	request, err := parseOne(t, packet).Create()
	if err != nil || request.Name != "file" || request.SecurityFlags != 0xff || request.OplockLevel != 9 ||
		request.Impersonation != 2 || request.DesiredAccess != 0x123 || request.Attributes != 0x20 ||
		request.ShareAccess != 7 || request.Disposition != 3 || request.Options != 0x40 ||
		len(request.Contexts) != 1 || string(request.Contexts[0].Name) != "DH2Q" ||
		!bytes.Equal(request.Contexts[0].Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("CREATE = %+v, %v", request, err)
	}
	for _, test := range []struct {
		name    string
		corrupt func([]byte) []byte
	}{
		{"structure size", func(value []byte) []byte { le.PutUint16(value[64:66], 56); return value }},
		{"missing Buffer", func(value []byte) []byte { return value[:120] }},
		{"name in fixed body", func(value []byte) []byte { le.PutUint16(value[108:110], 112); return value }},
		{"name alignment", func(value []byte) []byte { le.PutUint16(value[108:110], 121); return value }},
		{"empty name alignment", func(value []byte) []byte {
			le.PutUint16(value[108:110], 121)
			le.PutUint16(value[110:112], 0)
			return value
		}},
		{"name length", func(value []byte) []byte { le.PutUint16(value[110:112], 0xffff); return value }},
		{"odd UTF16 length", func(value []byte) []byte { le.PutUint16(value[110:112], 7); return value }},
		{"unpaired surrogate", func(value []byte) []byte { le.PutUint16(value[120:122], 0xd800); return value }},
		{"contexts alignment", func(value []byte) []byte { le.PutUint32(value[112:116], 129); return value }},
		{"contexts in fixed body", func(value []byte) []byte { le.PutUint32(value[112:116], 112); return value }},
		{"contexts offset overflow", func(value []byte) []byte { le.PutUint32(value[112:116], 0xfffffff8); return value }},
		{"contexts length overflow", func(value []byte) []byte { le.PutUint32(value[116:120], 0xffffffff); return value }},
		{"empty contexts nonzero offset", func(value []byte) []byte { le.PutUint32(value[116:120], 0); return value }},
		{"name overlaps contexts", func(value []byte) []byte { le.PutUint16(value[110:112], 16); return value }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseOne(t, test.corrupt(bytes.Clone(packet))).Create(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed CREATE accepted: %v", err)
			}
		})
	}
}

func TestCreateContextsBeforeFilename(t *testing.T) {
	context := createContextPacket([]byte("QFid"), nil)
	packet := requestPacket(Create, 57, 56)
	le.PutUint32(packet[112:116], uint32(len(packet)))
	le.PutUint32(packet[116:120], uint32(len(context)))
	packet = append(packet, context...)
	le.PutUint16(packet[108:110], uint16(len(packet)))
	le.PutUint16(packet[110:112], 2)
	packet = append(packet, EncodeUTF16("x")...)
	request, err := parseOne(t, packet).Create()
	if err != nil || request.Name != "x" || len(request.Contexts) != 1 {
		t.Fatalf("contexts before filename = %+v, %v", request, err)
	}
	le.PutUint16(packet[108:110], 128)
	if _, err := parseOne(t, packet).Create(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("filename overlapping preceding contexts = %v", err)
	}
}

func TestCreateContextBounds(t *testing.T) {
	packet := createPacket("", createContextPacket([]byte("test"), []byte{1, 2, 3, 4}))
	for _, test := range []struct {
		name    string
		corrupt func([]byte)
	}{
		{"short name", func(value []byte) { le.PutUint16(value[126:128], 3) }},
		{"name fixed header", func(value []byte) { le.PutUint16(value[124:126], 8) }},
		{"name alignment", func(value []byte) { le.PutUint16(value[124:126], 17) }},
		{"name length beyond extent", func(value []byte) { le.PutUint16(value[126:128], 0xffff) }},
		{"data fixed header", func(value []byte) { le.PutUint16(value[130:132], 8) }},
		{"data alignment", func(value []byte) { le.PutUint16(value[130:132], 25) }},
		{"data overlap name", func(value []byte) { le.PutUint16(value[130:132], 16) }},
		{"data length overflow", func(value []byte) { le.PutUint32(value[132:136], 0xffffffff) }},
		{"Next too short", func(value []byte) { le.PutUint32(value[120:124], 8) }},
		{"Next alignment", func(value []byte) { le.PutUint32(value[120:124], 17) }},
		{"Next no following header", func(value []byte) { le.PutUint32(value[120:124], 24) }},
		{"Next beyond list", func(value []byte) { le.PutUint32(value[120:124], 0xfffffff8) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := bytes.Clone(packet)
			test.corrupt(malformed)
			if _, err := parseOne(t, malformed).Create(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed context accepted: %v", err)
			}
		})
	}
	for _, offset := range []uint16{0, 1, 8, 0xffff} {
		emptyData := createPacket("", createContextPacket([]byte("QFid"), nil))
		le.PutUint16(emptyData[130:132], offset)
		request, err := parseOne(t, emptyData).Create()
		if err != nil || len(request.Contexts) != 1 || len(request.Contexts[0].Data) != 0 {
			t.Fatalf("empty data offset %d = %+v, %v", offset, request, err)
		}
	}
	for length := 1; length < 16; length++ {
		short := createPacket("", make([]byte, length))
		if _, err := parseOne(t, short).Create(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("short context length %d accepted: %v", length, err)
		}
	}
}

func TestCreateContextNextExtentAndCount(t *testing.T) {
	context := createContextPacket([]byte("QFid"), nil)
	packet := createPacket("", context, context)
	request, err := parseOne(t, packet).Create()
	if err != nil || len(request.Contexts) != 2 {
		t.Fatalf("two contexts = %+v, %v", request, err)
	}
	for _, field := range []string{"name", "data"} {
		t.Run(field+" borrows next context", func(t *testing.T) {
			malformed := bytes.Clone(packet)
			if field == "name" {
				le.PutUint16(malformed[124:126], 24)
			} else {
				le.PutUint16(malformed[130:132], 24)
				le.PutUint32(malformed[132:136], 4)
			}
			if _, err := parseOne(t, malformed).Create(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("cross-context field = %v", err)
			}
		})
	}
	limits := testLimits
	limits.MaxContexts = 1
	for count := 1; count <= 2; count++ {
		packet := createPacket("", makeContextCopies(context, count)...)
		requests, err := ParseFrame(packet, limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = requests[0].Create()
		if count == 1 && err != nil || count == 2 && !errors.Is(err, ErrMalformed) {
			t.Fatalf("context count %d at limit 1 = %v", count, err)
		}
	}
}

func TestCreateContextDataBeforeName(t *testing.T) {
	context := make([]byte, 32)
	le.PutUint16(context[4:6], 24)
	le.PutUint16(context[6:8], 4)
	le.PutUint16(context[10:12], 16)
	le.PutUint32(context[12:16], 8)
	copy(context[16:24], []byte{1, 2, 3, 4, 5, 6, 7, 8})
	copy(context[24:], "AlSi")
	request, err := parseOne(t, createPacket("", context)).Create()
	if err != nil || len(request.Contexts) != 1 || string(request.Contexts[0].Name) != "AlSi" ||
		!bytes.Equal(request.Contexts[0].Data, []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("context data before name = %+v, %v", request, err)
	}
}

func makeContextCopies(context []byte, count int) [][]byte {
	contexts := make([][]byte, count)
	for index := range contexts {
		contexts[index] = context
	}
	return contexts
}

func TestCreateCannotBorrowFollowingCompoundBytes(t *testing.T) {
	for _, field := range []string{"filename", "contexts"} {
		t.Run(field, func(t *testing.T) {
			first := createPacket("")
			first = append(first, make([]byte, 128-len(first))...)
			le.PutUint32(first[20:24], uint32(len(first)))
			if field == "filename" {
				le.PutUint16(first[108:110], 120)
				le.PutUint16(first[110:112], 16)
			} else {
				le.PutUint32(first[112:116], 120)
				le.PutUint32(first[116:120], 32)
			}
			packet := append(first, requestPacket(Close, 24, 24)...)
			requests, err := ParseFrame(packet, testLimits)
			if err != nil || len(requests) != 2 {
				t.Fatalf("compound frame = %v", err)
			}
			if _, err := requests[0].Create(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("field borrowed following command = %v", err)
			}
		})
	}
}

func TestCreateContextTruncation(t *testing.T) {
	packet := createPacket("file", createContextPacket([]byte("DH2Q"), make([]byte, 32)))
	for length := HeaderSize + 2; length < len(packet); length++ {
		requests, err := ParseFrame(packet[:length], testLimits)
		if err != nil {
			continue
		}
		if _, err := requests[0].Create(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("CREATE truncation at %d accepted: %v", length, err)
		}
	}
}

func TestCloseRequest(t *testing.T) {
	packet := requestPacket(Close, 24, 24)
	le.PutUint16(packet[66:68], 1)
	le.PutUint32(packet[68:72], 0xffffffff)
	packet[72] = 55
	request, err := parseOne(t, packet).Close()
	if err != nil || request.Flags != 1 || request.FileID[0] != 55 {
		t.Fatalf("CLOSE = %+v, %v", request, err)
	}
	for _, corrupt := range []func([]byte) []byte{
		func(value []byte) []byte { le.PutUint16(value[64:66], 23); return value },
		func(value []byte) []byte { return value[:len(value)-1] },
		func(value []byte) []byte { return append(value, 0) },
	} {
		if _, err := parseOne(t, corrupt(bytes.Clone(packet))).Close(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed CLOSE accepted: %v", err)
		}
	}
	compound := append(bytes.Clone(packet), requestPacket(Echo, 4, 4)...)
	le.PutUint32(compound[20:24], uint32(len(packet)))
	requests, err := ParseFrame(compound, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requests[0].Close(); err != nil {
		t.Fatalf("CLOSE in compound = %v", err)
	}
}

func TestCloseFileIDPlaceholders(t *testing.T) {
	for _, test := range []struct {
		name    string
		id      FileID
		related bool
		partial bool
	}{
		{"ordinary", FileID{1, 2, 3}, false, false},
		{"zero", FileID{}, false, false},
		{"full placeholder", InvalidFileID, true, false},
		{"persistent placeholder", FileID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}, false, true},
		{"volatile placeholder", FileID{1, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.id.IsRelatedPlaceholder() != test.related || test.id.HasPartialRelatedPlaceholder() != test.partial {
				t.Fatalf("FileId classification = %x", test.id)
			}
			packet := requestPacket(Close, 24, 24)
			copy(packet[72:], test.id[:])
			request, err := parseOne(t, packet).Close()
			if test.partial {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("partial placeholder = %v", err)
				}
			} else if err != nil || request.FileID != test.id {
				t.Fatalf("CLOSE FileId = %x, %v", request.FileID, err)
			}
		})
	}
}

func TestFileResponseBodies(t *testing.T) {
	create := CreateResponseBody(CreateResponse{
		OplockLevel: 1, Flags: 2, CreateAction: 2, CreationTime: 3, LastAccessTime: 4,
		LastWriteTime: 5, ChangeTime: 6, AllocationSize: 7, EndOfFile: 8,
		Attributes: 9, FileID: FileID{10},
	})
	if len(create) != 88 || le.Uint16(create) != 89 || create[2] != 1 || create[3] != 2 ||
		le.Uint32(create[4:8]) != 2 || le.Uint64(create[8:16]) != 3 ||
		le.Uint64(create[16:24]) != 4 || le.Uint64(create[24:32]) != 5 ||
		le.Uint64(create[32:40]) != 6 || le.Uint64(create[40:48]) != 7 ||
		le.Uint64(create[48:56]) != 8 || le.Uint32(create[56:60]) != 9 ||
		create[64] != 10 || le.Uint32(create[60:64]) != 0 ||
		le.Uint32(create[80:84]) != 0 || le.Uint32(create[84:88]) != 0 {
		t.Fatalf("CREATE response = %x", create)
	}
	close := CloseResponseBody(CloseResponse{
		Flags: 1, CreationTime: 2, LastAccessTime: 3, LastWriteTime: 4,
		ChangeTime: 5, AllocationSize: 6, EndOfFile: 7, Attributes: 8,
	})
	if len(close) != 60 || le.Uint16(close) != 60 || le.Uint16(close[2:4]) != 1 ||
		le.Uint32(close[4:8]) != 0 || le.Uint64(close[8:16]) != 2 || le.Uint64(close[16:24]) != 3 ||
		le.Uint64(close[24:32]) != 4 || le.Uint64(close[32:40]) != 5 ||
		le.Uint64(close[40:48]) != 6 || le.Uint64(close[48:56]) != 7 ||
		le.Uint32(close[56:60]) != 8 {
		t.Fatalf("CLOSE response = %x", close)
	}
}

func FuzzCreateCloseDecoders(f *testing.F) {
	f.Add(createPacket(""))
	f.Add(createPacket("file", createContextPacket([]byte("DH2Q"), make([]byte, 32))))
	f.Add(createPacket("", createContextPacket([]byte("QFid"), nil), createContextPacket([]byte("AlSi"), make([]byte, 8))))
	f.Add(requestPacket(Close, 24, 24))
	f.Fuzz(func(t *testing.T, packet []byte) {
		original := bytes.Clone(packet)
		requests, err := ParseFrame(packet, testLimits)
		if err != nil {
			return
		}
		for _, request := range requests {
			switch request.Header.Command {
			case Create:
				created, err := request.Create()
				if err == nil {
					if len(created.Contexts) > testLimits.MaxContexts {
						t.Fatalf("unbounded context count %d", len(created.Contexts))
					}
					for _, context := range created.Contexts {
						if len(context.Name) < 4 || len(context.Name)+len(context.Data) > len(request.Packet)-HeaderSize-56 {
							t.Fatalf("context exceeds command buffer: %d/%d", len(context.Name), len(context.Data))
						}
					}
				}
			case Close:
				closed, err := request.Close()
				if err == nil && closed.FileID.HasPartialRelatedPlaceholder() {
					t.Fatalf("partial placeholder accepted: %x", closed.FileID)
				}
			}
		}
		if !bytes.Equal(packet, original) {
			t.Fatal("decoder changed signed packet bytes")
		}
	})
}
