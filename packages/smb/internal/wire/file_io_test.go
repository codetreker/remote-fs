package wire

import (
	"bytes"
	"errors"
	"testing"
)

func readPacket() []byte {
	packet := requestPacket(Read, 49, 48)
	packet[66], packet[67] = 0xa5, ReadFlagRequestCompressed
	le.PutUint32(packet[68:72], 65537)
	le.PutUint64(packet[72:80], 0x1020304050607080)
	copy(packet[80:96], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	le.PutUint32(packet[96:100], 123)
	return packet
}

func writePacket(data []byte, offset uint16) []byte {
	packet := requestPacket(Write, 49, 48)
	le.PutUint16(packet[66:68], offset)
	le.PutUint32(packet[68:72], uint32(len(data)))
	le.PutUint64(packet[72:80], ^uint64(0))
	copy(packet[80:96], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	if len(data) != 0 {
		packet = append(packet, make([]byte, int(offset)-len(packet))...)
		packet = append(packet, data...)
	}
	return packet
}

func TestReadRequest(t *testing.T) {
	packet := readPacket()
	original := bytes.Clone(packet)
	request, err := parseOne(t, packet).Read()
	if err != nil || request.Padding != 0xa5 || request.Flags != ReadFlagRequestCompressed ||
		request.Length != 65537 || request.Offset != 0x1020304050607080 ||
		request.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) ||
		request.MinimumCount != 123 || request.Channel != ChannelNone || request.ChannelInfo != nil {
		t.Fatalf("READ = %+v, %v", request, err)
	}
	if !bytes.Equal(packet, original) {
		t.Fatal("READ decoder changed signed command bytes")
	}
	packet[80] = 99
	if request.FileID[0] != 1 {
		t.Fatal("READ FileId borrows mutable signed buffer")
	}
	for _, test := range []struct {
		name    string
		corrupt func([]byte) []byte
	}{
		{"structure size", func(value []byte) []byte { le.PutUint16(value[64:66], 48); return value }},
		{"short fixed fields", func(value []byte) []byte { return value[:111] }},
		{"wrong command", func(value []byte) []byte { le.PutUint16(value[12:14], Write); return value }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseOne(t, test.corrupt(bytes.Clone(original))).Read(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed READ accepted: %v", err)
			}
		})
	}
}

func TestWriteRequest(t *testing.T) {
	for _, offset := range []uint16{112, 113, 255, 256} {
		packet := writePacket([]byte{0xa1, 0xb2, 0xc3}, offset)
		le.PutUint32(packet[108:112], WriteFlagWriteThrough|WriteFlagUnbuffered)
		original := bytes.Clone(packet)
		request, err := parseOne(t, packet).Write()
		if err != nil || request.DataOffset != offset || request.Length != 3 || request.Offset != ^uint64(0) ||
			request.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) ||
			request.Flags != WriteFlagWriteThrough|WriteFlagUnbuffered || request.Channel != ChannelNone ||
			!bytes.Equal(request.Data, []byte{0xa1, 0xb2, 0xc3}) {
			t.Fatalf("WRITE offset %d = %+v, %v", offset, request, err)
		}
		if !bytes.Equal(packet, original) {
			t.Fatal("WRITE decoder changed signed command bytes")
		}
		packet[80] = 99
		packet[offset] = 0xee
		if request.FileID[0] != 1 || request.Data[0] != 0xee {
			t.Fatal("WRITE must copy FileId and borrow exact bounded Data")
		}
	}
	packet := writePacket([]byte("bytes"), 112)
	for _, test := range []struct {
		name    string
		corrupt func([]byte) []byte
	}{
		{"structure size", func(value []byte) []byte { le.PutUint16(value[64:66], 48); return value }},
		{"short fixed fields", func(value []byte) []byte { return value[:111] }},
		{"wrong command", func(value []byte) []byte { le.PutUint16(value[12:14], Read); return value }},
		{"data in fixed fields", func(value []byte) []byte { le.PutUint16(value[66:68], 111); return value }},
		{"data offset too high", func(value []byte) []byte { le.PutUint16(value[66:68], 257); return value }},
		{"data length beyond extent", func(value []byte) []byte { le.PutUint32(value[68:72], 6); return value }},
		{"data length overflow", func(value []byte) []byte { le.PutUint32(value[68:72], ^uint32(0)); return value }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseOne(t, test.corrupt(bytes.Clone(packet))).Write(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed WRITE accepted: %v", err)
			}
		})
	}
}

func TestEmptyWriteRequest(t *testing.T) {
	for _, offset := range []uint16{0, 1, 112} {
		request, err := parseOne(t, writePacket(nil, offset)).Write()
		if err != nil || request.Length != 0 || request.Data != nil || request.DataOffset != offset {
			t.Fatalf("empty WRITE offset %d = %+v, %v", offset, request, err)
		}
	}
	for _, offset := range []uint16{113, 256, 0xffff} {
		if _, err := parseOne(t, writePacket(nil, offset)).Write(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("empty WRITE offset %d exceeds command extent: %v", offset, err)
		}
	}
	packet := writePacket(nil, 256)
	packet = append(packet, make([]byte, 256-len(packet))...)
	if _, err := parseOne(t, packet).Write(); err != nil {
		t.Fatalf("empty WRITE offset at command end: %v", err)
	}
}

func TestFileIOChannelNoneIgnoresReservedFields(t *testing.T) {
	read := readPacket()
	le.PutUint32(read[104:108], ^uint32(0))
	le.PutUint16(read[108:110], 0xffff)
	le.PutUint16(read[110:112], 0xffff)
	parsedRead, err := parseOne(t, read).Read()
	if err != nil || parsedRead.ChannelInfo != nil || parsedRead.RemainingBytes != ^uint32(0) ||
		parsedRead.ChannelInfoOffset != 0xffff || parsedRead.ChannelInfoLength != 0xffff {
		t.Fatalf("READ Channel NONE reserved garbage = %+v, %v", parsedRead, err)
	}
	write := writePacket([]byte("data"), 112)
	le.PutUint32(write[100:104], ^uint32(0))
	le.PutUint16(write[104:106], 0xffff)
	le.PutUint16(write[106:108], 0xffff)
	parsedWrite, err := parseOne(t, write).Write()
	if err != nil || parsedWrite.ChannelInfo != nil || parsedWrite.RemainingBytes != ^uint32(0) ||
		parsedWrite.ChannelInfoOffset != 0xffff || parsedWrite.ChannelInfoLength != 0xffff || string(parsedWrite.Data) != "data" {
		t.Fatalf("WRITE Channel NONE reserved garbage = %+v, %v", parsedWrite, err)
	}
}

func TestFileIOChannelInformation(t *testing.T) {
	for _, command := range []uint16{Read, Write} {
		for _, channel := range []uint32{ChannelRDMAV1, ChannelRDMAV1Invalidate, ChannelRDMATransform, ^uint32(0)} {
			packet := requestPacket(command, 49, 48)
			channelStart, offsetStart := 100, 108
			if command == Write {
				channelStart, offsetStart = 96, 104
			}
			le.PutUint32(packet[channelStart:channelStart+4], channel)
			le.PutUint16(packet[offsetStart:offsetStart+2], 112)
			le.PutUint16(packet[offsetStart+2:offsetStart+4], 4)
			packet = append(packet, []byte{1, 2, 3, 4}...)
			request := parseOne(t, packet)
			var info []byte
			var err error
			if command == Read {
				var parsed ReadRequest
				parsed, err = request.Read()
				info = parsed.ChannelInfo
			} else {
				var parsed WriteRequest
				parsed, err = request.Write()
				info = parsed.ChannelInfo
			}
			if err != nil || !bytes.Equal(info, []byte{1, 2, 3, 4}) {
				t.Fatalf("command %d channel %d = %x, %v", command, channel, info, err)
			}
			for _, corrupt := range []func([]byte){
				func(value []byte) { le.PutUint16(value[offsetStart:offsetStart+2], 111) },
				func(value []byte) { le.PutUint16(value[offsetStart:offsetStart+2], 0xffff) },
				func(value []byte) { le.PutUint16(value[offsetStart+2:offsetStart+4], 0xffff) },
			} {
				malformed := bytes.Clone(packet)
				corrupt(malformed)
				request := parseOne(t, malformed)
				if command == Read {
					_, err = request.Read()
				} else {
					_, err = request.Write()
				}
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("command %d accepted channel extent: %v", command, err)
				}
			}
		}
	}
}

func TestFileIOCannotBorrowFollowingCompoundBytes(t *testing.T) {
	for _, command := range []uint16{Read, Write} {
		first := requestPacket(command, 49, 48)
		le.PutUint32(first[20:24], uint32(len(first)))
		if command == Read {
			le.PutUint32(first[100:104], ChannelRDMAV1)
			le.PutUint16(first[108:110], 112)
			le.PutUint16(first[110:112], 4)
		} else {
			le.PutUint16(first[66:68], 112)
			le.PutUint32(first[68:72], 4)
		}
		packet := append(first, requestPacket(Echo, 4, 4)...)
		requests, err := ParseFrame(packet, testLimits)
		if err != nil || len(requests) != 2 {
			t.Fatalf("compound parse = %v", err)
		}
		if command == Read {
			_, err = requests[0].Read()
		} else {
			_, err = requests[0].Write()
		}
		if !errors.Is(err, ErrMalformed) {
			t.Fatalf("command %d borrowed following command: %v", command, err)
		}
	}
	first := writePacket([]byte("data"), 112)
	first = append(first, make([]byte, align8(len(first))-len(first))...)
	le.PutUint32(first[20:24], uint32(len(first)))
	packet := append(first, requestPacket(Flush, 24, 24)...)
	requests, err := ParseFrame(packet, testLimits)
	if err != nil || len(requests) != 2 {
		t.Fatalf("padded compound parse = %v", err)
	}
	write, err := requests[0].Write()
	if err != nil || string(write.Data) != "data" {
		t.Fatalf("padded compound WRITE = %+v, %v", write, err)
	}
	if _, err := requests[1].Flush(); err != nil {
		t.Fatalf("final compound FLUSH = %v", err)
	}
}

func TestFlushRequest(t *testing.T) {
	packet := requestPacket(Flush, 24, 24)
	le.PutUint16(packet[66:68], 0xffff)
	le.PutUint32(packet[68:72], ^uint32(0))
	packet[72] = 42
	original := bytes.Clone(packet)
	request, err := parseOne(t, packet).Flush()
	if err != nil || request.FileID != (FileID{42}) {
		t.Fatalf("FLUSH = %+v, %v", request, err)
	}
	if !bytes.Equal(packet, original) {
		t.Fatal("FLUSH decoder changed signed command bytes")
	}
	packet[72] = 99
	if request.FileID[0] != 42 {
		t.Fatal("FLUSH FileId borrows mutable command bytes")
	}
	for _, corrupt := range []func([]byte) []byte{
		func(value []byte) []byte { le.PutUint16(value[64:66], 23); return value },
		func(value []byte) []byte { le.PutUint16(value[12:14], Close); return value },
		func(value []byte) []byte { return value[:87] },
		func(value []byte) []byte { return append(value, 0) },
	} {
		if _, err := parseOne(t, corrupt(bytes.Clone(original))).Flush(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed FLUSH accepted: %v", err)
		}
	}
	compound := append(bytes.Clone(original), requestPacket(Echo, 4, 4)...)
	le.PutUint32(compound[20:24], uint32(len(original)))
	requests, err := ParseFrame(compound, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requests[0].Flush(); err != nil {
		t.Fatalf("compound FLUSH = %v", err)
	}
}

func TestFileIOFileIDPlaceholders(t *testing.T) {
	for _, command := range []uint16{Read, Write, Flush} {
		for _, id := range []FileID{
			{}, {1, 2, 3}, InvalidFileID,
			{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1},
			{1, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		} {
			packet := requestPacket(command, 49, 48)
			idOffset := 80
			if command == Flush {
				packet = requestPacket(command, 24, 24)
				idOffset = 72
			}
			copy(packet[idOffset:idOffset+16], id[:])
			request := parseOne(t, packet)
			var actual FileID
			var err error
			switch command {
			case Read:
				var parsed ReadRequest
				parsed, err = request.Read()
				actual = parsed.FileID
			case Write:
				var parsed WriteRequest
				parsed, err = request.Write()
				actual = parsed.FileID
			case Flush:
				var parsed FlushRequest
				parsed, err = request.Flush()
				actual = parsed.FileID
			}
			if id.HasPartialRelatedPlaceholder() {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("command %d accepted partial FileId %x: %v", command, id, err)
				}
			} else if err != nil || actual != id {
				t.Fatalf("command %d FileId = %x, %v; want %x", command, actual, err, id)
			}
		}
	}
}

func TestFileIOResponseBodies(t *testing.T) {
	data := []byte{0xa1, 0xb2, 0xc3}
	read, err := ReadResponseBody(data)
	if err != nil || !bytes.Equal(read, []byte{17, 0, 80, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xa1, 0xb2, 0xc3}) {
		t.Fatalf("READ response = %x, %v", read, err)
	}
	data[0] = 99
	if read[16] != 0xa1 {
		t.Fatal("READ response borrows backend result bytes")
	}
	empty, err := ReadResponseBody(nil)
	if err != nil || !bytes.Equal(empty, []byte{17, 0, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("empty READ response = %x, %v", empty, err)
	}
	if _, err := ReadResponseBody(make([]byte, maxDirectTCPPayload-HeaderSize-16+1)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized READ response = %v", err)
	}
	if body := WriteResponseBody(0x01020304); !bytes.Equal(body, []byte{17, 0, 0, 0, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("WRITE response = %x", body)
	}
	if body := FlushResponseBody(); !bytes.Equal(body, []byte{4, 0, 0, 0}) {
		t.Fatalf("FLUSH response = %x", body)
	}
}
