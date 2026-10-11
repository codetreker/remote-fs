package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func queryInfoPacket() []byte {
	packet := requestPacket(QueryInfo, 41, 40)
	packet[66], packet[67] = InfoFile, FileStandardInformation
	le.PutUint32(packet[68:72], 24)
	copy(packet[88:104], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	return packet
}

func setInfoPacket(data []byte, offset uint16) []byte {
	packet := requestPacket(SetInfo, 33, 32)
	packet[66], packet[67] = InfoFile, FileBasicInformation
	le.PutUint32(packet[68:72], uint32(len(data)))
	le.PutUint16(packet[72:74], offset)
	copy(packet[80:96], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	if len(data) != 0 {
		packet = append(packet, make([]byte, int(offset)-len(packet))...)
		packet = append(packet, data...)
	}
	return packet
}

func TestQueryInfoRequest(t *testing.T) {
	packet := queryInfoPacket()
	le.PutUint16(packet[72:74], 0xffff)
	le.PutUint16(packet[74:76], 0xabcd)
	le.PutUint32(packet[76:80], ^uint32(0))
	le.PutUint32(packet[80:84], 0x12345678)
	le.PutUint32(packet[84:88], ^uint32(0))
	original := bytes.Clone(packet)
	for _, class := range []byte{FileBasicInformation, FileStandardInformation, FileInternalInformation, FileNetworkOpenInformation, FileIdInformation} {
		packet[67] = class
		result, err := parseOne(t, packet).QueryInfo()
		if err != nil || result.InfoType != InfoFile || result.FileInfoClass != class || result.OutputBufferLength != 24 ||
			result.InputBufferOffset != 0xffff || result.InputBufferLength != ^uint32(0) || result.AdditionalInformation != 0x12345678 ||
			result.Flags != ^uint32(0) || result.InputBuffer != nil || result.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) {
			t.Fatalf("QUERY_INFO class %d = %+v, %v", class, result, err)
		}
		original[67] = class
		if !bytes.Equal(packet, original) {
			t.Fatal("QUERY_INFO decoder changed signed command bytes")
		}
		packet[88] = 99
		if result.FileID[0] != 1 {
			t.Fatal("QUERY_INFO FileId borrows frame bytes")
		}
		packet[88] = 1
	}
	for _, corrupt := range []func([]byte) []byte{
		func(p []byte) []byte { le.PutUint16(p[64:66], 40); return p },
		func(p []byte) []byte { return p[:103] },
		func(p []byte) []byte { le.PutUint16(p[12:14], SetInfo); return p },
	} {
		if _, err := parseOne(t, corrupt(bytes.Clone(packet))).QueryInfo(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed QUERY_INFO accepted: %v", err)
		}
	}
}

func TestQueryInfoInputExtent(t *testing.T) {
	for _, infoType := range []byte{InfoFile, InfoQuota} {
		packet := queryInfoPacket()
		packet[66], packet[67] = infoType, FileFullEaInformation
		le.PutUint16(packet[72:74], 104)
		le.PutUint32(packet[76:80], 4)
		packet = append(packet, 1, 2, 3, 4)
		result, err := parseOne(t, packet).QueryInfo()
		if err != nil || !bytes.Equal(result.InputBuffer, []byte{1, 2, 3, 4}) {
			t.Fatalf("QUERY_INFO input = %+v, %v", result, err)
		}
		packet[104] = 9
		if result.InputBuffer[0] != 9 {
			t.Fatal("QUERY_INFO must borrow bounded input bytes")
		}
		for _, corrupt := range []func([]byte){
			func(p []byte) { le.PutUint16(p[72:74], 103) },
			func(p []byte) { le.PutUint16(p[72:74], 0xffff) },
			func(p []byte) { le.PutUint32(p[76:80], ^uint32(0)) },
		} {
			malformed := bytes.Clone(packet)
			corrupt(malformed)
			if _, err := parseOne(t, malformed).QueryInfo(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("QUERY_INFO accepted input extent: %v", err)
			}
		}
	}
}

func TestSetInfoRequest(t *testing.T) {
	for _, offset := range []uint16{96, 97, 104} {
		packet := setInfoPacket([]byte{0xa1, 0xb2, 0xc3}, offset)
		le.PutUint16(packet[74:76], 0xffff)
		le.PutUint32(packet[76:80], 0x12345678)
		original := bytes.Clone(packet)
		result, err := parseOne(t, packet).SetInfo()
		if err != nil || result.InfoType != InfoFile || result.FileInfoClass != FileBasicInformation ||
			result.BufferLength != 3 || result.BufferOffset != offset || result.AdditionalInformation != 0x12345678 ||
			result.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) || !bytes.Equal(result.Buffer, []byte{0xa1, 0xb2, 0xc3}) {
			t.Fatalf("SET_INFO = %+v, %v", result, err)
		}
		if !bytes.Equal(packet, original) {
			t.Fatal("SET_INFO decoder changed signed command bytes")
		}
		packet[80], packet[offset] = 99, 99
		if result.FileID[0] != 1 || result.Buffer[0] != 99 {
			t.Fatal("SET_INFO must copy FileId and borrow bounded payload")
		}
	}
	packet := setInfoPacket([]byte{1, 2, 3, 4}, 96)
	for _, corrupt := range []func([]byte) []byte{
		func(p []byte) []byte { le.PutUint16(p[64:66], 32); return p },
		func(p []byte) []byte { return p[:95] },
		func(p []byte) []byte { le.PutUint16(p[12:14], QueryInfo); return p },
		func(p []byte) []byte { le.PutUint16(p[72:74], 95); return p },
		func(p []byte) []byte { le.PutUint16(p[72:74], 0xffff); return p },
		func(p []byte) []byte { le.PutUint32(p[68:72], ^uint32(0)); return p },
	} {
		if _, err := parseOne(t, corrupt(bytes.Clone(packet))).SetInfo(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed SET_INFO accepted: %v", err)
		}
	}
	if result, err := parseOne(t, setInfoPacket(nil, 0)).SetInfo(); err != nil || result.Buffer != nil {
		t.Fatalf("empty SET_INFO = %+v, %v", result, err)
	}
}

func TestFileInfoRelatedAndCompoundExtent(t *testing.T) {
	for _, command := range []uint16{QueryInfo, SetInfo} {
		packet := queryInfoPacket()
		idOffset := 88
		if command == SetInfo {
			packet, idOffset = setInfoPacket([]byte{1, 2, 3, 4}, 96), 80
		}
		for _, id := range []FileID{InvalidFileID, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}} {
			copy(packet[idOffset:idOffset+16], id[:])
			var got FileID
			var err error
			if command == QueryInfo {
				result, failure := parseOne(t, packet).QueryInfo()
				got, err = result.FileID, failure
			} else {
				result, failure := parseOne(t, packet).SetInfo()
				got, err = result.FileID, failure
			}
			if id == InvalidFileID {
				if err != nil || got != id {
					t.Fatalf("related FileId %d = %x, %v", command, got, err)
				}
			} else if !errors.Is(err, ErrMalformed) {
				t.Fatalf("partial related FileId %d accepted: %v", command, err)
			}
		}
		first := queryInfoPacket()
		if command == QueryInfo {
			first[67] = FileFullEaInformation
			le.PutUint16(first[72:74], 104)
			le.PutUint32(first[76:80], 4)
		} else {
			first = setInfoPacket(nil, 96)
			le.PutUint32(first[68:72], 4)
		}
		le.PutUint32(first[20:24], uint32(len(first)))
		compound := append(first, requestPacket(Echo, 4, 4)...)
		requests, err := ParseFrame(compound, testLimits)
		if err != nil || len(requests) != 2 {
			t.Fatalf("compound parse = %v", err)
		}
		if command == QueryInfo {
			_, err = requests[0].QueryInfo()
		} else {
			_, err = requests[0].SetInfo()
		}
		if !errors.Is(err, ErrMalformed) {
			t.Fatalf("file info %d borrowed next command bytes: %v", command, err)
		}
	}
}

func TestFileInformationGoldenVectors(t *testing.T) {
	basic := FileBasicInfo{CreationTime: 0x0102030405060708, LastAccessTime: 0x1112131415161718,
		LastWriteTime: 0x2122232425262728, ChangeTime: 0x3132333435363738, Attributes: 0x41424344}
	for _, test := range []struct {
		name string
		body []byte
		hex  string
	}{
		{"basic", FileBasicInformationBody(basic), "08070605040302011817161514131211282726252423222138373635343332314443424100000000"},
		{"standard", FileStandardInformationBody(FileStandardInfo{AllocationSize: 0x0102030405060708, EndOfFile: 0x1112131415161718, NumberOfLinks: 0x21222324, DeletePending: true, Directory: true}), "080706050403020118171615141312112423222101010000"},
		{"standard false", FileStandardInformationBody(FileStandardInfo{}), "000000000000000000000000000000000000000000000000"},
		{"internal", FileInternalInformationBody(0x0102030405060708), "0807060504030201"},
		{"network open", FileNetworkOpenInformationBody(FileNetworkOpenInfo{FileBasicInfo: basic, AllocationSize: 0x5152535455565758, EndOfFile: 0x6162636465666768}), "0807060504030201181716151413121128272625242322213837363534333231585756555453525168676665646362614443424100000000"},
		{"file id", FileIdInformationBody(0x0102030405060708, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), "08070605040302010102030405060708090a0b0c0d0e0f10"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := hex.EncodeToString(test.body); got != test.hex {
				t.Fatalf("encoded %s = %s, want %s", test.name, got, test.hex)
			}
		})
	}
}

func TestFileInformationSetRecords(t *testing.T) {
	for _, value := range []int64{0, -1, -2, -3, 0x7fffffffffffffff} {
		info := FileBasicInfo{CreationTime: value, LastAccessTime: value, LastWriteTime: value, ChangeTime: value, Attributes: 0x80}
		body := FileBasicInformationBody(info)
		le.PutUint32(body[36:40], ^uint32(0))
		got, err := ParseFileBasicInformation(body)
		if err != nil || got != info {
			t.Fatalf("Basic SET %d = %+v, %v", value, got, err)
		}
		body = make([]byte, 8)
		le.PutUint64(body, uint64(value))
		if got, err := ParseFileEndOfFileInformation(body); err != nil || got != value {
			t.Fatalf("EOF SET %d = %d, %v", value, got, err)
		}
	}
	for _, length := range []int{0, 7, 9, 39, 41} {
		if _, err := ParseFileBasicInformation(make([]byte, length)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("Basic SET accepted length %d", length)
		}
		if _, err := ParseFileEndOfFileInformation(make([]byte, length)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("EOF SET accepted length %d", length)
		}
	}
}

func TestFileInfoResponseBodies(t *testing.T) {
	for _, data := range [][]byte{nil, {0xa1, 0xb2, 0xc3}} {
		body, err := QueryInfoResponseBody(data)
		if err != nil || len(body) != 8+len(data) || le.Uint16(body[:2]) != 9 || le.Uint16(body[2:4]) != 72 ||
			le.Uint32(body[4:8]) != uint32(len(data)) || !bytes.Equal(body[8:], data) {
			t.Fatalf("QUERY_INFO response = %x, %v", body, err)
		}
	}
	if body, err := QueryInfoResponseBody(make([]byte, maxDirectTCPPayload-HeaderSize-7)); body != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized QUERY_INFO response = %d bytes, %v", len(body), err)
	}
	if got := SetInfoResponseBody(); !bytes.Equal(got, []byte{2, 0}) {
		t.Fatalf("SET_INFO response = %x", got)
	}
	if got := hex.EncodeToString(InfoLengthMismatchErrorBody()); got != "09000100080000000000000000000000" {
		t.Fatalf("INFO_LENGTH_MISMATCH error context = %s", got)
	}
}
