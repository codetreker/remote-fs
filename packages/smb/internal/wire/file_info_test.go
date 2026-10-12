package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestFilesystemInformationGoldenBodies(t *testing.T) {
	tests := []struct {
		name string
		body func() ([]byte, error)
		want string
	}{
		{"volume", func() ([]byte, error) {
			return FileFsVolumeInformationBody(FileFsVolumeInfo{CreationTime: 0x0807060504030201, Serial: 0x0c0b0a09, Label: "A😀"})
		}, "0102030405060708090a0b0c06000000000041003dd800de"},
		{"unlabeled volume", func() ([]byte, error) {
			return FileFsVolumeInformationBody(FileFsVolumeInfo{})
		}, "000000000000000000000000000000000000"},
		{"size", func() ([]byte, error) {
			return FileFsSizeInformationBody(FileFsSizeInfo{TotalAllocationUnits: 0x100000001, AvailableAllocationUnits: 0x100000000, SectorsPerAllocationUnit: 1, BytesPerSector: 4096})
		}, "010000000100000000000000010000000100000000100000"},
		{"device", func() ([]byte, error) {
			return FileFsDeviceInformationBody(FileFsDeviceInfo{DeviceType: 7, Characteristics: 0x10}), nil
		}, "0700000010000000"},
		{"attributes", func() ([]byte, error) {
			return FileFsAttributeInformationBody(FileFsAttributeInfo{Attributes: 6, MaximumComponentNameLength: 255, FileSystemName: "RFS"})
		}, "06000000ff00000006000000520046005300"},
		{"full size", func() ([]byte, error) {
			return FileFsFullSizeInformationBody(FileFsFullSizeInfo{TotalAllocationUnits: 0x100000002, CallerAvailableAllocationUnits: 0x100000000, ActualAvailableAllocationUnits: 0x100000001, SectorsPerAllocationUnit: 1, BytesPerSector: 4096})
		}, "0200000001000000000000000100000001000000010000000100000000100000"},
		{"unsigned geometry", func() ([]byte, error) {
			return FileFsSizeInformationBody(FileFsSizeInfo{SectorsPerAllocationUnit: math.MaxUint32, BytesPerSector: math.MaxUint32})
		}, "00000000000000000000000000000000ffffffffffffffff"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.body()
			if err != nil || hex.EncodeToString(got) != test.want {
				t.Fatalf("body = %x, %v; want %s", got, err, test.want)
			}
			response, err := QueryInfoResponseBody(got)
			if err != nil || le.Uint32(response[4:8]) != uint32(len(got)) || !bytes.Equal(response[8:], got) {
				t.Fatalf("QUERY_INFO response = %x, %v", response, err)
			}
		})
	}
}

func TestFilesystemInformationRejectsInvalidNamesAndCreationTime(t *testing.T) {
	for _, label := range []string{"nul\x00label", "bad\xffutf8"} {
		if body, err := FileFsVolumeInformationBody(FileFsVolumeInfo{Label: label}); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("volume label %q = %x, %v", label, body, err)
		}
	}
	if body, err := FileFsVolumeInformationBody(FileFsVolumeInfo{CreationTime: -1}); body != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("negative creation time = %x, %v", body, err)
	}
	for _, name := range []string{"", "nul\x00name", "bad\xffutf8"} {
		if body, err := FileFsAttributeInformationBody(FileFsAttributeInfo{MaximumComponentNameLength: 255, FileSystemName: name}); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("filesystem name %q = %x, %v", name, body, err)
		}
	}
	for _, maximum := range []int32{0, -1} {
		if body, err := FileFsAttributeInformationBody(FileFsAttributeInfo{MaximumComponentNameLength: maximum, FileSystemName: "RFS"}); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("maximum name length %d = %x, %v", maximum, body, err)
		}
	}
	name := strings.Repeat("A", (maxDirectTCPPayload-HeaderSize-8-12)/2+1)
	if body, err := FileFsAttributeInformationBody(FileFsAttributeInfo{MaximumComponentNameLength: 255, FileSystemName: name}); body != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversized filesystem name = %d bytes, %v", len(body), err)
	}
}

func TestFilesystemSizeInformationRejectsInvalidCountsAndGeometry(t *testing.T) {
	for _, info := range []FileFsSizeInfo{
		{TotalAllocationUnits: -1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{AvailableAllocationUnits: -1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{TotalAllocationUnits: 1, AvailableAllocationUnits: 2, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{BytesPerSector: 4096},
		{SectorsPerAllocationUnit: 1},
	} {
		if body, err := FileFsSizeInformationBody(info); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("size %+v = %x, %v", info, body, err)
		}
	}
	for _, info := range []FileFsFullSizeInfo{
		{TotalAllocationUnits: -1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{CallerAvailableAllocationUnits: -1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{ActualAvailableAllocationUnits: -1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{TotalAllocationUnits: 1, ActualAvailableAllocationUnits: 2, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{TotalAllocationUnits: 2, CallerAvailableAllocationUnits: 2, ActualAvailableAllocationUnits: 1, SectorsPerAllocationUnit: 1, BytesPerSector: 4096},
		{BytesPerSector: 4096},
		{SectorsPerAllocationUnit: 1},
	} {
		if body, err := FileFsFullSizeInformationBody(info); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("full size %+v = %x, %v", info, body, err)
		}
	}
	info := FileFsFullSizeInfo{TotalAllocationUnits: math.MaxInt64, CallerAvailableAllocationUnits: math.MaxInt64, ActualAvailableAllocationUnits: math.MaxInt64, SectorsPerAllocationUnit: 1, BytesPerSector: 4096}
	if body, err := FileFsFullSizeInformationBody(info); err != nil || le.Uint64(body[:8]) != math.MaxInt64 || le.Uint64(body[8:16]) != math.MaxInt64 || le.Uint64(body[16:24]) != math.MaxInt64 {
		t.Fatalf("maximum signed counts = %x, %v", body, err)
	}
}

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
			result.InputBufferLength != ^uint32(0) || result.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) {
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

		if command == SetInfo {
			first := setInfoPacket(nil, 96)
			le.PutUint32(first[68:72], 4)
			le.PutUint32(first[20:24], uint32(len(first)))
			compound := append(first, requestPacket(Echo, 4, 4)...)
			requests, err := ParseFrame(compound, testLimits)
			if err != nil || len(requests) != 2 {
				t.Fatalf("compound parse = %v", err)
			}
			if _, err := requests[0].SetInfo(); !errors.Is(err, ErrMalformed) {
				t.Fatalf("SET_INFO borrowed next command bytes: %v", err)
			}
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
